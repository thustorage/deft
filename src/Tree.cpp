#include "Tree.h"

#include <city.h>
#include <algorithm>
#include <iostream>
#include <queue>
#include <utility>
#include <vector>

#include "IndexCache.h"
#include "RdmaBuffer.h"
#include "Timer.h"

#define USE_SX_LOCK
#define BATCH_LOCK_READ

bool enter_debug = false;

uint64_t cache_miss[MAX_APP_THREAD][8];
uint64_t cache_hit[MAX_APP_THREAD][8];
uint64_t latency[MAX_APP_THREAD][LATENCY_WINDOWS];

StatHelper stat_helper;

thread_local CoroCall Tree::worker[define::kMaxCoro];
thread_local CoroCall Tree::master;
thread_local GlobalAddress path_stack[define::kMaxCoro]
                                     [define::kMaxLevelOfTree];

constexpr uint64_t XS_LOCK_FAA_MASK = 0x8000800080008000;

thread_local Timer timer;
thread_local std::queue<uint16_t> hot_wait_queue;

Tree::Tree(DSMClient *dsm_client, uint16_t tree_id)
    : dsm_client_(dsm_client), tree_id(tree_id) {
  for (int i = 0; i < dsm_client_->get_server_size(); ++i) {
    local_locks[i] = new LocalLockNode[define::kNumOfLock];
    for (size_t k = 0; k < define::kNumOfLock; ++k) {
      auto &n = local_locks[i][k];
      n.ticket_lock.store(0);
      n.hand_over = false;
      n.hand_time = 0;
    }
  }

  assert(dsm_client_->IsRegistered());
  print_verbose();

  index_cache = new IndexCache(define::kIndexCacheSize);

  root_ptr_ptr = get_root_ptr_ptr();

  // try to init tree and install root pointer
  auto page_buffer = (dsm_client_->get_rbuf(0)).get_page_buffer();
  auto root_addr = dsm_client_->Alloc(kLeafPageSize);
  [[maybe_unused]] auto root_page = new (page_buffer) LeafPage;

  dsm_client_->WriteSync(page_buffer, root_addr, kLeafPageSize);

  auto cas_buffer = (dsm_client_->get_rbuf(0)).get_cas_buffer();
  bool res = dsm_client_->CasSync(root_ptr_ptr, 0, root_addr.val, cas_buffer);
  if (res) {
    std::cout << "Tree root pointer value " << root_addr << std::endl;
  } else {
    // std::cout << "fail\n";
  }
}

void Tree::print_verbose() {
  constexpr int kLeafHdrOffset = offsetof(LeafPage, hdr);
  constexpr int kInternalHdrOffset = offsetof(InternalPage, hdr);
  static_assert(kLeafHdrOffset == kInternalHdrOffset, "format error");
  // if (kLeafHdrOffset != kInternalHdrOffset) {
  //   std::cerr << "format error" << std::endl;
  // }

  if (dsm_client_->get_my_client_id() == 0) {
    std::cout << "Header size: " << sizeof(Header) << std::endl;
    std::cout << "Internal Page size: " << sizeof(InternalPage) << " ["
              << kInternalPageSize << "]" << std::endl;
    std::cout << "Internal per Page: " << kInternalCardinality << std::endl;
    std::cout << "Leaf Page size: " << sizeof(LeafPage) << " [" << kLeafPageSize
              << "]" << std::endl;
    std::cout << "Leaf per Page: " << kLeafCardinality << std::endl;
    std::cout << "LeafEntry size: " << sizeof(LeafEntry) << std::endl;
    std::cout << "InternalEntry size: " << sizeof(InternalEntry) << std::endl;
    static_assert(sizeof(InternalPage) <= kInternalPageSize);
    static_assert(sizeof(LeafPage) <= kLeafPageSize);
  }
}

inline void Tree::before_operation(CoroContext *cxt, int coro_id) {
  for (size_t i = 0; i < define::kMaxLevelOfTree; ++i) {
    path_stack[coro_id][i] = GlobalAddress::Null();
  }
}

GlobalAddress Tree::get_root_ptr_ptr() {
  GlobalAddress addr;
  addr.group_node_version = 0;
  addr.nodeID = 0;
  addr.offset =
      define::kRootPointerStoreOffest + sizeof(GlobalAddress) * tree_id;

  return addr;
}

extern GlobalAddress g_root_ptr;
extern int g_root_level;
extern bool enable_cache;
GlobalAddress Tree::get_root_ptr(CoroContext *cxt, int coro_id,
                                 bool force_read) {
  if (force_read || g_root_ptr == GlobalAddress::Null()) {
    auto page_buffer = (dsm_client_->get_rbuf(coro_id)).get_page_buffer();
    dsm_client_->ReadSync(page_buffer, root_ptr_ptr, sizeof(GlobalAddress),
                          cxt);
    GlobalAddress root_ptr = *(GlobalAddress *)page_buffer;
    g_root_ptr = root_ptr;
    return root_ptr;
  } else {
    return g_root_ptr;
  }

  // std::cout << "root ptr " << root_ptr << std::endl;
}

// void Tree::broadcast_new_root(GlobalAddress new_root_addr, int root_level) {
//   RawMessage m;
//   m.type = RpcType::NEW_ROOT;
//   m.addr = new_root_addr;
//   m.level = root_level;
//   for (int i = 0; i < dsm_client_->get_server_size(); ++i) {
//     dsm_client_->RpcCallDir(m, i);
//   }
//   // TODO:
//   g_root_ptr = new_root_addr;
//   g_root_level = root_level;
//   if (root_level >= 3) {
//     enable_cache = true;
//   }
// }

bool Tree::update_new_root(GlobalAddress left, const Key &k,
                           GlobalAddress right, int level,
                           GlobalAddress old_root, CoroContext *cxt,
                           int coro_id) {
  left.group_gran = right.group_gran = gran_full;
  auto page_buffer = dsm_client_->get_rbuf(coro_id).get_page_buffer();
  auto cas_buffer = dsm_client_->get_rbuf(coro_id).get_cas_buffer();
  auto new_root = new (page_buffer) InternalPage(left, k, right, level);
  new_root->hdr.is_root = true;

  auto new_root_addr = dsm_client_->Alloc(kInternalPageSize);

  new_root->set_consistent();
  dsm_client_->WriteSync(page_buffer, new_root_addr, kInternalPageSize, cxt);
  if (dsm_client_->CasSync(root_ptr_ptr, old_root, new_root_addr, cas_buffer,
                           cxt)) {
    // broadcast_new_root(new_root_addr, level);
    printf("new root level %d [%d, %ld]\n", level, new_root_addr.nodeID,
           new_root_addr.offset);
    g_root_ptr = new_root_addr;
    return true;
  } else {
    printf(
        "cas root fail: left [%d,%lu] right [%d,%lu] old root [%d,%lu] try new "
        "root [%d,%lu]\n",
        left.nodeID, left.offset, right.nodeID, right.offset, old_root.nodeID,
        old_root.offset, new_root_addr.nodeID, new_root_addr.offset);
  }

  return false;
}

void Tree::print_and_check_tree(CoroContext *cxt, int coro_id) {
//   assert(dsm_client_->IsRegistered());

//   auto root = get_root_ptr(cxt, coro_id);
//   // SearchResult result;

//   GlobalAddress p = root;
//   GlobalAddress levels[define::kMaxLevelOfTree];
//   int level_cnt = 0;
//   auto page_buffer = (dsm_client_->get_rbuf(coro_id)).get_page_buffer();
//   GlobalAddress leaf_head;

// next_level:

//   dsm_client_->ReadSync(page_buffer, p, kLeafPageSize);
//   auto header = (Header *)(page_buffer + (STRUCT_OFFSET(LeafPage, hdr)));
//   levels[level_cnt++] = p;
//   if (header->level != 0) {
//     p = header->leftmost_ptr;
//     goto next_level;
//   } else {
//     leaf_head = p;
//   }

// next:
//   dsm_client_->ReadSync(page_buffer, leaf_head, kLeafPageSize);
//   auto page = (LeafPage *)page_buffer;
//   for (int i = 0; i < kLeafCardinality; ++i) {
//     if (page->records[i].value != kValueNull) {
//     }
//   }
//   while (page->hdr.sibling_ptr != GlobalAddress::Null()) {
//     leaf_head = page->hdr.sibling_ptr;
//     goto next;
//   }

  // for (int i = 0; i < level_cnt; ++i) {
  //   dsm->read_sync(page_buffer, levels[i], kLeafPageSize);
  //   auto header = (Header *)(page_buffer + (STRUCT_OFFSET(LeafPage, hdr)));
  //   // std::cout << "addr: " << levels[i] << " ";
  //   // header->debug();
  //   // std::cout << " | ";
  //   while (header->sibling_ptr != GlobalAddress::Null()) {
  //     dsm->read_sync(page_buffer, header->sibling_ptr, kLeafPageSize);
  //     header = (Header *)(page_buffer + (STRUCT_OFFSET(LeafPage, hdr)));
  //     // std::cout << "addr: " << header->sibling_ptr << " ";
  //     // header->debug();
  //     // std::cout << " | ";
  //   }
  //   // std::cout << "\n------------------------------------" << std::endl;
  //   // std::cout << "------------------------------------" << std::endl;
  // }
}

inline bool Tree::try_lock_addr(GlobalAddress lock_addr, uint64_t *buf,
                                CoroContext *cxt, int coro_id) {
  // bool hand_over = acquire_local_lock(lock_addr, cxt, coro_id);
  // if (hand_over) {
  //   return true;
  // }

  {
    uint64_t retry_cnt = 0;
    uint64_t pre_tag = 0;
    uint64_t conflict_tag = 0;
  retry:
    retry_cnt++;
    if (retry_cnt > 1000000) {
      std::cout << "Deadlock " << lock_addr << std::endl;

      std::cout << dsm_client_->get_my_client_id() << ", "
                << dsm_client_->get_my_thread_id() << " locked by "
                << (conflict_tag >> 32) << ", " << (conflict_tag << 32 >> 32)
                << std::endl;
      assert(false);
    }
    auto tag = dsm_client_->get_thread_tag();
    bool res = dsm_client_->CasDmSync(lock_addr, 0, tag, buf, cxt);

    if (!res) {
      // conflict_tag = *buf - 1;
      conflict_tag = *buf;
      if (conflict_tag != pre_tag) {
        retry_cnt = 0;
        pre_tag = conflict_tag;
      }
      goto retry;
    }
  }

  return true;
}

inline void Tree::unlock_addr(GlobalAddress lock_addr, uint64_t *buf,
                              CoroContext *cxt, int coro_id, bool async) {
  // bool hand_over_other = can_hand_over(lock_addr);
  // if (hand_over_other) {
  //   releases_local_lock(lock_addr);
  //   return;
  // }

  auto cas_buf = dsm_client_->get_rbuf(coro_id).get_cas_buffer();

  *cas_buf = 0;
  if (async) {
    dsm_client_->WriteDm((char *)cas_buf, lock_addr, sizeof(uint64_t), false);
  } else {
    dsm_client_->WriteDmSync((char *)cas_buf, lock_addr, sizeof(uint64_t), cxt);
  }

  // releases_local_lock(lock_addr);
}

inline bool Tree::try_x_lock_addr(GlobalAddress lock_addr, uint64_t *buf,
                                  CoroContext *cxt, int coro_id) {
  // bool hand_over = acquire_local_lock(lock_addr, cxt, coro_id);
  // if (hand_over) {
  //   return true;
  // }

  {
    uint64_t x_lock_add = 0x1;
    dsm_client_->FaaDmBoundSync(lock_addr, x_lock_add, buf, XS_LOCK_FAA_MASK,
                                cxt);
    // dsm->faa_boundary_sync(lock_addr, x_lock_add, buf, XS_LOCK_FAA_MASK,
    // cxt);
    uint64_t origin = *buf;
    uint16_t ori_x_max = origin & 0xffff;
    uint16_t ori_s_max = (origin >> 16) & 0xffff;
    uint16_t cur_x_cnt = (origin >> 32) & 0xffff;
    uint16_t cur_s_cnt = (origin >> 48) & 0xffff;

    uint64_t retry_cnt = 0;
    // uint64_t new_x_max = ori_x_max;

  retry:
    if (cur_x_cnt == ori_x_max && cur_s_cnt == ori_s_max) {
      // get
      // printf("thread %d get cur_x_cnt %u\n", dsm->getMyThreadID(),
      // cur_x_cnt);
    } else {
      retry_cnt++;
      // if (cur_x_cnt + 1 < ori_x_max) {
      //   printf("error : thread %d x cur %u max %u new max %u, retry_cnt
      //   %lu\n",
      //          dsm->getMyThreadID(), cur_x_cnt, ori_x_max, new_x_max,
      //          retry_cnt);
      //   assert(false);
      // }
      if (retry_cnt > 1000000) {
        printf("Deadlock [%u, %lu]\n", lock_addr.nodeID, lock_addr.offset);
        printf("ori s %u x %u max s %u x %u\n", cur_s_cnt, cur_x_cnt, ori_s_max,
               ori_x_max);
        assert(false);
      }

      dsm_client_->ReadDmSync((char *)buf, lock_addr, 8, cxt);
      // dsm->read_sync((char *)buf, lock_addr, 8, cxt);
      // dsm->faa_boundary_sync(lock_addr, 0, buf, XS_LOCK_FAA_MASK, cxt);
      uint64_t cur = *buf;
      cur_x_cnt = (cur >> 32) & 0xffff;
      cur_s_cnt = (cur >> 48) & 0xffff;
      // new_x_max = cur & 0xffff;
      goto retry;
    }
  }

  return true;
}

inline void Tree::unlock_x_addr(GlobalAddress lock_addr, uint64_t *buf,
                                CoroContext *cxt, int coro_id, bool async) {
  // bool hand_over_other = can_hand_over(lock_addr);
  // if (hand_over_other) {
  //   releases_local_lock(lock_addr);
  //   return;
  // }

  auto cas_buf = dsm_client_->get_rbuf(coro_id).get_cas_buffer();

  *cas_buf = 0;
  if (async) {
    dsm_client_->FaaDmBound(lock_addr, 0x1ul << 32, cas_buf, XS_LOCK_FAA_MASK,
                            false);
  } else {
    dsm_client_->FaaDmBoundSync(lock_addr, 0x1ul << 32, cas_buf,
                                XS_LOCK_FAA_MASK, cxt);
  }
  // releases_local_lock(lock_addr);
}

inline bool Tree::try_s_lock_addr(GlobalAddress lock_addr, uint64_t *buf,
                                  CoroContext *cxt, int coro_id) {
  // bool hand_over = acquire_local_lock(lock_addr, cxt, coro_id);
  // if (hand_over) {
  //   return true;
  // }

  {
    uint64_t s_lock_add = 0x1ul << 16;
    dsm_client_->FaaDmBoundSync(lock_addr, s_lock_add, buf, XS_LOCK_FAA_MASK,
                                cxt);
    uint64_t origin = *buf;
    uint16_t ori_x_max = origin & 0xffff;
    uint16_t ori_s_max = (origin >> 16) & 0xffff;
    uint16_t cur_x_cnt = (origin >> 32) & 0xffff;
    uint16_t cur_s_cnt = (origin >> 48) & 0xffff;

    uint64_t retry_cnt = 0;

  retry:
    if (cur_x_cnt == ori_x_max) {
      // get
    } else {
      retry_cnt++;
      if (retry_cnt > 1000000) {
        std::cout << "Deadlock " << lock_addr << std::endl;
        std::cout << "ori s x " << cur_s_cnt << " " << cur_x_cnt << " max s x "
                  << ori_s_max << " " << ori_x_max << std::endl;

        assert(false);
      }

      dsm_client_->ReadDmSync((char *)buf, lock_addr, 8, cxt);
      uint64_t cur = *buf;
      cur_x_cnt = (cur >> 32) & 0xffff;
      cur_s_cnt = (cur >> 48) & 0xffff;
      goto retry;
    }
  }

  return true;
}

inline void Tree::unlock_s_addr(GlobalAddress lock_addr, uint64_t *buf,
                                CoroContext *cxt, int coro_id, bool async) {
  // bool hand_over_other = can_hand_over(lock_addr);
  // if (hand_over_other) {
  //   releases_local_lock(lock_addr);
  //   return;
  // }
  auto cas_buf = dsm_client_->get_rbuf(coro_id).get_cas_buffer();

  *cas_buf = 0;
  if (async) {
    dsm_client_->FaaDmBound(lock_addr, 0x1ul << 48, cas_buf, XS_LOCK_FAA_MASK,
                            false);
  } else {
    dsm_client_->FaaDmBoundSync(lock_addr, 0x1ul << 48, cas_buf,
                                XS_LOCK_FAA_MASK, cxt);
  }
  // releases_local_lock(lock_addr);
}

inline void Tree::try_sx_lock_addr(GlobalAddress lock_addr, uint64_t *buf,
                                   CoroContext *cxt, int coro_id,
                                   bool sx_lock) {
  if (sx_lock) {
    try_s_lock_addr(lock_addr, buf, cxt, coro_id);
  } else {
    try_x_lock_addr(lock_addr, buf, cxt, coro_id);
  }
}

inline void Tree::unlock_sx_addr(GlobalAddress lock_addr, uint64_t *buf,
                                 CoroContext *cxt, int coro_id, bool async,
                                 bool sx_lock) {
  if (sx_lock) {
    unlock_s_addr(lock_addr, buf, cxt, coro_id, async);
  } else {
    unlock_x_addr(lock_addr, buf, cxt, coro_id, async);
  }
}

void Tree::write_page_and_unlock(char *page_buffer, GlobalAddress page_addr,
                                 int page_size, uint64_t *cas_buffer,
                                 GlobalAddress lock_addr, CoroContext *cxt,
                                 int coro_id, bool async, bool sx_lock) {
  Timer timer;
  timer.begin();

  bool hand_over_other = can_hand_over(lock_addr);
  if (hand_over_other) {
    dsm_client_->WriteSync(page_buffer, page_addr, page_size, cxt);
    // releases_local_lock(lock_addr);
    return;
  }

#ifdef USE_SX_LOCK
  dsm_client_->Write(page_buffer, page_addr, page_size, false);
  unlock_sx_addr(lock_addr, cas_buffer, cxt, coro_id, async, sx_lock);
#else
  RdmaOpRegion rs[2];
  rs[0].source = (uint64_t)page_buffer;
  rs[0].dest = page_addr;
  rs[0].size = page_size;
  rs[0].is_on_chip = false;

  rs[1].source = (uint64_t)dsm_client_->get_rbuf(coro_id).get_cas_buffer();
  rs[1].dest = lock_addr;
  rs[1].size = sizeof(uint64_t);

  rs[1].is_on_chip = true;

  *(uint64_t *)rs[1].source = 0;
  if (async) {
    dsm_client_->WriteBatch(rs, 2, false);
  } else {
    dsm_client_->WriteBatchSync(rs, 2, cxt);
  }
#endif
  // releases_local_lock(lock_addr);
  auto t = timer.end();
  stat_helper.add(dsm_client_->get_my_thread_id(), lat_write_page, t);
}

void Tree::lock_and_read_page(char *page_buffer, GlobalAddress page_addr,
                              int page_size, uint64_t *cas_buffer,
                              GlobalAddress lock_addr, CoroContext *cxt,
                              int coro_id, bool sx_lock) {
#ifdef BATCH_LOCK_READ
  batch_lock_and_read_page(page_buffer, page_addr, page_size, cas_buffer,
                           lock_addr, cxt, coro_id, sx_lock);
#else
  Timer timer;
  timer.begin();
#ifdef USE_SX_LOCK
  try_sx_lock_addr(lock_addr, cas_buffer, cxt, coro_id, sx_lock);
#else
  try_lock_addr(lock_addr, cas_buffer, cxt, coro_id);
#endif
  auto t = timer.end();
  stat_helper.add(dsm->getMyThreadID(), lat_lock, t);

  timer.begin();
  dsm->read_sync(page_buffer, page_addr, page_size, cxt);
  t = timer.end();
  stat_helper.add(dsm->getMyThreadID(), lat_read_page, t);
#endif
}

void Tree::batch_lock_and_read_page(char *page_buffer, GlobalAddress page_addr,
                                    int page_size, uint64_t *cas_buffer,
                                    GlobalAddress lock_addr, CoroContext *cxt,
                                    int coro_id, bool sx_lock) {
#ifdef USE_SX_LOCK

  // RdmaOpRegion rs[2];

  // rs[0].source = (uint64_t)cas_buffer;
  // rs[0].dest = lock_addr;
  // rs[0].size = 8;
  // rs[0].is_on_chip = true;

  // rs[1].source = (uint64_t)(page_buffer);
  // rs[1].dest = page_addr;
  // rs[1].size = page_size;
  // rs[1].is_on_chip = false;

  uint64_t add = sx_lock ? 1ul << 16 : 1ul;

  Timer timer;
  timer.begin();

  // dsm->faab_read_sync(rs[0], rs[1], add, XS_LOCK_FAA_MASK, cxt);
  dsm_client_->FaaDmBound(lock_addr, add, cas_buffer, XS_LOCK_FAA_MASK, false);
  dsm_client_->ReadSync(page_buffer, page_addr, page_size, cxt);

  auto t = timer.end();
  stat_helper.add(dsm_client_->get_my_thread_id(), lat_read_page, t);

  uint64_t origin = *cas_buffer;
  uint16_t ori_x_max = origin & 0xffff;
  uint16_t ori_s_max = (origin >> 16) & 0xffff;
  uint16_t cur_x_cnt = (origin >> 32) & 0xffff;
  uint16_t cur_s_cnt = (origin >> 48) & 0xffff;

  {
    uint64_t retry_cnt = 0;
  retry:
    if (sx_lock && cur_x_cnt == ori_x_max) {
      // ok
    } else if (!sx_lock && cur_x_cnt == ori_x_max && cur_s_cnt == ori_s_max) {
      // ok
    } else {
      // rs[0].source = (uint64_t)cas_buffer;
      // rs[0].dest = lock_addr;
      // rs[0].size = 8;
      // rs[0].is_on_chip = true;

      // rs[1].source = (uint64_t)(page_buffer);
      // rs[1].dest = page_addr;
      // rs[1].size = page_size;
      // rs[1].is_on_chip = false;

      retry_cnt++;
      if (retry_cnt > 1000000) {
        printf("Deadlock [%u, %lu]\n", lock_addr.nodeID, lock_addr.offset);
        printf("cur s %u x %u max s %u x %u\n", cur_s_cnt, cur_x_cnt, ori_s_max,
               ori_x_max);
        assert(false);
      }

      timer.begin();
      // dsm->read_batch_sync(rs, 2, cxt);
      // dsm->faab_read_sync(rs[0], rs[1], 0, XS_LOCK_FAA_MASK, cxt);
      dsm_client_->ReadDm((char *)cas_buffer, lock_addr, 8, false);
      dsm_client_->ReadSync(page_buffer, page_addr, page_size, cxt);
      t = timer.end();
      stat_helper.add(dsm_client_->get_my_thread_id(), lat_read_page, t);

      uint64_t cur = *cas_buffer;
      cur_x_cnt = (cur >> 32) & 0xffff;
      cur_s_cnt = (cur >> 48) & 0xffff;
      goto retry;
    }
  }
#else
  // RdmaOpRegion rs[2];
  {
    uint64_t retry_cnt = 0;
    uint64_t pre_tag = 0;
    uint64_t conflict_tag = 0;
    auto tag = dsm_client_->get_thread_tag();
  retry:
    // rs[0].source = (uint64_t)cas_buffer;
    // rs[0].dest = lock_addr;
    // rs[0].size = 8;
    // rs[0].is_on_chip = true;

    // rs[1].source = (uint64_t)(page_buffer);
    // rs[1].dest = page_addr;
    // rs[1].size = page_size;
    // rs[1].is_on_chip = false;
    retry_cnt++;
    if (retry_cnt > 1000000) {
      std::cout << "Deadlock " << lock_addr << std::endl;

      std::cout << dsm_client_->get_my_client_id() << ", "
                << dsm_client_->get_my_thread_id() << " locked by "
                << (conflict_tag >> 32) << ", " << (conflict_tag << 32 >> 32)
                << std::endl;
      assert(false);
    }

    Timer timer;
    timer.begin();

    dsm_client_->CasDm(lock_addr, 0, tag, cas_buffer, false);
    dsm_client_->ReadSync(page_buffer, page_addr, page_size, cxt);
    bool res = *(cas_buffer) == 0;

    // bool res = dsm->cas_read_sync(rs[0], rs[1], 0, tag, cxt);

    auto t = timer.end();
    stat_helper.add(dsm_client_->get_my_thread_id(), lat_read_page, t);

    if (!res) {
      // conflict_tag = *buf - 1;
      conflict_tag = *cas_buffer;
      if (conflict_tag != pre_tag) {
        retry_cnt = 0;
        pre_tag = conflict_tag;
      }
      goto retry;
    }
  }
#endif
}

void Tree::lock_bench(const Key &k, CoroContext *cxt, int coro_id) {
  // uint64_t lock_index = CityHash64((char *)&k, sizeof(k)) %
  // define::kNumOfLock;

  // GlobalAddress lock_addr;
  // lock_addr.nodeID = 0;
  // lock_addr.offset = lock_index * sizeof(uint64_t);
  // auto cas_buffer = dsm->get_rbuf(coro_id).get_cas_buffer();

  // // bool res = dsm->cas_sync(lock_addr, 0, 1, cas_buffer, cxt);
  // // try_lock_addr(lock_addr, 1, cas_buffer, cxt, coro_id);
  // // unlock_addr(lock_addr, 1, cas_buffer, cxt, coro_id, true);
  // bool sx_lock = false;
  // try_sx_lock_addr(lock_addr, 1, cas_buffer, cxt, coro_id, sx_lock);
  // unlock_sx_addr(lock_addr, 1, cas_buffer, cxt, coro_id, true, sx_lock);

  // read page test
  auto &rbuf = dsm_client_->get_rbuf(coro_id);
  uint64_t *cas_buffer = rbuf.get_cas_buffer();
  auto page_buffer = rbuf.get_page_buffer();

  GlobalAddress lock_addr;
  lock_addr.nodeID = 0;
  uint64_t lock_index = k % define::kNumOfLock;
  lock_addr.offset = lock_index * sizeof(uint64_t);

  GlobalAddress page_addr;
  page_addr.nodeID = 0;
  constexpr uint64_t page_num = 4ul << 20;
  page_addr.offset = (k % page_num) * kLeafPageSize;
  // GlobalAddress entry_addr = page_addr;
  // entry_addr.offset += 512;

  // dsm->read(page_buffer, page_addr, 32, false, cxt);
  // dsm->read_sync(page_buffer, page_addr, kLeafPageSize, cxt);

  write_page_and_unlock(page_buffer, page_addr, 128, cas_buffer, lock_addr, cxt,
                        coro_id, false, false);

  // lock_and_read_page(page_buffer, page_addr, kLeafPageSize, cas_buffer,
  //                    lock_addr, cxt, coro_id, true);
  // unlock_sx_addr(lock_addr, cas_buffer, cxt, coro_id, false, true);
}

void Tree::insert_internal_update_left_child(const Key &k, GlobalAddress v,
                                             const Key &left_child,
                                             GlobalAddress left_child_val,
                                             CoroContext *cxt, int coro_id,
                                             int level) {
  auto root = get_root_ptr(cxt, coro_id);
  SearchResult result;

  GlobalAddress p = root;
  int level_hint = -1;
  Key min = kKeyMin;
  Key max = kKeyMax;

next:

  if (!page_search(p, level_hint, p.child_gran, min, max, k, result, cxt,
                   coro_id)) {
    std::cout << "SEARCH WARNING insert" << std::endl;
    p = get_root_ptr(cxt, coro_id);
    level_hint = -1;
    sleep(1);
    goto next;
  }

  assert(result.level != 0);
  if (result.sibling != GlobalAddress::Null()) {
    p = result.sibling;
    level_hint = result.level;
    min = result.min; // remain the max
    goto next;
  }

  if (result.level >= level + 1) {
    p = result.next_level;
    if (result.level > level + 1) {
      level_hint = result.level - 1;
      min = result.min;
      max = result.max;
      goto next;
    }
  }

  // internal_page_store(p, k, v, root, level, cxt, coro_id);
  assert(result.level == level + 1 || result.level == level);
  internal_page_store_update_left_child(p, k, v, left_child, left_child_val,
                                        root, level, cxt, coro_id);
}

void Tree::insert(const Key &k, const Value &v, CoroContext *cxt, int coro_id) {
  assert(dsm_client_->IsRegistered());

  before_operation(cxt, coro_id);

  Key min = kKeyMin;
  Key max = kKeyMax;

  if (enable_cache) {
    GlobalAddress cache_addr;
    auto entry = index_cache->search_from_cache(
        k, &cache_addr, dsm_client_->get_my_thread_id() == 0);
    if (entry) {  // cache hit
      auto root = get_root_ptr(cxt, coro_id);
#ifdef USE_SX_LOCK
      bool status =
          leaf_page_store(cache_addr, k, v, root, 0, cxt, coro_id, true, true);
#else
      bool status =
          leaf_page_store(cache_addr, k, v, root, 0, cxt, coro_id, true);
#endif
      if (status) {
        cache_hit[dsm_client_->get_my_thread_id()][0]++;
        return;
      }
      // cache stale, from root,
      index_cache->invalidate(entry);
    }
    cache_miss[dsm_client_->get_my_thread_id()][0]++;
  }

  auto root = get_root_ptr(cxt, coro_id);
  SearchResult result;

  GlobalAddress p = root;
  int level_hint = -1;

next:
  if (!page_search(p, level_hint, p.child_gran, min, max, k, result, cxt,
                   coro_id)) {
    std::cout << "SEARCH WARNING insert" << std::endl;
    p = get_root_ptr(cxt, coro_id);
    level_hint = -1;
    min = kKeyMin;
    max = kKeyMax;
    sleep(1);
    goto next;
  }

  if (!result.is_leaf) {
    assert(result.level != 0);
    if (result.sibling != GlobalAddress::Null()) {
      p = result.sibling;
      min = result.min; // remain the max
      level_hint = result.level;
      goto next;
    }

    p = result.next_level;
    level_hint = result.level - 1;
    if (result.level != 1) {
      min = result.min;
      max = result.max;
      goto next;
    }
  }

  bool res = false;
  int cnt = 0;
  while (!res) {
    if (cnt > 1) {
      printf("retry insert <k:%lu v:%lu> %d\n", k, v, cnt);
    }
#ifdef USE_SX_LOCK
    res = leaf_page_store(p, k, v, root, 0, cxt, coro_id, false, true);
#else
    res = leaf_page_store(p, k, v, root, 0, cxt, coro_id, false);
#endif
    ++cnt;
  }
}

bool Tree::search(const Key &k, Value &v, CoroContext *cxt, int coro_id) {
  assert(dsm_client_->IsRegistered());

  auto p = get_root_ptr(cxt, coro_id);
  SearchResult result;
  Key min = kKeyMin;
  Key max = kKeyMax;

  int level_hint = -1;

  bool from_cache = false;
  const CacheEntry *entry = nullptr;
  if (enable_cache) {
    // Timer timer;
    // timer.begin();
    GlobalAddress cache_addr;
    entry = index_cache->search_from_cache(
        k, &cache_addr, dsm_client_->get_my_thread_id() == 0);
    if (entry) {  // cache hit
      cache_hit[dsm_client_->get_my_thread_id()][0]++;
      from_cache = true;
      p = cache_addr;
      level_hint = 0;

    } else {
      cache_miss[dsm_client_->get_my_thread_id()][0]++;
    }
    // auto t = timer.end();
    // stat_helper.add(dsm_client_->get_my_thread_id(), lat_cache_search, t);
  }

next:
  if (!page_search(p, level_hint, p.child_gran, min, max, k, result, cxt,
                   coro_id, from_cache)) {
    if (from_cache) {  // cache stale
      index_cache->invalidate(entry);
      cache_hit[dsm_client_->get_my_thread_id()][0]--;
      cache_miss[dsm_client_->get_my_thread_id()][0]++;
      from_cache = false;

      p = get_root_ptr(cxt, coro_id);
      level_hint = -1;
    } else {
      std::cout << "SEARCH WARNING search" << std::endl;
      sleep(1);
    }
    min = kKeyMin;
    max = kKeyMax;
    goto next;
  }
  if (result.is_leaf) {
    if (result.val != kValueNull) {  // find
      v = result.val;
      return true;
    }
    if (result.sibling != GlobalAddress::Null()) {  // turn right
      p = result.sibling;
      min = result.min; // remain the max
      level_hint = 0;
      goto next;
    }
    return false;  // not found
  } else {         // internal
    if (result.sibling != GlobalAddress::Null()) {
      p = result.sibling;
      min = result.min; // remain the max
      level_hint = result.level;
    } else {
      p = result.next_level;
      min = result.min;
      max = result.max;
      level_hint = result.level - 1;
    }
    goto next;
  }
}

uint64_t Tree::range_query(const Key &from, const Key &to, Value *value_buffer,
                           CoroContext *cxt, int coro_id) {
  // const int kParaFetch = 32;
  // thread_local std::vector<InternalPage *> result;
  // thread_local std::vector<GlobalAddress> leaves;

  // result.clear();
  // leaves.clear();
  // index_cache->search_range_from_cache(from, to, result);

  // // FIXME: here, we assume all innernal nodes are cached in compute node
  // if (result.empty()) {
  //   return 0;
  // }

  // uint64_t counter = 0;
  // for (auto page : result) {
  //   auto cnt = page->hdr.cnt;
  //   auto addr = page->hdr.leftmost_ptr;

  //   // [from, to]
  //   // [lowest, page->records[0].key);
  //   bool no_fetch = from > page->records[0].key || to < page->hdr.lowest;
  //   if (!no_fetch) {
  //     leaves.push_back(addr);
  //   }
  //   for (int i = 1; i < cnt; ++i) {
  //     no_fetch = from > page->records[i].key || to < page->records[i - 1].key;
  //     if (!no_fetch) {
  //       leaves.push_back(page->records[i - 1].ptr);
  //     }
  //   }

  //   no_fetch = from > page->hdr.highest || to < page->records[cnt - 1].key;
  //   if (!no_fetch) {
  //     leaves.push_back(page->records[cnt - 1].ptr);
  //   }
  // }

  // int cq_cnt = 0;
  // char *range_buffer = (dsm_client_->get_rbuf(coro_id)).get_range_buffer();
  // for (size_t i = 0; i < leaves.size(); ++i) {
  //   if (i > 0 && i % kParaFetch == 0) {
  //     dsm_client_->PollRdmaCq(kParaFetch);
  //     cq_cnt -= kParaFetch;
  //     for (int k = 0; k < kParaFetch; ++k) {
  //       auto page = (LeafPage *)(range_buffer + k * kLeafPageSize);
  //       for (int i = 0; i < kLeafCardinality; ++i) {
  //         auto &r = page->records[i];
  //         if (r.value != kValueNull) {
  //           if (r.key >= from && r.key <= to) {
  //             value_buffer[counter++] = r.value;
  //           }
  //         }
  //       }
  //     }
  //   }
  //   dsm_client_->Read(range_buffer + kLeafPageSize * (i % kParaFetch),
  //                     leaves[i], kLeafPageSize, true);
  //   cq_cnt++;
  // }

  // if (cq_cnt != 0) {
  //   dsm_client_->PollRdmaCq(cq_cnt);
  //   for (int k = 0; k < cq_cnt; ++k) {
  //     auto page = (LeafPage *)(range_buffer + k * kLeafPageSize);
  //     for (int i = 0; i < kLeafCardinality; ++i) {
  //       auto &r = page->records[i];
  //       if (r.value != kValueNull) {
  //         if (r.key >= from && r.key <= to) {
  //           value_buffer[counter++] = r.value;
  //         }
  //       }
  //     }
  //   }
  // }

  Debug::notifyError("range query not implemented");
  return 0;
}

void Tree::del(const Key &k, CoroContext *cxt, int coro_id) {
  assert(dsm_client_->IsRegistered());

  before_operation(cxt, coro_id);
  Key min = kKeyMin;
  Key max = kKeyMax;

  if (enable_cache) {
    GlobalAddress cache_addr;
    auto entry = index_cache->search_from_cache(
        k, &cache_addr, dsm_client_->get_my_thread_id() == 0);
    if (entry) {  // cache hit
      if (leaf_page_del(cache_addr, k, 0, cxt, coro_id, true)) {
        cache_hit[dsm_client_->get_my_thread_id()][0]++;
        return;
      }
      // cache stale, from root,
      index_cache->invalidate(entry);
    }
    cache_miss[dsm_client_->get_my_thread_id()][0]++;
  }

  auto root = get_root_ptr(cxt, coro_id);
  SearchResult result;

  GlobalAddress p = root;
  int level_hint = -1;

next:

  if (!page_search(p, level_hint, p.child_gran, min, max, k, result, cxt,
                   coro_id)) {
    std::cout << "SEARCH WARNING del" << std::endl;
    p = get_root_ptr(cxt, coro_id);
    min = kKeyMin;
    max = kKeyMax;
    level_hint = -1;
    sleep(1);
    goto next;
  }

  if (!result.is_leaf) {
    assert(result.level != 0);
    if (result.sibling != GlobalAddress::Null()) {
      p = result.sibling;
      min = result.min;
      level_hint = result.level;
      goto next;
    }

    p = result.next_level;
    level_hint = result.level - 1;
    if (result.level != 1) {
      min = result.min;
      max = result.max;
      goto next;
    }
  }

  leaf_page_del(p, k, 0, cxt, coro_id);
}

bool Tree::leaf_page_group_search(GlobalAddress page_addr, const Key &k,
                                  SearchResult &result, CoroContext *cxt,
                                  int coro_id, bool from_cache) {
  auto page_buffer = (dsm_client_->get_rbuf(coro_id)).get_page_buffer();
  uint8_t leaf_version = page_addr.child_version;

  int bucket_id = key_hash_bucket(k);
  int group_id = bucket_id / 2;

  int group_offset =
      offsetof(LeafPage, groups) + sizeof(LeafEntryGroup) * group_id;
  LeafEntryGroup *group = (LeafEntryGroup *)(page_buffer + group_offset);
  int pos_offset = bucket_id % 2 ? kBackOffset : kFrontOffset;

  int read_counter = 0;
re_read:
  if (++read_counter > 10) {
    printf("re-read too many times\n");
    sleep(1);
  }
  dsm_client_->ReadSync((char *)group + pos_offset,
                        GADD(page_addr, group_offset + pos_offset),
                        kReadBucketSize, cxt);
  result.clear();
  result.is_leaf = true;
  result.level = 0;
  uint8_t front_version, back_version;
  if (bucket_id % 2) {
    front_version = group->version_back_front;
    back_version = group->version_back_back;
  } else {
    front_version = group->version_front_front;
    back_version = group->version_front_back;
  }
  if (front_version != back_version) {
    // not consistent, page is during split
    printf("front_version %d back_version %d\n", front_version, back_version);
    if (from_cache)
      return false;
    else
      goto re_read;
  } else {
    bool res = group->find(k, result, !(bucket_id % 2));
    // TODO: find, but version incorrect, invalidate cache
    if (!res) {
      if (leaf_version != front_version) {
        // version doesn't match, page has been splitted, read header
        auto header = (Header *)(page_buffer + offsetof(LeafPage, hdr));
        dsm_client_->ReadSync((char *)header,
                              GADD(page_addr, offsetof(LeafPage, hdr)),
                              sizeof(Header), cxt);
        if (k >= header->highest) {
          result.sibling = header->sibling_ptr;
          result.min = header->highest;
        }  // else not exist
      }
    }
  }
  return true;
}

bool Tree::page_search(GlobalAddress page_addr, int level_hint, int read_gran,
                       Key min, Key max, const Key &k, SearchResult &result,
                       CoroContext *cxt, int coro_id, bool from_cache) {
  if (page_addr != g_root_ptr && level_hint == 0) {
    return leaf_page_group_search(page_addr, k, result, cxt, coro_id,
                                  from_cache);
  }

  auto page_buffer = (dsm_client_->get_rbuf(coro_id)).get_page_buffer();
  auto header = (Header *)(page_buffer + offsetof(LeafPage, hdr));

  int read_counter = 0;
re_read:
  if (++read_counter > 10) {
    printf("re read too many times\n");
    sleep(1);
    assert(false);
  }

  result.clear();
  InternalEntry *guard = nullptr;
  int group_id = -1;
  uint8_t actual_gran = read_gran;
  if (read_gran == gran_full) {
    dsm_client_->ReadSync(page_buffer, page_addr, kLeafPageSize, cxt);
    size_t start_offset =
        offsetof(InternalPage, records) - sizeof(InternalEntry);
    guard = reinterpret_cast<InternalEntry *>(page_buffer + start_offset);
    // has header
    result.is_leaf = header->level == 0;
    result.level = header->level;
    if (page_addr == g_root_ptr) {
      if (header->is_root == false) {
        // update root ptr
        get_root_ptr(cxt, coro_id, true);
      }
    }
  } else {
    size_t start_offset, read_size;
    group_id = get_key_group(k, min, max);
    // read granularity
    if (read_gran == gran_quarter) {
      start_offset = offsetof(InternalPage, records) +
                     (group_id * kGroupCardinality - 1) * sizeof(InternalEntry);
      read_size = sizeof(InternalEntry) * (kGroupCardinality + 1);
    } else {
      // half
      int begin = group_id < 2 ? 0 : kInternalCardinality / 2;
      start_offset =
          offsetof(InternalPage, records) + (begin - 1) * sizeof(InternalEntry);
      read_size = sizeof(InternalEntry) * (kInternalCardinality / 2 + 1);
    }
    dsm_client_->ReadSync(page_buffer + start_offset,
                          GADD(page_addr, start_offset), read_size, cxt);
    guard = reinterpret_cast<InternalEntry *>(page_buffer + start_offset);
    uint8_t actual_version = guard->ptr.group_node_version;
    actual_gran = guard->ptr.group_gran;
    if (actual_version != page_addr.child_version || actual_gran != read_gran) {
      // though we may know the actual granularity, we don't know the actual min
      // and max
      read_gran = gran_full;
      goto re_read;
    }
    assert(level_hint != 0);
    result.is_leaf = false;
    result.level = level_hint;
  }

  path_stack[coro_id][result.level] = page_addr;

  if (result.is_leaf) {
    assert(page_addr.child_gran == 0);
    auto page = (LeafPage *)page_buffer;

    if (from_cache &&
        (k < page->hdr.lowest || k >= page->hdr.highest)) {  // cache is stale
      return false;
    }

    if (k >= page->hdr.highest) {  // should turn right
      result.sibling = page->hdr.sibling_ptr;
      result.min = page->hdr.highest;
      return true;
    }
    if (k < page->hdr.lowest) {
      assert(false);
      return false;
    }
    int bucket_id = key_hash_bucket(k);
    LeafEntryGroup *g = &page->groups[bucket_id / 2];
    // check consistency
    if (bucket_id % 2) {
      if (g->version_back_front != g->version_back_back) {
        goto re_read;
      }
    } else {
      if (g->version_front_front != g->version_front_back) {
        goto re_read;
      }
    }
    g->find(k, result, !(bucket_id % 2));
  } else {
    // Internal Page
    assert(!from_cache);
    auto page = (InternalPage *)page_buffer;
    if (read_gran == gran_full) {
      // TODO: how to add cache
      if (result.level == 1 && enable_cache) {
        index_cache->add_to_cache(page);
      }
      if (k >= page->hdr.highest) {  // should turn right
        result.sibling = page->hdr.sibling_ptr;
        result.min = page->hdr.highest;
        return true;
      }
      if (k < page->hdr.lowest) {
        // printf("key %ld error in level %d\n", k, page->hdr.level);
        // sleep(10);
        // print_and_check_tree();
        assert(false);
        return false;
      }
      // maybe is a retry: update group id, gran, guard
      actual_gran = guard->ptr.group_gran;
      if (actual_gran != gran_full) {
        group_id = get_key_group(k, page->hdr.lowest, page->hdr.highest);
        if (actual_gran == gran_quarter) {
          guard = page->records + group_id * kGroupCardinality - 1;
        } else {
          // half
          int begin = group_id < 2 ? 0 : kInternalCardinality / 2;
          guard = page->records + begin - 1;
        }
      }
    }

    // Timer timer;
    // timer.begin();
    size_t search_cnt;
    if (actual_gran == gran_quarter) {
      search_cnt = kGroupCardinality + 1;
    } else if (actual_gran == gran_half) {
      search_cnt = kInternalCardinality / 2 + 1;
    } else {
      search_cnt = kInternalCardinality + 1;
    }
    // check consistency
    if (guard->ptr.group_node_version !=
        (guard + search_cnt - 1)->ptr.group_node_version) {
      goto re_read;
    }
    internal_page_slice_search(guard, search_cnt, k, result);
    // auto t = timer.end();
    // stat_helper.add(dsm_client_->get_my_thread_id(), lat_internal_search, t);
  }

  return true;
}

inline void Tree::internal_page_slice_search(InternalEntry *entries, int cnt,
                                             const Key k,
                                             SearchResult &result) {
  // entries[0] is the last entry of previous group or lowest of header
  InternalEntry *p = entries + cnt - 1;
  InternalEntry *bigger = p;
  // from tail to front
  while (p >= entries) {
    if (p->ptr == GlobalAddress::Null()) {
      // result.next_level = entries->ptr;
      p = entries;
      break;
    } else if (k >= p->key) {
      // result.next_level = p->ptr;
      break;
    }
    bigger = p;
    --p;
  }
  assert(p >= entries);
  result.next_level = p->ptr;
  if (p == entries + cnt - 1) {
    // can't know max, so read full page
    result.next_level.child_gran = gran_full;
  } else {
    result.min = p->key;
    result.max = bigger->key;
  }
}

inline bool Tree::try_group_search(LeafEntry *records, const Key &k,
                                   SearchResult &result) {
  for (int i = 0; i < kAssociativity * 2; ++i) {
    if (records[i].value != kValueNull) {
      if (records[i].key == k) {
        result.val = records[i].value;
        return true;
      } else if (result.other_in_group == 0) {
        result.other_in_group = records[i].key;
      }
    }
  }
  return false;
}

void Tree::internal_page_store(GlobalAddress page_addr, const Key &k,
                               GlobalAddress v, GlobalAddress root, int level,
                               CoroContext *cxt, int coro_id) {
  uint64_t lock_index = get_lock_index(page_addr);

  GlobalAddress lock_addr;
  lock_addr.nodeID = page_addr.nodeID;
  lock_addr.offset = lock_index * sizeof(uint64_t);

  auto &rbuf = dsm_client_->get_rbuf(coro_id);
  uint64_t *cas_buffer = rbuf.get_cas_buffer();
  auto page_buffer = rbuf.get_page_buffer();

  lock_and_read_page(page_buffer, page_addr, kInternalPageSize, cas_buffer,
                     lock_addr, cxt, coro_id, false);

  auto page = (InternalPage *)page_buffer;

  assert(page->hdr.level == level);
  assert(page->check_consistent());
  if (k >= page->hdr.highest) {
#ifdef USE_SX_LOCK
    this->unlock_sx_addr(lock_addr, cas_buffer, cxt, coro_id, true, false);
#else
    this->unlock_addr(lock_addr, cas_buffer, cxt, coro_id, true);
#endif

    assert(page->hdr.sibling_ptr != GlobalAddress::Null());

    this->internal_page_store(page->hdr.sibling_ptr, k, v, root, level, cxt,
                              coro_id);
    return;
  }
  assert(k >= page->hdr.lowest);



  int group_id = get_key_group(k, page->hdr.lowest, page->hdr.highest);
  uint8_t cur_group_gran =
      page->records[kGroupCardinality * (group_id + 1) - 1].ptr.group_gran;
  int end;  // not inclusive
  int max_cnt;
  if (cur_group_gran == gran_quarter) {
    end = kGroupCardinality * (group_id + 1);
    max_cnt = kGroupCardinality;
  } else if (cur_group_gran == gran_half) {
    end = group_id < 2 ? kGroupCardinality * 2 : kInternalCardinality;
    max_cnt = kGroupCardinality * 2;
  } else {
    assert(cur_group_gran == gran_full);
    end = kInternalCardinality;
    max_cnt = kInternalCardinality;
  }

  bool is_update = false;
  int i = 0;
  for (; i < max_cnt; ++i) {
    int idx = end - 1 - i;
    if (page->records[idx].ptr == GlobalAddress::Null()) {
      break;
    } else if (k == page->records[idx].key) {
      page->records[idx].ptr = v;
      is_update = true;
      break;
    } else if (k > page->records[idx].key) {
      break;
    }
  }
  int16_t insert_index = end - 1 - i;  // may be -1 of start
  while (i < max_cnt &&
         page->records[end - 1 - i].ptr != GlobalAddress::Null()) {
    ++i;
  }
  int group_cnt = i;

  // assert(!is_update);

  if (is_update) {
    page->set_consistent();
    write_page_and_unlock(page_buffer, page_addr, kInternalPageSize, cas_buffer,
                          lock_addr, cxt, coro_id, false, false);
    return;
  }

  // insert

  if (group_cnt < max_cnt) {
    // not exceed, shift to left
    // we redefine operator= for GlobalAddress, don't use memmove
    for (int i = end - group_cnt; i <= insert_index; ++i) {
      page->records[i - 1].key = page->records[i].key;
      page->records[i - 1].ptr = page->records[i].ptr;
    }
    page->records[insert_index].key = k;
    page->records[insert_index].ptr = v;

    // modify range: [end - group_cnt - 1, insert_index]
    ++group_cnt;
    // allow full
    size_t modify_offset = offsetof(InternalPage, records) +
                          (end - group_cnt - 1) * sizeof(InternalEntry);
    size_t modify_size =
        (insert_index - (end - group_cnt - 1) + 1) * sizeof(InternalEntry);
    page->set_consistent();
    write_page_and_unlock(page_buffer + modify_offset,
                          GADD(page_addr, modify_offset), modify_size,
                          cas_buffer, lock_addr, cxt, coro_id, true, false);
  } else {
    // current group is full, need span
    bool need_split = false;
    int new_gran = gran_half;
    if (cur_group_gran == gran_quarter) {
      bool succ =
          page->gran_quarter_to_half_and_insert(group_id, insert_index, k, v);
      // rearrange other half
      bool other_is_left = group_id >= 2;
      page->gran_quarter_to_half(other_is_left);

      if (!succ) {
        new_gran = gran_full;
        // both full
        need_split = !page->gran_half_to_full_and_insert(insert_index, k, v);
      }
    } else if (cur_group_gran == gran_half) {
      new_gran = gran_full;
      need_split = !page->gran_half_to_full_and_insert(insert_index, k, v);
    } else {
      // already full
      assert(group_cnt == kInternalCardinality);
      need_split = true;
    }

    if (!need_split) {
      // already span and insert
      page->hdr.leftmost_ptr.group_gran = new_gran;
      page->records[kGroupCardinality - 1].ptr.group_gran = new_gran;
      page->records[2 * kGroupCardinality - 1].ptr.group_gran = new_gran;
      page->records[3 * kGroupCardinality - 1].ptr.group_gran = new_gran;
      page->records[kInternalCardinality - 1].ptr.group_gran = new_gran;
      write_page_and_unlock(page_buffer, page_addr, kInternalPageSize,
                            cas_buffer, lock_addr, cxt, coro_id, false, false);
      // update parent ptr
      page_addr.child_gran = new_gran;
      auto up_level = path_stack[coro_id][level + 1];
      if (up_level != GlobalAddress::Null()) {
        internal_page_store(up_level, page->hdr.lowest, page_addr, root,
                            level + 1, cxt, coro_id);
      } else {
        // TODO:
        assert(false);
      }
    } else {
      // need split and insert
      GlobalAddress sibling_addr = dsm_client_->Alloc(kInternalPageSize);
      auto sibling_buf = rbuf.get_sibling_buffer();
      auto sibling = new (sibling_buf) InternalPage(page->hdr.level);

      // split
      std::vector<InternalEntry> tmp_records(
          page->records, page->records + kInternalCardinality);
      tmp_records.insert(tmp_records.begin() + insert_index + 1, {k, v});
      int m = kInternalCardinality / 2;
      Key split_key = tmp_records[m].key;
      GlobalAddress split_val = tmp_records[m].ptr;

      int sib_gran = sibling->rearrange_records(tmp_records.data() + m + 1, m,
                                                split_key, page->hdr.highest);
      sibling_addr.child_gran = sib_gran;
      split_val.group_gran = sib_gran;
      sibling->hdr.leftmost_ptr = split_val;
      sibling->hdr.sibling_ptr = page->hdr.sibling_ptr;

      page_addr.child_version = page->update_node_version();
      int cur_gran = page->rearrange_records(tmp_records.data(), m,
                                             page->hdr.lowest, split_key);
      page_addr.child_gran = cur_gran;
      page->hdr.sibling_ptr = sibling_addr;

      if (root == page_addr) {
        page->hdr.is_root = false;
      }

      dsm_client_->WriteSync(sibling_buf, sibling_addr, kInternalPageSize, cxt);
      write_page_and_unlock(page_buffer, page_addr, kInternalPageSize,
                            cas_buffer, lock_addr, cxt, coro_id, true, false);
      if (root == page_addr) {
        if (update_new_root(page_addr, split_key, sibling_addr, level + 1, root,
                            cxt, coro_id)) {
          return;
        }
      }
      auto up_level = path_stack[coro_id][level + 1];
      if (up_level != GlobalAddress::Null()) {
        // internal_page_store(up_level, split_key, sibling_addr, root, level +
        // 1,
        //                     cxt, coro_id);
        internal_page_store_update_left_child(up_level, split_key, sibling_addr,
                                              page->hdr.lowest, page_addr, root,
                                              level + 1, cxt, coro_id);
      } else {
        assert(false);
      }
    }
  }
}

void Tree::internal_page_store_update_left_child(
    GlobalAddress page_addr, const Key &k, GlobalAddress v,
    const Key &left_child, GlobalAddress left_child_val, GlobalAddress root,
    int level, CoroContext *cxt, int coro_id) {
  // assert(level == 1);
  uint64_t lock_index = get_lock_index(page_addr);

  GlobalAddress lock_addr;
  lock_addr.nodeID = page_addr.nodeID;
  lock_addr.offset = lock_index * sizeof(uint64_t);

  auto &rbuf = dsm_client_->get_rbuf(coro_id);
  uint64_t *cas_buffer = rbuf.get_cas_buffer();
  auto page_buffer = rbuf.get_page_buffer();

  lock_and_read_page(page_buffer, page_addr, kInternalPageSize, cas_buffer,
                     lock_addr, cxt, coro_id, false);

  auto page = (InternalPage *)page_buffer;

  assert(page->hdr.level == level);
  assert(page->check_consistent());

  // auto cnt = page->hdr.cnt;

  if (left_child >= page->hdr.highest) {
#ifdef USE_SX_LOCK
    this->unlock_sx_addr(lock_addr, cas_buffer, cxt, coro_id, true, false);
#else
    this->unlock_addr(lock_addr, cas_buffer, cxt, coro_id, true);
#endif

    assert(page->hdr.sibling_ptr != GlobalAddress::Null());

    // this->internal_page_store(page->hdr.sibling_ptr, k, v, root, level, cxt,
    //                           coro_id);
    this->internal_page_store_update_left_child(page->hdr.sibling_ptr, k, v,
                                                left_child, left_child_val,
                                                root, level, cxt, coro_id);
    return;
  } else if (k >= page->hdr.highest) {
    // left child in current node, new sibling leaf in sibling node
    if (left_child == page->hdr.lowest) {
      assert(page->hdr.leftmost_ptr == left_child_val);
      page->hdr.leftmost_ptr = left_child_val;
      size_t modify_offset = (char *)&page->hdr.leftmost_ptr - (char *)page;
      write_page_and_unlock(page_buffer + modify_offset,
                            GADD(page_addr, modify_offset),
                            sizeof(GlobalAddress), cas_buffer, lock_addr, cxt,
                            coro_id, true, false);
    } else {
      [[maybe_unused]] int idx = page->try_update(left_child, left_child_val);
      assert(idx != -1);
      if (idx != -1) {
        size_t modify_offset =
            offsetof(InternalPage, records) + idx * sizeof(InternalEntry);
        write_page_and_unlock(page_buffer + modify_offset,
                              GADD(page_addr, modify_offset),
                              sizeof(InternalEntry), cas_buffer, lock_addr, cxt,
                              coro_id, true, false);
      }
    }
    this->internal_page_store(page->hdr.sibling_ptr, k, v, root, level, cxt,
                              coro_id);
    return;
  }
  assert(k >= page->hdr.lowest);

  int group_id = get_key_group(k, page->hdr.lowest, page->hdr.highest);
  uint8_t cur_group_gran =
      page->records[kGroupCardinality * (group_id + 1) - 1].ptr.group_gran;
  int end;  // not inclusive
  int max_cnt;
  if (cur_group_gran == gran_quarter) {
    end = kGroupCardinality * (group_id + 1);
    max_cnt = kGroupCardinality;
  } else if (cur_group_gran == gran_half) {
    end = group_id < 2 ? kGroupCardinality * 2 : kInternalCardinality;
    max_cnt = kGroupCardinality * 2;
  } else {
    assert(cur_group_gran == gran_full);
    end = kInternalCardinality;
    max_cnt = kInternalCardinality;
  }

  bool is_update = false;
  int i = 0;
  // update k, not found key, can't be the last one of previous group
  for (; i < max_cnt; ++i) {
    int idx = end - 1 - i;
    if (page->records[idx].ptr == GlobalAddress::Null()) {
      break;
    } else if (k == page->records[idx].key) {
      page->records[idx].ptr = v;
      is_update = true;
      break;
    } else if (k > page->records[idx].key) {
      break;
    }
  }
  int16_t insert_index = end - 1 - i;  // may be -1 of start
  while (i < max_cnt &&
         page->records[end - 1 - i].ptr != GlobalAddress::Null()) {
    ++i;
  }
  int group_cnt = i;

  assert(!is_update);

  if (is_update) {
    page->set_consistent();
    write_page_and_unlock(page_buffer, page_addr, kInternalPageSize, cas_buffer,
                          lock_addr, cxt, coro_id, false, false);
    return;
  }

  // insert
  if (group_cnt < max_cnt) {
    // not exceed, shift to left
    // we redefine operator= for GlobalAddress, don't use memmove
    for (int i = end - group_cnt; i <= insert_index; ++i) {
      page->records[i - 1].key = page->records[i].key;
      page->records[i - 1].ptr = page->records[i].ptr;
    }
    page->records[insert_index].key = k;
    page->records[insert_index].ptr = v;

    // modify range: [end - group_cnt - 1, insert_index]
    size_t modify_offset, modify_size;
    if (insert_index >= end - group_cnt) {
      assert(page->records[insert_index - 1].key == left_child &&
             page->records[insert_index - 1].ptr == left_child_val);
      page->records[insert_index - 1].ptr = left_child_val;
      modify_offset = offsetof(InternalPage, records) +
                      (end - group_cnt - 1) * sizeof(InternalEntry);
      modify_size =
          (insert_index - (end - group_cnt - 1) + 1) * sizeof(InternalEntry);
    } else {
      // records[-1] is leftmost_ptr in header
      assert((page->records + end - max_cnt - 1)->ptr == left_child_val);
      (page->records + end - max_cnt - 1)->ptr = left_child_val;
      // (page->records + end - max_cnt - 1)->ptr.group_gran = cur_group_gran;
      modify_offset = offsetof(InternalPage, records) +
                      (end - max_cnt - 1) * sizeof(InternalEntry);
      modify_size =
          (insert_index - (end - max_cnt - 1) + 1) * sizeof(InternalEntry);
    }

    ++group_cnt;
    // allow full
    page->set_consistent();
    write_page_and_unlock(page_buffer + modify_offset,
                          GADD(page_addr, modify_offset), modify_size,
                          cas_buffer, lock_addr, cxt, coro_id, true, false);
  } else {
    // current group is full, need span
    // before shift, left is insert_index
    if (insert_index >= end - group_cnt) {
      assert(page->records[insert_index].key == left_child &&
             page->records[insert_index].ptr == left_child_val);
      page->records[insert_index].ptr = left_child_val;
    } else {
      // records[-1] is leftmost_ptr in header
      assert((page->records + end - max_cnt - 1)->ptr == left_child_val);
      (page->records + end - max_cnt - 1)->ptr = left_child_val;
      // (page->records + end - max_cnt - 1)->ptr.group_gran = cur_group_gran;
    }

    bool need_split = false;
    int new_gran = gran_half;
    if (cur_group_gran == gran_quarter) {
      bool succ =
          page->gran_quarter_to_half_and_insert(group_id, insert_index, k, v);
      // rearrange other half
      bool other_is_left = group_id >= 2;
      page->gran_quarter_to_half(other_is_left);

      if (!succ) {
        new_gran = gran_full;
        // both full
        need_split = !page->gran_half_to_full_and_insert(insert_index, k, v);
      }
    } else if (cur_group_gran == gran_half) {
      new_gran = gran_full;
      need_split = !page->gran_half_to_full_and_insert(insert_index, k, v);
    } else {
      // already full
      assert(group_cnt == kInternalCardinality);
      need_split = true;
    }

    if (!need_split) {
      // already span and insert
      page->hdr.leftmost_ptr.group_gran = new_gran;
      page->records[kGroupCardinality - 1].ptr.group_gran = new_gran;
      page->records[2 * kGroupCardinality - 1].ptr.group_gran = new_gran;
      page->records[3 * kGroupCardinality - 1].ptr.group_gran = new_gran;
      page->records[kInternalCardinality - 1].ptr.group_gran = new_gran;
      write_page_and_unlock(page_buffer, page_addr, kInternalPageSize,
                            cas_buffer, lock_addr, cxt, coro_id, false, false);
      // update parent ptr
      page_addr.child_gran = new_gran;
      auto up_level = path_stack[coro_id][level + 1];
      if (up_level != GlobalAddress::Null()) {
        internal_page_store(up_level, page->hdr.lowest, page_addr, root,
                            level + 1, cxt, coro_id);
      } else {
        // TODO:
        assert(false);
      }
    } else {
      // need split and insert
      GlobalAddress sibling_addr = dsm_client_->Alloc(kInternalPageSize);
      auto sibling_buf = rbuf.get_sibling_buffer();
      auto sibling = new (sibling_buf) InternalPage(page->hdr.level);

      // split
      std::vector<InternalEntry> tmp_records(
          page->records, page->records + kInternalCardinality);
      tmp_records.insert(tmp_records.begin() + insert_index + 1, {k, v});
      int m = kInternalCardinality / 2;
      Key split_key = tmp_records[m].key;
      GlobalAddress split_val = tmp_records[m].ptr;

      int sib_gran = sibling->rearrange_records(tmp_records.data() + m + 1, m,
                                                split_key, page->hdr.highest);
      sibling_addr.child_gran = sib_gran;
      split_val.group_gran = sib_gran;
      sibling->hdr.leftmost_ptr = split_val;
      sibling->hdr.sibling_ptr = page->hdr.sibling_ptr;

      page_addr.child_version = page->update_node_version();
      int cur_gran = page->rearrange_records(tmp_records.data(), m,
                                             page->hdr.lowest, split_key);
      page_addr.child_gran = cur_gran;
      page->hdr.sibling_ptr = sibling_addr;

      if (root == page_addr) {
        page->hdr.is_root = false;
      }
 
      dsm_client_->WriteSync(sibling_buf, sibling_addr, kInternalPageSize, cxt);
      write_page_and_unlock(page_buffer, page_addr, kInternalPageSize,
                            cas_buffer, lock_addr, cxt, coro_id, true, false);
      if (root == page_addr) {
        if (update_new_root(page_addr, split_key, sibling_addr, level + 1, root,
                            cxt, coro_id)) {
          return;
        }
      }
      auto up_level = path_stack[coro_id][level + 1];
      if (up_level != GlobalAddress::Null()) {
        // internal_page_store(up_level, split_key, sibling_addr, root, level + 1,
        //                     cxt, coro_id);
        internal_page_store_update_left_child(up_level, split_key, sibling_addr,
                                              page->hdr.lowest, page_addr, root,
                                              level + 1, cxt, coro_id);
      } else {
        assert(false);
      }
    }
  }
}

bool Tree::try_leaf_page_update(GlobalAddress page_addr,
                                GlobalAddress lock_addr, const Key &k,
                                const Value &v, CoroContext *cxt, int coro_id,
                                bool sx_lock) {
  auto &rbuf = dsm_client_->get_rbuf(coro_id);
  uint64_t *cas_buffer = rbuf.get_cas_buffer();
  auto page_buffer = rbuf.get_page_buffer();

  int bucket_id = key_hash_bucket(k);
  int group_id = bucket_id / 2;
  int group_offset =
      offsetof(LeafPage, groups) + sizeof(LeafEntryGroup) * group_id;
  int bucket_offset = bucket_id % 2 ? kBackOffset : kFrontOffset;

  lock_and_read_page(page_buffer + group_offset + bucket_offset,
                     GADD(page_addr, group_offset + bucket_offset),
                     kReadBucketSize, cas_buffer, lock_addr, cxt, coro_id,
                     sx_lock);
  // auto page = (LeafPage *)page_buffer;
  LeafEntryGroup *g = (LeafEntryGroup *)(page_buffer + group_offset);
  char *update_addr = nullptr;
  if (bucket_id % 2) {
    // back
    for (int i = 0; i < kAssociativity; ++i) {
      LeafEntry *p = &g->back[i];
      if (p->value != kValueNull && p->key == k) {
        p->value = v;
        update_addr = (char *)p;
        break;
      }
    }
  } else {
    // front
    for (int i = 0; i < kAssociativity; ++i) {
      LeafEntry *p = &g->front[i];
      if (p->value != kValueNull && p->key == k) {
        p->value = v;
        update_addr = (char *)p;
        break;
      }
    }
  }
  if (!update_addr) {
    // overflow
    for (int i = 0; i < kAssociativity - 1; ++i) {
      LeafEntry *p = &g->overflow[i];
      if (p->value != kValueNull && p->key == k) {
        p->value = v;
        update_addr = (char *)p;
        break;
      }
    }
  }

  if (update_addr) {
    write_page_and_unlock(
        update_addr, GADD(page_addr, (update_addr - page_buffer)),
        sizeof(LeafEntry), cas_buffer, lock_addr, cxt, coro_id, false, sx_lock);
    return true;
  }

  unlock_sx_addr(lock_addr, cas_buffer, cxt, coro_id, true, sx_lock);
  return false;
}

bool Tree::leaf_page_store(GlobalAddress page_addr, const Key &k,
                           const Value &v, GlobalAddress root, int level,
                           CoroContext *cxt, int coro_id, bool from_cache,
                           bool sx_lock) {
  uint64_t lock_index = get_lock_index(page_addr);

  GlobalAddress lock_addr;

#ifdef CONFIG_ENABLE_EMBEDDING_LOCK
  lock_addr = page_addr;
#else
  lock_addr.nodeID = page_addr.nodeID;
  lock_addr.offset = lock_index * sizeof(uint64_t);
#endif

  auto &rbuf = dsm_client_->get_rbuf(coro_id);
  uint64_t *cas_buffer = rbuf.get_cas_buffer();
  auto page_buffer = rbuf.get_page_buffer();

  // try update hash group
  if (sx_lock) {
    bool update_res =
        try_leaf_page_update(page_addr, lock_addr, k, v, cxt, coro_id, sx_lock);
    if (update_res) {
      return true;
    }
  }
  // update failed
  if (!from_cache) {
    // possibly insert
    sx_lock = false;
  }

  lock_and_read_page(page_buffer, page_addr, kLeafPageSize, cas_buffer,
                     lock_addr, cxt, coro_id, sx_lock);

  auto page = (LeafPage *)page_buffer;

  assert(page->hdr.level == level);

  if (from_cache &&
      (k < page->hdr.lowest || k >= page->hdr.highest)) {  // cache is stale
#ifdef USE_SX_LOCK
    this->unlock_sx_addr(lock_addr, cas_buffer, cxt, coro_id, true, sx_lock);
#else
    this->unlock_addr(lock_addr, cas_buffer, cxt, coro_id, true);
#endif
    return false;
  }

  if (k >= page->hdr.highest) {
    // note that retry may also get here
#ifdef USE_SX_LOCK
    this->unlock_sx_addr(lock_addr, cas_buffer, cxt, coro_id, true, sx_lock);
#else
    this->unlock_addr(lock_addr, cas_buffer, cxt, coro_id, true);
#endif

    assert(page->hdr.sibling_ptr != GlobalAddress::Null());
    this->leaf_page_store(page->hdr.sibling_ptr, k, v, root, level, cxt,
                          coro_id, false, sx_lock);
    return true;
  }
  assert(k >= page->hdr.lowest);

  if (page_addr.child_version != page->hdr.node_version) {
    if (from_cache) {
#ifdef USE_SX_LOCK
      this->unlock_sx_addr(lock_addr, cas_buffer, cxt, coro_id, true, sx_lock);
#else
      this->unlock_addr(lock_addr, cas_buffer, cxt, coro_id, true);
#endif
      return false;
    } else {
      // when does this happen?
      page_addr.child_version = page->hdr.node_version;
    }
  }

  // hash-based
  LeafEntry *insert_addr = nullptr;
  LeafEntry *update_addr = nullptr;
  int bucket_id = key_hash_bucket(k);
  LeafEntryGroup *g = &page->groups[bucket_id / 2];
  // [0, x, 1] [2, x, 3]
  // base bucket
  if (bucket_id % 2) {
    // back
    for (int i = 0; i < kAssociativity; ++i) {
      // auto r = &g->back[i];
      LeafEntry *p = &g->back[i];
      if (p->value != kValueNull) {
        if (p->key == k) {
          p->value = v;
          update_addr = p;
          break;
        }
      } else if (!insert_addr) {
        insert_addr = p;
      }
    }
  } else {
    // front
    for (int i = 0; i < kAssociativity; ++i) {
      LeafEntry *p = &g->front[i];
      if (p->value != kValueNull) {
        if (p->key == k) {
          p->value = v;
          update_addr = p;
          break;
        }
      } else if (!insert_addr) {
        insert_addr = p;
      }
    }
  }

  // overflow bucket
  if (update_addr == nullptr) {
    for (int i = 0; i < kAssociativity - 1; ++i) {
      LeafEntry *p = &g->overflow[i];
      if (p->value != kValueNull) {
        if (p->key == k) {
          p->value = v;
          update_addr = p;
          break;
        }
      } else if (!insert_addr) {
        insert_addr = p;
      }
    }
  }

  if (update_addr == nullptr) {  // insert new item
#ifdef USE_SX_LOCK
    // update to x lock
    if (sx_lock) {
      this->unlock_s_addr(lock_addr, cas_buffer, cxt, coro_id, true);
      return this->leaf_page_store(page_addr, k, v, root, level, cxt, coro_id,
                                   from_cache, false);
    }
#endif

    if (insert_addr) {
      insert_addr->key = k;
      insert_addr->value = v;
      update_addr = insert_addr;
    }
    // cnt++;
  }

  if (update_addr) {
    write_page_and_unlock((char *)update_addr,
                          GADD(page_addr, ((char *)update_addr - page_buffer)),
                          sizeof(LeafEntry), cas_buffer, lock_addr, cxt,
                          coro_id, false, sx_lock);
    return true;
  }

  // split
  LeafEntry tmp_records[kLeafCardinality];
  int cnt = 0;
  // for (int i = 0; i < kLeafCardinality; ++i) {
  //   if (page->records[i].value != kValueNull) {
  //     tmp_records[cnt++] = page->records[i];
  //   }
  // }
  for (int i = 0; i < kNumGroup; ++i) {
    LeafEntryGroup *g = &page->groups[i];
    for (int j = 0; j < kAssociativity; ++j) {
      if (g->front[j].value != kValueNull) {
        tmp_records[cnt++] = g->front[j];
      }
      if (g->back[j].value != kValueNull) {
        tmp_records[cnt++] = g->back[j];
      }
    }
    for (int j = 0; j < kAssociativity - 1; ++j) {
      if (g->overflow[j].value != kValueNull) {
        tmp_records[cnt++] = g->overflow[j];
      }
    }
  }
  std::sort(tmp_records, tmp_records + cnt);

  GlobalAddress sibling_addr;
  sibling_addr = dsm_client_->Alloc(kLeafPageSize);
  auto sibling_buf = rbuf.get_sibling_buffer();

  auto sibling = new (sibling_buf) LeafPage(page->hdr.level);

  // std::cout << "addr " <<  sibling_addr << " | level " <<
  // (int)(page->hdr.level) << std::endl;

  int m = cnt / 2;
  Key split_key = tmp_records[m].key;
  assert(split_key > page->hdr.lowest);
  assert(split_key < page->hdr.highest);

  // memset(reinterpret_cast<void *>(page->records), 0,
  //        sizeof(LeafEntry) * kLeafCardinality);
  memset(reinterpret_cast<void *>(page->groups), 0, sizeof(page->groups));
  page_addr.child_version = page->update_node_version();
  for (int i = 0; i < m; ++i) {
    int bucket_id = key_hash_bucket(tmp_records[i].key);
    page->groups[bucket_id / 2].insert(tmp_records[i].key, tmp_records[i].value,
                                       !(bucket_id % 2));
  }
  for (int i = m; i < cnt; ++i) {
    int bucket_id = key_hash_bucket(tmp_records[i].key);
    sibling->groups[bucket_id / 2].insert(
        tmp_records[i].key, tmp_records[i].value, !(bucket_id % 2));
  }

  sibling->hdr.lowest = split_key;
  sibling->hdr.highest = page->hdr.highest;
  page->hdr.highest = split_key;

  sibling_addr.child_version = sibling->hdr.node_version;

  // link
  sibling->hdr.sibling_ptr = page->hdr.sibling_ptr;
  page->hdr.sibling_ptr = sibling_addr;

  // insert k
  bool res;
  if (k < split_key) {
    int bucket_id = key_hash_bucket(k);
    res = page->groups[bucket_id / 2].insert(k, v, !(bucket_id % 2));
  } else {
    int bucket_id = key_hash_bucket(k);
    res = sibling->groups[bucket_id / 2].insert(k, v, !(bucket_id % 2));
  }

  if (sibling_addr.nodeID == page_addr.nodeID) {
    dsm_client_->Write(sibling_buf, sibling_addr, kLeafPageSize, false);
  } else {
    dsm_client_->WriteSync(sibling_buf, sibling_addr, kLeafPageSize, cxt);
  }

  if (root == page_addr) {
    page->hdr.is_root = false;
  }

  // async since we need to insert split_key in upper layer
  write_page_and_unlock(page_buffer, page_addr, kLeafPageSize, cas_buffer,
                        lock_addr, cxt, coro_id, true, sx_lock);

  // assert(page_addr.child_version % 2 == 0);
  // assert(sibling_addr.child_version % 2 == 0);
  if (root == page_addr) {  // update root
    if (update_new_root(page_addr, split_key, sibling_addr, level + 1, root,
                        cxt, coro_id)) {
      return res;
    }
  }

  auto up_level = path_stack[coro_id][level + 1];

  if (up_level != GlobalAddress::Null()) {
    internal_page_store_update_left_child(up_level, split_key, sibling_addr,
                                          page->hdr.lowest, page_addr, root,
                                          level + 1, cxt, coro_id);
    // internal_page_store(up_level, split_key, sibling_addr, root, level + 1,
    // cxt,
    //                     coro_id);
  } else {
    assert(from_cache);
    insert_internal_update_left_child(split_key, sibling_addr, page->hdr.lowest,
                                      page_addr, cxt, coro_id, level + 1);
  }

  return res;
}

bool Tree::leaf_page_del(GlobalAddress page_addr, const Key &k, int level,
                         CoroContext *cxt, int coro_id, bool from_cache) {
  uint64_t lock_index = get_lock_index(page_addr);

  GlobalAddress lock_addr;

#ifdef CONFIG_ENABLE_EMBEDDING_LOCK
  lock_addr = page_addr;
#else
  lock_addr.nodeID = page_addr.nodeID;
  lock_addr.offset = lock_index * sizeof(uint64_t);
#endif

  auto &rbuf = dsm_client_->get_rbuf(coro_id);
  uint64_t *cas_buffer = rbuf.get_cas_buffer();
  auto page_buffer = rbuf.get_page_buffer();

  lock_and_read_page(page_buffer, page_addr, kLeafPageSize, cas_buffer,
                     lock_addr, cxt, coro_id, false);

  auto page = (LeafPage *)page_buffer;

  assert(page->hdr.level == level);

  if (from_cache &&
      (k < page->hdr.lowest || k >= page->hdr.highest)) {  // cache is stale
#ifdef USE_SX_LOCK
    this->unlock_sx_addr(lock_addr, cas_buffer, cxt, coro_id, true, false);
#else
    this->unlock_addr(lock_addr, cas_buffer, cxt, coro_id, true);
#endif
    return false;
  }

  if (k >= page->hdr.highest) {
#ifdef USE_SX_LOCK
    this->unlock_sx_addr(lock_addr, cas_buffer, cxt, coro_id, true, false);
#else
    this->unlock_addr(lock_addr, cas_buffer, cxt, coro_id, true);
#endif
    assert(page->hdr.sibling_ptr != GlobalAddress::Null());
    this->leaf_page_del(page->hdr.sibling_ptr, k, level, cxt, coro_id);
    return true;
  }

  assert(k >= page->hdr.lowest);

  LeafEntry *update_addr = nullptr;
  int bucket_id = key_hash_bucket(k);
  LeafEntryGroup *g = &page->groups[bucket_id / 2];
  if (bucket_id % 2) {
    // back
    for (int i = 0; i < kAssociativity; ++i) {
      LeafEntry *p = &g->back[i];
      if (p->key == k) {
        p->value = kValueNull;
        update_addr = p;
        break;
      }
    }
  } else {
    // front
    for (int i = 0; i < kAssociativity; ++i) {
      LeafEntry *p = &g->front[i];
      if (p->key == k) {
        p->value = kValueNull;
        update_addr = p;
        break;
      }
    }
  }

  // overflow
  if (update_addr == nullptr) {
    for (int i = 0; i < kAssociativity - 1; ++i) {
      LeafEntry *p = &g->overflow[i];
      if (p->key == k) {
        p->value = kValueNull;
        update_addr = p;
        break;
      }
    }
  }

  if (update_addr) {
    write_page_and_unlock((char *)update_addr,
                          GADD(page_addr, ((char *)update_addr - (char *)page)),
                          sizeof(LeafEntry), cas_buffer, lock_addr, cxt,
                          coro_id, false, false);
  } else {
    this->unlock_addr(lock_addr, cas_buffer, cxt, coro_id, false);
  }
  return true;
}

void Tree::run_coroutine(CoroFunc func, int id, int coro_cnt, bool lock_bench) {
  using namespace std::placeholders;

  assert(coro_cnt <= define::kMaxCoro);
  for (int i = 0; i < coro_cnt; ++i) {
    auto gen = func(i, dsm_client_, id);
    worker[i] =
        CoroCall(std::bind(&Tree::coro_worker, this, _1, gen, i, lock_bench));
  }

  master = CoroCall(std::bind(&Tree::coro_master, this, _1, coro_cnt));

  master();
}

void Tree::coro_worker(CoroYield &yield, RequstGen *gen, int coro_id,
                       bool lock_bench) {
  CoroContext ctx;
  ctx.coro_id = coro_id;
  ctx.master = &master;
  ctx.yield = &yield;

  Timer coro_timer;
  auto thread_id = dsm_client_->get_my_thread_id();

  while (true) {
    auto r = gen->next();

    coro_timer.begin();
    if (lock_bench) {
      this->lock_bench(r.k, &ctx, coro_id);
    } else {
      if (r.is_search) {
        Value v;
        this->search(r.k, v, &ctx, coro_id);
      } else {
        this->insert(r.k, r.v, &ctx, coro_id);
      }
    }
    auto t = coro_timer.end();
    auto us_10 = t / 100;
    if (us_10 >= LATENCY_WINDOWS) {
      us_10 = LATENCY_WINDOWS - 1;
    }
    latency[thread_id][us_10]++;
    stat_helper.add(thread_id, lat_op, t);
  }
}

void Tree::coro_master(CoroYield &yield, int coro_cnt) {
  for (int i = 0; i < coro_cnt; ++i) {
    yield(worker[i]);
  }

  while (true) {
    uint64_t next_coro_id;

    if (dsm_client_->PollRdmaCqOnce(next_coro_id)) {
      yield(worker[next_coro_id]);
    }

    if (!hot_wait_queue.empty()) {
      next_coro_id = hot_wait_queue.front();
      hot_wait_queue.pop();
      yield(worker[next_coro_id]);
    }
  }
}

// Local Locks
inline bool Tree::acquire_local_lock(GlobalAddress lock_addr, CoroContext *cxt,
                                     int coro_id) {
  auto &node = local_locks[lock_addr.nodeID][lock_addr.offset / 8];

  uint64_t lock_val = node.ticket_lock.fetch_add(1);

  uint32_t ticket = lock_val << 32 >> 32;
  uint32_t current = lock_val >> 32;

  while (ticket != current) {  // lock failed

    if (cxt != nullptr) {
      hot_wait_queue.push(coro_id);
      (*cxt->yield)(*cxt->master);
    }

    current = node.ticket_lock.load(std::memory_order_relaxed) >> 32;
  }

  node.hand_time++;

  return node.hand_over;
}

inline bool Tree::can_hand_over(GlobalAddress lock_addr) {
  auto &node = local_locks[lock_addr.nodeID][lock_addr.offset / 8];
  uint64_t lock_val = node.ticket_lock.load(std::memory_order_relaxed);

  uint32_t ticket = lock_val << 32 >> 32;
  uint32_t current = lock_val >> 32;

  if (ticket <= current + 1) {  // no pending locks
    node.hand_over = false;
  } else {
    node.hand_over = node.hand_time < define::kMaxHandOverTime;
  }
  if (!node.hand_over) {
    node.hand_time = 0;
  }

  return node.hand_over;
}

inline void Tree::releases_local_lock(GlobalAddress lock_addr) {
  auto &node = local_locks[lock_addr.nodeID][lock_addr.offset / 8];

  node.ticket_lock.fetch_add((1ull << 32));
}

void Tree::index_cache_statistics() {
  index_cache->statistics();
  // index_cache->bench();
}

void Tree::clear_statistics() {
  for (int i = 0; i < MAX_APP_THREAD; ++i) {
    cache_hit[i][0] = 0;
    cache_miss[i][0] = 0;
  }
}
