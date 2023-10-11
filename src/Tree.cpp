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
#define FINE_GRAINED_LEAF_NODE
#define FINE_GRAINED_INTERNAL_NODE
#define BATCH_LOCK_READ

uint64_t cache_miss[MAX_APP_THREAD][8];
uint64_t cache_hit[MAX_APP_THREAD][8];
uint64_t latency[MAX_APP_THREAD][LATENCY_WINDOWS];

StatHelper stat_helper;

thread_local CoroCall Tree::worker[define::kMaxCoro];
thread_local CoroCall Tree::master;
thread_local uint64_t Tree::coro_ops_total;
thread_local uint64_t Tree::coro_ops_cnt_start;
thread_local uint64_t Tree::coro_ops_cnt_finish;
thread_local GlobalAddress path_stack[define::kMaxCoro]
                                     [define::kMaxLevelOfTree];

constexpr uint64_t XS_LOCK_FAA_MASK = 0x8000800080008000ul;
// high->low
// X_CUR X_TIC S_CUR S_TIC
// lock: increase TIC, unlock: increase CUR

constexpr uint64_t ADD_S_LOCK = 1;
constexpr uint64_t ADD_S_UNLOCK = 1ul << 16;
constexpr uint64_t ADD_X_LOCK = 1ul << 32;
constexpr uint64_t ADD_X_UNLOCK = 1ul << 48;

// thread_local std::queue<uint16_t> hot_wait_queue;

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
  char *page_buffer = (dsm_client_->get_rbuf(0)).get_page_buffer();
  GlobalAddress root_addr = dsm_client_->Alloc(kLeafPageSize);
  [[maybe_unused]] LeafPage *root_page = new (page_buffer) LeafPage;

  dsm_client_->WriteSync(page_buffer, root_addr, kLeafPageSize);

  uint64_t *cas_buffer = (dsm_client_->get_rbuf(0)).get_cas_buffer();
  bool res = dsm_client_->CasSync(root_ptr_ptr, 0, root_addr.raw, cas_buffer);
  if (res) {
    std::cout << "Tree root pointer value " << root_addr << std::endl;
  } else {
    // std::cout << "fail\n";
  }
}

Tree::~Tree() { delete index_cache; }

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

inline void Tree::before_operation(CoroContext *ctx, int coro_id) {
  for (size_t i = 0; i < define::kMaxLevelOfTree; ++i) {
    path_stack[coro_id][i] = GlobalAddress::Null();
  }
}

GlobalAddress Tree::get_root_ptr_ptr() {
  GlobalAddress addr;
  addr.node_version = 0;
  addr.nodeID = 0;
  addr.offset =
      define::kRootPointerStoreOffest + sizeof(GlobalAddress) * tree_id;

  return addr;
}

extern GlobalAddress g_root_ptr;
extern int g_root_level;
extern bool enable_cache;
GlobalAddress Tree::get_root_ptr(CoroContext *ctx, bool force_read) {
  if (force_read || g_root_ptr == GlobalAddress::Null()) {
    char *page_buffer =
        (dsm_client_->get_rbuf(ctx ? ctx->coro_id : 0)).get_page_buffer();
    dsm_client_->ReadSync(page_buffer, root_ptr_ptr, sizeof(GlobalAddress),
                          ctx);
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
//   g_root_ptr = new_root_addr;
//   g_root_level = root_level;
//   if (root_level >= 3) {
//     enable_cache = true;
//   }
// }

bool Tree::update_new_root(GlobalAddress left, const Key &k,
                           GlobalAddress right, int level,
                           GlobalAddress old_root, CoroContext *ctx) {
  auto &rbuf = dsm_client_->get_rbuf(ctx ? ctx->coro_id : 0);
  char *page_buffer = rbuf.get_page_buffer();
  uint64_t *cas_buffer = rbuf.get_cas_buffer();
  InternalPage *new_root =
      new (page_buffer) InternalPage(left, k, right, level);
  new_root->hdr.is_root = true;

  GlobalAddress new_root_addr = dsm_client_->Alloc(kInternalPageSize);

  new_root->hdr.my_addr = new_root_addr;
  dsm_client_->WriteSync(page_buffer, new_root_addr, kInternalPageSize, ctx);
  if (dsm_client_->CasSync(root_ptr_ptr, old_root, new_root_addr, cas_buffer,
                           ctx)) {
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
inline bool Tree::try_lock_addr(GlobalAddress lock_addr, uint64_t *buf,
                                CoroContext *ctx) {
  // bool hand_over = acquire_local_lock(lock_addr, ctx, coro_id);
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
      exit(-1);
    }
    auto tag = dsm_client_->get_thread_tag();
    bool res = dsm_client_->CasDmSync(lock_addr, 0, tag, buf, ctx);

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
                              CoroContext *ctx, bool async) {
  // bool hand_over_other = can_hand_over(lock_addr);
  // if (hand_over_other) {
  //   releases_local_lock(lock_addr);
  //   return;
  // }

  *buf = 0;
  if (async) {
    dsm_client_->WriteDm((char *)buf, lock_addr, sizeof(uint64_t), false);
  } else {
    dsm_client_->WriteDmSync((char *)buf, lock_addr, sizeof(uint64_t), ctx);
  }

  // releases_local_lock(lock_addr);
}

inline void Tree::acquire_sx_lock(GlobalAddress lock_addr,
                                  uint64_t *lock_buffer, CoroContext *ctx,
                                  bool share_lock, bool upgrade_from_s) {
  assert(!upgrade_from_s || !share_lock);
  uint64_t add_val;
  if (share_lock) {
    add_val = ADD_S_LOCK;
  } else {
    add_val = upgrade_from_s ? (ADD_X_LOCK | ADD_S_UNLOCK) : ADD_X_LOCK;
  }
  // Timer timer;
  // timer.begin();

  dsm_client_->FaaDmBoundSync(lock_addr, 3, add_val, lock_buffer,
                              XS_LOCK_FAA_MASK, ctx);

  uint16_t s_tic = *lock_buffer & 0xffff;
  uint16_t s_cnt = (*lock_buffer >> 16) & 0xffff;
  uint16_t x_tic = (*lock_buffer >> 32) & 0xffff;
  uint16_t x_cnt = (*lock_buffer >> 48) & 0xffff;

  if (upgrade_from_s) {
    ++s_cnt;
  }
  uint64_t retry_cnt = 0;
retry:
  if (share_lock && x_cnt == x_tic) {
    // ok
  } else if (!share_lock && x_cnt == x_tic && s_cnt == s_tic) {
    // ok
  } else {
    ++retry_cnt;
    if (retry_cnt > 5000000) {
      printf("Deadlock [%u, %lu] my thread %d coro_id %d\n", lock_addr.nodeID,
             lock_addr.offset, dsm_client_->get_my_thread_id(),
             ctx ? ctx->coro_id : 0);
      printf("s [%u, %u] x [%u, %u]\n", s_tic, s_cnt, x_tic, x_cnt);
      fflush(stdout);
      assert(false);
      exit(-1);
    }
    dsm_client_->ReadDmSync((char *)lock_buffer, lock_addr, 8, ctx);
    s_cnt = (*lock_buffer >> 16) & 0xffff;
    x_cnt = (*lock_buffer >> 48) & 0xffff;
    goto retry;
  }
  // uint64_t t = timer.end();
  // stat_helper.add(dsm_client_->get_my_thread_id(), lat_lock, t);
}

inline void Tree::release_sx_lock(GlobalAddress lock_addr,
                                  uint64_t *lock_buffer, CoroContext *ctx,
                                  bool async, bool share_lock) {
  uint64_t add_val = share_lock ? ADD_S_UNLOCK : ADD_X_UNLOCK;
  if (async) {
    dsm_client_->FaaDmBound(lock_addr, 3, add_val, lock_buffer,
                            XS_LOCK_FAA_MASK, false);
  } else {
    dsm_client_->FaaDmBoundSync(lock_addr, 3, add_val, lock_buffer,
                                XS_LOCK_FAA_MASK, ctx);
  }
}

inline void Tree::acquire_lock(GlobalAddress lock_addr, uint64_t *lock_buffer,
                               CoroContext *ctx, bool share_lock,
                               bool upgrade_from_s) {
#ifdef USE_SX_LOCK
  acquire_sx_lock(lock_addr, lock_buffer, ctx, share_lock, upgrade_from_s);
#else
  try_lock_addr(lock_addr, lock_buffer, ctx);
#endif
}

inline void Tree::release_lock(GlobalAddress lock_addr, uint64_t *lock_buffer,
                               CoroContext *ctx, bool async, bool share_lock) {
#ifdef USE_SX_LOCK
  uint64_t add_val = share_lock ? ADD_S_UNLOCK : ADD_X_UNLOCK;
  if (async) {
    dsm_client_->FaaDmBound(lock_addr, 3, add_val, lock_buffer,
                            XS_LOCK_FAA_MASK, false);
  } else {
    dsm_client_->FaaDmBoundSync(lock_addr, 3, add_val, lock_buffer,
                                XS_LOCK_FAA_MASK, ctx);
  }
#else
  *lock_buffer = 0;
  if (async) {
    dsm_client_->WriteDm((char *)lock_buffer, lock_addr, sizeof(uint64_t),
                         false);
  } else {
    dsm_client_->WriteDmSync((char *)lock_buffer, lock_addr, sizeof(uint64_t),
                             ctx);
  }
#endif
}

void Tree::write_and_unlock(char *write_buffer, GlobalAddress write_addr,
                            int write_size, uint64_t *cas_buffer,
                            GlobalAddress lock_addr, CoroContext *ctx,
                            bool async, bool sx_lock) {
  Timer timer;
  timer.begin();

  // bool hand_over_other = can_hand_over(lock_addr);
  // if (hand_over_other) {
  //   dsm_client_->WriteSync(write_buffer, write_addr, write_size, ctx);
  //   // releases_local_lock(lock_addr);
  //   return;
  // }

#ifdef USE_SX_LOCK
  dsm_client_->Write(write_buffer, write_addr, write_size, false);
  release_sx_lock(lock_addr, cas_buffer, ctx, async, sx_lock);
#else
  RdmaOpRegion rs[2];
  rs[0].source = (uint64_t)write_buffer;
  rs[0].dest = write_addr;
  rs[0].size = write_size;
  rs[0].is_on_chip = false;

  rs[1].source = (uint64_t)cas_buffer;
  rs[1].dest = lock_addr;
  rs[1].size = sizeof(uint64_t);
  rs[1].is_on_chip = true;

  *(uint64_t *)rs[1].source = 0;
  if (async) {
    dsm_client_->WriteBatch(rs, 2, false);
  } else {
    dsm_client_->WriteBatchSync(rs, 2, ctx);
  }
#endif
  // releases_local_lock(lock_addr);
  auto t = timer.end();
  stat_helper.add(dsm_client_->get_my_thread_id(), lat_write_page, t);
}

void Tree::cas_and_unlock(GlobalAddress cas_addr, int log_cas_size,
                          uint64_t *cas_buffer, uint64_t equal, uint64_t swap,
                          uint64_t mask, GlobalAddress lock_addr,
                          uint64_t *lock_buffer, bool share_lock,
                          CoroContext *ctx, bool async) {
  Timer timer;
  timer.begin();

#ifdef USE_SX_LOCK
  dsm_client_->CasMask(cas_addr, log_cas_size, equal, swap, cas_buffer, ~(0ull),
                       false);
  release_sx_lock(lock_addr, lock_buffer, ctx, async, share_lock);
#else  // not USE_SX_LOCK
  RdmaOpRegion rs[2];
  rs[0].source = (uint64_t)cas_buffer;
  rs[0].dest = cas_addr;
  rs[0].log_sz = log_cas_size;
  rs[0].is_on_chip = false;

  rs[1].source = (uint64_t)lock_buffer;
  rs[1].dest = lock_addr;
  rs[1].size = sizeof(uint64_t);
  rs[1].is_on_chip = true;
  *(uint64_t *)rs[1].source = 0;
  if (async) {
    dsm_client_->CasMaskWrite(rs[0], equal, swap, mask, rs[1], false);
  } else {
    dsm_client_->CasMaskWriteSync(rs[0], equal, swap, mask, rs[1], ctx);
  }
#endif

  uint64_t t = timer.end();
  stat_helper.add(dsm_client_->get_my_thread_id(), lat_write_page, t);
}

void Tree::lock_and_read(GlobalAddress lock_addr, bool share_lock,
                         bool upgrade_from_s, uint64_t *lock_buffer,
                         GlobalAddress read_addr, int read_size,
                         char *read_buffer, CoroContext *ctx) {
#ifdef BATCH_LOCK_READ

#ifdef USE_SX_LOCK
  assert(!upgrade_from_s || !share_lock);
  uint64_t add_val;
  if (share_lock) {
    add_val = ADD_S_LOCK;
  } else {
    add_val = upgrade_from_s ? (ADD_X_LOCK | ADD_S_UNLOCK) : ADD_X_LOCK;
  }
  dsm_client_->FaaDmBound(lock_addr, 3, add_val, lock_buffer, XS_LOCK_FAA_MASK,
                          false);
  dsm_client_->ReadSync(read_buffer, read_addr, read_size, ctx);

  uint16_t s_tic = *lock_buffer & 0xffff;
  uint16_t s_cnt = (*lock_buffer >> 16) & 0xffff;
  uint16_t x_tic = (*lock_buffer >> 32) & 0xffff;
  uint16_t x_cnt = (*lock_buffer >> 48) & 0xffff;

  if (upgrade_from_s) {
    ++s_cnt;
  }

  uint64_t retry_cnt = 0;
retry:
  if (share_lock && x_cnt == x_tic) {
    // ok
  } else if (!share_lock && x_cnt == x_tic && s_cnt == s_tic) {
    // ok
  } else {
    ++retry_cnt;
    if (retry_cnt > 5000000) {
      printf("Deadlock [%u, %lu] my thread %d coro_id %d\n", lock_addr.nodeID,
             lock_addr.offset, dsm_client_->get_my_thread_id(),
             ctx ? ctx->coro_id : 0);
      printf("s [%u, %u] x [%u, %u]\n", s_tic, s_cnt, x_tic, x_cnt);
      fflush(stdout);
      assert(false);
      exit(-1);
    }

    dsm_client_->ReadDm((char *)lock_buffer, lock_addr, 8, false);
    dsm_client_->ReadSync(read_buffer, read_addr, read_size, ctx);

    s_cnt = (*lock_buffer >> 16) & 0xffff;
    x_cnt = (*lock_buffer >> 48) & 0xffff;
    goto retry;
  }

#else  // not sx lock
  RdmaOpRegion rs[2];
  {
    uint64_t retry_cnt = 0;
    uint64_t pre_tag = 0;
    uint64_t conflict_tag = 0;
    auto tag = dsm_client_->get_thread_tag();
  retry:
    rs[0].source = (uint64_t)lock_buffer;
    rs[0].dest = lock_addr;
    rs[0].size = 8;
    rs[0].is_on_chip = true;

    rs[1].source = (uint64_t)(read_buffer);
    rs[1].dest = read_addr;
    rs[1].size = read_size;
    rs[1].is_on_chip = false;
    if (retry_cnt > 1000000) {
      printf("Deadlock [%u, %lu] my thread %d coro_id %d\n", lock_addr.nodeID,
             lock_addr.offset, dsm_client_->get_my_thread_id(),
             ctx ? ctx->coro_id : 0);
      fflush(stdout);
      assert(false);
      exit(-1);
    }

    Timer timer;
    timer.begin();
    bool res = dsm_client_->CasReadSync(rs[0], rs[1], 0, tag, ctx);
    uint64_t t = timer.end();
    stat_helper.add(dsm_client_->get_my_thread_id(), lat_read_page, t);
    if (!res) {
      // conflict_tag = *buf - 1;
      conflict_tag = *lock_buffer;
      if (conflict_tag != pre_tag) {
        retry_cnt = 0;
        pre_tag = conflict_tag;
      }
      goto retry;
    }
  }
#endif

#else  // not batch

  Timer timer;
  timer.begin();
  acquire_lock(lock_addr, lock_buffer, ctx, share_lock, upgrade_from_s);
  dsm_client_->ReadSync(read_buffer, read_addr, read_size, ctx);
  uint64_t t = timer.end();
  stat_helper.add(dsm_client_->get_my_thread_id(), lat_read_page, t);
#endif
}

void Tree::lock_read_read(GlobalAddress lock_addr, bool share_lock,
                          bool upgrade_from_s, uint64_t *lock_buffer,
                          GlobalAddress r1_addr, int r1_size, char *r1_buffer,
                          GlobalAddress r2_addr, int r2_size, char *r2_buffer,
                          CoroContext *ctx) {
#ifdef BATCH_LOCK_READ

#ifdef USE_SX_LOCK
  assert(!upgrade_from_s || !share_lock);
  uint64_t add_val;
  if (share_lock) {
    add_val = ADD_S_LOCK;
  } else {
    add_val = upgrade_from_s ? (ADD_X_LOCK | ADD_S_UNLOCK) : ADD_X_LOCK;
  }

  // Timer timer;
  // timer.begin();

  dsm_client_->FaaDmBound(lock_addr, 3, add_val, lock_buffer, XS_LOCK_FAA_MASK,
                          false);
  dsm_client_->Read(r1_buffer, r1_addr, r1_size, false);
  dsm_client_->ReadSync(r2_buffer, r2_addr, r2_size, ctx);

  uint16_t s_tic = *lock_buffer & 0xffff;
  uint16_t s_cnt = (*lock_buffer >> 16) & 0xffff;
  uint16_t x_tic = (*lock_buffer >> 32) & 0xffff;
  uint16_t x_cnt = (*lock_buffer >> 48) & 0xffff;

  if (upgrade_from_s) {
    ++s_cnt;
  }

  uint64_t retry_cnt = 0;
retry:
  if (share_lock && x_cnt == x_tic) {
    // ok
  } else if (!share_lock && x_cnt == x_tic && s_cnt == s_tic) {
    // ok
  } else {
    ++retry_cnt;
    if (retry_cnt > 5000000) {
      printf("Deadlock [%u, %lu] my thread %d coro_id %d\n", lock_addr.nodeID,
             lock_addr.offset, dsm_client_->get_my_thread_id(),
             ctx ? ctx->coro_id : 0);
      printf("s [%u, %u] x [%u, %u]\n", s_tic, s_cnt, x_tic, x_cnt);
      fflush(stdout);
      assert(false);
      exit(-1);
    }

    dsm_client_->ReadDm((char *)lock_buffer, lock_addr, 8, false);
    dsm_client_->Read(r1_buffer, r1_addr, r1_size, false);
    dsm_client_->ReadSync(r2_buffer, r2_addr, r2_size, ctx);

    s_cnt = (*lock_buffer >> 16) & 0xffff;
    x_cnt = (*lock_buffer >> 48) & 0xffff;
    goto retry;
  }
  // uint64_t t = timer.end();
  // stat_helper.add(dsm_client_->get_my_thread_id(), lat_read_page, t);

#else // else not sx lock
  uint64_t retry_cnt = 0;
  uint64_t pre_tag = 0;
  uint64_t conflict_tag = 0;
  uint64_t tag = dsm_client_->get_thread_tag();

  // Timer timer;
  // timer.begin();

retry:
  if (retry_cnt > 1000000) {
    printf("Deadlock [%u, %lu] my thread %d coro_id %d locked by %ld,%ld\n",
           lock_addr.nodeID, lock_addr.offset, dsm_client_->get_my_thread_id(),
           ctx ? ctx->coro_id : 0, conflict_tag >> 32,
           conflict_tag << 32 >> 32);
    assert(false);
    exit(-1);
  }

  dsm_client_->CasDm(lock_addr, 0, tag, lock_buffer, false);
  dsm_client_->Read(r1_buffer, r1_addr, r1_size, false);
  dsm_client_->ReadSync(r2_buffer, r2_addr, r2_size, ctx);

  bool res = *(lock_buffer) == 0;
  if (!res) {
    conflict_tag = *lock_buffer;
    if (conflict_tag != pre_tag) {
      retry_cnt = 0;
      pre_tag = conflict_tag;
    }
    goto retry;
  }

  // uint64_t t = timer.end();
  // stat_helper.add(dsm_client_->get_my_thread_id(), lat_read_page, t);
  
#endif  // USE_SX_LOCK

#else // else not batch lock read
  // Timer timer;
  // timer.begin();
  acquire_lock(lock_addr, lock_buffer, ctx, share_lock, upgrade_from_s);
  // uint64_t t_lock = timer.end();
  // stat_helper.add(dsm_client_->get_my_thread_id(), lat_lock, t_lock);

  // timer.begin();
  dsm_client_->ReadSync(r1_buffer, r1_addr, r1_size, ctx);
  dsm_client_->ReadSync(r2_buffer, r2_addr, r2_size, ctx);
  // uint64_t t_read = timer.end();
  // stat_helper.add(dsm_client_->get_my_thread_id(), lat_read_page, t_read);
#endif  // BATCH_LOCK_READ
}

void Tree::lock_bench(const Key &k, CoroContext *ctx, int coro_id) {
  // uint64_t lock_index = CityHash64((char *)&k, sizeof(k)) %
  // define::kNumOfLock;

  // GlobalAddress lock_addr;
  // lock_addr.nodeID = 0;
  // lock_addr.offset = lock_index * sizeof(uint64_t);
  // auto cas_buffer = dsm->get_rbuf(coro_id).get_cas_buffer();

  // // bool res = dsm->cas_sync(lock_addr, 0, 1, cas_buffer, ctx);
  // // try_lock_addr(lock_addr, 1, cas_buffer, ctx, coro_id);
  // // unlock_addr(lock_addr, 1, cas_buffer, ctx, coro_id, true);
  // bool sx_lock = false;
  // acquire_sx_lock(lock_addr, 1, cas_buffer, ctx, coro_id, sx_lock);
  // release_sx_lock(lock_addr, 1, cas_buffer, ctx, coro_id, true, sx_lock);

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

  // dsm->read(page_buffer, page_addr, 32, false, ctx);
  // dsm->read_sync(page_buffer, page_addr, kLeafPageSize, ctx);

  write_and_unlock(page_buffer, page_addr, 128, cas_buffer, lock_addr, ctx,
                   false, false);

  // lock_and_read_page(page_buffer, page_addr, kLeafPageSize, cas_buffer,
  //                    lock_addr, ctx, coro_id, true);
  // release_sx_lock(lock_addr, cas_buffer, ctx, coro_id, false, true);
}

void Tree::insert_internal_update_left_child(const Key &k, GlobalAddress v,
                                             const Key &left_child,
                                             GlobalAddress left_child_val,
                                             CoroContext *ctx, int level) {
  assert(left_child_val != GlobalAddress::Null());
  auto root = get_root_ptr(ctx);
  SearchResult result;

  GlobalAddress p = root;
  int level_hint = -1;
  Key min = kKeyMin;
  Key max = kKeyMax;

next:
  if (!page_search(p, level_hint, p.read_gran, min, max, left_child, result,
                   ctx)) {
    std::cout << "SEARCH WARNING insert" << std::endl;
    p = get_root_ptr(ctx);
    level_hint = -1;
    sleep(1);
    goto next;
  }

  assert(result.level != 0);
  if (result.sibling != GlobalAddress::Null()) {
    p = result.sibling;
    level_hint = result.level;
    min = result.next_min; // remain the max
    goto next;
  }

  if (result.level >= level + 1) {
    p = result.next_level;
    if (result.level > level + 1) {
      level_hint = result.level - 1;
      min = result.next_min;
      max = result.next_max;
      goto next;
    }
  }

  // internal_page_store(p, k, v, root, level, ctx, coro_id);
  assert(result.level == level + 1 || result.level == level);
  internal_page_store_update_left_child(p, k, v, left_child, left_child_val,
                                        root, level, ctx);
}

void Tree::insert(const Key &k, const Value &v, CoroContext *ctx) {
  assert(dsm_client_->IsRegistered());
  int coro_id = ctx ? ctx->coro_id : 0;

  before_operation(ctx, coro_id);

  Key min = kKeyMin;
  Key max = kKeyMax;

  if (enable_cache) {
    GlobalAddress cache_addr, parent_addr;
    auto entry = index_cache->search_from_cache(k, &cache_addr, &parent_addr);
    if (entry) {  // cache hit
      path_stack[coro_id][1] = parent_addr;
      auto root = get_root_ptr(ctx);
#ifdef USE_SX_LOCK
      bool status = leaf_page_store(cache_addr, k, v, root, 0, ctx, true, true);
#else
      bool status = leaf_page_store(cache_addr, k, v, root, 0, ctx, true);
#endif
      if (status) {
        cache_hit[dsm_client_->get_my_thread_id()][0]++;
        return;
      }
      // cache stale, from root,
      index_cache->invalidate(entry, dsm_client_->get_my_thread_id());
    }
    cache_miss[dsm_client_->get_my_thread_id()][0]++;
  }

  auto root = get_root_ptr(ctx);
  SearchResult result;

  GlobalAddress p = root;
  int level_hint = -1;

next:
  if (!page_search(p, level_hint, p.read_gran, min, max, k, result, ctx)) {
    std::cout << "SEARCH WARNING insert" << std::endl;
    p = get_root_ptr(ctx);
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
      min = result.next_min; // remain the max
      level_hint = result.level;
      goto next;
    }

    p = result.next_level;
    level_hint = result.level - 1;
    if (result.level != 1) {
      min = result.next_min;
      max = result.next_max;
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
    res = leaf_page_store(p, k, v, root, 0, ctx, false, true);
#else
    res = leaf_page_store(p, k, v, root, 0, ctx, false);
#endif
    ++cnt;
  }
}

bool Tree::search(const Key &k, Value &v, CoroContext *ctx, int coro_id) {
  assert(dsm_client_->IsRegistered());

  auto p = get_root_ptr(ctx);
  SearchResult result;
  Key min = kKeyMin;
  Key max = kKeyMax;

  int level_hint = -1;

  bool from_cache = false;
  const CacheEntry *entry = nullptr;
  if (enable_cache) {
    // Timer timer;
    // timer.begin();
    GlobalAddress cache_addr, parent_addr;
    entry = index_cache->search_from_cache(k, &cache_addr, &parent_addr);
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
  if (!page_search(p, level_hint, p.read_gran, min, max, k, result, ctx,
                   from_cache)) {
    if (from_cache) {  // cache stale
      index_cache->invalidate(entry, dsm_client_->get_my_thread_id());
      cache_hit[dsm_client_->get_my_thread_id()][0]--;
      cache_miss[dsm_client_->get_my_thread_id()][0]++;
      from_cache = false;

      p = get_root_ptr(ctx);
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
      min = result.next_min; // remain the max
      level_hint = 0;
      goto next;
    }
    return false;  // not found
  } else {         // internal
    if (result.sibling != GlobalAddress::Null()) {
      p = result.sibling;
      min = result.next_min; // remain the max
      level_hint = result.level;
    } else {
      p = result.next_level;
      min = result.next_min;
      max = result.next_max;
      level_hint = result.level - 1;
    }
    goto next;
  }
}

int Tree::range_query(const Key &from, const Key &to, Value *value_buffer,
                      int max_cnt, CoroContext *ctx, int coro_id) {
  const int kParaFetch = 32;
  thread_local std::vector<InternalPage *> result;
  thread_local std::vector<GlobalAddress> leaves;

  result.clear();
  leaves.clear();

  index_cache->thread_status->rcu_progress(dsm_client_->get_my_thread_id());

  index_cache->search_range_from_cache(from, to, result);

  // FIXME: here, we assume all innernal nodes are cached in compute node
  if (result.empty()) {
    index_cache->thread_status->rcu_exit(dsm_client_->get_my_thread_id());
    return 0;
  }

  int counter = 0;
  for (auto page : result) {
    std::vector<InternalEntry> tmp_records;
    tmp_records.reserve(kInternalCardinality);
    for (int i = 0; i < kInternalCardinality; ++i) {
      if (page->records[i].ptr != GlobalAddress::Null()) {
        tmp_records.push_back(page->records[i]);
      }
    }
    std::sort(tmp_records.begin(), tmp_records.end());
    int cnt = tmp_records.size();
    if (cnt > 0) {
      if (page->hdr.leftmost_ptr != GlobalAddress::Null()) {
        if (tmp_records[0].key > from && page->hdr.lowest < to) {
          leaves.push_back(page->hdr.leftmost_ptr);
        }
      }
      for (int i = 1; i < cnt; i++) {
        if (tmp_records[i].key > from && tmp_records[i - 1].key < to) {
          leaves.push_back(tmp_records[i - 1].ptr);
        }
      }
      if (page->hdr.highest > from && tmp_records[cnt - 1].key < to) {
        leaves.push_back(tmp_records[cnt - 1].ptr);
      }
    }
  }

  int cq_cnt = 0;
  char *range_buffer = (dsm_client_->get_rbuf(coro_id)).get_range_buffer();
  for (size_t i = 0; i < leaves.size(); ++i) {
    if (i > 0 && i % kParaFetch == 0) {
      dsm_client_->PollRdmaCq(kParaFetch);
      cq_cnt -= kParaFetch;
      for (int k = 0; counter < max_cnt && k < kParaFetch; ++k) {
        auto page = (LeafPage *)(range_buffer + k * kLeafPageSize);
        for (int idx = 0; counter < max_cnt && idx < kNumGroup; ++idx) {
          LeafEntryGroup *g = &page->groups[idx];
          for (int j = 0; counter < max_cnt && j < kAssociativity; ++j) {
            auto &r = g->front[j];
            if (r.lv.val != kValueNull && r.key >= from && r.key < to) {
              value_buffer[counter++] = r.lv.val;
            }
          }
          for (int j = 0; counter < max_cnt && j < kAssociativity; ++j) {
            auto &r = g->back[j];
            if (r.lv.val != kValueNull && r.key >= from && r.key < to) {
              value_buffer[counter++] = r.lv.val;
            }
          }
          for (int j = 0; counter < max_cnt && j < kAssociativity; ++j) {
            auto &r = g->overflow[j];
            if (r.lv.val != kValueNull && r.key >= from && r.key < to) {
              value_buffer[counter++] = r.lv.val;
            }
          }
        }
      }
    }
    dsm_client_->Read(range_buffer + kLeafPageSize * (i % kParaFetch),
                      leaves[i], kLeafPageSize, true);
    cq_cnt++;
  }

  if (cq_cnt != 0) {
    dsm_client_->PollRdmaCq(cq_cnt);
    for (int k = 0; counter < max_cnt && k < cq_cnt; ++k) {
      auto page = (LeafPage *)(range_buffer + k * kLeafPageSize);
      for (int idx = 0; counter < max_cnt && idx < kNumGroup; ++idx) {
        LeafEntryGroup *g = &page->groups[idx];
        for (int j = 0; counter < max_cnt && j < kAssociativity; ++j) {
          LeafEntry &r = g->front[j];
          if (r.lv.val != kValueNull && r.key >= from && r.key < to) {
            value_buffer[counter++] = r.lv.val;
          }
        }
        for (int j = 0; counter < max_cnt && j < kAssociativity; ++j) {
          auto &r = g->back[j];
          if (r.lv.val != kValueNull && r.key >= from && r.key < to) {
            value_buffer[counter++] = r.lv.val;
          }
        }
        for (int j = 0; counter < max_cnt && j < kAssociativity; ++j) {
          auto &r = g->overflow[j];
          if (r.lv.val != kValueNull && r.key >= from && r.key < to) {
            value_buffer[counter++] = r.lv.val;
          }
        }
      }
    }
  }

  index_cache->thread_status->rcu_exit(dsm_client_->get_my_thread_id());
  return counter;
}

void Tree::del(const Key &k, CoroContext *ctx, int coro_id) {
  assert(dsm_client_->IsRegistered());

  before_operation(ctx, coro_id);
  Key min = kKeyMin;
  Key max = kKeyMax;

  if (enable_cache) {
    GlobalAddress cache_addr, parent_addr;
    auto entry = index_cache->search_from_cache(k, &cache_addr, &parent_addr);
    if (entry) {  // cache hit
      if (leaf_page_del(cache_addr, k, 0, ctx, coro_id, true)) {
        cache_hit[dsm_client_->get_my_thread_id()][0]++;
        return;
      }
      // cache stale, from root,
      index_cache->invalidate(entry, dsm_client_->get_my_thread_id());
    }
    cache_miss[dsm_client_->get_my_thread_id()][0]++;
  }

  auto root = get_root_ptr(ctx);
  SearchResult result;

  GlobalAddress p = root;
  int level_hint = -1;

next:

  if (!page_search(p, level_hint, p.read_gran, min, max, k, result, ctx)) {
    std::cout << "SEARCH WARNING del" << std::endl;
    p = get_root_ptr(ctx);
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
      min = result.next_min;
      level_hint = result.level;
      goto next;
    }

    p = result.next_level;
    level_hint = result.level - 1;
    if (result.level != 1) {
      min = result.next_min;
      max = result.next_max;
      goto next;
    }
  }

  leaf_page_del(p, k, 0, ctx, coro_id);
}

bool Tree::leaf_page_group_search(GlobalAddress page_addr, const Key &k,
                                  SearchResult &result, CoroContext *ctx,
                                  bool from_cache) {
  char *page_buffer =
      (dsm_client_->get_rbuf(ctx ? ctx->coro_id : 0)).get_page_buffer();

  int bucket_id = key_hash_bucket(k);
  int group_id = bucket_id / 2;

  Header *hdr = (Header *)(page_buffer + offsetof(LeafPage, hdr));
  int group_offset =
      offsetof(LeafPage, groups) + sizeof(LeafEntryGroup) * group_id;
  LeafEntryGroup *group = (LeafEntryGroup *)(page_buffer + group_offset);
  int bucket_offset =
      group_offset + (bucket_id % 2 ? kBackOffset : kFrontOffset);
  bool has_header = false;
  int header_offset = offsetof(LeafPage, hdr);

  int read_counter = 0;
re_read:
  if (++read_counter > 10) {
    printf("re-read (leaf_page_group) too many times\n");
    sleep(1);
  }

  if (has_header) {
    dsm_client_->Read(page_buffer + bucket_offset,
                      GADD(page_addr, bucket_offset), kReadBucketSize, false);
    // read header
    dsm_client_->ReadSync(page_buffer + header_offset,
                          GADD(page_addr, header_offset), sizeof(Header), ctx);
  } else {
    dsm_client_->ReadSync(page_buffer + bucket_offset,
                          GADD(page_addr, bucket_offset), kReadBucketSize, ctx);
  }

  result.clear();
  result.is_leaf = true;
  result.level = 0;
  uint8_t actual_version;
  if (!group->check_consistency(!(bucket_id % 2), page_addr.node_version,
                                actual_version)) {
    if (from_cache) {
      return false;
    } else {
      has_header = true;
      page_addr.node_version = actual_version;
      goto re_read;
    }
  }
  if (has_header && k >= hdr->highest) {
    result.sibling = hdr->sibling_ptr;
    result.next_min = hdr->highest;
    return true;
  }

  bool res = group->find(k, result, !(bucket_id % 2));
  if (!res && from_cache) {
    // not found, but maybe from stale cache, check header
    if (!has_header) {
      dsm_client_->ReadSync(page_buffer + header_offset,
                            GADD(page_addr, header_offset), sizeof(Header),
                            ctx);
    }
    if (k >= hdr->highest || k < hdr->lowest) {
      // cache is stale
      return false;
    }
  }
  return true;
}

bool Tree::page_search(GlobalAddress page_addr, int level_hint, int read_gran,
                       Key min, Key max, const Key &k, SearchResult &result,
                       CoroContext *ctx, bool from_cache) {
#ifdef FINE_GRAINED_LEAF_NODE
  if (page_addr != g_root_ptr && level_hint == 0) {
    return leaf_page_group_search(page_addr, k, result, ctx, from_cache);
  }
#endif

#ifndef FINE_GRAINED_INTERNAL_NODE
  read_gran = gran_full;
#endif

  char *page_buffer =
      (dsm_client_->get_rbuf(ctx ? ctx->coro_id : 0)).get_page_buffer();
  Header *hdr = (Header *)(page_buffer + offsetof(LeafPage, hdr));
  bool has_header = false;
  InternalEntry *guard = nullptr;
  size_t start_offset = 0;
  int internal_read_cnt;
  int group_id = -1;

  int read_counter = 0;
re_read:
  if (++read_counter > 10) {
    printf("re-read (page_search) too many times\n");
    sleep(1);
    assert(false);
  }

  result.clear();
  if (read_gran == gran_full) {
    has_header = true;
    dsm_client_->ReadSync(page_buffer, page_addr,
                          std::max(kInternalPageSize, kLeafPageSize), ctx);
    size_t start_offset =
        offsetof(InternalPage, records) - sizeof(InternalEntry);
    internal_read_cnt = kInternalCardinality + 1;
    guard = reinterpret_cast<InternalEntry *>(page_buffer + start_offset);
    // has header
    result.is_leaf = hdr->level == 0;
    result.level = hdr->level;
    if (page_addr == g_root_ptr) {
      if (hdr->is_root == false) {
        // update root ptr
        get_root_ptr(ctx, true);
      }
    }
  } else {
    group_id = get_key_group(k, min, max);
    // read one more entry
    if (read_gran == gran_quarter) {
      start_offset = offsetof(InternalPage, records) +
                     (group_id * kGroupCardinality - 1) * sizeof(InternalEntry);
      internal_read_cnt = kGroupCardinality + 1;
    } else {
      // half
      int begin = (group_id < 2 ? 0 : kGroupCardinality * 2) - 1;
      start_offset =
          offsetof(InternalPage, records) + begin * sizeof(InternalEntry);
      internal_read_cnt = kGroupCardinality * 2 + 1;
    }
    // DEBUG:
    // dsm_client_->ReadSync(page_buffer, page_addr,
    //                       std::max(kInternalPageSize, kLeafPageSize), ctx);
    dsm_client_->ReadSync(page_buffer + start_offset,
                          GADD(page_addr, start_offset),
                          internal_read_cnt * sizeof(InternalEntry), ctx);
    guard = reinterpret_cast<InternalEntry *>(page_buffer + start_offset);
    assert(level_hint != 0);
    result.is_leaf = false;
    result.level = level_hint;
  }

  path_stack[ctx ? ctx->coro_id : 0][result.level] = page_addr;

  if (result.is_leaf) {
    LeafPage *page = (LeafPage *)page_buffer;
    int bucket_id = key_hash_bucket(k);
    LeafEntryGroup *g = &page->groups[bucket_id / 2];
    // check version
    uint8_t actual_version;
    if (!g->check_consistency(!(bucket_id % 2), page_addr.node_version,
                              actual_version)) {
      if (from_cache) {
        return false;
      } else {
        page_addr.node_version = actual_version;
        goto re_read;
      }
    }
    if (has_header) {
      if (k >= hdr->highest) {
        result.sibling = hdr->sibling_ptr;
        result.next_min = hdr->highest;
        return true;
      } else if (k < hdr->lowest) {
        assert(false);
        return false;
      }
    }
    bool res = g->find(k, result, !(bucket_id % 2));
    if (!res && from_cache && !has_header) {
      // check header
      size_t header_offset = offsetof(LeafPage, hdr);
      dsm_client_->ReadSync(page_buffer + header_offset,
                            GADD(page_addr, header_offset), sizeof(Header),
                            ctx);
      if (k >= hdr->highest || k < hdr->lowest) {
        // cache is stale
        return false;
      }
    }
  } else {
    // Internal Page
    assert(!from_cache);
    InternalPage *page = (InternalPage *)page_buffer;
    // check version
    char *end = (char *)(guard + internal_read_cnt);
    uint8_t actual_version;
    if (!page->check_consistency((char *)guard, end, page_addr.node_version,
                                 actual_version)) {
      page_addr.node_version = actual_version;
      read_gran = gran_full;
      goto re_read;
    }
    if (has_header) {
      if (k >= hdr->highest) {
        result.sibling = hdr->sibling_ptr;
        result.next_min = hdr->highest;
        return true;
      } else if (k < hdr->lowest) {
        assert(false);
        return false;
      }
    }

    if (read_gran == gran_full) {
      // maybe is a retry: update group id, gran, guard
      uint8_t actual_gran = hdr->read_gran;
      if (actual_gran != gran_full) {
        group_id = get_key_group(k, hdr->lowest, hdr->highest);
        if (actual_gran == gran_quarter) {
          start_offset =
              offsetof(InternalPage, records) +
              (group_id * kGroupCardinality - 1) * sizeof(InternalEntry);
          internal_read_cnt = kGroupCardinality + 1;
        } else {
          // half
          int begin = (group_id < 2 ? 0 : kGroupCardinality * 2) - 1;
          start_offset =
              offsetof(InternalPage, records) + begin * sizeof(InternalEntry);
          internal_read_cnt = kGroupCardinality * 2 + 1;
        }
        guard = reinterpret_cast<InternalEntry *>(page_buffer + start_offset);
      }
    }

    // Timer timer;
    // timer.begin();
    bool s = internal_page_slice_search(guard, internal_read_cnt, k, result);
    if (!s) {
      goto re_read;
    }
    assert(result.sibling != GlobalAddress::Null() ||
           result.next_level != GlobalAddress::Null());
    if (result.level == 1 && enable_cache) {
      if (read_gran == gran_full) {
        index_cache->add_to_cache(page, dsm_client_->get_my_thread_id());
      } else {
        index_cache->add_sub_node(page_addr, group_id, read_gran, start_offset,
                                  guard,
                                  internal_read_cnt * sizeof(InternalEntry),
                                  min, max, dsm_client_->get_my_thread_id());
      }
    }
    // auto t = timer.end();
    // stat_helper.add(dsm_client_->get_my_thread_id(), lat_internal_search, t);
  }

  return true;
}

void Tree::update_ptr_internal(const Key &k, GlobalAddress v, CoroContext *ctx,
                               int level) {
  GlobalAddress root = get_root_ptr(ctx);
  SearchResult result;

  GlobalAddress p = root;
  int level_hint = -1;
  Key min = kKeyMin;
  Key max = kKeyMax;

next:
  if (!page_search(p, level_hint, p.read_gran, min, max, k, result, ctx)) {
    std::cout << "SEARCH WARNING insert" << std::endl;
    p = get_root_ptr(ctx);
    level_hint = -1;
    sleep(1);
    goto next;
  }

  assert(result.level != 0);
  if (result.sibling != GlobalAddress::Null()) {
    p = result.sibling;
    level_hint = result.level;
    min = result.next_min; // remain the max
    goto next;
  }

  if (result.level >= level + 1) {
    p = result.next_level;
    if (result.level > level + 1) {
      level_hint = result.level - 1;
      min = result.next_min;
      max = result.next_max;
      goto next;
    }
  }

  // internal_page_store(p, k, v, root, level, ctx, coro_id);
  assert(result.level == level + 1 || result.level == level);
#ifdef USE_SX_LOCK
  internal_page_update(p, k, v, level, ctx, true);
#else
  internal_page_update(p, k, v, level, ctx, false);
#endif
}

inline bool Tree::internal_page_slice_search(InternalEntry *entries, int cnt,
                                             const Key k,
                                             SearchResult &result) {
  // find next_min: maxium <= k; next_max: minium > k
  int min_idx = -1, max_idx = -1;
  for (int i = 0; i < cnt; ++i) {
    if (entries[i].ptr != GlobalAddress::Null()) {
      if (entries[i].key <= k) {
        if (min_idx == -1 || entries[i].key > entries[min_idx].key) {
          min_idx = i;
        }
      } else {
        if (max_idx == -1 || entries[i].key < entries[max_idx].key) {
          max_idx = i;
        }
      }
    }
  }
  assert(min_idx != -1);
  result.next_level = entries[min_idx].ptr;
  if (max_idx == -1) {
    // can't know max of next level, so read full page
    result.next_level.read_gran = gran_full;
  } else {
    result.next_min = entries[min_idx].key;
    result.next_max = entries[max_idx].key;
  }

  return true;
}

void Tree::internal_page_update(GlobalAddress page_addr, const Key &k,
                                GlobalAddress value, int level,
                                CoroContext *ctx, bool sx_lock) {
  GlobalAddress lock_addr = get_lock_addr(page_addr);

  auto &rbuf = dsm_client_->get_rbuf(ctx ? ctx->coro_id : 0);
  uint64_t *lock_buffer = rbuf.get_cas_buffer();
  char *page_buffer = rbuf.get_page_buffer();

  lock_and_read(lock_addr, sx_lock, false, lock_buffer, page_addr,
                kInternalPageSize, page_buffer, ctx);

  InternalPage *page = reinterpret_cast<InternalPage *>(page_buffer);
  Header *hdr = &page->hdr;
  assert(hdr->level == level);
  if (k >= hdr->highest) {
    assert(page->hdr.sibling_ptr != GlobalAddress::Null());
    release_lock(lock_addr, lock_buffer, ctx, true, sx_lock);
    internal_page_update(page->hdr.sibling_ptr, k, value, level, ctx, sx_lock);
    return;
  }
  assert(k >= page->hdr.lowest);
  if (k == page->hdr.lowest) {
    assert(page->hdr.leftmost_ptr == value);
    page->hdr.leftmost_ptr = value;
    char *modfiy = (char *)&page->hdr.leftmost_ptr;
    write_and_unlock(modfiy, GADD(page_addr, (modfiy - page_buffer)),
                     sizeof(GlobalAddress), lock_buffer, lock_addr, ctx, true,
                     sx_lock);
    return;
  } else {
    int group_id = get_key_group(k, page->hdr.lowest, page->hdr.highest);
    uint8_t cur_group_gran = page->hdr.read_gran;
    int begin_idx, end_idx;
    if (cur_group_gran == gran_quarter) {
      begin_idx = group_id * kGroupCardinality;
      end_idx = begin_idx + kGroupCardinality;
    } else if (cur_group_gran == gran_half) {
      begin_idx = group_id < 2 ? 0 : kGroupCardinality * 2;
      end_idx = begin_idx + kGroupCardinality * 2;
    } else {
      assert(cur_group_gran == gran_full);
      begin_idx = 0;
      end_idx = kInternalCardinality;
    }

    for (int i = begin_idx; i < end_idx; ++i) {
      if (page->records[i].ptr == value && page->records[i].key == k) {
        page->records[i].ptr = value;
        char *modify = (char *)&page->records[i].ptr;
        write_and_unlock(modify, GADD(page_addr, (modify - page_buffer)),
                         sizeof(GlobalAddress), lock_buffer, lock_addr, ctx,
                         true, sx_lock);
        return;
      }
    }
  }

  assert(false);
  release_lock(lock_addr, lock_buffer, ctx, true, sx_lock);
}

void Tree::internal_page_store_update_left_child(GlobalAddress page_addr,
                                                 const Key &k, GlobalAddress v,
                                                 const Key &left_child,
                                                 GlobalAddress left_child_val,
                                                 GlobalAddress root, int level,
                                                 CoroContext *ctx) {
  GlobalAddress lock_addr = get_lock_addr(page_addr);

  auto &rbuf = dsm_client_->get_rbuf(ctx ? ctx->coro_id : 0);
  uint64_t *lock_buffer = rbuf.get_cas_buffer();
  char *page_buffer = rbuf.get_page_buffer();

  lock_and_read(lock_addr, false, false, lock_buffer, page_addr,
                kInternalPageSize, page_buffer, ctx);

  InternalPage *page = (InternalPage *)page_buffer;

  assert(page->hdr.level == level);
  // auto cnt = page->hdr.cnt;

  if (left_child_val != GlobalAddress::Null() &&
      left_child >= page->hdr.highest) {
    assert(page->hdr.sibling_ptr != GlobalAddress::Null());
    release_lock(lock_addr, lock_buffer, ctx, true, false);
    internal_page_store_update_left_child(page->hdr.sibling_ptr, k, v,
                                          left_child, left_child_val, root,
                                          level, ctx);
    return;
  }
  if (k >= page->hdr.highest) {
    // left child in current node, new sibling leaf in sibling node
    if (left_child_val != GlobalAddress::Null()) {
      GlobalAddress *modify = nullptr;
      if (left_child == page->hdr.lowest) {
        assert(page->hdr.leftmost_ptr == left_child_val);
        page->hdr.leftmost_ptr = left_child_val;
        modify = &page->hdr.leftmost_ptr;
      } else {
        int idx = page->find_records_not_null(left_child);
        if (idx != -1) {
          page->records[idx].ptr = left_child_val;
          modify = &page->records[idx].ptr;
        }
      }
      if (modify) {
        dsm_client_->Write((char *)modify,
                           GADD(page_addr, ((char *)modify - page_buffer)),
                           sizeof(GlobalAddress), false);
      }
    }
    release_lock(lock_addr, lock_buffer, ctx, true, false);
    internal_page_store_update_left_child(page->hdr.sibling_ptr, k, v, kKeyMin,
                                          GlobalAddress::Null(), root, level,
                                          ctx);
    return;
  }
  assert(k >= page->hdr.lowest);

  int group_id = get_key_group(k, page->hdr.lowest, page->hdr.highest);
  uint8_t cur_gran = page->hdr.read_gran;
  int begin_idx, max_cnt;

  InternalEntry *left_update_addr = nullptr;
  InternalEntry *insert_addr = nullptr;

  int new_gran = cur_gran;
  for (; new_gran >= gran_full; --new_gran) {
    // update k, not found key, can't be the last one of previous group
    if (new_gran == gran_quarter) {
      begin_idx = kGroupCardinality * group_id;
      max_cnt = kGroupCardinality;
    } else if (new_gran == gran_half) {
      begin_idx = group_id < 2 ? 0 : kGroupCardinality * 2;
      max_cnt = kGroupCardinality * 2;
    } else {
      begin_idx = 0;
      max_cnt = kInternalCardinality;
    }

    if (new_gran == cur_gran && left_child_val != GlobalAddress::Null()) {
      // find left_child
      if (left_child == page->hdr.lowest) {
        assert(page->hdr.leftmost_ptr == left_child_val);
        left_update_addr = (InternalEntry *)&page->hdr.leftmost_ptr;
      } else {
        int left_idx = page->find_records_not_null(left_child);
        if (left_idx != -1) {
          assert(page->records[left_idx].ptr == left_child_val);
          left_update_addr = &page->records[left_idx];
        }
      }
    }

    int empty_idx = page->find_empty(begin_idx, max_cnt);
    if (empty_idx != -1) {
      insert_addr = page->records + empty_idx;
      break;
    }
  }

  assert(left_child_val == GlobalAddress::Null() ||
         left_update_addr != nullptr);

  if (insert_addr) {  // has empty slot
    uint64_t *mask_buffer = rbuf.get_cas_buffer();
    mask_buffer[0] = mask_buffer[1] = ~0ull;

    int last_idx = begin_idx + max_cnt - 1;  // the largest in group
    if (k > page->records[last_idx].key) {
      // swap k to current rightmost
      uint64_t *old_buffer = rbuf.get_cas_buffer();
      memcpy(old_buffer, insert_addr, sizeof(InternalEntry));
      insert_addr->key = page->records[last_idx].key;
      if (left_update_addr && left_update_addr == &page->records[last_idx]) {
        // if current rightmost is the left_child
        assert(page->records[last_idx].key == left_child);
        insert_addr->ptr = left_child_val;
        left_update_addr = nullptr; // already update left_child
      } else {
        insert_addr->ptr = page->records[last_idx].ptr;
      }
      uint64_t *cas_ret_buffer = rbuf.get_cas_buffer();
      // lock-based, must cas succeed
      dsm_client_->CasMask(GADD(page_addr, ((char *)insert_addr - page_buffer)),
                           4, (uint64_t)old_buffer, (uint64_t)insert_addr,
                           cas_ret_buffer, (uint64_t)mask_buffer, false);
      insert_addr = page->records + last_idx;  // update insert_addr
    }

    uint64_t *old_buffer = rbuf.get_cas_buffer();
    memcpy(old_buffer, insert_addr, sizeof(InternalEntry));
    insert_addr->key = k;
    insert_addr->ptr = v;
    uint64_t *cas_ret_buffer = rbuf.get_cas_buffer();
    dsm_client_->CasMask(GADD(page_addr, ((char *)insert_addr - page_buffer)),
                         4, (uint64_t)old_buffer, (uint64_t)insert_addr,
                         cas_ret_buffer, (uint64_t)mask_buffer, false);

    if (left_update_addr) {
      uint64_t *cas_ret_buffer = rbuf.get_cas_buffer();
      uint64_t old = left_update_addr->ptr.raw;
      left_update_addr->ptr = left_child_val;
      dsm_client_->Cas(
          GADD(page_addr, ((char *)&(left_update_addr->ptr) - page_buffer)),
          old, left_update_addr->ptr.raw, cas_ret_buffer, false);
    }

    if (new_gran != cur_gran) {
      // update header and parent ptr;
      page->hdr.read_gran = new_gran;
      page_addr.read_gran = new_gran;
      write_and_unlock(page_buffer + offsetof(InternalPage, hdr),
                       GADD(page_addr, offsetof(InternalPage, hdr)),
                       sizeof(Header), lock_buffer, lock_addr, ctx, true,
                       false);

      GlobalAddress up_level = path_stack[ctx ? ctx->coro_id : 0][level + 1];
      if (up_level != GlobalAddress::Null()) {
        internal_page_update(up_level, page->hdr.lowest, page_addr, level + 1,
                             ctx, true);
      } else {
        update_ptr_internal(page->hdr.lowest, page_addr, ctx, level + 1);
      }
    } else {
      release_lock(lock_addr, lock_buffer, ctx, true, false);
    }
    if (level == 1 && enable_cache) {
      index_cache->add_to_cache(page, dsm_client_->get_my_thread_id());
    }
  } else {
    // need split and insert
    GlobalAddress sibling_addr = dsm_client_->Alloc(kInternalPageSize);
    char *sibling_buf = rbuf.get_sibling_buffer();
    InternalPage *sibling = new (sibling_buf) InternalPage(page->hdr.level);
    sibling->hdr.my_addr = sibling_addr;

    if (left_update_addr) {
      left_update_addr->ptr = left_child_val;
    }
    std::vector<InternalEntry> tmp_records(
        page->records, page->records + kInternalCardinality);
    tmp_records.push_back({.ptr = v, .key = k});
    std::sort(tmp_records.begin(), tmp_records.end());
    int m = kInternalCardinality / 2;
    Key split_key = tmp_records[m].key;
    GlobalAddress split_val = tmp_records[m].ptr;
    int sib_gran = sibling->rearrange_records(tmp_records.data() + m + 1,
                                              kInternalCardinality - m,
                                              split_key, page->hdr.highest);
    sibling->hdr.sibling_ptr = page->hdr.sibling_ptr;
    sibling->hdr.leftmost_ptr = split_val;
    sibling->hdr.read_gran = sib_gran;
    sibling_addr.read_gran = sib_gran;
    sibling_addr.node_version = sibling->hdr.version;

    page_addr.read_gran = page->rearrange_records(tmp_records.data(), m,
                                                  page->hdr.lowest, split_key);
    page_addr.node_version = page->update_version();
    page->hdr.sibling_ptr = sibling_addr;

    if (root == page_addr) {
      page->hdr.is_root = false;
    }

    if (sibling_addr.nodeID == page_addr.nodeID) {
      dsm_client_->Write(sibling_buf, sibling_addr, kInternalPageSize, false);
    } else {
      dsm_client_->WriteSync(sibling_buf, sibling_addr, kInternalPageSize, ctx);
    }

    write_and_unlock(page_buffer, page_addr, kInternalPageSize, lock_buffer,
                     lock_addr, ctx, true, false);
    if (root == page_addr) {
      if (update_new_root(page_addr, split_key, sibling_addr, level + 1, root,
                          ctx)) {
        return;
      }
    }

    GlobalAddress up_level = path_stack[ctx ? ctx->coro_id : 0][level + 1];
    if (up_level != GlobalAddress::Null()) {
      internal_page_store_update_left_child(up_level, split_key, sibling_addr,
                                            page->hdr.lowest, page_addr, root,
                                            level + 1, ctx);
    } else {
      insert_internal_update_left_child(
          split_key, sibling_addr, page->hdr.lowest, page_addr, ctx, level + 1);
    }
    if (level == 1 && enable_cache) {
      int my_thread_id = dsm_client_->get_my_thread_id();
      index_cache->add_to_cache(page, my_thread_id);
      index_cache->add_to_cache(sibling, my_thread_id);
      index_cache->invalidate(page->hdr.lowest, sibling->hdr.highest,
                              my_thread_id);
    }
  }
}

bool Tree::leaf_page_store(GlobalAddress page_addr, const Key &k,
                           const Value &v, GlobalAddress root, int level,
                           CoroContext *ctx, bool from_cache, bool share_lock) {
  GlobalAddress lock_addr = get_lock_addr(page_addr);

  auto &rbuf = dsm_client_->get_rbuf(ctx ? ctx->coro_id : 0);
  uint64_t *lock_buffer = rbuf.get_cas_buffer();
  uint64_t *cas_ret_buffer = rbuf.get_cas_buffer();
  char *page_buffer = rbuf.get_page_buffer();

  [[maybe_unused]] bool upgrade_from_s = false;
  int bucket_id = key_hash_bucket(k);
  int group_id = bucket_id / 2;
  Header *header = (Header *)(page_buffer + offsetof(LeafPage, hdr));
  LeafEntry *update_addr = nullptr;
  LeafEntry *insert_addr = nullptr;
  int group_offset =
      offsetof(LeafPage, groups) + sizeof(LeafEntryGroup) * group_id;
  LeafEntryGroup *group = (LeafEntryGroup *)(page_buffer + group_offset);
  // try upsert hash group
#ifdef FINE_GRAINED_LEAF_NODE
  // 1. lock, read, and check consistency
  size_t bucket_offset =
      group_offset + (bucket_id % 2 ? kBackOffset : kFrontOffset);
  lock_and_read(lock_addr, share_lock, false, lock_buffer,
                GADD(page_addr, bucket_offset), kReadBucketSize,
                page_buffer + bucket_offset, ctx);

  uint8_t actual_version;
  if (!group->check_consistency(!(bucket_id % 2), page_addr.node_version,
                                actual_version)) {
    if (from_cache) {
      release_lock(lock_addr, lock_buffer, ctx, true, share_lock);
      return false;
    } else {
      // lock-based, no need to re-read, just read header to check sibling
      dsm_client_->ReadSync(page_buffer + offsetof(LeafPage, hdr),
                            GADD(page_addr, offsetof(LeafPage, hdr)),
                            sizeof(Header), ctx);
      if (k >= header->highest) {
        release_lock(lock_addr, lock_buffer, ctx, true, share_lock);
        return leaf_page_store(header->sibling_ptr, k, v, root, level, ctx,
                               false, share_lock);
      }
    }
  }

  // 2. try update
  // 2.1 check main bucket
  int retry_cnt = 0;
retry_insert:
  update_addr = nullptr;
  insert_addr = nullptr;
  group->find(k, !(bucket_id % 2), &update_addr, &insert_addr);
  if (update_addr) {
    LeafValue cas_val(update_addr->lv.cl_ver, v);
    cas_and_unlock(GADD(page_addr, ((char *)(&update_addr->lv) - page_buffer)),
                   3, cas_ret_buffer, update_addr->lv.raw, cas_val.raw, ~0ull,
                   lock_addr, lock_buffer, share_lock, ctx, false);
    return true;
  } else if (insert_addr) {
    // 3. try insert via CAS
    uint64_t *swap_buffer = rbuf.get_cas_buffer();
    LeafEntry *swap_entry = (LeafEntry *)swap_buffer;
    swap_entry->key = k;
    swap_entry->lv.cl_ver = insert_addr->lv.cl_ver;
    swap_entry->lv.val = v;
    uint64_t *mask_buffer = rbuf.get_cas_buffer();
    mask_buffer[0] = mask_buffer[1] = ~0ull;
    bool cas_ok = dsm_client_->CasMaskSync(
        GADD(page_addr, ((char *)insert_addr - page_buffer)), 4,
        (uint64_t)insert_addr, (uint64_t)swap_buffer, cas_ret_buffer,
        (uint64_t)mask_buffer, ctx);
    // cas succeed or same key inserted by other thread
    if (cas_ok || (__bswap_64(cas_ret_buffer[0]) == k)) {
      release_lock(lock_addr, lock_buffer, ctx, true, share_lock);
      return true;
    }
    // 3.1 retry insert
    // big-endian for 16-byte CAS
    insert_addr->key = __bswap_64(cas_ret_buffer[0]);
    insert_addr->lv.raw = __bswap_64(cas_ret_buffer[1]);
    if (++retry_cnt > 10) {
      printf("retry insert %d times\n", retry_cnt);
      assert(false);
    }
    goto retry_insert;
  }

  // 4. prepare to split
#ifdef USE_SX_LOCK
  // upgrade to x lock
  upgrade_from_s = share_lock;
  share_lock = false;
#else
  // already hold lock
  dsm_client_->ReadSync(page_buffer, page_addr, kLeafPageSize, ctx);
#endif

#endif  // FINE_GRAINED_LEAF_NODE

#if defined(USE_SX_LOCK) || !defined(FINE_GRAINED_LEAF_NODE)
#ifdef USE_SX_LOCK
retry_with_xlock:
#endif
  // not holding lock, or only share lock
  lock_and_read(lock_addr, share_lock, upgrade_from_s, lock_buffer, page_addr,
                kLeafPageSize, page_buffer, ctx);
#endif
  LeafPage *page = (LeafPage *)page_buffer;

  assert(header->level == level);

  if (from_cache &&
      (k < header->lowest || k >= header->highest ||
       page_addr.node_version != header->version)) {  // cache is stale
    release_lock(lock_addr, lock_buffer, ctx, true, share_lock);
    return false;
  }

  if (k >= header->highest) {
    // note that retry may also get here
    assert(header->sibling_ptr != GlobalAddress::Null());
    release_lock(lock_addr, lock_buffer, ctx, true, share_lock);
    leaf_page_store(header->sibling_ptr, k, v, root, level, ctx, from_cache,
                    share_lock);
    return true;
  }
  assert(k >= header->lowest);

  if (header->version != page_addr.node_version) {
    page_addr.node_version = header->version;
  }

  // maybe split by others? check again
  // hash-based
retry_insert_2:
  update_addr = nullptr;
  insert_addr = nullptr;
  group->find(k, !(bucket_id % 2), &update_addr, &insert_addr);

  if (update_addr) {
    LeafValue cas_val(update_addr->lv.cl_ver, v);
    cas_and_unlock(GADD(page_addr, ((char *)&(update_addr->lv) - page_buffer)),
                   3, cas_ret_buffer, update_addr->lv.raw, cas_val.raw, ~0ull,
                   lock_addr, lock_buffer, share_lock, ctx, false);
    return true;
  } else if (insert_addr) {
    uint64_t *swap_buffer = rbuf.get_cas_buffer();
    LeafEntry *swap_entry = (LeafEntry *)swap_buffer;
    swap_entry->key = k;
    swap_entry->lv.cl_ver = insert_addr->lv.cl_ver;
    swap_entry->lv.val = v;
    uint64_t *mask_buffer = rbuf.get_cas_buffer();
    mask_buffer[0] = mask_buffer[1] = ~0ull;
    bool cas_ok = dsm_client_->CasMaskSync(
        GADD(page_addr, ((char *)insert_addr - page_buffer)), 4,
        (uint64_t)insert_addr, (uint64_t)swap_buffer, cas_ret_buffer,
        (uint64_t)mask_buffer, ctx);
    // cas succeed or same key inserted by other thread
    if (cas_ok || (__bswap_64(cas_ret_buffer[0]) == k)) {
      release_lock(lock_addr, lock_buffer, ctx, true, share_lock);
      return true;
    }
    insert_addr->key = __bswap_64(cas_ret_buffer[0]);
    insert_addr->lv.raw = __bswap_64(cas_ret_buffer[1]);
    goto retry_insert_2;
  }

  // should hold x lock
#ifdef USE_SX_LOCK
  if (share_lock) {
    upgrade_from_s = true;
    share_lock = false;
    goto retry_with_xlock;
  }
#endif

  // split
  GlobalAddress sibling_addr;
  sibling_addr = dsm_client_->Alloc(kLeafPageSize);
  char *sibling_buf = rbuf.get_sibling_buffer();
  LeafPage *sibling = new (sibling_buf) LeafPage(page->hdr.level);

  LeafKVEntry tmp_records[kLeafCardinality];
  int cnt = 0;
  for (int i = 0; i < kNumGroup; ++i) {
    LeafEntryGroup *g = &page->groups[i];
    for (int j = 0; j < kAssociativity; ++j) {
      if (g->front[j].lv.val != kValueNull) {
        tmp_records[cnt++] = g->front[j];
      }
      if (g->back[j].lv.val != kValueNull) {
        tmp_records[cnt++] = g->back[j];
      }
    }
    for (int j = 0; j < kAssociativity; ++j) {
      if (g->overflow[j].lv.val != kValueNull) {
        tmp_records[cnt++] = g->overflow[j];
      }
    }
  }
  std::sort(tmp_records, tmp_records + cnt);

  int m = cnt / 2;
  Key split_key = tmp_records[m].key;
  assert(split_key > page->hdr.lowest);
  assert(split_key < page->hdr.highest);

  memset(reinterpret_cast<void *>(page->groups), 0, sizeof(page->groups));
  for (int i = 0; i < m; ++i) {
    int bucket_id = key_hash_bucket(tmp_records[i].key);
    page->groups[bucket_id / 2].insert_for_split(
        tmp_records[i].key, tmp_records[i].val, !(bucket_id % 2));
  }
  for (int i = m; i < cnt; ++i) {
    int bucket_id = key_hash_bucket(tmp_records[i].key);
    sibling->groups[bucket_id / 2].insert_for_split(
        tmp_records[i].key, tmp_records[i].val, !(bucket_id % 2));
  }

  sibling->hdr.lowest = split_key;
  sibling->hdr.highest = page->hdr.highest;
  page->hdr.highest = split_key;

  page_addr.node_version = page->update_version();
  sibling_addr.node_version = sibling->hdr.version;

  // link
  sibling->hdr.sibling_ptr = page->hdr.sibling_ptr;
  page->hdr.sibling_ptr = sibling_addr;

  // insert k
  bool res;
  if (k < split_key) {
    int bucket_id = key_hash_bucket(k);
    res = page->groups[bucket_id / 2].insert_for_split(k, v, !(bucket_id % 2));
  } else {
    int bucket_id = key_hash_bucket(k);
    res =
        sibling->groups[bucket_id / 2].insert_for_split(k, v, !(bucket_id % 2));
  }

  if (sibling_addr.nodeID == page_addr.nodeID) {
    dsm_client_->Write(sibling_buf, sibling_addr, kLeafPageSize, false);
  } else {
    dsm_client_->WriteSync(sibling_buf, sibling_addr, kLeafPageSize, ctx);
  }

  if (root == page_addr) {
    page->hdr.is_root = false;
  }

  // async since we need to insert split_key in upper layer
  write_and_unlock(page_buffer, page_addr, kLeafPageSize, lock_buffer,
                   lock_addr, ctx, true, false);

  if (root == page_addr) {  // update root
    if (update_new_root(page_addr, split_key, sibling_addr, level + 1, root,
                        ctx)) {
      return res;
    }
  }

  GlobalAddress up_level = path_stack[ctx ? ctx->coro_id : 0][level + 1];

  if (up_level != GlobalAddress::Null()) {
    internal_page_store_update_left_child(up_level, split_key, sibling_addr,
                                          page->hdr.lowest, page_addr, root,
                                          level + 1, ctx);
  } else {
    assert(false);
    insert_internal_update_left_child(split_key, sibling_addr, page->hdr.lowest,
                                      page_addr, ctx, level + 1);
  }

  return res;
}

bool Tree::leaf_page_del(GlobalAddress page_addr, const Key &k, int level,
                         CoroContext *ctx, int coro_id, bool from_cache) {
  GlobalAddress lock_addr = get_lock_addr(page_addr);

  auto &rbuf = dsm_client_->get_rbuf(coro_id);
  uint64_t *cas_buffer = rbuf.get_cas_buffer();
  auto page_buffer = rbuf.get_page_buffer();

  lock_and_read(lock_addr, false, false, cas_buffer, page_addr, kLeafPageSize,
                page_buffer, ctx);

  auto page = (LeafPage *)page_buffer;

  assert(page->hdr.level == level);

  if (from_cache &&
      (k < page->hdr.lowest || k >= page->hdr.highest)) {  // cache is stale
    release_lock(lock_addr, cas_buffer, ctx, true, false);
    return false;
  }

  if (k >= page->hdr.highest) {
    release_lock(lock_addr, cas_buffer, ctx, true, false);
    assert(page->hdr.sibling_ptr != GlobalAddress::Null());
    this->leaf_page_del(page->hdr.sibling_ptr, k, level, ctx, coro_id);
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
        p->lv.val = kValueNull;
        update_addr = p;
        break;
      }
    }
  } else {
    // front
    for (int i = 0; i < kAssociativity; ++i) {
      LeafEntry *p = &g->front[i];
      if (p->key == k) {
        p->lv.val = kValueNull;
        update_addr = p;
        break;
      }
    }
  }

  // overflow
  if (update_addr == nullptr) {
    for (int i = 0; i < kAssociativity; ++i) {
      LeafEntry *p = &g->overflow[i];
      if (p->key == k) {
        p->lv.val = kValueNull;
        update_addr = p;
        break;
      }
    }
  }

  if (update_addr) {
    write_and_unlock((char *)update_addr,
                     GADD(page_addr, ((char *)update_addr - (char *)page)),
                     sizeof(LeafEntry), cas_buffer, lock_addr, ctx, false,
                     false);
  } else {
    this->unlock_addr(lock_addr, cas_buffer, ctx, false);
  }
  return true;
}

void Tree::run_coroutine(CoroFunc func, int id, int coro_cnt, bool lock_bench,
                         uint64_t total_ops) {
  using namespace std::placeholders;
  coro_ops_total = total_ops;
  coro_ops_cnt_start = 0;
  coro_ops_cnt_finish = 0;

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
  // auto thread_id = dsm_client_->get_my_thread_id();

  // while (true) {
  while (coro_ops_cnt_start < coro_ops_total) {
    auto r = gen->next();

    // coro_timer.begin();
    ++coro_ops_cnt_start;
    if (lock_bench) {
      this->lock_bench(r.k, &ctx, coro_id);
    } else {
      if (r.is_search) {
        Value v;
        this->search(r.k, v, &ctx);
      } else {
        this->insert(r.k, r.v, &ctx);
      }
    }
    // auto t = coro_timer.end();
    // auto us_10 = t / 100;
    // if (us_10 >= LATENCY_WINDOWS) {
    //   us_10 = LATENCY_WINDOWS - 1;
    // }
    // latency[thread_id][us_10]++;
    // stat_helper.add(thread_id, lat_op, t);
    ++coro_ops_cnt_finish;
  }
  // printf("thread %d coro_id %d start %lu finish %lu\n",
  //        dsm_client_->get_my_thread_id(), coro_id, coro_ops_cnt_start,
  //        coro_ops_cnt_finish);
  // fflush(stdout);
  yield(master);
}

void Tree::coro_master(CoroYield &yield, int coro_cnt) {
  for (int i = 0; i < coro_cnt; ++i) {
    yield(worker[i]);
  }

  // while (true) {
  while (coro_ops_cnt_finish < coro_ops_total) {
    uint64_t next_coro_id;

    if (dsm_client_->PollRdmaCqOnce(next_coro_id)) {
      yield(worker[next_coro_id]);
    }

    // if (!hot_wait_queue.empty()) {
    //   next_coro_id = hot_wait_queue.front();
    //   hot_wait_queue.pop();
    //   yield(worker[next_coro_id]);
    // }
  }
  // printf("thread %d master start %lu finish %lu\n",
  //        dsm_client_->get_my_thread_id(), coro_ops_cnt_start,
  //        coro_ops_cnt_finish);
  // fflush(stdout);
}

// // Local Locks
// inline bool Tree::acquire_local_lock(GlobalAddress lock_addr, CoroContext *ctx,
//                                      int coro_id) {
//   auto &node = local_locks[lock_addr.nodeID][lock_addr.offset / 8];

//   uint64_t lock_val = node.ticket_lock.fetch_add(1);

//   uint32_t ticket = lock_val << 32 >> 32;
//   uint32_t current = lock_val >> 32;

//   while (ticket != current) {  // lock failed

//     if (ctx != nullptr) {
//       hot_wait_queue.push(coro_id);
//       (*ctx->yield)(*ctx->master);
//     }

//     current = node.ticket_lock.load(std::memory_order_relaxed) >> 32;
//   }

//   node.hand_time++;

//   return node.hand_over;
// }

// inline bool Tree::can_hand_over(GlobalAddress lock_addr) {
//   auto &node = local_locks[lock_addr.nodeID][lock_addr.offset / 8];
//   uint64_t lock_val = node.ticket_lock.load(std::memory_order_relaxed);

//   uint32_t ticket = lock_val << 32 >> 32;
//   uint32_t current = lock_val >> 32;

//   if (ticket <= current + 1) {  // no pending locks
//     node.hand_over = false;
//   } else {
//     node.hand_over = node.hand_time < define::kMaxHandOverTime;
//   }
//   if (!node.hand_over) {
//     node.hand_time = 0;
//   }

//   return node.hand_over;
// }

// inline void Tree::releases_local_lock(GlobalAddress lock_addr) {
//   auto &node = local_locks[lock_addr.nodeID][lock_addr.offset / 8];

//   node.ticket_lock.fetch_add((1ull << 32));
// }

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
