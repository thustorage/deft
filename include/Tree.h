#pragma once

// #include "DSM.h"
#include "dsm_client.h"
#include <atomic>
#include <city.h>
#include <functional>
#include <iostream>

class IndexCache;
extern uint64_t cache_miss[MAX_APP_THREAD][8];
extern uint64_t cache_hit[MAX_APP_THREAD][8];
extern uint64_t latency[MAX_APP_THREAD][LATENCY_WINDOWS];

enum latency_enum {
  lat_lock,
  lat_read_page,
  lat_write_page,
  lat_internal_search,
  lat_cache_search,
  lat_op,
  lat_end
};

struct alignas(64) StatHelper {
  uint64_t latency_[MAX_APP_THREAD][8];
  uint64_t counter_[MAX_APP_THREAD][8];

  StatHelper() {
    memset(this, 0, sizeof(StatHelper));
  }

  void add(int th_id, int enum_id, uint64_t stat) {
    latency_[th_id][enum_id] += stat;
    ++counter_[th_id][enum_id];
  }
};

extern StatHelper stat_helper;

struct LocalLockNode {
  std::atomic<uint64_t> ticket_lock;
  bool hand_over;
  uint8_t hand_time;
};

struct Request {
  bool is_search;
  Key k;
  Value v;
};

class RequstGen {
public:
  RequstGen() = default;
  virtual Request next() { return Request{}; }
};

using CoroFunc = std::function<RequstGen *(int, DSMClient *, int)>;

struct SearchResult {
  bool is_leaf;
  uint8_t level;
  GlobalAddress sibling;
  GlobalAddress next_level;
  Value val;
  Key other_in_group = 0;
  Key min;
  Key max;

  SearchResult() = default;
  void clear() {
    memset(reinterpret_cast<void *>(this), 0, sizeof(SearchResult));
  }
};

struct alignas(64) Header {
// uint16_t level;
  uint8_t padding[28];
  uint8_t node_version;
  // int8_t cnt = 0;
  uint8_t level : 7;
  uint8_t is_root : 1;
  uint16_t index_cache_freq;

  Key highest = kKeyMax;
  GlobalAddress sibling_ptr = GlobalAddress::Null();
  Key lowest = kKeyMin;
  GlobalAddress leftmost_ptr = GlobalAddress::Null();

  Header() : node_version(0), level(0), is_root(false), index_cache_freq(0) {}

  void debug() const {
    std::cout << "leftmost=" << leftmost_ptr << ", "
              << "sibling=" << sibling_ptr << ", "
              << "level=" << (int)level << ","
              // << "cnt=" << cnt << ","
              << "range=[" << lowest << " - " << highest << "]";
  }
};
static_assert(sizeof(Header) == 64);


struct InternalEntry {
  Key key = 0;
  GlobalAddress ptr;
};

struct LeafEntry {
  // uint8_t f_version : 4;
  Key key = 0;
  Value value = 0;
  // uint8_t r_version : 4;
  bool operator<(const LeafEntry &rhs) const { return key < rhs.key; }
};

constexpr int kInternalCardinality =
    (kInternalPageSize - sizeof(Header)) / sizeof(InternalEntry);
constexpr int kGroupCardinality = kInternalCardinality / 4;

constexpr int kAssociativity = 4;
constexpr int kNumBucket = 10;
constexpr int kGroupSize = kAssociativity * 3;
constexpr int kLeafCardinality = kNumBucket * kAssociativity / 2 * 3;

static inline int key_hash_bucket(const Key &k, uint64_t hash_offset) {
  return (k + hash_offset) % kNumBucket;
}

static inline double get_quarter(Key min, Key max) { return (max - min) / 4.0; }

static inline int get_key_group(const Key k, Key min, Key max) {
  double quarter = get_quarter(min, max);
  return (k - min) / quarter;
}

static inline uint64_t get_lock_index(GlobalAddress addr) {
  GlobalAddress a;
  a.offset = addr.offset;
  a.nodeID = addr.nodeID;
  return CityHash64((char *)&a, sizeof(a)) % define::kNumOfLock;
}

class InternalPage {
 private:
  Header hdr;
  InternalEntry records[kInternalCardinality];

  // uint8_t padding[3];

  friend class Tree;
  friend class IndexCache;

 public:
  // this is called when tree grows
  // only for new root node
  InternalPage(GlobalAddress left, const Key &key, GlobalAddress right,
               uint32_t level = 0) {
    assert(left.group_gran == gran_full);
    hdr.leftmost_ptr = left;
    hdr.level = level;
    records[kInternalCardinality - 1].key = key;
    records[kInternalCardinality - 1].ptr = right;
    // records[1].ptr = GlobalAddress::Null();

    // hdr.cnt = 1;
  }

  InternalPage(uint32_t level = 0) {
    hdr.level = level;
    // records[0].ptr = GlobalAddress::Null();
  }

  void set_consistent() {}

  bool check_consistent() const { return true; }

  int try_update(const Key k, GlobalAddress v) {
    // if (k == hdr.lowest) {
    //   assert(hdr.leftmost_ptr == v);
    //   hdr.leftmost_ptr = v;
    //   return true;
    // }

    int group_id = get_key_group(k, hdr.lowest, hdr.highest);
    uint8_t cur_group_gran =
        records[kGroupCardinality * (group_id + 1) - 1].ptr.group_gran;
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

    int i = 0;
    for (; i < max_cnt; ++i) {
      int idx = end - 1 - i;
      if (records[idx].ptr == GlobalAddress::Null()) {
        break;
      } else if (k == records[idx].key) {
        records[idx].ptr = v;
        return idx;
      } else if (k > records[idx].key) {
        break;
      }
    }
    return -1;
  }

  bool gran_quarter_to_half_and_insert(int cur_group_id, int cur_insert_index,
                                       const Key k, GlobalAddress v) {
    // cur_group is full
    int cur_end = (cur_group_id + 1) * kGroupCardinality;
    assert(cur_insert_index < cur_end &&
           cur_insert_index >= cur_end - kGroupCardinality - 1);
    if (cur_group_id % 2 == 0) {
      // left, shift to right
      // cur_left, insert_index, cur_right, right group
      int i = cur_end;
      for (; i < cur_end + kGroupCardinality; ++i) {
        if (records[i].ptr != GlobalAddress::Null()) {
          break;
        }
      }
      if (i == cur_end) {
        // neighbour is full
        return false;
      }
      // [insert_index + 1, end - 1] -> [x, i - 1]
      int diff = i - cur_end;
      for (int j = cur_end - 1; j > cur_insert_index; --j) {
        records[j + diff] = records[j];
      }
      records[cur_insert_index + diff].key = k;
      records[cur_insert_index + diff].ptr = v;
      // [end - group_cnt, insert_index] shift to right by (diff - 1)
      for (int j = cur_insert_index; j >= cur_end - kGroupCardinality; --j) {
        records[j + diff - 1] = records[j];
      }
      memset(reinterpret_cast<void *>(records + cur_end - kGroupCardinality), 0,
             sizeof(InternalEntry) * (diff - 1));
    } else {
      // right
      // left group, cur left, insert, cur right
      int left_margin = cur_end - kGroupCardinality * 2;
      if (records[left_margin].ptr != GlobalAddress::Null()) {
        // neighbour is full
        return false;
      }
      // [left_margin + 1, insert_index], shift to left by 1
      for (int j = left_margin + 1; j <= cur_insert_index; ++j) {
        records[j - 1] = records[j];
      }
      records[cur_insert_index].key = k;
      records[cur_insert_index].ptr = v;
    }
    // ++hdr.cnt;
    return true;
  }

  void gran_quarter_to_half(bool left_half) {
    int mid = left_half ? kGroupCardinality : kGroupCardinality * 3;

    int i = mid;
    for (; i < mid + kGroupCardinality; ++i) {
      if (records[i].ptr != GlobalAddress::Null()) {
        break;
      }
    }
    if (i == mid) {
      // already in half, don't forget to update gran in ptr
      return;
    } else {
      int diff = i - mid;
      // shift right
      for (int j = mid - 1; j >= mid - kGroupCardinality; --j) {
        records[j + diff] = records[j];
      }
      memset(reinterpret_cast<void *>(records + mid - kGroupCardinality), 0,
             sizeof(InternalEntry) * diff);
    }
  }

  bool gran_half_to_full_and_insert(int insert_index, const Key k,
                                    GlobalAddress v) {
    if (insert_index < kGroupCardinality * 2) {
      // cur in left half
      // cur_left, insert, cur_right, right half
      int cur_end = kGroupCardinality * 2;
      int i = cur_end;
      for (; i < kInternalCardinality; ++i) {
        if (records[i].ptr != GlobalAddress::Null()) {
          break;
        }
      }
      if (i == cur_end) {
        // neighbour is full
        return false;
      }
      // [insert_index + 1, end - 1] -> [x, i - 1]
      int diff = i - cur_end;
      for (int j = cur_end - 1; j > insert_index; --j) {
        records[j + diff] = records[j];
      }
      records[insert_index + diff].key = k;
      records[insert_index + diff].ptr = v;
      for (int j = insert_index; j >= 0; --j) {
        records[j + diff - 1] = records[j];
      }
      memset(reinterpret_cast<void *>(records), 0,
             sizeof(InternalEntry) * (diff - 1));
    } else {
      // right
      // left half, cur left, insert, cur right
      if (records[0].ptr != GlobalAddress::Null()) {
        // neighbour is full
        return false;
      }
      // [1, insert_index], shift left by 1
      for (int i = 1; i <= insert_index; ++i) {
        records[i - 1] = records[i];
      }
      records[insert_index].key = k;
      records[insert_index].ptr = v;
    }
    // ++hdr.cnt;
    return true;
  }

  int rearrange_records(InternalEntry *entries, size_t cnt, Key min, Key max) {
    assert(cnt == kInternalCardinality / 2);
    hdr.lowest = min;
    hdr.highest = max;
    // hdr.cnt = cnt;
    memset(reinterpret_cast<void *>(records), 0,
           sizeof(InternalEntry) * kInternalCardinality);

    double quarter = get_quarter(min, max);
    int group_cnt[4];
    InternalEntry *p = entries;
    InternalEntry *end = entries + cnt;
    // count each group's number
    for (int i = 0; i < 3; i++) {
      double cur_max = min + (i + 1) * quarter;
      group_cnt[i] = 0;
      while (p < end && p->key < cur_max) {
        ++group_cnt[i];
        ++p;
      }
    }
    group_cnt[3] = cnt - group_cnt[0] - group_cnt[1] - group_cnt[2];

    int new_gran = gran_quarter;
    for (int i = 0; i < 4; ++i) {
      if (group_cnt[i] > kGroupCardinality) {
        new_gran = gran_half;
        // half group
        break;
      }
    }

    p = entries;
    if (new_gran != gran_half) {
      for (int i = 0; i < 4; ++i) {
        if (group_cnt[i] > 0) {
          int cur_start = (i + 1) * kGroupCardinality - group_cnt[i];
          for (int j = 0; j < group_cnt[i]; ++j) {
            records[cur_start + j] = *p;
            ++p;
          }
        }
      }
    } else {
      group_cnt[0] = group_cnt[0] + group_cnt[1];
      group_cnt[1] = group_cnt[2] + group_cnt[3];
      for (int i = 0; i < 2; ++i) {
        if (group_cnt[i] > 0) {
          int cur_start = (i + 1) * kGroupCardinality * 2 - group_cnt[i];
          for (int j = 0; j < group_cnt[i]; ++j) {
            records[cur_start + j] = *p;
            ++p;
          }
        }
      }
    }

    hdr.leftmost_ptr.group_gran = new_gran;
    records[kGroupCardinality - 1].ptr.group_gran = new_gran;
    records[2 * kGroupCardinality - 1].ptr.group_gran = new_gran;
    records[3 * kGroupCardinality - 1].ptr.group_gran = new_gran;
    records[kInternalCardinality - 1].ptr.group_gran = new_gran;

    hdr.leftmost_ptr.group_node_version = hdr.node_version;
    records[kGroupCardinality - 1].ptr.group_node_version = hdr.node_version;
    records[2 * kGroupCardinality - 1].ptr.group_node_version =
        hdr.node_version;
    records[3 * kGroupCardinality - 1].ptr.group_node_version =
        hdr.node_version;
    records[kInternalCardinality - 1].ptr.group_node_version = hdr.node_version;
    return new_gran;
  }

  uint8_t update_node_version() {
    hdr.node_version = (hdr.node_version + 1) % (1u << 4);  // only 4 bit in ptr
    return hdr.node_version;
  }

  void debug() const {
    std::cout << "InternalPage@ ";
    hdr.debug();
  }

  void verbose_debug() const {
    this->debug();
    for (int i = 0; i < kInternalCardinality; ++i) {
      printf("[%lu %lu] ", this->records[i].key, this->records[i].ptr.val);
    }
    printf("\n");
  }

};

class LeafPage {
 private:
  // uint8_t front_version;
  Header hdr;
  LeafEntry records[kLeafCardinality];

  // uint8_t padding[1];
  // uint8_t rear_version;

  friend class Tree;

 public:
  LeafPage(uint32_t level = 0) {
    hdr.level = level;
    records[0].value = kValueNull;
  }

  void set_consistent() {}

  void update_hash_offset() {
    hdr.node_version = (hdr.node_version + 2) % kNumBucket;
  }

  bool insert_for_split(const Key &k, const Value &v, int bucket_id) {
    int pos_start, overflow_start;
    if (bucket_id % 2) {
      pos_start = (bucket_id / 2) * kGroupSize + kAssociativity * 2;
      overflow_start = pos_start - kAssociativity;
    } else {
      pos_start = (bucket_id / 2) * kGroupSize;
      overflow_start = pos_start + kAssociativity;
    }
    for (int i = pos_start; i < pos_start + kAssociativity; ++i) {
      assert(records[i].key != k);
      if (records[i].value == kValueNull) {
        records[i].key = k;
        records[i].value = v;
        return true;
      }
    }
    for (int i = overflow_start; i < overflow_start + kAssociativity; ++i) {
      assert(records[i].key != k);
      if (records[i].value == kValueNull) {
        records[i].key = k;
        records[i].value = v;
        return true;
      }
    }
    return false;
  }

  bool check_consistent() const { return true; }

  void debug() const {
    std::cout << "LeafPage@ ";
    hdr.debug();
  }
};

class Tree {
 public:
  Tree(DSMClient *dsm, uint16_t tree_id = 0);

  void insert(const Key &k, const Value &v, CoroContext *cxt = nullptr,
              int coro_id = 0);
  bool search(const Key &k, Value &v, CoroContext *cxt = nullptr,
              int coro_id = 0);
  void del(const Key &k, CoroContext *cxt = nullptr, int coro_id = 0);

  uint64_t range_query(const Key &from, const Key &to, Value *buffer,
                       CoroContext *cxt = nullptr, int coro_id = 0);

  void print_and_check_tree(CoroContext *cxt = nullptr, int coro_id = 0);

  void run_coroutine(CoroFunc func, int id, int coro_cnt, bool lock_bench);

  void lock_bench(const Key &k, CoroContext *cxt = nullptr, int coro_id = 0);

  void index_cache_statistics();
  void clear_statistics();

 private:
  DSMClient *dsm_client_;
  uint64_t tree_id;
  GlobalAddress root_ptr_ptr; // the address which stores root pointer;

  // static thread_local int coro_id;
  static thread_local CoroCall worker[define::kMaxCoro];
  static thread_local CoroCall master;

  LocalLockNode *local_locks[MAX_MACHINE];

  IndexCache *index_cache;

  void print_verbose();

  void before_operation(CoroContext *cxt, int coro_id);

  GlobalAddress get_root_ptr_ptr();
  GlobalAddress get_root_ptr(CoroContext *cxt, int coro_id,
                             bool force_read = false);

  void coro_worker(CoroYield &yield, RequstGen *gen, int coro_id,
                   bool lock_bench);
  void coro_master(CoroYield &yield, int coro_cnt);

  // void broadcast_new_root(GlobalAddress new_root_addr, int root_level);
  bool update_new_root(GlobalAddress left, const Key &k, GlobalAddress right,
                       int level, GlobalAddress old_root, CoroContext *cxt,
                       int coro_id);

  void insert_internal_update_left_child(const Key &k, GlobalAddress v,
                                         const Key &left_child,
                                         GlobalAddress left_child_val,
                                         CoroContext *cxt, int coro_id,
                                         int level);

  bool try_lock_addr(GlobalAddress lock_addr, uint64_t *buf, CoroContext *cxt,
                     int coro_id);
  void unlock_addr(GlobalAddress lock_addr, uint64_t *buf, CoroContext *cxt,
                   int coro_id, bool async);
  bool try_x_lock_addr(GlobalAddress lock_addr, uint64_t *buf, CoroContext *cxt,
                       int coro_id);
  void unlock_x_addr(GlobalAddress lock_addr, uint64_t *buf, CoroContext *cxt,
                     int coro_id, bool async);
  bool try_s_lock_addr(GlobalAddress lock_addr, uint64_t *buf, CoroContext *cxt,
                       int coro_id);
  void unlock_s_addr(GlobalAddress lock_addr, uint64_t *buf, CoroContext *cxt,
                     int coro_id, bool async);
  void try_sx_lock_addr(GlobalAddress lock_addr, uint64_t *buf,
                        CoroContext *cxt, int coro_id, bool sx_lock);
  void unlock_sx_addr(GlobalAddress lock_addr, uint64_t *buf, CoroContext *cxt,
                      int coro_id, bool async, bool sx_lock);
  void write_page_and_unlock(char *page_buffer, GlobalAddress page_addr,
                             int page_size, uint64_t *cas_buffer,
                             GlobalAddress lock_addr, CoroContext *cxt,
                             int coro_id, bool async, bool sx_lock);
  void batch_lock_and_read_page(char *page_buffer, GlobalAddress page_addr,
                                int page_size, uint64_t *cas_buffer,
                                GlobalAddress lock_addr, CoroContext *cxt,
                                int coro_id, bool sx_lock);
  void lock_and_read_page(char *page_buffer, GlobalAddress page_addr,
                          int page_size, uint64_t *cas_buffer,
                          GlobalAddress lock_addr, CoroContext *cxt,
                          int coro_id, bool sx_lock);

  bool page_search(GlobalAddress page_addr, int level_hint, int read_gran,
                   Key min, Key max, const Key &k, SearchResult &result,
                   CoroContext *cxt, int coro_id, bool from_cache = false);
  void internal_page_slice_search(InternalEntry *entries, int cnt, const Key k,
                                  SearchResult &result);
  bool try_group_search(LeafEntry *records, const Key &k, SearchResult &result);

  void internal_page_store(GlobalAddress page_addr, const Key &k,
                           GlobalAddress value, GlobalAddress root, int level,
                           CoroContext *cxt, int coro_id);
  void internal_page_store_update_left_child(GlobalAddress page_addr,
                                             const Key &k, GlobalAddress value,
                                             const Key &left_child,
                                             GlobalAddress left_child_val,
                                             GlobalAddress root, int level,
                                             CoroContext *cxt, int coro_id);
  bool try_leaf_page_update(GlobalAddress page_addr, GlobalAddress lock_addr,
                            const Key &k, const Value &v, uint64_t hash_offset,
                            CoroContext *cxt, int coro_id,
                            bool sx_lock = false);
  bool leaf_page_store(GlobalAddress page_addr, const Key &k, const Value &v,
                       GlobalAddress root, int level, uint64_t hash_offset,
                       CoroContext *cxt, int coro_id, bool from_cache = false,
                       bool sx_lock = false);
  bool leaf_page_del(GlobalAddress page_addr, const Key &k, int level,
                     CoroContext *cxt, int coro_id, bool from_cache = false);

  bool acquire_local_lock(GlobalAddress lock_addr, CoroContext *cxt,
                          int coro_id);
  bool can_hand_over(GlobalAddress lock_addr);
  void releases_local_lock(GlobalAddress lock_addr);
};
