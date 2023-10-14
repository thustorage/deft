#pragma once

// #include "DSM.h"
#include "dsm_client.h"
#include "Common.h"
#include <atomic>
#include <stddef.h>
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
  Key next_min;
  Key next_max;

  SearchResult() = default;
  void clear() {
    memset(reinterpret_cast<void *>(this), 0, sizeof(SearchResult));
  }
};

constexpr int kMaxInternalGroup = 4;

struct alignas(64) Header {
  uint8_t padding[kHeaderSize - kHeaderRawSize];
  // int8_t cnt = 0;
  uint8_t version : 4;
  uint8_t read_gran : 4;
  uint8_t level : 7;
  uint8_t is_root : 1;  
  uint8_t cache_read_gran[kMaxInternalGroup];  // used in cache
  uint8_t grp_in_cache[kMaxInternalGroup];     // used in cache
  uint64_t index_cache_freq;
  GlobalAddress my_addr = GlobalAddress::Null();

  GlobalAddress sibling_ptr = GlobalAddress::Null();
  InternalKey highest = kKeyMax;

  // align as a value-key pair
  GlobalAddress leftmost_ptr = GlobalAddress::Null();
  InternalKey lowest = kKeyMin;

  Header()
      : version(0),
        read_gran(gran_full),
        level(0),
        is_root(false),
        index_cache_freq(0) {}

  void debug() const {
    // std::cout << "leftmost=" << leftmost_ptr << ", "
    //           << "sibling=" << sibling_ptr << ", "
    //           << "level=" << (int)level
    //           << ","
    //           // << "cnt=" << cnt << ","
    //           << "range=[" << lowest << " - " << highest << "]";
  }
};
#if KEY_SIZE == 8
static_assert(sizeof(Header) == 64);
#endif

struct InternalEntry {
  GlobalAddress ptr;
  InternalKey key = 0;

  bool operator<(const InternalEntry &rhs) const { return key < rhs.key; }
};

constexpr size_t kEntryPerCacheLine =
    sizeof(InternalEntry) <= 64 ? 64 / sizeof(InternalEntry) : 1;

struct __attribute__((packed)) LeafValue {
  union {
    struct {
      uint64_t cl_ver : 4;  // cache line version
      uint64_t val : 60;
    };
    uint64_t raw = 0;
  };

  LeafValue() = default;
  LeafValue(uint8_t ver, uint64_t val) : cl_ver(ver), val(val) {}
  // don't allow copy or assign
  LeafValue(const LeafValue &other) = delete;
  LeafValue &operator=(const LeafValue &other) = delete;
  void copy(const LeafValue &other) { raw = other.raw; }
};

struct __attribute__((packed)) LeafEntry {
  // uint8_t f_version : 4;
  LeafValue lv;
  InternalKey key = 0;
  // uint8_t r_version : 4;
};

// used for sort
struct LeafKVEntry {
  Value val = 0;
  InternalKey key = 0;

  LeafKVEntry &operator=(const LeafEntry &rhs) {
    val = rhs.lv.val;
    key = rhs.key;
    return *this;
  }
  bool operator<(const LeafKVEntry &rhs) const { return key < rhs.key; }
};

constexpr int kInternalCardinality = 60;
constexpr int kGroupCardinality = kInternalCardinality / kMaxInternalGroup;

static inline double get_quarter(Key min, Key max) { return (max - min) / 4.0; }

static inline int get_key_group(const Key k, Key min, Key max) {
  double quarter = get_quarter(min, max);
  return (k - min) / quarter;
}

// static inline uint64_t get_lock_index(GlobalAddress addr) {
//   GlobalAddress a;
//   a.offset = addr.offset;
//   a.nodeID = addr.nodeID;
//   return CityHash64((char *)&a, sizeof(a)) % define::kNumOfLock;
// }

static inline GlobalAddress get_lock_addr(GlobalAddress addr) {
  GlobalAddress a;
  a.offset = addr.offset;
  a.nodeID = addr.nodeID;
  uint64_t lock_idx = CityHash64((char *)&a, sizeof(a)) % define::kNumOfLock;

  GlobalAddress lock_addr;
  lock_addr.nodeID = addr.nodeID;
  lock_addr.offset = lock_idx * define::kLockSize;
  return lock_addr;
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

  uint8_t update_version() {
    hdr.version += 1;
    for (int i = 0; i < kInternalCardinality; i += kEntryPerCacheLine) {
      records[i].ptr.cl_ver = hdr.version;
    }
    return hdr.version;
  }

  void restore_version(int begin_idx, int end_idx) {
    int start_cache_idx = (begin_idx + kEntryPerCacheLine - 1) /
                          kEntryPerCacheLine * kEntryPerCacheLine;
    for (int i = start_cache_idx; i < end_idx; i += kEntryPerCacheLine) {
      records[i].ptr.cl_ver = hdr.version;
    }
  }

  bool check_consistency(char *start, char *end, uint8_t version,
                         uint8_t &actual_version) {
    assert(start >= (char *)(records - 1) &&
           start <= (char *)(records + kInternalCardinality));
    assert(end >= (char *)records &&
           end <= (char *)(records + kInternalCardinality));
    int start_idx = start >= (char *)records
                        ? ((start - (char *)records) / sizeof(InternalEntry))
                        : 0;
    int end_idx = (end - (char *)records) / (int)(sizeof(InternalEntry));
    int start_cache_idx = (start_idx + kEntryPerCacheLine - 1) /
                          kEntryPerCacheLine * kEntryPerCacheLine;
    for (int i = start_cache_idx; i < end_idx; i += kEntryPerCacheLine) {
      if (records[i].ptr.cl_ver != version) {
        actual_version = records[i].ptr.cl_ver;
        return false;
      }
    }
    return true;
  }

  int find_empty(int begin, int cnt) {
    for (int i = begin; i < begin + cnt; ++i) {
      if (records[i].ptr == GlobalAddress::Null()) {
        return i;
      }
    }
    return -1;
  }

  int find_records_not_null(const Key &k) {
    int group_id = get_key_group(k, hdr.lowest, hdr.highest);
    int start_idx, end_idx;
    if (hdr.read_gran == gran_quarter) {
      start_idx = group_id * kGroupCardinality;
      end_idx = start_idx + kGroupCardinality;
    } else if (hdr.read_gran == gran_half) {
      start_idx = group_id < 2 ? 0 : kGroupCardinality * 2;
      end_idx = start_idx + kGroupCardinality * 2;
    } else {
      assert(hdr.read_gran == gran_full);
      start_idx = 0;
      end_idx = kInternalCardinality;
    }
    for (int i = start_idx; i < end_idx; ++i) {
      if (records[i].key == k && records[i].ptr != GlobalAddress::Null()) {
        return i;
      }
    }
    return -1;
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
      while (p < end && (uint64_t)p->key < cur_max) {
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
          int start = i * kGroupCardinality;
          for (int j = 0; j < group_cnt[i] - 1; ++j) {
            records[start + j] = *p;
            ++p;
          }
          // put the last one in the rightmost
          records[start + kGroupCardinality - 1] = *p;
          ++p;
        }
      }
    } else {
      group_cnt[0] = group_cnt[0] + group_cnt[1];
      group_cnt[1] = group_cnt[2] + group_cnt[3];
      for (int i = 0; i < 2; ++i) {
        if (group_cnt[i] > 0) {
          int start = i * kGroupCardinality * 2;
          for (int j = 0; j < group_cnt[i] - 1; ++j) {
            records[start + j] = *p;
            ++p;
          }
          // put the last one in the rightmost
          records[start + kGroupCardinality * 2 - 1] = *p;
          ++p;
        }
      }
    }

    hdr.read_gran = new_gran;
    return new_gran;
  }

  void debug() const {
    std::cout << "InternalPage@ ";
    hdr.debug();
  }

  void verbose_debug() const {
    this->debug();
    for (int i = 0; i < kInternalCardinality; ++i) {
      printf("[%lu %lu] ", (uint64_t)records[i].key, records[i].ptr.raw);
    }
    printf("\n");
  }

};

// LeafEntry Group in leaf page

constexpr int kAssociativity = 4;
constexpr int kNumBucket = 10;
constexpr int kNumGroup = kNumBucket / 2;
constexpr int kGroupSize = kAssociativity * 3;
constexpr int kLeafCardinality = kNumGroup * kAssociativity * 3;

static inline int key_hash_bucket(const Key &k) { return k % kNumBucket; }

struct __attribute__((packed)) LeafEntryGroup {
  LeafEntry front[kAssociativity];
  LeafEntry overflow[kAssociativity];
  LeafEntry back[kAssociativity];

  bool check_consistency(bool is_front, uint8_t version,
                         uint8_t &actual_version) {
    if (is_front) {
      if (front[0].lv.cl_ver != version) {
        actual_version = front[0].lv.cl_ver;
        return false;
      }
    } else {
      if (back[0].lv.cl_ver != version) {
        actual_version = back[0].lv.cl_ver;
        return false;
      }
    }
    if (overflow[0].lv.cl_ver != version) {
      actual_version = overflow[0].lv.cl_ver;
      return false;
    }
    return true;
  }

  void set_version(uint8_t ver) {
    front[0].lv.cl_ver = ver;
    overflow[0].lv.cl_ver = ver;
    back[0].lv.cl_ver = ver;
  }

  bool find(const Key &k, SearchResult &result, bool is_front) {
    if (is_front) {
      for (int i = 0; i < kAssociativity; ++i) {
        if (front[i].key == k && front[i].lv.val != kValueNull) {
          result.val = front[i].lv.val;
          return true;
        }
      }
    } else {
      for (int i = 0; i < kAssociativity; ++i) {
        if (back[i].key == k && back[i].lv.val != kValueNull) {
          result.val = back[i].lv.val;
          return true;
        }
      }
    }
    for (int i = 0; i < kAssociativity; ++i) {
      if (overflow[i].key == k && overflow[i].lv.val != kValueNull) {
        result.val = overflow[i].lv.val;
        return true;
      }
    }
    return false;
  }

  bool insert_for_split(const Key &k, const Value v, bool is_front) {
    if (is_front) {
      for (int i = 0; i < kAssociativity; ++i) {
        if (front[i].key == k) {
          front[i].lv.val = v;
          return true;
        } else if (front[i].lv.val == kValueNull) {
          front[i].key = k;
          front[i].lv.val = v;
          return true;
        }
      }
    } else {
      for (int i = 0; i < kAssociativity; ++i) {
        if (back[i].key == k) {
          back[i].lv.val = v;
          return true;
        } else if (back[i].lv.val == kValueNull) {
          back[i].key = k;
          back[i].lv.val = v;
          return true;
        }
      }
    }
    for (int i = 0; i < kAssociativity; ++i) {
      if (overflow[i].key == k) {
        overflow[i].lv.val = v;
        return true;
      } else if (overflow[i].lv.val == kValueNull) {
        overflow[i].key = k;
        overflow[i].lv.val = v;
        return true;
      }
    }
    return false;
  }

  bool find(const Key &k, bool is_front, LeafEntry **entry_addr,
            LeafEntry **empty_addr) {
    // find main bucket
    LeafEntry *bucket = is_front ? front : back;
    for (int i = 0; i < kAssociativity; ++i) {
      if (bucket[i].key == k) {
        if (entry_addr) {
          *entry_addr = &bucket[i];
        }
        return true;
      } else if (bucket[i].lv.val == kValueNull && empty_addr &&
                 *empty_addr == nullptr) {
        *empty_addr = &bucket[i];
      }
    }
    // find overflow
    for (int i = 0; i < kAssociativity; ++i) {
      if (overflow[i].key == k) {
        if (entry_addr) {
          *entry_addr = &overflow[i];
        }
        return true;
      } else if (overflow[i].lv.val == kValueNull && empty_addr &&
                 *empty_addr == nullptr) {
        *empty_addr = &overflow[i];
      }
    }
    return false;
  }
};
constexpr size_t kFrontOffset = 0;
constexpr size_t kBackOffset = offsetof(LeafEntryGroup, overflow);
constexpr size_t kReadBucketSize = sizeof(LeafEntry) * kAssociativity * 2;

class LeafPage {
 private:
  // uint8_t front_version;
  Header hdr;
  // LeafEntry records[kLeafCardinality];
  LeafEntryGroup groups[kNumGroup];

  // uint8_t padding[1];
  // uint8_t rear_version;

  friend class Tree;

 public:
  LeafPage(uint32_t level = 0) {
    hdr.level = level;
    // records[0].value = kValueNull;
  }

  uint8_t update_version() {
    hdr.version += 1;
    for (int i = 0; i < kNumGroup; ++i) {
      groups[i].set_version(hdr.version);
    }
    return hdr.version;
  }
  void debug() const {
    std::cout << "LeafPage@ ";
    hdr.debug();
  }
};

class Tree {
 public:
  Tree(DSMClient *dsm, uint16_t tree_id = 0);
  ~Tree();

  void insert(const Key &k, const Value &v, CoroContext *ctx = nullptr);
  bool search(const Key &k, Value &v, CoroContext *ctx = nullptr,
              int coro_id = 0);
  void del(const Key &k, CoroContext *ctx = nullptr, int coro_id = 0);

  int range_query(const Key &from, const Key &to, Value *buffer, int max_cnt,
                  CoroContext *ctx = nullptr, int coro_id = 0);

  void run_coroutine(CoroFunc func, int id, int coro_cnt, bool lock_bench,
                     uint64_t total_ops);

  void lock_bench(const Key &k, CoroContext *ctx = nullptr, int coro_id = 0);

  void index_cache_statistics();
  void clear_statistics();

 private:
  DSMClient *dsm_client_;
  uint64_t tree_id;
  GlobalAddress root_ptr_ptr; // the address which stores root pointer;

  // static thread_local int coro_id;
  static thread_local CoroCall worker[define::kMaxCoro];
  static thread_local CoroCall master;
  static thread_local uint64_t coro_ops_total;
  static thread_local uint64_t coro_ops_cnt_start;
  static thread_local uint64_t coro_ops_cnt_finish;

  LocalLockNode *local_locks[MAX_MACHINE];

  IndexCache *index_cache;

  void print_verbose();

  void before_operation(CoroContext *ctx, int coro_id);

  GlobalAddress get_root_ptr_ptr();
  GlobalAddress get_root_ptr(CoroContext *ctx, bool force_read = false);

  void coro_worker(CoroYield &yield, RequstGen *gen, int coro_id,
                   bool lock_bench);
  void coro_master(CoroYield &yield, int coro_cnt);

  // void broadcast_new_root(GlobalAddress new_root_addr, int root_level);
  bool update_new_root(GlobalAddress left, const Key &k, GlobalAddress right,
                       int level, GlobalAddress old_root, CoroContext *ctx);

  void insert_internal_update_left_child(const Key &k, GlobalAddress v,
                                         const Key &left_child,
                                         GlobalAddress left_child_val,
                                         CoroContext *ctx, int level);

  bool try_lock_addr(GlobalAddress lock_addr, uint64_t *buf, CoroContext *ctx);
  void unlock_addr(GlobalAddress lock_addr, uint64_t *buf, CoroContext *ctx,
                   bool async);
  void acquire_sx_lock(GlobalAddress lock_addr, uint64_t *lock_buffer,
                       CoroContext *ctx, bool share_lock,
                       bool upgrade_from_s = false);
  void release_sx_lock(GlobalAddress lock_addr, uint64_t *lock_buffer,
                       CoroContext *ctx, bool async, bool share_lock);
  void acquire_lock(GlobalAddress lock_addr, uint64_t *lock_buffer,
                    CoroContext *ctx, bool share_lock,
                    bool upgrade_from_s = false);
  void release_lock(GlobalAddress lock_addr, uint64_t *lock_buffer,
                    CoroContext *ctx, bool async, bool share_lock);
  void write_and_unlock(char *write_buffer, GlobalAddress write_addr,
                        int write_size, uint64_t *cas_buffer,
                        GlobalAddress lock_addr, CoroContext *ctx, bool async,
                        bool sx_lock);
  void cas_and_unlock(GlobalAddress cas_addr, int log_cas_size,
                      uint64_t *cas_buffer, uint64_t equal, uint64_t swap,
                      uint64_t mask, GlobalAddress lock_addr,
                      uint64_t *lock_buffer, bool share_lock, CoroContext *ctx,
                      bool async);
  void lock_and_read(GlobalAddress lock_addr, bool share_lock,
                     bool upgrade_from_s, uint64_t *lock_buffer,
                     GlobalAddress read_addr, int read_size, char *read_buffer,
                     CoroContext *ctx);

  bool leaf_page_group_search(GlobalAddress page_addr, const Key &k,
                              SearchResult &result, CoroContext *ctx,
                              bool from_cache);
  bool page_search(GlobalAddress page_addr, int level_hint, int read_gran,
                   Key min, Key max, const Key &k, SearchResult &result,
                   CoroContext *ctx, bool from_cache = false);
  void internal_page_slice_search(InternalEntry *entries, int cnt, const Key k,
                                  SearchResult &result);

  void update_ptr_internal(const Key &k, GlobalAddress v, CoroContext *ctx,
                           int level);
  // void internal_page_store(GlobalAddress page_addr, const Key &k,
  //                          GlobalAddress value, GlobalAddress root, int level,
  //                          CoroContext *ctx);
  void internal_page_update(GlobalAddress page_addr, const Key &k,
                            GlobalAddress value, int level, CoroContext *ctx,
                            bool sx_lock);
  void internal_page_store_update_left_child(GlobalAddress page_addr,
                                             const Key &k, GlobalAddress value,
                                             const Key &left_child,
                                             GlobalAddress left_child_val,
                                             GlobalAddress root, int level,
                                             CoroContext *ctx);
  // bool try_leaf_page_upsert(GlobalAddress page_addr, char *page_buffer,
  //                           GlobalAddress lock_addr, uint64_t *lock_buffer,
  //                           const Key &k, const Value &v, CoroContext *ctx,
  //                           int coro_id, bool share_lock);
  bool leaf_page_store(GlobalAddress page_addr, const Key &k, const Value &v,
                       GlobalAddress root, int level, CoroContext *ctx,
                       bool from_cache = false, bool share_lock = false);
  bool leaf_page_del(GlobalAddress page_addr, const Key &k, int level,
                     CoroContext *ctx, int coro_id, bool from_cache = false);

  // bool acquire_local_lock(GlobalAddress lock_addr, CoroContext *ctx,
  //                         int coro_id);
  // bool can_hand_over(GlobalAddress lock_addr);
  // void releases_local_lock(GlobalAddress lock_addr);
};
