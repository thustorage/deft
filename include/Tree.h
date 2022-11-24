#if !defined(_TREE_H_)
#define _TREE_H_

#include "DSM.h"
#include <atomic>
#include <city.h>
#include <functional>
#include <iostream>

class IndexCache;
extern uint64_t cache_miss[MAX_APP_THREAD][8];
extern uint64_t cache_hit[MAX_APP_THREAD][8];
extern uint64_t latency[MAX_APP_THREAD][LATENCY_WINDOWS];

enum latency_enum { lat_lock, lat_read_page, lat_write_page, lat_op };

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

using CoroFunc = std::function<RequstGen *(int, DSM *, int)>;

struct SearchResult {
  bool is_leaf;
  uint8_t level;
  GlobalAddress slibing;
  GlobalAddress next_level;
  Value val;
  Key other_in_group = 0;

  SearchResult() = default;
  void clear() {
    memset(this, 0, sizeof(SearchResult));
  }
};

class Header {
//  private:
 public:
  GlobalAddress leftmost_ptr;
  GlobalAddress sibling_ptr;
  Key lowest;
  Key highest;
  // uint16_t level;
  uint8_t hash_offset;  // in leaf node
  uint8_t level;
  int16_t last_index;
  // int8_t cnt;

  friend class InternalPage;
  friend class LeafPage;
  friend class Tree;
  friend class IndexCache;

 public:
  Header() {
    leftmost_ptr = GlobalAddress::Null();
    sibling_ptr = GlobalAddress::Null();
    hash_offset = 0;
    last_index = -1;
    lowest = kKeyMin;
    highest = kKeyMax;
  }

  void debug() const {
    std::cout << "leftmost=" << leftmost_ptr << ", "
              << "sibling=" << sibling_ptr << ", "
              << "level=" << (int)level << ","
              << "cnt=" << last_index + 1 << ","
              << "range=[" << lowest << " - " << highest << "]";
  }
} __attribute__((packed));


class InternalEntry {
 public:
  Key key;
  GlobalAddress ptr;

  InternalEntry() {
    ptr = GlobalAddress::Null();
    key = 0;
  }
} __attribute__((packed));

class LeafEntry {
 public:
  // uint8_t f_version : 4;
  Key key;
  Value value;
  // uint8_t r_version : 4;

  LeafEntry() {
    // f_version = 0;
    // r_version = 0;
    value = kValueNull;
    key = 0;
  }
} __attribute__((packed));

constexpr int kInternalCardinality = (kInternalPageSize - sizeof(Header) -
                                      sizeof(uint8_t) * 2 - sizeof(uint64_t)) /
                                     sizeof(InternalEntry);

// constexpr int kLeafCardinality =
//     (kLeafPageSize - sizeof(Header) - sizeof(uint8_t) * 2 - sizeof(uint64_t)) /
//     sizeof(LeafEntry);
constexpr int kAssociativity = 4;
constexpr int kNumBucket = 10;
constexpr int kGroupSize = kAssociativity * 3;
constexpr int kLeafCardinality = kNumBucket * kAssociativity / 2 * 3;

// constexpr int kLeafPosBaseMax = kLeafCardinality - NUM_LINEAR_PROBING + 1;


class InternalPage {
// private:
  union {
    uint32_t crc;
    uint64_t embedding_lock;
    uint64_t index_cache_freq;
  };

  uint8_t front_version;
  Header hdr;
  InternalEntry records[kInternalCardinality];

  // uint8_t padding[3];
  uint8_t rear_version;

  friend class Tree;
  friend class IndexCache;

public:
  // this is called when tree grows
  InternalPage(GlobalAddress left, const Key &key, GlobalAddress right,
               uint32_t level = 0) {
    hdr.leftmost_ptr = left;
    hdr.level = level;
    records[0].key = key;
    records[0].ptr = right;
    records[1].ptr = GlobalAddress::Null();

    hdr.last_index = 0;

    front_version = 0;
    rear_version = 0;
  }

  InternalPage(uint32_t level = 0) {
    hdr.level = level;
    records[0].ptr = GlobalAddress::Null();

    front_version = 0;
    rear_version = 0;

    embedding_lock = 0;
  }

  void set_consistent() {
    front_version++;
    rear_version = front_version;
#ifdef CONFIG_ENABLE_CRC
    this->crc =
        CityHash32((char *)&front_version, (&rear_version) - (&front_version));
#endif
  }

  bool check_consistent() const {

    bool succ = true;
#ifdef CONFIG_ENABLE_CRC
    auto cal_crc =
        CityHash32((char *)&front_version, (&rear_version) - (&front_version));
    succ = cal_crc == this->crc;
#endif
    succ = succ && (rear_version == front_version);

    return succ;
  }

  void debug() const {
    std::cout << "InternalPage@ ";
    hdr.debug();
    std::cout << "version: [" << (int)front_version << ", " << (int)rear_version
              << "]" << std::endl;
  }

  void verbose_debug() const {
    this->debug();
    for (int i = 0; i < this->hdr.last_index + 1; ++i) {
      printf("[%lu %lu] ", this->records[i].key, this->records[i].ptr.val);
    }
    printf("\n");
  }

} __attribute__((packed));

class LeafPage {
private:
  union {
    uint32_t crc;
    uint64_t embedding_lock;
  };
  uint8_t front_version;
  Header hdr;
  LeafEntry records[kLeafCardinality];

  // uint8_t padding[1];
  uint8_t rear_version;

  friend class Tree;

public:
  LeafPage(uint32_t level = 0) {
    hdr.level = level;
    records[0].value = kValueNull;

    front_version = 0;
    rear_version = 0;

    embedding_lock = 0;
  }

  void set_consistent() {
    front_version++;
    rear_version = front_version;
#ifdef CONFIG_ENABLE_CRC
    this->crc =
        CityHash32((char *)&front_version, (&rear_version) - (&front_version));
#endif
  }

  void update_hash_offset() {
    hdr.hash_offset = (hdr.hash_offset + 2) % kNumBucket;
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

  bool check_consistent() const {
    bool succ = true;
#ifdef CONFIG_ENABLE_CRC
    auto cal_crc =
        CityHash32((char *)&front_version, (&rear_version) - (&front_version));
    succ = cal_crc == this->crc;
#endif
    succ = succ && (rear_version == front_version);
    return succ;
  }

  void debug() const {
    std::cout << "LeafPage@ ";
    hdr.debug();
    std::cout << "version: [" << (int)front_version << ", " << (int)rear_version
              << "]" << std::endl;
  }

} __attribute__((packed));


class Tree {

public:
  Tree(DSM *dsm, uint16_t tree_id = 0);

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
  DSM *dsm;
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
  GlobalAddress get_root_ptr(CoroContext *cxt, int coro_id);

  void coro_worker(CoroYield &yield, RequstGen *gen, int coro_id,
                   bool lock_bench);
  void coro_master(CoroYield &yield, int coro_cnt);

  void broadcast_new_root(GlobalAddress new_root_addr, int root_level);
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

  bool page_search(GlobalAddress page_addr, int level_hint, const Key &k,
                   SearchResult &result, CoroContext *cxt, int coro_id,
                   bool from_cache = false);
  void internal_page_search(InternalPage *page, const Key &k,
                            SearchResult &result);
  void leaf_page_search(LeafPage *page, const Key &k, SearchResult &result);
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

  // int key_hash_pos(const Key &k, uint64_t page_hash, uint64_t hash_offset) {
  //   return (k + page_hash + hash_offset) % kLeafPosBaseMax;
  // }
  int key_hash_bucket(const Key &k, uint64_t hash_offset) {
    return (k + hash_offset) % kNumBucket;
  }
};

#endif // _TREE_H_
