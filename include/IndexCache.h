#pragma once

#include <atomic>
// #include <queue>
#include <list>
#include <vector>

#include "CacheEntry.h"
#include "HugePageAlloc.h"
#include "Timer.h"
#include "WRLock.h"
#include "third_party/inlineskiplist.h"
#include "thread_epoch.h"

using CacheSkipList = InlineSkipList<CacheEntryComparator>;

struct alignas(64) DelayFreeList {
  std::list<std::pair<void *, uint64_t>> list;
  WRLock lock;
};

class IndexCache {
 public:
  IndexCache(int cache_size);
  ~IndexCache();

  bool add_to_cache(InternalPage *page, int thread_id);
  bool add_sub_node(GlobalAddress addr, int group_id, int granularity,
                    int guard_offset, InternalEntry *guard, int size, Key min,
                    Key max, int thread_id);
  const CacheEntry *search_from_cache(const Key &k, GlobalAddress *addr,
                                      GlobalAddress *parent_addr);

  void search_range_from_cache(const Key &from, const Key &to,
                               std::vector<InternalPage *> &result);

  bool add_entry(const Key &from, const Key &to, InternalPage *ptr);
  const CacheEntry *find_entry(const Key &k);
  const CacheEntry *find_entry(const Key &from, const Key &to);

  bool invalidate(const CacheEntry *entry, int thread_id);
  bool invalidate(const Key &from, const Key &to, int thread_id);

  const CacheEntry *get_a_random_entry(uint64_t &freq);

  void statistics();

  void bench();

  void free_delay();
  
  ThreadStatus *thread_status;

 private:
  uint64_t cache_size;  // MB;
  std::atomic<int64_t> free_page_cnt;
  std::atomic<int64_t> skiplist_node_cnt;
  std::atomic<uint64_t> max_key{0};
  int64_t all_page_cnt;

  // std::queue<std::pair<void *, uint64_t>> delay_free_list;
  // WRLock free_lock;

  DelayFreeList delay_free_lists[MAX_APP_THREAD];
  std::atomic_bool delay_free_stop_flag{false};
  std::thread free_delay_thread_;

  // SkipList
  CacheSkipList *skiplist;
  CacheEntryComparator cmp;
  Allocator alloc;

  void evict_one(int thread_id);
};

inline IndexCache::IndexCache(int cache_size) : cache_size(cache_size) {
  skiplist = new CacheSkipList(cmp, &alloc, 21);
  uint64_t memory_size = define::MB * cache_size;

  thread_status = new ThreadStatus(MAX_APP_THREAD);

  all_page_cnt = memory_size / kInternalPageSize * kMaxInternalGroup;
  free_page_cnt.store(all_page_cnt);
  skiplist_node_cnt.store(0);
  free_delay_thread_ = std::thread(&IndexCache::free_delay, this);
}

IndexCache::~IndexCache() {
  delay_free_stop_flag.store(true, std::memory_order_release);
  if (free_delay_thread_.joinable()) {
    free_delay_thread_.join();
  }
  delete skiplist;
  delete thread_status;
}

inline bool IndexCache::add_entry(const Key &from, const Key &to,
                                  InternalPage *ptr) {
  // TODO memory leak
  auto buf = skiplist->AllocateKey(sizeof(CacheEntry));
  auto &e = *(CacheEntry *)buf;
  e.from = from;
  e.to = to - 1; // !IMPORTANT;
  e.ptr = ptr;

  bool res = skiplist->InsertConcurrently(buf);
  if (res) {
    skiplist_node_cnt.fetch_add(1);
    if (from > max_key.load(std::memory_order_acquire)) {
      max_key.store(from, std::memory_order_release);
    }
  }
  return res;
}

inline const CacheEntry *IndexCache::find_entry(const Key &from,
                                                const Key &to) {
  CacheSkipList::Iterator iter(skiplist);

  CacheEntry e;
  e.from = from;
  e.to = to - 1;
  iter.Seek((char *)&e);
  if (iter.Valid()) {
    auto val = (const CacheEntry *)iter.key();
    return val;
  } else {
    return nullptr;
  }
}

inline const CacheEntry *IndexCache::find_entry(const Key &k) {
  return find_entry(k, k + 1);
}

inline bool IndexCache::add_to_cache(InternalPage *page, int thread_id) {
  InternalPage *new_page = (InternalPage *)malloc(kInternalPageSize);
  memcpy(reinterpret_cast<void *>(new_page), page, kInternalPageSize);
  new_page->hdr.index_cache_freq = 0;
  for (int i = 0 ; i < kMaxInternalGroup; ++i) {
    new_page->hdr.grp_in_cache[i] = true;
    new_page->hdr.cache_read_gran[i] = page->hdr.read_gran;
  }
  assert(new_page->hdr.my_addr != GlobalAddress::Null());

  if (this->add_entry(page->hdr.lowest, page->hdr.highest, new_page)) {
    auto v = free_page_cnt.fetch_sub(kMaxInternalGroup);
    if (v <= 0) {
      evict_one(thread_id);
    }

    return true;
  } else {  // conflicted
    auto e = this->find_entry(page->hdr.lowest, page->hdr.highest);
    if (e && e->from == page->hdr.lowest && e->to == page->hdr.highest - 1) {
      auto ptr = e->ptr;

      if (__sync_bool_compare_and_swap(&(e->ptr), ptr, new_page)) {
        if (ptr == nullptr) {
          auto v = free_page_cnt.fetch_sub(kMaxInternalGroup);
          if (v <= 0) {
            evict_one(thread_id);
          }
        } else {
          int old_cnt = 0;
          for (int i = 0; i < kMaxInternalGroup; ++i) {
            old_cnt += ptr->hdr.grp_in_cache[i];
          }
          if (old_cnt < kMaxInternalGroup) {
            auto v = free_page_cnt.fetch_sub(kMaxInternalGroup - old_cnt);
            if (v <= 0) {
              evict_one(thread_id);
            }
          }
          delay_free_lists[thread_id].lock.wLock();
          delay_free_lists[thread_id].list.push_back(
              std::make_pair(ptr, asm_rdtsc()));
          delay_free_lists[thread_id].lock.wUnlock();
        }
        return true;
      }
    }

    free(new_page);
    return false;
  }
}

inline bool IndexCache::add_sub_node(GlobalAddress addr, int group_id,
                                     int granularity, int guard_offset,
                                     InternalEntry *guard, int size, Key min,
                                     Key max, int thread_id) {
  InternalPage *new_page = (InternalPage *)malloc(kInternalPageSize);
  // memset(new_page, 0, kInternalPageSize);

  auto e = this->find_entry(min, max);
  if (e && e->from == min && e->to == max - 1) {  // update sub-node
    InternalPage *ptr = e->ptr;
    if (ptr) {
      // update
      memcpy(reinterpret_cast<char *>(new_page), ptr, kInternalPageSize);
      memcpy(reinterpret_cast<char *>(new_page) + guard_offset, guard, size);
    } else {
      // add
      memset(reinterpret_cast<char *>(new_page), 0, kInternalPageSize);
      memcpy(reinterpret_cast<char *>(new_page) + guard_offset, guard, size);
      new_page->hdr.lowest = min;
      new_page->hdr.highest = max;
      new_page->hdr.sibling_ptr = GlobalAddress::Null();
      new_page->hdr.level = 1;
      new_page->hdr.index_cache_freq = 0;
      new_page->hdr.my_addr = addr;
    }
    int cnt = 0;
    if (granularity == gran_quarter) {
      new_page->hdr.grp_in_cache[group_id] = true;
      new_page->hdr.cache_read_gran[group_id] = gran_quarter;
      cnt = 1;
    } else {
      cnt = 2;
      if (group_id < 2) {
        new_page->hdr.grp_in_cache[0] = true;
        new_page->hdr.grp_in_cache[1] = true;
        new_page->hdr.cache_read_gran[0] = gran_half;
        new_page->hdr.cache_read_gran[1] = gran_half;
      } else {
        new_page->hdr.grp_in_cache[2] = true;
        new_page->hdr.grp_in_cache[3] = true;
        new_page->hdr.cache_read_gran[2] = gran_half;
        new_page->hdr.cache_read_gran[3] = gran_half;
      }
    }
    if (__sync_bool_compare_and_swap(&(e->ptr), ptr, new_page)) {
      if (ptr == nullptr) {
        auto v = free_page_cnt.fetch_sub(cnt);
        if (v <= 0) {
          evict_one(thread_id);
        }
      } else {
        int old_cnt = 0;
        int new_cnt = 0;
        for (int i = 0; i < kMaxInternalGroup; ++i) {
          old_cnt += ptr->hdr.grp_in_cache[i];
          new_cnt += new_page->hdr.grp_in_cache[i];
        }
        if (old_cnt != new_cnt) {
          auto v = free_page_cnt.fetch_sub(new_cnt - old_cnt);
          if (v <= 0) {
            evict_one(thread_id);
          }
        }
        delay_free_lists[thread_id].lock.wLock();
        delay_free_lists[thread_id].list.push_back(
            std::make_pair(ptr, asm_rdtsc()));
        delay_free_lists[thread_id].lock.wUnlock();
      }
      return true;
    }
  } else {
    // add
    memset(reinterpret_cast<char *>(new_page), 0, kInternalPageSize);
    memcpy(reinterpret_cast<char *>(new_page) + guard_offset, guard, size);
    new_page->hdr.lowest = min;
    new_page->hdr.highest = max;
    new_page->hdr.sibling_ptr = GlobalAddress::Null();
    new_page->hdr.level = 1;
    new_page->hdr.index_cache_freq = 0;
    new_page->hdr.my_addr = addr;
    int cnt = 0;
    if (granularity == gran_quarter) {
      cnt = 1;
      new_page->hdr.grp_in_cache[group_id] = true;
      new_page->hdr.cache_read_gran[group_id] = gran_quarter;

    } else {
      cnt = 2;
      if (group_id < 2) {
        new_page->hdr.grp_in_cache[0] = true;
        new_page->hdr.grp_in_cache[1] = true;
        new_page->hdr.cache_read_gran[0] = gran_half;
        new_page->hdr.cache_read_gran[1] = gran_half;
      } else {
        new_page->hdr.grp_in_cache[2] = true;
        new_page->hdr.grp_in_cache[3] = true;
        new_page->hdr.cache_read_gran[2] = gran_half;
        new_page->hdr.cache_read_gran[3] = gran_half;
      }
    }
    if (this->add_entry(min, max, new_page)) {
      auto v = free_page_cnt.fetch_sub(cnt);
      if (v <= 0) {
        evict_one(thread_id);
      }
      return true;
    }
  }
  free(new_page);
  return false;
}

inline const CacheEntry *IndexCache::search_from_cache(
    const Key &k, GlobalAddress *addr, GlobalAddress *parent_addr) {
  auto entry = find_entry(k);

  InternalPage *page = entry ? entry->ptr : nullptr;

  if (page && k >= page->hdr.lowest && k < page->hdr.highest) {
    if (page->hdr.index_cache_freq < UINT64_MAX) {
      page->hdr.index_cache_freq++;
    }

    int group_id = get_key_group(k, page->hdr.lowest, page->hdr.highest);
    if (page->hdr.grp_in_cache[group_id] == false) {
      return nullptr;
    }
    uint8_t cur_group_gran = page->hdr.cache_read_gran[group_id];

    int start_idx, cnt;
    if (cur_group_gran == gran_quarter) {
      start_idx = kGroupCardinality * group_id;
      cnt = kGroupCardinality;
    } else if (cur_group_gran == gran_half) {
      start_idx = group_id < 2 ? 0 : kGroupCardinality * 2;
      cnt = kGroupCardinality * 2;
    } else {
      assert(cur_group_gran == gran_full);
      start_idx = 0;
      cnt = kInternalCardinality;
    }
    // find one more in previous group
    --start_idx;
    ++cnt;
    InternalEntry *entries = page->records + start_idx;
    int idx = -1;
    for (int i = 0; i < cnt; ++i) {
      if (entries[i].ptr != GlobalAddress::Null()) {
        if (k >= entries[i].key) {
          if (idx == -1 || entries[i].key > entries[idx].key) {
            idx = i;
          }
        }
      }
    }
    if (idx != -1) {
      *addr = entries[idx].ptr;
    } else {
      *addr = GlobalAddress::Null();
    }
    if (entry->ptr && *addr != GlobalAddress::Null()) {
      *parent_addr = page->hdr.my_addr;
      return entry;
    }
  }

  return nullptr;
}

inline void IndexCache::search_range_from_cache(
    const Key &from, const Key &to, std::vector<InternalPage *> &result) {
  CacheSkipList::Iterator iter(skiplist);

  result.clear();
  CacheEntry e;
  e.from = from;
  e.to = from;
  iter.Seek((char *)&e);

  while (iter.Valid()) {
    auto val = (const CacheEntry *)iter.key();
    if (val->ptr) {
      if (val->from > to) {
        return;
      }
      if (result.size() == 0 ||
          (result.back()->hdr.lowest < val->ptr->hdr.lowest &&
           result.back()->hdr.highest > val->ptr->hdr.highest)) {
        result.push_back(val->ptr);
      }
    }
    iter.Next();
  }
}

inline bool IndexCache::invalidate(const CacheEntry *entry, int thread_id) {
  auto ptr = entry->ptr;

  if (ptr == nullptr) {
    return false;
  }

  if (__sync_bool_compare_and_swap(&(entry->ptr), ptr, 0)) {
    int cnt = 0;
    for (int i = 0; i < kMaxInternalGroup; ++i) {
      cnt += ptr->hdr.grp_in_cache[i];
    }
    delay_free_lists[thread_id].lock.wLock();
    delay_free_lists[thread_id].list.push_back(
        std::make_pair(ptr, asm_rdtsc()));
    delay_free_lists[thread_id].lock.wUnlock();
    free_page_cnt.fetch_add(cnt);
    return true;
  }

  return false;
}

inline bool IndexCache::invalidate(const Key &from, const Key &to,
                                   int thread_id) {
  auto e = find_entry(from, to);
  if (e && e->from == from && e->to == to - 1) {
    return invalidate(e, thread_id);
  }
  return false;
}

inline const CacheEntry *IndexCache::get_a_random_entry(uint64_t &freq) {
  uint32_t seed = asm_rdtsc();
retry:
  auto k = rand_r(&seed) % max_key.load(std::memory_order_relaxed);
  CacheSkipList::Iterator iter(skiplist);
  CacheEntry tmp;
  tmp.from = k;
  tmp.to = k;
  iter.Seek((char *)&tmp);

  while (iter.Valid()) {
    CacheEntry *e = (CacheEntry *)iter.key();
    InternalPage *ptr = e ? e->ptr : nullptr;
    if (ptr) {
      freq = ptr->hdr.index_cache_freq;
      if (e->ptr == ptr) {
        return e;
      }
    }
    iter.Next();
  }
  goto retry;
}

inline void IndexCache::evict_one(int thread_id) {
  uint64_t freq1, freq2;
  auto e1 = get_a_random_entry(freq1);
  auto e2 = get_a_random_entry(freq2);

  if (freq1 < freq2) {
    invalidate(e1, thread_id);
  } else {
    invalidate(e2, thread_id);
  }
}

inline void IndexCache::statistics() {
  printf("[skiplist node: %ld]  [page cache: %ld]\n", skiplist_node_cnt.load(),
         all_page_cnt - free_page_cnt.load());
}

inline void IndexCache::bench() {
  Timer t;
  t.begin();
  const int loop = 100000;

  for (int i = 0; i < loop; ++i) {
    uint64_t r = rand() % (5 * define::MB);
    this->find_entry(r);
  }

  t.end_print(loop);
}


void IndexCache::free_delay() {
  std::list<std::pair<void *, uint64_t>> local_list;

  while (!delay_free_stop_flag.load(std::memory_order_acquire)) {
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
      delay_free_lists[i].lock.wLock();
      local_list.splice(local_list.end(), delay_free_lists[i].list);
      delay_free_lists[i].lock.wUnlock();
    }
    thread_status->rcu_barrier();
    auto it = local_list.begin();
    for (; it != local_list.end(); ++it) {
      if (asm_rdtsc() - it->second > 5000ul * 10) {
        free(it->first);
      } else {
        break;
      }
    }
    local_list.erase(local_list.begin(), it);  // erase not include it
    usleep(5);
  }
}
