#define STRIP_FLAG_HELP 1    // this must go before the #include!
#include <gflags/gflags.h>
#include "Timer.h"
#include "Tree.h"
#include "zipf.h"

#include <city.h>
#include <stdlib.h>
#include <thread>
#include <time.h>
#include <unistd.h>
#include <vector>
#include <random>


//////////////////// workload parameters /////////////////////

// #define USE_CORO
const int kCoroCnt = 3;

// #define BENCH_LOCK
// #define YCSB_D
// #define YCSB_E

#if defined(USE_CORO) && defined(YCSB_E)
#error "USE_CORO and YCSB_E cannot be defined at the same time"
#endif

DEFINE_int32(numa_id, 0, "numa node id");
DEFINE_int32(server_count, 1, "server count");
DEFINE_int32(client_count, 1, "client count");
// DEFINE_int32(thread_count, 1, "thread count");
DEFINE_int32(num_prefill_threads, 1, "prefill thread");
DEFINE_int32(num_bench_threads, 1, "bench thread");
DEFINE_int32(read_ratio, 50, "read ratio");
DEFINE_int32(key_space, 200 * 1e6, "key space");
DEFINE_int32(ops_per_thread, 10 * 1e6, "ops per thread");
DEFINE_double(prefill_ratio, 1, "prefill ratio");
DEFINE_double(zipf, 0.99, "zipf");

constexpr double YCSBD_READ_RANGE = 0.02;
constexpr int NUM_WARMUP_OPS = 1e6;

//////////////////// workload parameters /////////////////////

constexpr int MEASURE_SAMPLE = 32;

std::thread th[MAX_APP_THREAD];
uint64_t tp[MAX_APP_THREAD][8];
uint64_t total_time[MAX_APP_THREAD][8];
double prefill_tp = 0.;
double prefill_lat = 0.;
int MAX_TOTAL_THREADS = 0;

Tree *tree;
DSMClient *dsm_client;

inline Key to_key(uint64_t k) {
  return (CityHash64((char *)&k, sizeof(k))) % FLAGS_key_space + 1;
}

class RequsetGenBench : public RequstGen {
 public:
  RequsetGenBench(int coro_id, DSMClient *dsm_client, int thread_id)
      : coro_id(coro_id), dsm_client_(dsm_client), thread_id_(thread_id) {
    //  seed = rdtsc();
    seed = (dsm_client->get_my_client_id() << 10) + (thread_id_ << 5) + coro_id;
    mehcached_zipf_init(&state, FLAGS_key_space, FLAGS_zipf, seed);
  }

  Request next() override {
    Request r;

    r.is_search = rand_r(&seed) % 100 < FLAGS_read_ratio;
#ifdef YCSB_D
    uint64_t window = FLAGS_key_space * YCSBD_READ_RANGE;
    uint64_t all_thread =
        FLAGS_num_bench_threads * dsm_client_->get_client_size();
    uint64_t my_id =
        FLAGS_num_bench_threads * dsm_client_->get_my_client_id() + thread_id_;
    uint64_t cur_key = FLAGS_key_space * FLAGS_prefill_ratio + window + my_id;

    if (r.is_search) {
      r.k = to_key(
          (cur_key + FLAGS_key_space - all_thread - rand_r(&seed) % window) %
          FLAGS_key_space);
    } else {
      r.k = to_key(cur_key);
      cur_key = (cur_key + all_thread) % FLAGS_key_space;
    }
    r.v = 23 + cur_key + seed;
#else
    uint64_t dis = mehcached_zipf_next(&state);
    r.k = to_key(dis);
    r.v = 23 + dis + seed;
#endif

    return r;
  }

 private:
  int coro_id;
  DSMClient *dsm_client_;
  int thread_id_;

  unsigned int seed;
  struct zipf_gen_state state;
};

RequstGen *coro_func(int coro_id, DSMClient *dsm_client, int id) {
  return new RequsetGenBench(coro_id, dsm_client, id);
}

#ifdef YCSB_D
void ycsb_d(int id) {
  uint32_t seed = (dsm_client->get_my_client_id() << 10) + id;
  uint64_t window = FLAGS_key_space * YCSBD_READ_RANGE;
  uint64_t all_thread = FLAGS_num_bench_threads * dsm_client->get_client_size();
  uint64_t my_id =
      FLAGS_num_bench_threads * dsm_client->get_my_client_id() + id;
  uint64_t cur_key = FLAGS_key_space * FLAGS_prefill_ratio + window + my_id;

  Timer timer, total_timer;
  total_timer.begin();
  for (int i = 0; i < FLAGS_ops_per_thread; ++i) {
    Value v;
    bool measure_lat = i % MEASURE_SAMPLE == 0;
    if (measure_lat) {
      timer.begin();
    }
    if (rand_r(&seed) % 100 < FLAGS_read_ratio) {
      uint64_t k =
          (cur_key + FLAGS_key_space - all_thread - rand_r(&seed) % window) %
          FLAGS_key_space;
      tree->search(to_key(k), v);
    } else {
      v = 12;
      tree->insert(to_key(cur_key), v);
      cur_key = (cur_key + all_thread) % FLAGS_key_space;
    }
    if (measure_lat) {
      auto t = timer.end();
      stat_helper.add(id, lat_op, t);
    }
  }
  total_time[id][0] = total_timer.end();
}
#endif


std::atomic<int64_t> prefill_cnt{0};
void thread_run(int id) {

  // bindCore(id);

  dsm_client->RegisterThread();

#ifndef BENCH_LOCK
  uint64_t my_id = MAX_TOTAL_THREADS * dsm_client->get_my_client_id() + id;
  printf("I am thread %ld on compute nodes\n", my_id);
  // uint64_t all_thread = MAX_TOTAL_THREADS * dsm_client->get_client_size();
  uint64_t all_prefill_threads =
      FLAGS_num_prefill_threads * dsm_client->get_client_size();

  Timer timer;
  Timer total_timer;
  // prefill
  if (id < FLAGS_num_prefill_threads) {
    uint64_t end_prefill_key = FLAGS_prefill_ratio * FLAGS_key_space;
    uint64_t begin_prefill_key = my_id;
    std::vector<uint64_t> prefill_keys;
    prefill_keys.reserve(
        (end_prefill_key - begin_prefill_key) / all_prefill_threads + 1);
    for (uint64_t i = begin_prefill_key; i < end_prefill_key;
         i += all_prefill_threads) {
      prefill_keys.push_back(i + 1);
    }

    std::shuffle(prefill_keys.begin(), prefill_keys.end(),
                 std::default_random_engine(my_id));
    total_timer.begin();
    for (size_t i = 0; i < prefill_keys.size(); ++i) {
      bool measure_lat = i % MEASURE_SAMPLE == 0;
      if (measure_lat) {
        timer.begin();
      }
      tree->insert(to_key(prefill_keys[i]), prefill_keys[i] * 2);
      if (measure_lat) {
        auto t = timer.end();
        stat_helper.add(id, lat_op, t);
      }
      // if (id == 0 && i % 100000 == 0) {
      //   printf("prefill %ld\n", i);
      //   fflush(stdout);
      // }
    }
#ifdef YCSB_D
    uint64_t window = FLAGS_key_space * YCSBD_READ_RANGE;
    for (uint64_t i = end_prefill_key + my_id; i < end_prefill_key + window;
         i += all_prefill_threads) {
      tree->insert(to_key(i), i * 2);
    }
#endif
    total_time[id][0] = total_timer.end();
    total_time[id][1] = prefill_keys.size();
  }
  prefill_cnt.fetch_add(1);

  if (id == 0) {
    while (prefill_cnt.load() != MAX_TOTAL_THREADS)
      ;
    printf("node %d finish\n", dsm_client->get_my_client_id());
    // dsm->barrier("prefill_finish");

    printf("prefill time %lds\n", total_time[0][0] / 1000 / 1000 / 1000);
    prefill_tp = 0.;
    uint64_t hit = 0;
    uint64_t all = 0;
    for (int i = 0; i < FLAGS_num_prefill_threads; ++i) {
      prefill_tp += (double)total_time[i][1] * 1e3 / total_time[i][0];  // Mops
      hit += cache_hit[i][0];
      all += (cache_hit[i][0] + cache_miss[i][0]);
    }
    uint64_t stat_lat[lat_end];
    uint64_t stat_cnt[lat_end];
    for (int k = 0; k < lat_end; k++) {
      stat_lat[k] = 0;
      stat_cnt[k] = 0;
      for (int i = 0; i < MAX_APP_THREAD; ++i) {
        stat_lat[k] += stat_helper.latency_[i][k];
        stat_helper.latency_[i][k] = 0;
        stat_cnt[k] += stat_helper.counter_[i][k];
        stat_helper.counter_[i][k] = 0;
      }
    }
    prefill_lat = (double)stat_lat[lat_op] / stat_cnt[lat_op];
    printf("Load Tp %.4lf Mops/s Lat %.1lf\n", prefill_tp, prefill_lat);
    printf("cache hit rate: %.1lf%%\n", hit * 100.0 / all);
    printf("avg lock latency: %.1lf\n",
           (double)stat_lat[lat_lock] / stat_cnt[lat_lock]);
    printf("avg read page latency: %.1lf\n",
           (double)stat_lat[lat_read_page] / stat_cnt[lat_read_page]);
    printf("avg write page latency: %.1lf\n",
           (double)stat_lat[lat_write_page] / stat_cnt[lat_write_page]);
    // printf("avg crc latency: %.1lf\n",
    //        (double)stat_lat[lat_crc] / stat_cnt[lat_crc]);
    fflush(stdout);

    tree->index_cache_statistics();
    tree->clear_statistics();
    memset(reinterpret_cast<void *>(&stat_helper), 0, sizeof(stat_helper));

    prefill_cnt.store(0);
  }

  while (prefill_cnt.load() != 0)
    ;

#endif  // ndef BENCH_LOCK

  // bench
  if (id >= FLAGS_num_bench_threads) {
    return;
  }
#ifdef USE_CORO

#ifdef BENCH_LOCK
  bool lock_bench = true;
#else
  bool lock_bench = false;
#endif
  tree->run_coroutine(coro_func, id, kCoroCnt, lock_bench, NUM_WARMUP_OPS);
  total_timer.begin();
  tree->run_coroutine(coro_func, id, kCoroCnt, lock_bench,
                      FLAGS_ops_per_thread);
  total_time[id][0] = total_timer.end();

#else

#ifdef YCSB_D
  // ycsb-d
  ycsb_d(id);
  return;
#endif

  /// without coro
  // uint32_t seed = rdtsc();
  uint32_t seed = (dsm_client->get_my_client_id() << 10) + id;
  struct zipf_gen_state state;
  mehcached_zipf_init(&state, FLAGS_key_space, FLAGS_zipf, seed);

  // warmup
  for (int i = 0; i < NUM_WARMUP_OPS; ++i) {
    uint64_t dis = mehcached_zipf_next(&state);
    uint64_t key = to_key(dis);
    Value v;
    if (rand_r(&seed) % 100 < FLAGS_read_ratio) {
      tree->search(key, v);
    } else {
      v = 12;
      tree->insert(key, v);
    }
  }

  total_timer.begin();
  for (int i = 0; i < FLAGS_ops_per_thread; ++i) {
    uint64_t dis = mehcached_zipf_next(&state);
    uint64_t key = to_key(dis);

    bool measure_lat = i % MEASURE_SAMPLE == 0;
    if (measure_lat) {
      timer.begin();
    }

#ifdef BENCH_LOCK
    tree->lock_bench(key);
#else
    Value v;
    if (rand_r(&seed) % 100 < FLAGS_read_ratio) {  // GET
#ifdef YCSB_E
      Value buffer[101];
      tree->range_query(key, key + 100, buffer, 100);
#else
      tree->search(key, v);
#endif
    } else {
      v = 12;
      tree->insert(key, v);
      // tree->lock_bench(key);
    }
#endif
    if (measure_lat) {
      auto t = timer.end();
      stat_helper.add(id, lat_op, t);
    }

    // tp[id][0]++;
  }
  total_time[id][0] = total_timer.end();
#endif
}

void print_args() {
  printf(
      "ServerCount %d, ClientCount %d, PrefillThreadCount %d, BenchThreadCount "
      "%d, ReadRatio %d, "
      "Zipfan %.3lf\n",
      FLAGS_server_count, FLAGS_client_count, FLAGS_num_prefill_threads,
      FLAGS_num_bench_threads, FLAGS_read_ratio, FLAGS_zipf);
}


int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  print_args();

#ifdef YCSB_D
  FLAGS_prefill_ratio = 0.90;
#endif

  DSMConfig config;
  config.rnic_id = FLAGS_numa_id;
  config.num_server = FLAGS_server_count;
  config.num_client = FLAGS_client_count;
  dsm_client = DSMClient::GetInstance(config);

  dsm_client->RegisterThread();
  tree = new Tree(dsm_client);

#ifndef BENCH_LOCK
  if (dsm_client->get_my_client_id() == 0) {
    tree->insert(to_key(0), 1);
    for (uint64_t i = 1; i < 1000000; ++i) {
      tree->insert(to_key(i), i * 2);
    }
  }
#endif

  dsm_client->Barrier("benchmark");

  MAX_TOTAL_THREADS =
      std::max(FLAGS_num_prefill_threads, FLAGS_num_bench_threads);
  for (int i = 1; i < MAX_TOTAL_THREADS; i++) {
    th[i] = std::thread(thread_run, i);
  }
  thread_run(0);

  // finish

  for (int i = 1; i < MAX_TOTAL_THREADS; i++) {
    if (th[i].joinable()) {
      th[i].join();
    }
  }
  delete tree;

  uint64_t cluster_prefill_tp = dsm_client->Sum((uint64_t)(prefill_tp * 1000));

  double all_tp = 0.;
  uint64_t hit = 0;
  uint64_t all = 0;
  for (int i = 0; i < FLAGS_num_bench_threads; ++i) {
    all_tp += (double)FLAGS_ops_per_thread * 1e3 / total_time[i][0];  // Mops
    hit += cache_hit[i][0];
    all += (cache_hit[i][0] + cache_miss[i][0]);
  }

  uint64_t stat_lat[lat_end];
  uint64_t stat_cnt[lat_end];
  for (int k = 0; k < lat_end; k++) {
    stat_lat[k] = 0;
    stat_cnt[k] = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
      stat_lat[k] += stat_helper.latency_[i][k];
      stat_helper.latency_[i][k] = 0;
      stat_cnt[k] += stat_helper.counter_[i][k];
      stat_helper.counter_[i][k] = 0;
    }
  }

  uint64_t cluster_tp = dsm_client->Sum((uint64_t)(all_tp * 1000));
  printf("CN: %d, throughput %.4f Mops/s\n", dsm_client->get_my_client_id(),
         all_tp);

  if (dsm_client->get_my_client_id() == 0) {
    printf("Loading Results TP: %.3lf Mops/s Lat: %.3lf us\n",
           cluster_prefill_tp / 1000.0, prefill_lat / 1000.0);
    printf("cluster throughput %.3f Mops/s\n", cluster_tp / 1000.0);
    printf("avg op latency: %.1lf\n",
           (double)stat_lat[lat_op] / stat_cnt[lat_op]);
    printf("cache hit rate: %.1lf%%\n", hit * 100.0 / all);
    printf("avg lock latency: %.1lf\n",
           (double)stat_lat[lat_lock] / stat_cnt[lat_lock]);
    printf("avg read page latency: %.1lf\n",
           (double)stat_lat[lat_read_page] / stat_cnt[lat_read_page]);
    printf("avg write page latency: %.1lf\n",
           (double)stat_lat[lat_write_page] / stat_cnt[lat_write_page]);
    printf("avg crc latency: %.1lf\n",
           (double)stat_lat[lat_crc] / stat_cnt[lat_crc]);
    printf(
        "%d avg internal page search latency: %.1lf\n",
        dsm_client->get_my_client_id(),
        (double)stat_lat[lat_internal_search] / stat_cnt[lat_internal_search]);
    // printf("%d avg cache search latency: %.1lf\n",
    //        dsm_client->get_my_client_id(),
    //        (double)stat_lat[lat_cache_search] / stat_cnt[lat_cache_search]);
    printf("Final Results: TP: %.3lf Mops/s Lat: %.3lf us\n",
           cluster_tp / 1000.0,
           (double)stat_lat[lat_op] / stat_cnt[lat_op] / 1000.0);
  }

  if (dsm_client->get_my_client_id() == 0) {
    RawMessage m;
    m.type = RpcType ::TERMINATE;
    for (uint32_t i = 0; i < config.num_server; ++i) {
      dsm_client->RpcCallDir(m, i);
    }
  }
  return 0;
}
