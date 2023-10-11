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

DEFINE_int32(server_count, 1, "server count");
DEFINE_int32(client_count, 1, "client count");
DEFINE_int32(thread_count, 1, "thread count");
DEFINE_int32(read_ratio, 50, "read ratio");
DEFINE_int32(key_space, 200 * 1e6, "key space");
DEFINE_int32(ops_per_thread, 10 * 1e6, "ops per thread");
DEFINE_double(warm_ratio, 1, "warm ratio");
DEFINE_double(zipf, 0.99, "zipf");

//////////////////// workload parameters /////////////////////

constexpr int MEASURE_SAMPLE = 32;

std::thread th[MAX_APP_THREAD];
uint64_t tp[MAX_APP_THREAD][8];
uint64_t total_time[MAX_APP_THREAD][8];
double warmup_tp = 0.;
double warmup_lat = 0.;

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
    uint64_t dis = mehcached_zipf_next(&state);

    r.k = to_key(dis);
    r.v = 23;
    r.is_search = rand_r(&seed) % 100 < FLAGS_read_ratio;

    // tp[thread_id_][0]++;

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
  uint64_t window = FLAGS_key_space * 0.01;
  uint64_t all_thread = FLAGS_thread_count * dsm_client->get_client_size();
  uint64_t my_id = FLAGS_thread_count * dsm_client->get_my_client_id() + id;
  uint64_t cur_key = FLAGS_key_space * FLAGS_warm_ratio + window + my_id;

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


std::atomic<int64_t> warmup_cnt{0};
void thread_run(int id) {

  // bindCore(id);

  dsm_client->RegisterThread();

#ifndef BENCH_LOCK
  uint64_t my_id = FLAGS_thread_count * dsm_client->get_my_client_id() + id;
  printf("I am thread %ld on compute nodes\n", my_id);
  uint64_t all_thread = FLAGS_thread_count * dsm_client->get_client_size();

  Timer timer;
  Timer total_timer;
  // warmup
  { // to free vector automatically
    uint64_t end_warm_key = FLAGS_warm_ratio * FLAGS_key_space;
    uint64_t begin_warm_key = my_id;
    // if (my_id == 0) {
    //   begin_warm_key = all_thread;
    // } else {
    //   begin_warm_key = my_id;
    // }
    // for (uint64_t i = begin_warm_key; i < end_warm_key; i += all_thread) {
    //   tree->insert(to_key(i), i * 2);
    // }
    std::vector<uint64_t> warm_keys;
    warm_keys.reserve((end_warm_key - begin_warm_key) / all_thread + 1);
    for (uint64_t i = begin_warm_key; i < end_warm_key; i += all_thread) {
      warm_keys.push_back(i + 1);
    }

    std::shuffle(warm_keys.begin(), warm_keys.end(),
                 std::default_random_engine(my_id));
    total_timer.begin();
    for (size_t i = 0; i < warm_keys.size(); ++i) {
      bool measure_lat = i % MEASURE_SAMPLE == 0;
      if (measure_lat) {
        timer.begin();
      }
      tree->insert(warm_keys[i], warm_keys[i] * 2);
      if (measure_lat) {
        auto t = timer.end();
        stat_helper.add(id, lat_op, t);
      }
    }
#ifdef YCSB_D
    uint64_t window = FLAGS_key_space * 0.01;
    for (uint64_t i = end_warm_key + my_id; i < end_warm_key + window;
         i += all_thread) {
      tree->insert(to_key(i), i * 2);
    }
#endif
    total_time[id][0] = total_timer.end();
    total_time[id][1] = warm_keys.size();
    warmup_cnt.fetch_add(1);
  }

  if (id == 0) {
    while (warmup_cnt.load() != FLAGS_thread_count)
      ;
    printf("node %d finish\n", dsm_client->get_my_client_id());
    // dsm->barrier("warm_finish");

    printf("warmup time %lds\n", total_time[0][0] / 1000 / 1000 / 1000);
    warmup_tp = 0.;
    uint64_t hit = 0;
    uint64_t all = 0;
    for (int i = 0; i < FLAGS_thread_count; ++i) {
      warmup_tp += (double)total_time[i][1] * 1e3 / total_time[i][0];  // Mops
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
    warmup_lat = (double)stat_lat[lat_op] / stat_cnt[lat_op];
    printf("Load Tp %.4lf Mops/s Lat %.1lf\n", warmup_tp, warmup_lat);
    printf("cache hit rate: %.1lf%%\n", hit * 100.0 / all);
    printf("avg lock latency: %.1lf\n",
           (double)stat_lat[lat_lock] / stat_cnt[lat_lock]);
    printf("avg read page latency: %.1lf\n",
           (double)stat_lat[lat_read_page] / stat_cnt[lat_read_page]);
    printf("avg write page latency: %.1lf\n",
           (double)stat_lat[lat_write_page] / stat_cnt[lat_write_page]);
    fflush(stdout);

    tree->index_cache_statistics();
    tree->clear_statistics();
    memset(reinterpret_cast<void *>(&stat_helper), 0, sizeof(stat_helper));

    warmup_cnt.store(0);
  }

  while (warmup_cnt.load() != 0)
    ;

#endif  // ndef BENCH_LOCK

#ifdef USE_CORO

#ifdef BENCH_LOCK
  bool lock_bench = true;
#else
  bool lock_bench = false;
#endif
  tree->run_coroutine(coro_func, id, kCoroCnt, lock_bench,
                      FLAGS_ops_per_thread / 3);
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
    // Value buffer[101];
    if (rand_r(&seed) % 100 < FLAGS_read_ratio) {  // GET
      tree->search(key, v);
      // tree->range_query(key, key + 100, buffer, 100);
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
      "ServerCount %d, ClientCount %d, ThreadCount %d, ReadRatio %d, "
      "Zipfan %.3lf\n",
      FLAGS_server_count, FLAGS_client_count, FLAGS_thread_count,
      FLAGS_read_ratio, FLAGS_zipf);
}


int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  print_args();

  DSMConfig config;
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

  for (int i = 1; i < FLAGS_thread_count; i++) {
    th[i] = std::thread(thread_run, i);
  }
  thread_run(0);

  // finish

  for (int i = 1; i < FLAGS_thread_count; i++) {
    if (th[i].joinable()) {
      th[i].join();
    }
  }
  delete tree;

  uint64_t cluster_warmup_tp = dsm_client->Sum((uint64_t)(warmup_tp * 1000));

  double all_tp = 0.;
  uint64_t hit = 0;
  uint64_t all = 0;
  for (int i = 0; i < FLAGS_thread_count; ++i) {
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
           cluster_warmup_tp / 1000.0, warmup_lat / 1000.0);
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
