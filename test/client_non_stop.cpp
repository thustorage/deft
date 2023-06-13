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


//////////////////// workload parameters /////////////////////

// #define USE_CORO
const int kCoroCnt = 3;

// #define BENCH_LOCK

DEFINE_int32(server_count, 1, "server count");
DEFINE_int32(client_count, 1, "client count");
DEFINE_int32(thread_count, 1, "thread count");
DEFINE_int32(read_ratio, 50, "read ratio");
DEFINE_int32(key_space, 200 * 1e6, "key space");
DEFINE_double(warm_ratio, 1, "warm ratio");
DEFINE_double(zipf, 0, "zipf");

//////////////////// workload parameters /////////////////////


std::thread th[MAX_APP_THREAD];
uint64_t tp[MAX_APP_THREAD][8];

uint64_t latency_th_all[LATENCY_WINDOWS];

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

    tp[thread_id_][0]++;

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

Timer bench_timer;
std::atomic<int64_t> warmup_cnt{0};
std::atomic_bool ready{false};
void thread_run(int id) {

  // bindCore(id);

  dsm_client->RegisterThread();

#ifndef BENCH_LOCK
  uint64_t my_id = FLAGS_thread_count * dsm_client->get_my_client_id() + id;
  printf("I am thread %ld on compute nodes\n", my_id);
  uint64_t all_thread = FLAGS_thread_count * dsm_client->get_client_size();

  if (id == 0) {
    bench_timer.begin();
  }

  uint64_t end_warm_key = FLAGS_warm_ratio * FLAGS_key_space;
  uint64_t begin_warm_key;
  if (my_id == 0) {
    begin_warm_key = all_thread;
  } else {
    begin_warm_key = my_id;
  }
  for (uint64_t i = begin_warm_key; i < end_warm_key; i += all_thread) {
    tree->insert(to_key(i), i * 2);
  }

  warmup_cnt.fetch_add(1);

  if (id == 0) {
    while (warmup_cnt.load() != FLAGS_thread_count)
      ;
    printf("node %d finish\n", dsm_client->get_my_client_id());
    // dsm->barrier("warm_finish");

    uint64_t ns = bench_timer.end();
    printf("warmup time %lds\n", ns / 1000 / 1000 / 1000);

    tree->index_cache_statistics();
    tree->clear_statistics();

    ready = true;

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
  tree->run_coroutine(coro_func, id, kCoroCnt, lock_bench);

#else

  /// without coro
  // uint32_t seed = rdtsc();
  uint32_t seed = (dsm_client->get_my_client_id() << 10) + id;
  struct zipf_gen_state state;
  mehcached_zipf_init(&state, FLAGS_key_space, FLAGS_zipf, seed);

  Timer timer;
  while (true) {
    uint64_t dis = mehcached_zipf_next(&state);
    uint64_t key = to_key(dis);

    timer.begin();

#ifdef BENCH_LOCK
    tree->lock_bench(key);
#else
    Value v;
    if (rand_r(&seed) % 100 < FLAGS_read_ratio) {  // GET
      tree->search(key, v);
    } else {
      v = 12;
      tree->insert(key, v);
      // tree->lock_bench(key);
    }
#endif
    auto t = timer.end();
    auto us_10 = t / 100;

    if (us_10 >= LATENCY_WINDOWS) {
      us_10 = LATENCY_WINDOWS - 1;
    }
    latency[id][us_10]++;
    stat_helper.add(id, lat_op, t);

    tp[id][0]++;
  }
#endif
}

void print_args() {
  printf(
      "ServerCount %d, ClientCount %d, ThreadCount %d, ReadRatio %d, "
      "Zipfan %.3lf\n",
      FLAGS_server_count, FLAGS_client_count, FLAGS_thread_count,
      FLAGS_read_ratio, FLAGS_zipf);
}

void cal_latency() {
  uint64_t all_lat = 0;
  for (int i = 0; i < LATENCY_WINDOWS; ++i) {
    latency_th_all[i] = 0;
    for (int k = 0; k < MAX_APP_THREAD; ++k) {
      latency_th_all[i] += latency[k][i];
    }
    all_lat += latency_th_all[i];
  }

  uint64_t th50 = all_lat / 2;
  uint64_t th90 = all_lat * 9 / 10;
  uint64_t th95 = all_lat * 95 / 100;
  uint64_t th99 = all_lat * 99 / 100;
  uint64_t th999 = all_lat * 999 / 1000;

  uint64_t cum = 0;
  for (int i = 0; i < LATENCY_WINDOWS; ++i) {
    cum += latency_th_all[i];

    if (cum >= th50) {
      printf("p50 %f\t", i / 10.0);
      th50 = -1;
    }
    if (cum >= th90) {
      printf("p90 %f\t", i / 10.0);
      th90 = -1;
    }
    if (cum >= th95) {
      printf("p95 %f\t", i / 10.0);
      th95 = -1;
    }
    if (cum >= th99) {
      printf("p99 %f\t", i / 10.0);
      th99 = -1;
    }
    if (cum >= th999) {
      printf("p999 %f\n", i / 10.0);
      th999 = -1;
      return;
    }
  }
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
    for (uint64_t i = 1; i < 1024000; ++i) {
      tree->insert(to_key(i), i * 2);
    }
  }
#endif

  dsm_client->Barrier("benchmark");
  dsm_client->ResetThread();

  for (int i = 0; i < FLAGS_thread_count; i++) {
    th[i] = std::thread(thread_run, i);
  }

#ifndef BENCH_LOCK
  while (!ready.load())
    ;
#endif

  timespec s, e;
  uint64_t pre_tp = 0;

  // int count = 0;

  clock_gettime(CLOCK_REALTIME, &s);
  while (true) {

    sleep(2);
    clock_gettime(CLOCK_REALTIME, &e);
    int microseconds = (e.tv_sec - s.tv_sec) * 1000000 +
                       (double)(e.tv_nsec - s.tv_nsec) / 1000;

    uint64_t all_tp = 0;
    for (int i = 0; i < FLAGS_thread_count; ++i) {
      all_tp += tp[i][0];
    }
    uint64_t cap = all_tp - pre_tp;
    pre_tp = all_tp;

    uint64_t all = 0;
    uint64_t hit = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
      all += (cache_hit[i][0] + cache_miss[i][0]);
      hit += cache_hit[i][0];
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

    clock_gettime(CLOCK_REALTIME, &s);

    // if (++count % 3 == 0 && dsm->getMyNodeID() == 0) {
    //   cal_latency();
    // }

    double per_node_tp = cap * 1.0 / microseconds;
    // uint64_t cluster_tp = dsm->sum((uint64_t)(per_node_tp * 1000));
    uint64_t cluster_tp = dsm_client->Sum((uint64_t)(per_node_tp * 1000));

    printf("%d, throughput %.4f\n", dsm_client->get_my_client_id(),
           per_node_tp);

    if (dsm_client->get_my_client_id() == 0) {
      printf("cluster throughput %.3f\n", cluster_tp / 1000.0);
      printf("cache hit rate: %lf\n", hit * 1.0 / all);
      printf("%d avg op latency: %.1lf\n", dsm_client->get_my_client_id(),
             (double)stat_lat[lat_op] / stat_cnt[lat_op]);
      printf("%d avg lock latency: %.1lf\n", dsm_client->get_my_client_id(),
             (double)stat_lat[lat_lock] / stat_cnt[lat_lock]);
      printf("%d avg read page latency: %.1lf\n",
             dsm_client->get_my_client_id(),
             (double)stat_lat[lat_read_page] / stat_cnt[lat_read_page]);
      printf("%d avg write page latency: %.1lf\n",
             dsm_client->get_my_client_id(),
             (double)stat_lat[lat_write_page] / stat_cnt[lat_write_page]);
      // printf("%d avg internal page search latency: %.1lf\n",
      //        dsm_client->get_my_client_id(),
      //        (double)stat_lat[lat_internal_search] /
      //            stat_cnt[lat_internal_search]);
      // printf("%d avg cache search latency: %.1lf\n",
      //        dsm_client->get_my_client_id(),
      //        (double)stat_lat[lat_cache_search] / stat_cnt[lat_cache_search]);
    }
  }

  return 0;
}
