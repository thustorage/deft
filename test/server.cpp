#include "Timer.h"
#include "Tree.h"
#include "zipf.h"
#include "dsm_server.h"

#include <city.h>
#include <stdlib.h>
#include <thread>
#include <time.h>
#include <unistd.h>
#include <vector>


//////////////////// workload parameters /////////////////////

int kServerCount;
int kClientCount;
int kThreadCount;
// int kReadRatio;
// double zipfan = 0;

//////////////////// workload parameters /////////////////////


DSMServer *dsm_server;

void parse_args(int argc, char *argv[]) {
  if (argc < 4) {
    printf("Usage: ./server kServerCount kClientCount kThreadCount\n");
    exit(-1);
  }

  kServerCount = atoi(argv[1]);
  kClientCount = atoi(argv[2]);
  kThreadCount = atoi(argv[3]);
  // kReadRatio = atoi(argv[4]);
  // zipfan = atof(argv[5]);

  printf("kServerCount %d, kClientCount %d, kThreadCount %d\n", kServerCount,
         kClientCount, kThreadCount);
}

int main(int argc, char *argv[]) {

  parse_args(argc, argv);

  DSMConfig config;
  config.num_server = kServerCount;
  config.num_client = kClientCount;
  dsm_server = DSMServer::GetInstance(config);

  while (true)
    ;

  return 0;
}
