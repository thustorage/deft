#! /bin/bash -e

NUMA_AFFINITY=0
APP_NAME=client

numactl --membind=${NUMA_AFFINITY} --cpunodebind=${NUMA_AFFINITY} \
./${APP_NAME} "$@"
