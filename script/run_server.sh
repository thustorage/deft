#! /bin/bash -e

NUMA_AFFINITY=0
APP_NAME=server

../script/restartMemc.sh
sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"

numactl --membind=${NUMA_AFFINITY} --cpunodebind=${NUMA_AFFINITY} \
./${APP_NAME} "$@"
