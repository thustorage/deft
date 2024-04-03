#! /bin/bash

NUMA_AFFINITY=1
APP_NAME=client

numactl --membind=${NUMA_AFFINITY} --cpunodebind=${NUMA_AFFINITY} \
./${APP_NAME} "$@" "--numa_id=${NUMA_AFFINITY}"

if [[ $? -ne 0 ]]; then
  echo "client failed"
  ../script/killall.sh
fi
