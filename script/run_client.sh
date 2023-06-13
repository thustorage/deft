#! /bin/bash

NUMA_AFFINITY=0
APP_NAME=client

numactl --membind=${NUMA_AFFINITY} --cpunodebind=${NUMA_AFFINITY} \
./${APP_NAME} "$@"

if [[ $? -ne 0 ]]; then
  echo "client failed"
  ../script/killall.sh
fi
