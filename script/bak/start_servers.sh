#! /bin/bash -e

source ../script/global_config.sh

NUMA_AFFINITY=0
APP_NAME=server

num_server=$2
num_client=$4

../script/restartMemc.sh

for ((i=0; i < ${num_server}; i++)); do
  ip=${servers[$i]}
  sshpass -p ${server_passwd} ssh ${ip} "cd ${exe_path} && ../script/run_server.sh $@ &> ../log/server_${i}.log" &
  sleep 1s
done

# wait for ssh command to finish
wait
