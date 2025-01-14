#! /bin/bash -e

source ../script/global_config.sh

NUMA_AFFINITY=0
APP_NAME=client

num_clients=$4
num_threads=$6

for ((i=1; i < ${num_clients}; i++)); do
  ip=${clients[$i]}
  echo "issue client ${i} ${ip} $@"
  sshpass -p ${server_passwd} ssh ${ip} "cd ${exe_path} && ../script/run_client.sh $@ &> ../log/client_${i}.log" &
  sleep 1s
done

# wait for ssh command to finish
wait
