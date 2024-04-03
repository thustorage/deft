#! /bin/bash -e

source ../script/global_config.sh

NUMA_AFFINITY=0
APP_NAME=client

num_threads=$6

ip=${clients[0]}
echo "issue client 0 ${ip} $@"
sshpass -p ${server_passwd} ssh ${ip} "cd ${exe_path} && ../script/run_client.sh $@ &> ../log/client_0.log"
