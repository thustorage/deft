#! /bin/bash

source ../script/global_config.sh

CLIENT_NAME=client
SERVER_NAME=server

num_servers=${#clients[@]}

# hugepage
for ((i=0; i < ${num_servers}; i++)); do
  ip=${clients[$i]}
  echo "hugepage ${ip}"
  sshpass -p ${server_passwd} ssh ${ip} 'sudo sh -c "echo 32768 > /sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages"'
  sshpass -p ${server_passwd} ssh ${ip} 'sudo sh -c "echo 32768 > /sys/devices/system/node/node1/hugepages/hugepages-2048kB/nr_hugepages"'
done
