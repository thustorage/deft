#! /bin/bash

source ../script/global_config.sh

CLIENT_NAME=client
SERVER_NAME=server

num_servers=${#clients[@]}

# killall if crash
for ((i=0; i < ${num_servers}; i++)); do
  ip=${clients[$i]}
  echo "killall ${ip}"
  sshpass -p ${server_passwd} ssh ${ip} "killall ${CLIENT_NAME} &> /dev/null; killall ${SERVER_NAME} &> /dev/null"
done
