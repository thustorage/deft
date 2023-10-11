#!/usr/bin/python3

import os
import sys
import subprocess
import time
from time import gmtime, strftime
from subprocess import Popen

if not os.path.exists('../result'):
	os.makedirs('../result')
if not os.path.exists('../log'):
	os.makedirs('../log')

def get_res_name(s):
    postfix = ""
    if len(sys.argv) > 1:
        postfix = sys.argv[1]
    return '../result/' + s + "-" + postfix + strftime("-%m-%d-%H-%M", gmtime())  + ".txt"

# async
def exec_command(command):
    return subprocess.Popen(command, shell=True)


# start benchmark

subprocess.run(f'make -j', shell=True)

total_threads_arr = [i for i in range(15, 151, 15)]
print(total_threads_arr)

num_core_per_server = 18
num_servers = 1
num_clients = 5

file_name = get_res_name("bench")
with open(file_name, 'w') as fp:
    for total_threads in total_threads_arr:
        num_threads = (total_threads // num_clients)

        print(f'start: {total_threads} {num_clients} {num_threads}')
        fp.write(f'total_threads: {total_threads} num_servers: {num_servers} num_clients: {num_clients} num_threads: {num_threads}\n')
        fp.flush()

        server_subproc = exec_command(f'bash ../script/start_servers.sh --server_count {num_servers} --client_count {num_clients}')

        time.sleep(2)

        num_0_threads = total_threads - (num_clients - 1) * num_threads
        client_0_subproc = exec_command(f'bash ../script/start_client0.sh --server_count {num_servers} --client_count {num_clients} --thread_count {num_0_threads}')

        time.sleep(2)

        client_other_subproc = exec_command(f'bash ../script/start_clients_other.sh --server_count {num_servers} --client_count {num_clients} --thread_count {num_threads}')

        server_subproc.wait()
        client_0_subproc.wait()
        client_other_subproc.wait()

        loading_subproc = subprocess.run(f'grep "Loading Results" ../log/client_0.log', stdout=subprocess.PIPE, shell=True)
        tmp = loading_subproc.stdout.decode("utf-8")
        print(tmp.strip())
        fp.write(tmp)

        res_subproc = subprocess.run(f'grep "Final Results" ../log/client_0.log', stdout=subprocess.PIPE, shell=True)
        tmp = res_subproc.stdout.decode("utf-8")
        print(tmp)
        fp.write(tmp + "\n")
        fp.flush()

        subprocess.run(f'../script/killall.sh', stdout=subprocess.DEVNULL, shell=True)

        time.sleep(10)
