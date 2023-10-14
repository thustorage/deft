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

num_core_per_server = 18
num_servers = 1
num_clients = 5

total_threads_arr = [150]
key_space_arr = [200e6]
read_ratio_arr = [50]
zipf_arr = [0.99]

# total_threads_arr = [i for i in range(15, 151, 15)]
# key_space_arr = [50e6, 100e6, 400e6, 800e6]
# read_ratio_arr = [80, 60, 40, 20, 0]
# zipf_arr = [0.6, 0.7, 0.8, 0.9]

print(total_threads_arr)
print(key_space_arr)
print(read_ratio_arr)
print(zipf_arr)

file_name = get_res_name("bench")
with open(file_name, 'w') as fp:
    for key_space in key_space_arr:
        for read_ratio in read_ratio_arr:
            for zipf in zipf_arr:
                for total_threads in total_threads_arr:
                    num_threads = (total_threads // num_clients)
                    key_space = int(key_space)

                    print(f'start: {total_threads} {num_clients} {num_threads} {key_space} {read_ratio} {zipf}')
                    fp.write(f'total_threads: {total_threads} num_servers: {num_servers} num_clients: {num_clients} num_threads: {num_threads} key_space: {key_space} read_ratio: {read_ratio} zipf: {zipf}\n')
                    fp.flush()

                    server_subproc = exec_command(f'bash ../script/start_servers.sh --server_count {num_servers} --client_count {num_clients}')

                    time.sleep(2)

                    num_0_threads = total_threads - (num_clients - 1) * num_threads
                    client_0_subproc = exec_command(f'bash ../script/start_client0.sh --server_count {num_servers} --client_count {num_clients} --thread_count {num_0_threads} --key_space {key_space} --read_ratio {read_ratio} --zipf {zipf}')

                    time.sleep(2)

                    client_other_subproc = exec_command(f'bash ../script/start_clients_other.sh --server_count {num_servers} --client_count {num_clients} --thread_count {num_threads} --key_space {key_space} --read_ratio {read_ratio} --zipf {zipf}')

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
