#!/usr/bin/python3

import os
import sys
import subprocess
import time
import yaml
import killall
from itertools import product
from time import gmtime, strftime
from ssh_connect import ssh_command

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

num_servers = 2
num_clients = 10

threads_CN_arr = [30]
key_space_arr = [400e6]
read_ratio_arr = [50]
zipf_arr = [0.99]

# threads_CN_arr = [1] + [i for i in range(3, 31, 3)]
# key_space_arr = [100e6, 200e6, 800e6, 1200e6]
# read_ratio_arr = [80, 60, 40, 20, 0]
# zipf_arr = [0.6, 0.7, 0.8, 0.9]

print(threads_CN_arr)
print(key_space_arr)
print(read_ratio_arr)
print(zipf_arr)

file_name = get_res_name("bench")

with open('../script/global_config.yaml', 'r') as f:
    g_cfg = yaml.safe_load(f)

exe_path = f'{g_cfg["src_path"]}/{g_cfg["app_rel_path"]}'
username = g_cfg['username']
password = g_cfg['password']

print(exe_path)


with open(file_name, 'w') as fp:
    product_list = list(product(key_space_arr, read_ratio_arr, zipf_arr, threads_CN_arr))

    for job_id, (key_space, read_ratio, zipf, num_threads) in enumerate( product_list):
        key_space = int(key_space)
        num_prefill_threads = 30

        print(f'start: {num_threads * num_clients} {num_clients} {num_threads} {key_space} {read_ratio} {zipf}')
        fp.write(f'total_threads: {num_threads * num_clients} num_servers: {num_servers} num_clients: {num_clients} num_threads: {num_threads} key_space: {key_space} read_ratio: {read_ratio} zipf: {zipf}\n')
        fp.flush()

        # start servers:
        subprocess.run(f'../script/restartMemc.sh', shell=True)

        server_sshs = []
        server_stdouts = []
        for i in range(num_servers):
            ip = g_cfg['servers'][i]['ip']
            numa_id = g_cfg['servers'][i]['numa_id']
            print(f'issue server {i} {ip} {numa_id}')
            cmd = f'cd {exe_path} && sudo sh -c "echo 3 > /proc/sys/vm/drop_caches" && numactl --membind={numa_id} --cpunodebind={numa_id} ./{g_cfg["server_app"]} --server_count {num_servers} --client_count {num_clients} --numa_id {numa_id} &> ../log/server_{i}.log'

            ssh, stdin, stdout, stderr = ssh_command(ip, username, password, cmd)
            server_sshs.append(ssh)
            server_stdouts.append(stdout)
            time.sleep(1)

        time.sleep(1)
        # start clients:

        client_sshs = []
        client_stdouts = []
        for i in range(num_clients):
            ip = g_cfg['clients'][i]['ip']
            numa_id = g_cfg['clients'][i]['numa_id']
            print(f'issue client {i} {ip} {numa_id}')
            cmd = f'cd {exe_path} && numactl --membind={numa_id} --cpunodebind={numa_id} ./{g_cfg["client_app"]} --server_count {num_servers} --client_count {num_clients} --numa_id {numa_id} --num_prefill_threads {num_prefill_threads} --num_bench_threads {num_threads} --key_space {key_space} --read_ratio {read_ratio} --zipf {zipf} &> ../log/client_{i}.log'
            ssh, stdin, stdout, stderr = ssh_command(ip, username, password, cmd)
            client_sshs.append(ssh)
            client_stdouts.append(stdout)
            if i == 0:
                time.sleep(1)
            time.sleep(1)

        # wait
        finish = False
        has_error = False
        while not finish and not has_error:
            time.sleep(1)
            finish = True
            for i in range(num_servers):
                if server_stdouts[i].channel.exit_status_ready():
                    if server_stdouts[i].channel.recv_exit_status() != 0:
                        has_error = True
                        print(f'server {i} error')
                        break
                else:
                    finish = False
            
            if not has_error:
                for i in range(num_clients):
                    if client_stdouts[i].channel.exit_status_ready():
                        if client_stdouts[i].channel.recv_exit_status() != 0:
                            has_error = True
                            print(f'client {i} error')
                            break
                    else:
                        finish = False

        if has_error:
            killall.killall()
        
        for i in range(num_servers):
            server_sshs[i].close()
        for i in range(num_clients):
            client_sshs[i].close()


        loading_subproc = subprocess.run(f'grep "Loading Results" ../log/client_0.log', stdout=subprocess.PIPE, shell=True)
        tmp = loading_subproc.stdout.decode("utf-8")
        print(tmp.strip())
        fp.write(tmp)

        res_subproc = subprocess.run(f'grep "Final Results" ../log/client_0.log', stdout=subprocess.PIPE, shell=True)
        tmp = res_subproc.stdout.decode("utf-8")
        print(tmp)
        fp.write(tmp + "\n")
        fp.flush()

        subprocess.run(f'../script/killall.py', stdout=subprocess.DEVNULL, shell=True)

        if job_id < len(product_list) - 1:
            time.sleep(10)
