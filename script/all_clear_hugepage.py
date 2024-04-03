#!/usr/bin/python3

import yaml
from ssh_connect import ssh_command

with open('../script/global_config.yaml', 'r') as f:
    g_cfg = yaml.safe_load(f)

def all_clear_hugepage():
    ip_set = set()
    username=g_cfg['username']
    password=g_cfg['password']
    for i in range(len(g_cfg['clients'])):
        ip = g_cfg['clients'][i]['ip']
        if ip in ip_set:
            continue
        ip_set.add(ip)
        print(f'clear_hugepage ${ip}')
        cmd = f'sudo sysctl -w vm.nr_hugepages=0'
        ssh, stdin, stdout, stderr = ssh_command(ip, username, password, cmd)
        ssh.close()

if __name__ == '__main__':
    all_clear_hugepage()
    print('done.')
