#!/bin/bash
# sudo sysctl -w vm.nr_hugepages=32768

sudo sh -c "echo 32768 > /sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages"
