#!/bin/bash
# sudo sysctl -w vm.nr_hugepages=0

sudo sh -c "echo 0 > /sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages"
