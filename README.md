# Deft: A Scalable Tree Index for Disaggregated Memory

## System Requirements

1. Mellanox ConnectX-5 NICs and above
2. RDMA Driver: MLNX_OFED_LINUX-4.7-3.2.9.0 (If you use MLNX_OFED_LINUX-5**, you should modify codes to resolve interface incompatibility)
3. NIC Firmware: version 16.26.4012 and above (to support on-chip memory, you can use `ibstat` to obtain the version)
4. memcached (to exchange QP information)
5. cityhash
6. boost 1.53 (to support `boost::coroutines::symmetric_coroutine`)

## Setup about RDMA Network

**1. RDMA NIC Selection.** 

You can modify this line according the RDMA NIC you want to use, where `ibv_get_device_name(deviceList[i])` is the name of RNIC (e.g., mlx5_0)

**2. Gid Selection.** 

If you use RoCE, modify `gidIndex` in this line according to the shell command `show_gids`, which is usually 3.

**3. MTU Selection.** 

If you use RoCE and the MTU of your NIC is not equal to 4200 (check with `ifconfig`), modify the value `path_mtu` in `src/rdma/StateTrans.cpp`

**4. On-Chip Memory Size Selection.** 

Change the constant ``kLockChipMemSize`` in `include/Commmon.h`, making it <= max size of on-chip memory.

