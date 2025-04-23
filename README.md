# Deft: A Scalable Tree Index for Disaggregated Memory

This repository contains the implementaion of Deft.
Deft is a tree-based index for disaggregated memory that delivers high performance and scalability.

For more details, please refer to our paper:
[EuroSys'25]: **Deft: A Scalable Tree Index for Disaggregated Memory**.

*This work is built on top of our previous work [Sherman](https://github.com/thustorage/Sherman).*

## System Requirements

1. Mellanox ConnectX-5 NICs and above
2. RDMA Driver: MLNX_OFED_LINUX-4.9-5.1.0.0 (If you use MLNX_OFED_LINUX-5**, you should modify codes to resolve interface incompatibility)
3. NIC Firmware: version 16.26.4012 and above (to support on-chip memory, you can use `ibstat` to obtain the version)
4. memcached (to exchange QP information)
5. cityhash
6. boost 1.53 and above (to support `boost::coroutines::symmetric_coroutine`)

## Setup about RDMA Network

**1. RDMA NIC Selection.** 

You can modify this line according the RDMA NIC you want to use, where `ibv_get_device_name(deviceList[i])` is the name of RNIC (e.g., mlx5_0)

**2. Gid Selection.** 

If you use RoCE, modify `gidIndex` in this line according to the shell command `show_gids`, which is usually 3.

**3. MTU Selection.** 

If you use RoCE and the MTU of your NIC is not equal to 4200 (check with `ifconfig`), modify the value `path_mtu` in `src/rdma/StateTrans.cpp`

**4. On-Chip Memory Size Selection.** 

Change the constant ``kLockChipMemSize`` in `include/Commmon.h`, making it <= max size of on-chip memory.

## Getting Started

### Manually (similar to [Sherman](https://github.com/thustorage/Sherman))
- `cd deft`
- `./script/hugepage.sh` to request huge pages from OS (use `./script/clear_hugepage.sh` to return huge pages)
- `mkdir build; cd build; cmake -DCMAKE_BUILD_TYPE=Release ..; make -j`
- `cp ../script/restartMemc.sh .`
- configure `../memcached.conf`, where the 1st line is memcached IP, the 2nd is memcached port

- `./restartMemc.sh` (to initialize memcached server)
- (You can use one numa node with one RDMA NIC to simulate one server or one client.)
- For each server, execute `./server --server_count kServerCount --client_count kClientCount --numa_id 0`
- For each client, execute `./client --server_count kServerCount --client_count kClientCount --numa_id 0`

### With scripts

- Mount this repository with nfs on all servers and clients with the same path.
- Edit `script/global_config.yaml` with reference to the template `script/global_config_sample.yaml`, to specify the server and client information. This config will be used by other scripts.
- `mkdir build; cd build; cmake -DCMAKE_BUILD_TYPE=Release ..; make -j`
- `../script/all_hugepage.py` to request huge pages in all servers.
- `../script/run_bench.py`, which will run benchmarks of the product of the paramters.
    - You can edit `script/run_bench.py` to change the parameters of the benchmark (e.g., `threads_CN_arr`, `read_ratio_arr` (type: python list)).
    - logs and results will be saved in the `log` and `result` directories, respectively.


## Citation
If you use Deft in your research, please cite our paper:
```bibtex
@inproceedings{deft2025eurosys,
    author = {Wang, Jing and Wang, Qing and Zhang, Yuhao and Shu, Jiwu},
    title = {Deft: A Scalable Tree Index for Disaggregated Memory},
    year = {2025},
    isbn = {9798400711961},
    publisher = {Association for Computing Machinery},
    address = {New York, NY, USA},
    url = {https://doi.org/10.1145/3689031.3696062},
    doi = {10.1145/3689031.3696062},
    booktitle = {Proceedings of the Twentieth European Conference on Computer Systems},
    pages = {886–901},
    numpages = {16},
    keywords = {Disaggregated Memory, Index, RDMA},
    location = {Rotterdam, Netherlands},
    series = {EuroSys '25}
}
```

