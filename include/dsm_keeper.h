#pragma once

#include <assert.h>
#include <infiniband/verbs.h>
#include <libmemcached/memcached.h>
#include <stdint.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include <functional>
#include <string>
#include <thread>
#include <vector>

#include "Config.h"
#include "Debug.h"
#include "Rdma.h"
#include "connection.h"

class Keeper {
 protected:
  memcached_st *memc;
  static constexpr char SERVER_NUM_KEY[] = "ServerNum";
  static constexpr char CLIENT_NUM_KEY[] = "ClientNum";

  bool ConnectMemcached();
  bool DisconnectMemcached();

  std::string S2CKey(uint16_t server_id, uint16_t client_id) {
    return "S" + std::to_string(server_id) + "-C" + std::to_string(client_id);
  }

  std::string C2SKey(uint16_t client_id, uint16_t server_id) {
    return "C" + std::to_string(client_id) + "-S" + std::to_string(server_id);
  }

 public:
  Keeper() = default;
  ~Keeper() { DisconnectMemcached(); }

  void MemcSet(const char *key, uint32_t klen, const char *val, uint32_t vlen);
  char *MemcGet(const char *key, uint32_t klen, size_t *v_size = nullptr);
  uint64_t MemcFetchAndAdd(const char *key, uint32_t klen);

  void Barrier(const std::string &barrier_key, int cnt, bool is_leader) {
    std::string key = std::string("barrier-") + barrier_key;
    // TOFIX: char key?
    if (is_leader) {
      MemcSet(key.c_str(), key.size(), "0", 1);
    }
    MemcFetchAndAdd(key.c_str(), key.size());
    while (true) {
      int v = std::stoi(MemcGet(key.c_str(), key.size()));
      if (v == cnt) {
        return;
      }
    }
  }

  uint64_t Sum(const std::string &sum_key, uint64_t value, uint16_t my_id,
               uint16_t cnt) {
    std::string key_prefix = std::string("sum-") + sum_key;
    std::string key = key_prefix + std::to_string(my_id);
    MemcSet(key.c_str(), key.size(), (char *)&value, sizeof(value));

    uint64_t ret = 0;
    for (int i = 0; i < cnt; ++i) {
      key = key_prefix + std::to_string(i);
      ret += *(uint64_t *)MemcGet(key.c_str(), key.size());
    }

    return ret;
  }
};

struct ExPerThread {
  uint16_t lid;
  uint8_t gid[16];

  uint32_t rkey;

  uint32_t lock_rkey; //for directory on-chip memory 
} __attribute__((packed));

struct C2SMeta {
  ExPerThread app_th[MAX_APP_THREAD];
  uint32_t app_ud_qpn[MAX_APP_THREAD];
  uint32_t app_rc_qpn_to_dir[MAX_APP_THREAD][NR_DIRECTORY];
} __attribute__((packed));

struct S2CMeta {
  uint64_t dsm_base;
  uint64_t lock_base;
  ExPerThread dir_th[NR_DIRECTORY];
  uint32_t dir_ud_qpn[NR_DIRECTORY];
  uint32_t dir_rc_qpn_to_app[NR_DIRECTORY][MAX_APP_THREAD];
} __attribute__((packed));


struct ThreadConnection;
struct DirectoryConnection;

class DSMServerKeeper : public Keeper {
 public:
  DSMServerKeeper(DirectoryConnection **dir_con,
                  RemoteConnectionToClient *remote_con_to_client,
                  uint16_t max_client = 12)
      : dir_con_(dir_con),
        remote_con_to_client_(remote_con_to_client),
        num_client_(max_client) {
    if (!ConnectMemcached()) {
      return;
    }
    ServerEnter();
    ConnectClients();
  }

  ~DSMServerKeeper() { DisconnectMemcached(); }

  uint16_t get_my_server_id() const { return my_server_id_; }

 private:
  DirectoryConnection **dir_con_;
  RemoteConnectionToClient *remote_con_to_client_;
  uint16_t my_server_id_;
  uint16_t num_client_;

  void ServerEnter();
  void ConnectClients();
  void ConnectClient(uint16_t client_id);
};

class DSMClientKeeper : public Keeper {
 public:
  DSMClientKeeper(ThreadConnection **th_con,
                  RemoteConnectionToServer *remote_con_to_server,
                  uint16_t max_server = 2)
      : th_con_(th_con),
        remote_con_to_server_(remote_con_to_server),
        num_server_(max_server) {
    if (!ConnectMemcached()) {
      return;
    }
    ClientEnter();
    ConnectServers();
  }

  ~DSMClientKeeper() { DisconnectMemcached(); };

  uint16_t get_my_client_id() const { return my_client_id_; }

 private:
  ThreadConnection **th_con_;
  RemoteConnectionToServer *remote_con_to_server_;
  uint16_t my_client_id_;
  uint16_t num_server_;

  void ClientEnter();
  void ConnectServers();
  void ConnectServer(uint16_t server_id);
};
