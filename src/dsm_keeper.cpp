#include "dsm_keeper.h"

#include <fstream>
#include <iostream>
#include <random>
#include "DirectoryConnection.h"
#include "ThreadConnection.h"

std::string Trim(const std::string &s) {
  std::string res = s;
  if (!res.empty()) {
    res.erase(0, res.find_first_not_of(" "));
    res.erase(res.find_last_not_of(" ") + 1);
  }
  return res;
}

// Keeper

bool Keeper::ConnectMemcached() {
  memcached_server_st *servers = NULL;
  memcached_return rc;

  std::ifstream conf("../memcached.conf");

  if (!conf) {
    fprintf(stderr, "can't open memcached.conf\n");
    return false;
  }

  std::string addr, port;
  std::getline(conf, addr);
  std::getline(conf, port);

  memc = memcached_create(NULL);
  servers = memcached_server_list_append(servers, Trim(addr).c_str(),
                                         std::stoi(Trim(port)), &rc);
  rc = memcached_server_push(memc, servers);

  if (rc != MEMCACHED_SUCCESS) {
    fprintf(stderr, "Counld't add server:%s\n", memcached_strerror(memc, rc));
    sleep(1);
    return false;
  }

  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_BINARY_PROTOCOL, 1);
  return true;
}

bool Keeper::DisconnectMemcached() {
  if (memc) {
    memcached_quit(memc);
    memcached_free(memc);
    memc = NULL;
  }
  return true;
}

void Keeper::MemcSet(const char *key, uint32_t klen, const char *val,
                     uint32_t vlen) {
  memcached_return rc;
  while (true) {
    rc = memcached_set(memc, key, klen, val, vlen, (time_t)0, (uint32_t)0);
    if (rc == MEMCACHED_SUCCESS) {
      break;
    }
    usleep(400);
  }
}

char *Keeper::MemcGet(const char *key, uint32_t klen, size_t *v_size) {
  size_t l;
  char *res;
  uint32_t flags;
  memcached_return rc;

  while (true) {
    res = memcached_get(memc, key, klen, &l, &flags, &rc);
    if (rc == MEMCACHED_SUCCESS) {
      break;
    }
    usleep(500);
  }

  if (v_size != nullptr) {
    *v_size = l;
  }

  return res;
}

uint64_t Keeper::MemcFetchAndAdd(const char *key, uint32_t klen) {
  uint64_t res;
  while (true) {
    memcached_return rc = memcached_increment(memc, key, klen, 1, &res);
    if (rc == MEMCACHED_SUCCESS) {
      return res;
    }
    usleep(10000);
  }
}


// DSMServerKeeper

void DSMServerKeeper::ServerEnter() {
  memcached_return rc;
  uint64_t server_num;
  while (true) {
    rc = memcached_increment(memc, SERVER_NUM_KEY, strlen(SERVER_NUM_KEY), 1,
                             &server_num);
    if (rc == MEMCACHED_SUCCESS) {
      my_server_id_ = server_num - 1;

      printf("I am server %d\n", my_server_id_);
      return;
    }
    fprintf(stderr, "Server %d Counld't incr value and get ID: %s, retry...\n",
            my_server_id_, memcached_strerror(memc, rc));
    usleep(10000);
  }
}

void DSMServerKeeper::ConnectClients() {
  size_t l;
  uint32_t flags;
  memcached_return rc;

  uint16_t cur_clients = 0;
  while (cur_clients < num_client_) {
    char *client_num_str = memcached_get(
        memc, CLIENT_NUM_KEY, strlen(CLIENT_NUM_KEY), &l, &flags, &rc);
    if (rc != MEMCACHED_SUCCESS) {
      fprintf(stderr, "Server %d Counld't get ClientNum: %s, retry\n",
              my_server_id_, memcached_strerror(memc, rc));
      continue;
    }
    uint32_t client_num = atoi(client_num_str);
    free(client_num_str);

    // connect client k
    for (size_t k = cur_clients; k < client_num; ++k) {
      ConnectClient(k);
      printf("I connect client %zu\n", k);
    }
    fflush(stdout);
    cur_clients = client_num;
  }
}

void DSMServerKeeper::ConnectClient(uint16_t client_id) {
  // set my S2CMeta for client
  S2CMeta s2c_meta;
  s2c_meta.dsm_base = (uint64_t)dir_con_[0]->dsmPool;
  s2c_meta.lock_base = (uint64_t)dir_con_[0]->lockPool;

  for (int i = 0; i < NR_DIRECTORY; ++i) {
    s2c_meta.dir_th[i].lid = dir_con_[i]->ctx.lid;
    s2c_meta.dir_th[i].rkey = dir_con_[i]->dsmMR->rkey;
    s2c_meta.dir_th[i].lock_rkey = dir_con_[i]->lockMR->rkey;
    memcpy((char *)s2c_meta.dir_th[i].gid, (char *)(&dir_con_[i]->ctx.gid),
           sizeof(s2c_meta.dir_th[i].gid));
    s2c_meta.dir_ud_qpn[i] = dir_con_[i]->message->getQPN();
  }

  for (int i = 0; i < NR_DIRECTORY; ++i) {
    auto &c = dir_con_[i];
    for (int k = 0; k < MAX_APP_THREAD; ++k) {
      s2c_meta.dir_rc_qpn_to_app[i][k] = c->data2app[k][client_id]->qp_num;
    }
  }
  
  std::string s2c_key = S2CKey(my_server_id_, client_id);
  MemcSet(s2c_key.c_str(), s2c_key.size(), (char *)(&s2c_meta),
          sizeof(S2CMeta));
  
  std::string c2s_key = C2SKey(client_id, my_server_id_);
  C2SMeta *c2s_meta = (C2SMeta *)MemcGet(c2s_key.c_str(), c2s_key.size());
  // set metadata from C2SMeta

  for (int i = 0; i < NR_DIRECTORY; ++i) {
    auto &c = dir_con_[i];
    for (int k = 0; k < MAX_APP_THREAD; ++k) {
      auto &qp = c->data2app[k][client_id];
      assert(qp->qp_type == IBV_QPT_RC);
      modifyQPtoInit(qp, &c->ctx);
      modifyQPtoRTR(qp, c2s_meta->app_rc_qpn_to_dir[k][i],
                    c2s_meta->app_th[k].lid, c2s_meta->app_th[k].gid, &c->ctx);
      modifyQPtoRTS(qp);
    }
  }

  auto &remote_con = remote_con_to_client_[client_id];
  for (int i = 0; i < MAX_APP_THREAD; ++i) {
    remote_con.app_rkey[i] = c2s_meta->app_th[i].rkey;
    remote_con.app_message_qpn[i] = c2s_meta->app_ud_qpn[i];

    for (int k = 0; k < NR_DIRECTORY; ++k) {
      struct ibv_ah_attr ah_attr;
      fillAhAttr(&ah_attr, c2s_meta->app_th[i].lid, c2s_meta->app_th[i].gid,
                 &dir_con_[k]->ctx);
      remote_con.dir_to_app_ah[k][i] =
          ibv_create_ah(dir_con_[k]->ctx.pd, &ah_attr);
      assert(remote_con.dir_to_app_ah[k][i]);
    }
  }

  free(c2s_meta);
}


// DSMClientKeeper

void DSMClientKeeper::ClientEnter() {
  memcached_return rc;
  uint64_t client_num;
  while (true) {
    rc = memcached_increment(memc, CLIENT_NUM_KEY, strlen(CLIENT_NUM_KEY), 1,
                             &client_num);
    if (rc == MEMCACHED_SUCCESS) {
      my_client_id_ = client_num - 1;

      printf("I am client %d\n", my_client_id_);
      return;
    }
    fprintf(stderr, "Client %d Counld't incr value and get ID: %s, retry...\n",
            my_client_id_, memcached_strerror(memc, rc));
    usleep(10000);
  }
}

void DSMClientKeeper::ConnectServers() {
  size_t l;
  uint32_t flags;
  memcached_return rc;

  uint16_t cur_servers = 0;
  while (cur_servers < num_server_) {
    char *server_num_str = memcached_get(
        memc, SERVER_NUM_KEY, strlen(SERVER_NUM_KEY), &l, &flags, &rc);
    if (rc != MEMCACHED_SUCCESS) {
      fprintf(stderr, "Client %d Counld't get ServerNum: %s, retry\n",
              my_client_id_, memcached_strerror(memc, rc));
      continue;
    }
    uint32_t server_num = atoi(server_num_str);
    free(server_num_str);

    // connect server k
    for (size_t k = cur_servers; k < server_num; ++k) {
      ConnectServer(k);
      printf("I connect server %zu\n", k);
    }
    cur_servers = server_num;
  }
}

void DSMClientKeeper::ConnectServer(uint16_t server_id) {
  // set my C2SMeta for server
  C2SMeta c2s_meta;
  for (int i = 0; i < MAX_APP_THREAD; ++i) {
    c2s_meta.app_th[i].lid = th_con_[i]->ctx.lid;
    c2s_meta.app_th[i].rkey = th_con_[i]->cacheMR->rkey;
    memcpy((char *)c2s_meta.app_th[i].gid, (char *)(&th_con_[i]->ctx.gid),
           sizeof(c2s_meta.app_th[i].gid));
    c2s_meta.app_ud_qpn[i] = th_con_[i]->message->getQPN();
  }

  for (int i = 0; i < MAX_APP_THREAD; ++i) {
    auto &c = th_con_[i];
    for (int k = 0; k < NR_DIRECTORY; ++k) {
      c2s_meta.app_rc_qpn_to_dir[i][k] = c->data[k][server_id]->qp_num;
    }
  }

  std::string c2s_key = C2SKey(my_client_id_, server_id);
  MemcSet(c2s_key.c_str(), c2s_key.size(), (char *)(&c2s_meta),
          sizeof(C2SMeta));

  std::string s2c_key = S2CKey(server_id, my_client_id_);
  S2CMeta *s2c_meta = (S2CMeta *)MemcGet(s2c_key.c_str(), s2c_key.size());
  // set metadata from S2CMeta

  for (int i = 0; i < MAX_APP_THREAD; ++i) {
    auto &c = th_con_[i];
    for (int k = 0; k < NR_DIRECTORY; ++k) {
      auto &qp = c->data[k][server_id];
      assert(qp->qp_type == IBV_QPT_RC);
      modifyQPtoInit(qp, &c->ctx);
      modifyQPtoRTR(qp, s2c_meta->dir_rc_qpn_to_app[k][i],
                    s2c_meta->dir_th[k].lid, s2c_meta->dir_th[k].gid, &c->ctx);
      modifyQPtoRTS(qp);
    }
  }

  auto &remote_con = remote_con_to_server_[server_id];
  remote_con.dsm_base = s2c_meta->dsm_base;
  remote_con.lock_base = s2c_meta->lock_base;

  for (int i = 0; i < NR_DIRECTORY; ++i) {
    remote_con.dsm_rkey[i] = s2c_meta->dir_th[i].rkey;
    remote_con.lock_rkey[i] = s2c_meta->dir_th[i].lock_rkey;
    remote_con.dir_message_qpn[i] = s2c_meta->dir_ud_qpn[i];

    for (int k = 0; k < MAX_APP_THREAD; ++k) {
      struct ibv_ah_attr ah_attr;
      fillAhAttr(&ah_attr, s2c_meta->dir_th[i].lid, s2c_meta->dir_th[i].gid,
                 &th_con_[k]->ctx);
      remote_con.app_to_dir_ah[k][i] =
          ibv_create_ah(th_con_[k]->ctx.pd, &ah_attr);
      assert(remote_con.app_to_dir_ah[k][i]);
    }
  }

  free(s2c_meta);
}
