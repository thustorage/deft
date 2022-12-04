#pragma once

#include <atomic>

#include "Config.h"
#include "connection.h"
#include "dsm_keeper.h"
#include "GlobalAddress.h"
#include "Directory.h"

class DSMServer {
 public:
  static DSMServer* GetInstance(const DSMConfig& conf) {
    static DSMServer server(conf);
    return &server;
  }

 private:
  DSMConfig conf_;
  uint64_t base_addr_;
  uint32_t my_server_id_;
  

  DSMServerKeeper *keeper_;
  RemoteConnectionToClient *conn_to_client_;
  DirectoryConnection *dir_con_[NR_DIRECTORY];
  Directory *dir_agent_[NR_DIRECTORY];

  DSMServer(const DSMConfig &conf);

  void InitRdmaConnection();
};

