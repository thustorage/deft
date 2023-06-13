#include "dsm_server.h"
#include "DirectoryConnection.h"

DSMServer::DSMServer(const DSMConfig &conf) : conf_(conf) {
  base_addr_ = (uint64_t)hugePageAlloc(conf.dsm_size * define::GB);
  Debug::notifyInfo("number of threads on memory node: %d", NR_DIRECTORY);

  // warmup
  for (uint64_t i = base_addr_; i < base_addr_ + conf.dsm_size * define::GB;
       i += 2 * define::MB) {
    *(char *)i = 0;
  }

  // clear up first chunk
  memset((char *)base_addr_, 0, define::kChunkSize);

  InitRdmaConnection();
  Debug::notifyInfo("number of threads on memory node: %d", NR_DIRECTORY);
  for (int i = 0; i < NR_DIRECTORY; ++i) {
    dir_agent_[i] = new Directory(dir_con_[i], i, my_server_id_);
  }

  keeper_->Barrier("DSMServer-init", conf_.num_server, my_server_id_ == 0);
}

void DSMServer::InitRdmaConnection() {
  Debug::notifyInfo("number of servers: %d", conf_.num_server);
  conn_to_client_ = new RemoteConnectionToClient[conf_.num_client];

  for (int i = 0; i < NR_DIRECTORY; ++i) {
    dir_con_[i] = new DirectoryConnection(i, (void *)base_addr_,
                                          conf_.dsm_size * define::GB,
                                          conf_.num_client, conn_to_client_);
  }

  keeper_ = new DSMServerKeeper(dir_con_, conn_to_client_, conf_.num_client);
  my_server_id_ = keeper_->get_my_server_id();
}

void DSMServer::Run() {
  for (int i = 1; i < NR_DIRECTORY; ++i) {
    dir_agent_[i]->dirTh =
        new std::thread(&Directory::dirThread, dir_agent_[i]);
  }

  dir_agent_[0]->dirThread();
  for (int i = 1; i < NR_DIRECTORY; ++i) {
    dir_agent_[i]->stop_flag.store(true, std::memory_order_release);
    if (dir_agent_[i]->dirTh->joinable()) {
      dir_agent_[i]->dirTh->join();
    }
  }
}
