#ifndef __THREADCONNECTION_H__
#define __THREADCONNECTION_H__

#include "Common.h"
#include "RawMessageConnection.h"

struct RemoteConnectionToServer;

// app thread
struct ThreadConnection {

  uint16_t threadID;

  RdmaContext ctx;
  ibv_cq *cq; // for one-side verbs
  ibv_cq *rpc_cq;

  RawMessageConnection *message;

  ibv_qp **data[NR_DIRECTORY];

  ibv_mr *cacheMR;
  void *cachePool;
  uint32_t cacheLKey;
  RemoteConnectionToServer *conn_to_server;

  ThreadConnection(uint16_t threadID, void *cachePool, uint64_t cacheSize,
                   uint32_t machineNR, uint16_t rnic_id,
                   RemoteConnectionToServer *remote_conn);

  void sendMessage2Dir(RawMessage *m, uint16_t node_id, uint16_t dir_id = 0);
};

#endif /* __THREADCONNECTION_H__ */
