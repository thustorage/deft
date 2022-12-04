#include "dsm_client.h"

thread_local int DSMClient::thread_id_ = -1;
thread_local ThreadConnection *DSMClient::i_con_ = nullptr;
thread_local char *DSMClient::rdma_buffer_ = nullptr;
thread_local LocalAllocator DSMClient::local_allocator_;
thread_local RdmaBuffer DSMClient::rbuf_[define::kMaxCoro];
thread_local uint64_t DSMClient::thread_tag_ = 0;

DSMClient::DSMClient(const DSMConfig &conf)
    : conf_(conf), app_id_(0), cache_(conf.cache_size) {
  Debug::notifyInfo("cache size: %dGB", conf_.cache_size);
  InitRdmaConnection();
  keeper_->Barrier("DSMClient-init", conf_.num_client, my_client_id_ == 0);
}

void DSMClient::InitRdmaConnection() {
  conn_to_server_ = new RemoteConnectionToServer[conf_.num_server];

  for (int i = 0; i < MAX_APP_THREAD; ++i) {
    // client thread to servers
    th_con_[i] =
        new ThreadConnection(i, (void *)cache_.data, cache_.size * define::GB,
                             conf_.num_server, conn_to_server_);
  }

  keeper_ = new DSMClientKeeper(th_con_, conn_to_server_, conf_.num_server);
  my_client_id_ = keeper_->get_my_client_id();
}

void DSMClient::RegisterThread() {
  static bool has_init[MAX_APP_THREAD];

  if (thread_id_ != -1) return;

  thread_id_ = app_id_.fetch_add(1);
  thread_tag_ = thread_id_ + (((uint64_t)get_my_client_id()) << 32) + 1;

  i_con_ = th_con_[thread_id_];

  if (!has_init[thread_id_]) {
    i_con_->message->initRecv();
    i_con_->message->initSend();

    has_init[thread_id_] = true;
  }

  rdma_buffer_ = (char *)cache_.data + thread_id_ * 12 * define::MB;

  for (int i = 0; i < define::kMaxCoro; ++i) {
    rbuf_[i].set_buffer(rdma_buffer_ + i * define::kPerCoroRdmaBuf);
  }
}

void DSMClient::Read(char *buffer, GlobalAddress gaddr, size_t size,
                     bool signal, CoroContext *ctx) {
  if (ctx == nullptr) {
    rdmaRead(i_con_->data[0][gaddr.nodeID], (uint64_t)buffer,
             conn_to_server_[gaddr.nodeID].dsm_base + gaddr.offset, size,
             i_con_->cacheLKey, conn_to_server_[gaddr.nodeID].dsm_rkey[0],
             signal);
  } else {
    rdmaRead(i_con_->data[0][gaddr.nodeID], (uint64_t)buffer,
             conn_to_server_[gaddr.nodeID].dsm_base + gaddr.offset, size,
             i_con_->cacheLKey, conn_to_server_[gaddr.nodeID].dsm_rkey[0], true,
             ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

void DSMClient::ReadSync(char *buffer, GlobalAddress gaddr, size_t size,
                         CoroContext *ctx) {
  Read(buffer, gaddr, size, true, ctx);

  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(i_con_->cq, 1, &wc);
  }
}

void DSMClient::Write(const char *buffer, GlobalAddress gaddr, size_t size,
                      bool signal, CoroContext *ctx) {
  if (ctx == nullptr) {
    rdmaWrite(i_con_->data[0][gaddr.nodeID], (uint64_t)buffer,
              conn_to_server_[gaddr.nodeID].dsm_base + gaddr.offset, size,
              i_con_->cacheLKey, conn_to_server_[gaddr.nodeID].dsm_rkey[0], -1,
              signal);
  } else {
    rdmaWrite(i_con_->data[0][gaddr.nodeID], (uint64_t)buffer,
              conn_to_server_[gaddr.nodeID].dsm_base + gaddr.offset, size,
              i_con_->cacheLKey, conn_to_server_[gaddr.nodeID].dsm_rkey[0], -1,
              true, ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

void DSMClient::WriteSync(const char *buffer, GlobalAddress gaddr, size_t size,
                          CoroContext *ctx) {
  Write(buffer, gaddr, size, true, ctx);

  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(i_con_->cq, 1, &wc);
  }
}

void DSMClient::FillKeysDest(RdmaOpRegion &ror, GlobalAddress gaddr,
                             bool is_chip) {
  ror.lkey = i_con_->cacheLKey;
  if (is_chip) {
    ror.dest = conn_to_server_[gaddr.nodeID].lock_base + gaddr.offset;
    ror.remoteRKey = conn_to_server_[gaddr.nodeID].lock_rkey[0];
  } else {
    ror.dest = conn_to_server_[gaddr.nodeID].dsm_base + gaddr.offset;
    ror.remoteRKey = conn_to_server_[gaddr.nodeID].dsm_rkey[0];
  }
}

void DSMClient::ReadBatch(RdmaOpRegion *rs, int k, bool signal,
                          CoroContext *ctx) {
  int node_id = -1;
  for (int i = 0; i < k; ++i) {
    GlobalAddress gaddr;
    gaddr.val = rs[i].dest;
    node_id = gaddr.nodeID;
    FillKeysDest(rs[i], gaddr, rs[i].is_on_chip);
  }

  if (ctx == nullptr) {
    rdmaReadBatch(i_con_->data[0][node_id], rs, k, signal);
  } else {
    rdmaReadBatch(i_con_->data[0][node_id], rs, k, true, ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

void DSMClient::ReadBatchSync(RdmaOpRegion *rs, int k, CoroContext *ctx) {
  ReadBatch(rs, k, true, ctx);

  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(i_con_->cq, 1, &wc);
  }
}

void DSMClient::WriteBatch(RdmaOpRegion *rs, int k, bool signal,
                           CoroContext *ctx) {
  int node_id = -1;
  for (int i = 0; i < k; ++i) {
    GlobalAddress gaddr;
    gaddr.val = rs[i].dest;
    node_id = gaddr.nodeID;
    FillKeysDest(rs[i], gaddr, rs[i].is_on_chip);
  }

  if (ctx == nullptr) {
    rdmaWriteBatch(i_con_->data[0][node_id], rs, k, signal);
  } else {
    rdmaWriteBatch(i_con_->data[0][node_id], rs, k, true, ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

void DSMClient::WriteBatchSync(RdmaOpRegion *rs, int k, CoroContext *ctx) {
  WriteBatch(rs, k, true, ctx);

  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(i_con_->cq, 1, &wc);
  }
}

void DSMClient::WriteFaa(RdmaOpRegion &write_ror, RdmaOpRegion &faa_ror,
                         uint64_t add_val, bool signal, CoroContext *ctx) {
  int node_id;
  {
    GlobalAddress gaddr;
    gaddr.val = write_ror.dest;
    node_id = gaddr.nodeID;

    FillKeysDest(write_ror, gaddr, write_ror.is_on_chip);
  }
  {
    GlobalAddress gaddr;
    gaddr.val = faa_ror.dest;

    FillKeysDest(faa_ror, gaddr, faa_ror.is_on_chip);
  }
  if (ctx == nullptr) {
    rdmaWriteFaa(i_con_->data[0][node_id], write_ror, faa_ror, add_val, signal);
  } else {
    rdmaWriteFaa(i_con_->data[0][node_id], write_ror, faa_ror, add_val, true,
                 ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

void DSMClient::WriteFaaSync(RdmaOpRegion &write_ror, RdmaOpRegion &faa_ror,
                             uint64_t add_val, CoroContext *ctx) {
  WriteFaa(write_ror, faa_ror, add_val, true, ctx);
  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(i_con_->cq, 1, &wc);
  }
}

void DSMClient::WriteCas(RdmaOpRegion &write_ror, RdmaOpRegion &cas_ror,
                         uint64_t equal, uint64_t val, bool signal,
                         CoroContext *ctx) {
  int node_id;
  {
    GlobalAddress gaddr;
    gaddr.val = write_ror.dest;
    node_id = gaddr.nodeID;

    FillKeysDest(write_ror, gaddr, write_ror.is_on_chip);
  }
  {
    GlobalAddress gaddr;
    gaddr.val = cas_ror.dest;

    FillKeysDest(cas_ror, gaddr, cas_ror.is_on_chip);
  }
  if (ctx == nullptr) {
    rdmaWriteCas(i_con_->data[0][node_id], write_ror, cas_ror, equal, val,
                 signal);
  } else {
    rdmaWriteCas(i_con_->data[0][node_id], write_ror, cas_ror, equal, val, true,
                 ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

void DSMClient::WriteCasSync(RdmaOpRegion &write_ror, RdmaOpRegion &cas_ror,
                             uint64_t equal, uint64_t val, CoroContext *ctx) {
  WriteCas(write_ror, cas_ror, equal, val, true, ctx);
  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(i_con_->cq, 1, &wc);
  }
}

void DSMClient::CasRead(RdmaOpRegion &cas_ror, RdmaOpRegion &read_ror,
                        uint64_t equal, uint64_t val, bool signal,
                        CoroContext *ctx) {
  int node_id;
  {
    GlobalAddress gaddr;
    gaddr.val = cas_ror.dest;
    node_id = gaddr.nodeID;
    FillKeysDest(cas_ror, gaddr, cas_ror.is_on_chip);
  }
  {
    GlobalAddress gaddr;
    gaddr.val = read_ror.dest;
    FillKeysDest(read_ror, gaddr, read_ror.is_on_chip);
  }

  if (ctx == nullptr) {
    rdmaCasRead(i_con_->data[0][node_id], cas_ror, read_ror, equal, val,
                signal);
  } else {
    rdmaCasRead(i_con_->data[0][node_id], cas_ror, read_ror, equal, val, true,
                ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

bool DSMClient::CasReadSync(RdmaOpRegion &cas_ror, RdmaOpRegion &read_ror,
                            uint64_t equal, uint64_t val, CoroContext *ctx) {
  CasRead(cas_ror, read_ror, equal, val, true, ctx);

  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(i_con_->cq, 1, &wc);
  }

  return equal == *(uint64_t *)cas_ror.source;
}

void DSMClient::FaaRead(RdmaOpRegion &faa_ror, RdmaOpRegion &read_ror,
                        uint64_t add, bool signal, CoroContext *ctx) {
  int node_id;
  {
    GlobalAddress gaddr;
    gaddr.val = faa_ror.dest;
    node_id = gaddr.nodeID;
    FillKeysDest(faa_ror, gaddr, faa_ror.is_on_chip);
  }
  {
    GlobalAddress gaddr;
    gaddr.val = read_ror.dest;
    FillKeysDest(read_ror, gaddr, read_ror.is_on_chip);
  }

  if (ctx == nullptr) {
    rdmaFaaRead(i_con_->data[0][node_id], faa_ror, read_ror, add, signal);
  } else {
    rdmaFaaRead(i_con_->data[0][node_id], faa_ror, read_ror, add, true,
                ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

void DSMClient::FaaReadSync(RdmaOpRegion &faa_ror, RdmaOpRegion &read_ror,
                            uint64_t add, CoroContext *ctx) {
  FaaRead(faa_ror, read_ror, add, true, ctx);

  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(i_con_->cq, 1, &wc);
  }
}

void DSMClient::FaaBoundRead(RdmaOpRegion &faab_ror, RdmaOpRegion &read_ror,
                             uint64_t add, uint64_t boundary, bool signal,
                             CoroContext *ctx) {
  int node_id;
  {
    GlobalAddress gaddr;
    gaddr.val = faab_ror.dest;
    node_id = gaddr.nodeID;
    FillKeysDest(faab_ror, gaddr, faab_ror.is_on_chip);
  }
  {
    GlobalAddress gaddr;
    gaddr.val = read_ror.dest;
    FillKeysDest(read_ror, gaddr, read_ror.is_on_chip);
  }

  if (ctx == nullptr) {
    rdmaFaaBoundRead(i_con_->data[0][node_id], faab_ror, read_ror, add,
                     boundary, signal);
  } else {
    rdmaFaaBoundRead(i_con_->data[0][node_id], faab_ror, read_ror, add,
                     boundary, true, ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

void DSMClient::FaaBoundReadSync(RdmaOpRegion &faab_ror, RdmaOpRegion &read_ror,
                                 uint64_t add, uint64_t boundary,
                                 CoroContext *ctx) {
  FaaBoundRead(faab_ror, read_ror, add, boundary, true, ctx);

  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(i_con_->cq, 1, &wc);
  }
}

void DSMClient::Cas(GlobalAddress gaddr, uint64_t equal, uint64_t val,
                    uint64_t *rdma_buffer, bool signal, CoroContext *ctx) {
  if (ctx == nullptr) {
    rdmaCompareAndSwap(i_con_->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
                       conn_to_server_[gaddr.nodeID].dsm_base + gaddr.offset,
                       equal, val, i_con_->cacheLKey,
                       conn_to_server_[gaddr.nodeID].dsm_rkey[0], signal);
  } else {
    rdmaCompareAndSwap(i_con_->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
                       conn_to_server_[gaddr.nodeID].dsm_base + gaddr.offset,
                       equal, val, i_con_->cacheLKey,
                       conn_to_server_[gaddr.nodeID].dsm_rkey[0], true,
                       ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

bool DSMClient::CasSync(GlobalAddress gaddr, uint64_t equal, uint64_t val,
                        uint64_t *rdma_buffer, CoroContext *ctx) {
  Cas(gaddr, equal, val, rdma_buffer, true, ctx);

  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(i_con_->cq, 1, &wc);
  }

  return equal == *rdma_buffer;
}

void DSMClient::CasMask(GlobalAddress gaddr, uint64_t equal, uint64_t val,
                        uint64_t *rdma_buffer, uint64_t mask, bool signal) {
  rdmaCompareAndSwapMask(i_con_->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
                         conn_to_server_[gaddr.nodeID].dsm_base + gaddr.offset,
                         equal, val, i_con_->cacheLKey,
                         conn_to_server_[gaddr.nodeID].dsm_rkey[0], mask,
                         signal);
}

bool DSMClient::CasMaskSync(GlobalAddress gaddr, uint64_t equal, uint64_t val,
                            uint64_t *rdma_buffer, uint64_t mask) {
  CasMask(gaddr, equal, val, rdma_buffer, mask);
  ibv_wc wc;
  pollWithCQ(i_con_->cq, 1, &wc);

  return (equal & mask) == (*rdma_buffer & mask);
}

void DSMClient::FaaBound(GlobalAddress gaddr, uint64_t add_val,
                         uint64_t *rdma_buffer, uint64_t mask, bool signal,
                         CoroContext *ctx) {
  if (ctx == nullptr) {
    rdmaFetchAndAddBoundary(
        i_con_->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
        conn_to_server_[gaddr.nodeID].dsm_base + gaddr.offset, add_val,
        i_con_->cacheLKey, conn_to_server_[gaddr.nodeID].dsm_rkey[0], mask,
        signal);
  } else {
    rdmaFetchAndAddBoundary(
        i_con_->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
        conn_to_server_[gaddr.nodeID].dsm_base + gaddr.offset, add_val,
        i_con_->cacheLKey, conn_to_server_[gaddr.nodeID].dsm_rkey[0], mask,
        true, ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

void DSMClient::FaaBoundSync(GlobalAddress gaddr, uint64_t add_val,
                             uint64_t *rdma_buffer, uint64_t mask,
                             CoroContext *ctx) {
  FaaBound(gaddr, add_val, rdma_buffer, mask, true, ctx);
  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(i_con_->cq, 1, &wc);
  }
}

void DSMClient::ReadDm(char *buffer, GlobalAddress gaddr, size_t size,
                       bool signal, CoroContext *ctx) {
  if (ctx == nullptr) {
    rdmaRead(i_con_->data[0][gaddr.nodeID], (uint64_t)buffer,
             conn_to_server_[gaddr.nodeID].lock_base + gaddr.offset, size,
             i_con_->cacheLKey, conn_to_server_[gaddr.nodeID].lock_rkey[0],
             signal);
  } else {
    rdmaRead(i_con_->data[0][gaddr.nodeID], (uint64_t)buffer,
             conn_to_server_[gaddr.nodeID].lock_base + gaddr.offset, size,
             i_con_->cacheLKey, conn_to_server_[gaddr.nodeID].lock_rkey[0],
             true, ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

void DSMClient::ReadDmSync(char *buffer, GlobalAddress gaddr, size_t size,
                           CoroContext *ctx) {
  ReadDm(buffer, gaddr, size, true, ctx);

  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(i_con_->cq, 1, &wc);
  }
}

void DSMClient::WriteDm(const char *buffer, GlobalAddress gaddr, size_t size,
                        bool signal, CoroContext *ctx) {
  if (ctx == nullptr) {
    rdmaWrite(i_con_->data[0][gaddr.nodeID], (uint64_t)buffer,
              conn_to_server_[gaddr.nodeID].lock_base + gaddr.offset, size,
              i_con_->cacheLKey, conn_to_server_[gaddr.nodeID].lock_rkey[0], -1,
              signal);
  } else {
    rdmaWrite(i_con_->data[0][gaddr.nodeID], (uint64_t)buffer,
              conn_to_server_[gaddr.nodeID].lock_base + gaddr.offset, size,
              i_con_->cacheLKey, conn_to_server_[gaddr.nodeID].lock_rkey[0], -1,
              true, ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

void DSMClient::WriteDmSync(const char *buffer, GlobalAddress gaddr,
                            size_t size, CoroContext *ctx) {
  WriteDm(buffer, gaddr, size, true, ctx);

  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(i_con_->cq, 1, &wc);
  }
}

void DSMClient::CasDm(GlobalAddress gaddr, uint64_t equal, uint64_t val,
                      uint64_t *rdma_buffer, bool signal, CoroContext *ctx) {
  if (ctx == nullptr) {
    rdmaCompareAndSwap(i_con_->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
                       conn_to_server_[gaddr.nodeID].lock_base + gaddr.offset,
                       equal, val, i_con_->cacheLKey,
                       conn_to_server_[gaddr.nodeID].lock_rkey[0], signal);
  } else {
    rdmaCompareAndSwap(i_con_->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
                       conn_to_server_[gaddr.nodeID].lock_base + gaddr.offset,
                       equal, val, i_con_->cacheLKey,
                       conn_to_server_[gaddr.nodeID].lock_rkey[0], true,
                       ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

bool DSMClient::CasDmSync(GlobalAddress gaddr, uint64_t equal, uint64_t val,
                          uint64_t *rdma_buffer, CoroContext *ctx) {
  CasDm(gaddr, equal, val, rdma_buffer, true, ctx);

  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(i_con_->cq, 1, &wc);
  }

  return equal == *rdma_buffer;
}

void DSMClient::CasDmMask(GlobalAddress gaddr, uint64_t equal, uint64_t val,
                          uint64_t *rdma_buffer, uint64_t mask, bool signal,
                          CoroContext *ctx) {
  if (ctx == nullptr) {
    rdmaCompareAndSwapMask(
        i_con_->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
        conn_to_server_[gaddr.nodeID].lock_base + gaddr.offset, equal, val,
        i_con_->cacheLKey, conn_to_server_[gaddr.nodeID].lock_rkey[0], mask,
        signal);
  } else {
    rdmaCompareAndSwapMask(
        i_con_->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
        conn_to_server_[gaddr.nodeID].lock_base + gaddr.offset, equal, val,
        i_con_->cacheLKey, conn_to_server_[gaddr.nodeID].lock_rkey[0], mask,
        true, ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

bool DSMClient::CasDmMaskSync(GlobalAddress gaddr, uint64_t equal, uint64_t val,
                              uint64_t *rdma_buffer, uint64_t mask,
                              CoroContext *ctx) {
  CasDmMask(gaddr, equal, val, rdma_buffer, mask, true, ctx);
  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(i_con_->cq, 1, &wc);
  }

  return (equal & mask) == (*rdma_buffer & mask);
}

void DSMClient::FaaDmBound(GlobalAddress gaddr, uint64_t add_val,
                           uint64_t *rdma_buffer, uint64_t mask, bool signal,
                           CoroContext *ctx) {
  if (ctx == nullptr) {
    rdmaFetchAndAddBoundary(
        i_con_->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
        conn_to_server_[gaddr.nodeID].lock_base + gaddr.offset, add_val,
        i_con_->cacheLKey, conn_to_server_[gaddr.nodeID].lock_rkey[0], mask,
        signal);
  } else {
    rdmaFetchAndAddBoundary(
        i_con_->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
        conn_to_server_[gaddr.nodeID].lock_base + gaddr.offset, add_val,
        i_con_->cacheLKey, conn_to_server_[gaddr.nodeID].lock_rkey[0], mask,
        true, ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

void DSMClient::FaaDmBoundSync(GlobalAddress gaddr, uint64_t add_val,
                               uint64_t *rdma_buffer, uint64_t mask,
                               CoroContext *ctx) {
  FaaDmBound(gaddr, add_val, rdma_buffer, mask, true, ctx);
  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(i_con_->cq, 1, &wc);
  }
}

uint64_t DSMClient::PollRdmaCq(int count) {
  ibv_wc wc;
  pollWithCQ(i_con_->cq, count, &wc);

  return wc.wr_id;
}

bool DSMClient::PollRdmaCqOnce(uint64_t &wr_id) {
  ibv_wc wc;
  int res = pollOnce(i_con_->cq, 1, &wc);

  wr_id = wc.wr_id;

  return res == 1;
}


