#pragma once

#include <atomic>

// #include "Config.h"
#include "Cache.h"
#include "connection.h"
#include "dsm_keeper.h"
#include "GlobalAddress.h"
#include "LocalAllocator.h"
#include "RdmaBuffer.h"
#include "RawMessageConnection.h"
#include "ThreadConnection.h"

class Directory;

class DSMClient {
 public:
  static DSMClient *GetInstance(const DSMConfig &conf) {
    static DSMClient dsm(conf);
    return &dsm;
  }

  // clear the network resources for all threads
  void ResetThread() { app_id_.store(0); }
  // obtain netowrk resources for a thread
  void RegisterThread();
  bool IsRegistered() { return thread_id_ != -1; }

  uint16_t get_my_client_id() { return my_client_id_; }
  uint16_t get_my_thread_id() { return thread_id_; }
  uint16_t get_server_size() { return conf_.num_server; }
  uint16_t get_client_size() { return conf_.num_client; }
  uint64_t get_thread_tag() { return thread_tag_; }

  void Barrier(const std::string &ss) {
    keeper_->Barrier(ss, conf_.num_client, my_client_id_ == 0);
  }

  char *get_rdma_buffer() { return rdma_buffer_; }
  RdmaBuffer &get_rbuf(int coro_id) { return rbuf_[coro_id]; }

  // RDMA operations
  // buffer is registered memory
  void Read(char *buffer, GlobalAddress gaddr, size_t size, bool signal = true,
            CoroContext *ctx = nullptr);
  void ReadSync(char *buffer, GlobalAddress gaddr, size_t size,
                CoroContext *ctx = nullptr);

  void Write(const char *buffer, GlobalAddress gaddr, size_t size,
             bool signal = true, CoroContext *ctx = nullptr);
  void WriteSync(const char *buffer, GlobalAddress gaddr, size_t size,
                 CoroContext *ctx = nullptr);

  void ReadBatch(RdmaOpRegion *rs, int k, bool signal = true,
                 CoroContext *ctx = nullptr);
  void ReadBatchSync(RdmaOpRegion *rs, int k, CoroContext *ctx = nullptr);

  void WriteBatch(RdmaOpRegion *rs, int k, bool signal = true,
                  CoroContext *ctx = nullptr);
  void WriteBatchSync(RdmaOpRegion *rs, int k, CoroContext *ctx = nullptr);

  void WriteFaa(RdmaOpRegion &write_ror, RdmaOpRegion &faa_ror,
                uint64_t add_val, bool signal = true,
                CoroContext *ctx = nullptr);
  void WriteFaaSync(RdmaOpRegion &write_ror, RdmaOpRegion &faa_ror,
                    uint64_t add_val, CoroContext *ctx = nullptr);

  void WriteCas(RdmaOpRegion &write_ror, RdmaOpRegion &cas_ror, uint64_t equal,
                uint64_t val, bool signal = true, CoroContext *ctx = nullptr);
  void WriteCasSync(RdmaOpRegion &write_ror, RdmaOpRegion &cas_ror,
                    uint64_t equal, uint64_t val, CoroContext *ctx = nullptr);

  void Cas(GlobalAddress gaddr, uint64_t equal, uint64_t val,
           uint64_t *rdma_buffer, bool signal = true,
           CoroContext *ctx = nullptr);
  bool CasSync(GlobalAddress gaddr, uint64_t equal, uint64_t val,
               uint64_t *rdma_buffer, CoroContext *ctx = nullptr);

  void CasRead(RdmaOpRegion &cas_ror, RdmaOpRegion &read_ror, uint64_t equal,
               uint64_t val, bool signal = true, CoroContext *ctx = nullptr);
  bool CasReadSync(RdmaOpRegion &cas_ror, RdmaOpRegion &read_ror,
                   uint64_t equal, uint64_t val, CoroContext *ctx = nullptr);

  void FaaRead(RdmaOpRegion &faab_ror, RdmaOpRegion &read_ror, uint64_t add,
               bool signal = true, CoroContext *ctx = nullptr);
  void FaaReadSync(RdmaOpRegion &faab_ror, RdmaOpRegion &read_ror, uint64_t add,
                   CoroContext *ctx = nullptr);

  void FaaBoundRead(RdmaOpRegion &faab_ror, RdmaOpRegion &read_ror,
                    uint64_t add, uint64_t boundary, bool signal = true,
                    CoroContext *ctx = nullptr);
  void FaaBoundReadSync(RdmaOpRegion &faab_ror, RdmaOpRegion &read_ror,
                        uint64_t add, uint64_t boundary,
                        CoroContext *ctx = nullptr);

  void CasMask(GlobalAddress gaddr, uint64_t equal, uint64_t val,
               uint64_t *rdma_buffer, uint64_t mask = ~(0ull),
               bool signal = true);
  bool CasMaskSync(GlobalAddress gaddr, uint64_t equal, uint64_t val,
                   uint64_t *rdma_buffer, uint64_t mask = ~(0ull));

  void FaaBound(GlobalAddress gaddr, uint64_t add_val, uint64_t *rdma_buffer,
                uint64_t mask = 63, bool signal = true,
                CoroContext *ctx = nullptr);
  void FaaBoundSync(GlobalAddress gaddr, uint64_t add_val,
                    uint64_t *rdma_buffer, uint64_t mask = 63,
                    CoroContext *ctx = nullptr);

  // for on-chip device memory
  void ReadDm(char *buffer, GlobalAddress gaddr, size_t size,
              bool signal = true, CoroContext *ctx = nullptr);
  void ReadDmSync(char *buffer, GlobalAddress gaddr, size_t size,
                  CoroContext *ctx = nullptr);

  void WriteDm(const char *buffer, GlobalAddress gaddr, size_t size,
               bool signal = true, CoroContext *ctx = nullptr);
  void WriteDmSync(const char *buffer, GlobalAddress gaddr, size_t size,
                   CoroContext *ctx = nullptr);

  void CasDm(GlobalAddress gaddr, uint64_t equal, uint64_t val,
             uint64_t *rdma_buffer, bool signal = true,
             CoroContext *ctx = nullptr);
  bool CasDmSync(GlobalAddress gaddr, uint64_t equal, uint64_t val,
                 uint64_t *rdma_buffer, CoroContext *ctx = nullptr);

  void CasDmMask(GlobalAddress gaddr, uint64_t equal, uint64_t val,
                 uint64_t *rdma_buffer, uint64_t mask = ~(0ull),
                 bool signal = true, CoroContext *ctx = nullptr);
  bool CasDmMaskSync(GlobalAddress gaddr, uint64_t equal, uint64_t val,
                     uint64_t *rdma_buffer, uint64_t mask = ~(0ull),
                     CoroContext *ctx = nullptr);

  void FaaDmBound(GlobalAddress gaddr, uint64_t add_val, uint64_t *rdma_buffer,
                  uint64_t mask = 63, bool signal = true,
                  CoroContext *ctx = nullptr);
  void FaaDmBoundSync(GlobalAddress gaddr, uint64_t add_val,
                      uint64_t *rdma_buffer, uint64_t mask = 63,
                      CoroContext *ctx = nullptr);

  uint64_t PollRdmaCq(int count = 1);
  bool PollRdmaCqOnce(uint64_t &wr_id);

  uint64_t Sum(uint64_t value) {
    static uint64_t count = 0;
    return keeper_->Sum(std::string("sum-") + std::to_string(count++), value,
                        my_client_id_, conf_.num_client);
  }

  GlobalAddress Alloc(size_t size) {
    thread_local int next_target_node =
        (get_my_thread_id() + get_my_client_id()) % conf_.num_server;
    thread_local int next_target_dir_id =
        (get_my_thread_id() + get_my_client_id()) % NR_DIRECTORY;

    bool need_chunk = false;
    auto addr = local_allocator_.malloc(size, need_chunk);
    if (need_chunk) {
      RawMessage m;
      m.type = RpcType::MALLOC;
      this->RpcCallDir(m, next_target_node, next_target_dir_id);
      local_allocator_.set_chunck(RpcWait()->addr);

      if (++next_target_dir_id == NR_DIRECTORY) {
        next_target_node = (next_target_node + 1) % conf_.num_server;
        next_target_dir_id = 0;
      }

      // retry
      addr = local_allocator_.malloc(size, need_chunk);
    }

    return addr;
  }

  void Free(GlobalAddress addr) { local_allocator_.free(addr); }

  void RpcCallDir(const RawMessage &m, uint16_t node_id, uint16_t dir_id = 0) {
    auto buffer = (RawMessage *)i_con_->message->getSendPool();
    memcpy(buffer, &m, sizeof(RawMessage));
    buffer->node_id = my_client_id_;
    buffer->app_id = thread_id_;
    i_con_->sendMessage2Dir(buffer, node_id, dir_id);
  }
  RawMessage *RpcWait() {
    ibv_wc wc;
    pollWithCQ(i_con_->rpc_cq, 1, &wc);
    return (RawMessage *)i_con_->message->getMessage();
  }

 private:
  DSMConfig conf_;
  std::atomic_int app_id_;
  Cache cache_;
  uint32_t my_client_id_;

  static thread_local int thread_id_;
  static thread_local ThreadConnection *i_con_;
  static thread_local char *rdma_buffer_;
  static thread_local LocalAllocator local_allocator_;
  static thread_local RdmaBuffer rbuf_[define::kMaxCoro];
  static thread_local uint64_t thread_tag_;

  RemoteConnectionToServer *conn_to_server_;

  ThreadConnection *th_con_[MAX_APP_THREAD];
  DSMClientKeeper *keeper_;
  Directory *dir_agent_[NR_DIRECTORY];

  DSMClient(const DSMConfig &conf);
  
  void InitRdmaConnection();
  void FillKeysDest(RdmaOpRegion &ror, GlobalAddress addr, bool is_chip);

};


