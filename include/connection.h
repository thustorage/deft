#pragma once

#include "Common.h"
// #include "RawMessageConnection.h"

struct RemoteConnectionToClient {
  uint32_t app_rkey[MAX_APP_THREAD];
  uint32_t app_message_qpn[MAX_APP_THREAD];
  ibv_ah *dir_to_app_ah[NR_DIRECTORY][MAX_APP_THREAD];
};

struct RemoteConnectionToServer {
  uint64_t dsm_base;
  uint32_t dsm_rkey[NR_DIRECTORY];
  uint32_t dir_message_qpn[NR_DIRECTORY];
  ibv_ah *app_to_dir_ah[MAX_APP_THREAD][NR_DIRECTORY];

  // lock memory
  uint64_t lock_base;
  uint32_t lock_rkey[NR_DIRECTORY];
};
