#ifndef __CONFIG_H__
#define __CONFIG_H__

#include "Common.h"

class CacheConfig {
public:
  uint32_t cacheSize;

  CacheConfig(uint32_t cacheSize = 1) : cacheSize(cacheSize) {}
};

struct DSMConfig {
  // CacheConfig cacheConfig;
  // uint32_t machineNR;
  // uint64_t dsmSize;  // G

  uint32_t dsm_size = 62;   // G
  uint32_t cache_size = 1;  // G
  uint32_t num_client = 1;
  uint32_t num_server = 1;
};

#endif /* __CONFIG_H__ */
