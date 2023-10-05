#ifndef __GLOBALADDRESS_H__
#define __GLOBALADDRESS_H__

#include "Common.h"

enum internal_granularity { gran_full, gran_half, gran_quarter };

struct GlobalAddress {
  union {
    struct {
      uint64_t cl_ver : 4;        // cache line version
      uint64_t node_version : 4;  // for child node in ptr
      uint64_t read_gran : 4;     // for child node in ptr
      uint64_t nodeID : 4;
      uint64_t offset : 48;
    };
    uint64_t raw = 0;
  };

  operator uint64_t() { return raw; }

  // remain self cache line version!
  GlobalAddress &operator=(const GlobalAddress &other) {
    uint8_t ver = cl_ver;
    raw = other.raw;
    cl_ver = ver;
    return *this;
  }

  static GlobalAddress Null() {
    static GlobalAddress zero;
    assert(zero.raw == 0);
    return zero;
  };
} __attribute__((packed));

static_assert(sizeof(GlobalAddress) == sizeof(uint64_t), "XXX");

inline GlobalAddress GADD(const GlobalAddress &addr, int off) {
  auto ret = addr;
  ret.offset += off;
  return ret;
}

inline bool operator==(const GlobalAddress &lhs, const GlobalAddress &rhs) {
  return (lhs.nodeID == rhs.nodeID) && (lhs.offset == rhs.offset);
}

inline bool operator!=(const GlobalAddress &lhs, const GlobalAddress &rhs) {
  return !(lhs == rhs);
}

inline std::ostream &operator<<(std::ostream &os, const GlobalAddress &obj) {
  os << "[" << (int)obj.nodeID << ", " << obj.offset << "]";
  return os;
}

#endif /* __GLOBALADDRESS_H__ */
