#ifndef __GLOBALADDRESS_H__
#define __GLOBALADDRESS_H__

#include "Common.h"

enum internal_granularity { gran_full, gran_half, gran_quarter };

struct GlobalAddress {
  union {
    struct {
      uint64_t offset : 48;
      // uint64_t nodeID: 16;
      uint64_t nodeID : 4;
      uint64_t child_gran : 2;  // for child
      uint64_t group_gran : 2;  // myself, used in the last pointer of a group
      uint64_t child_version : 4;
      uint64_t group_node_version : 4;
    };
    uint64_t val = 0;
  };

  operator uint64_t() { return val; }

  GlobalAddress &operator=(const GlobalAddress &other) {
    uint8_t old_group_gran = group_gran;
    uint8_t old_group_node_version = group_node_version;
    val = other.val;
    group_gran = old_group_gran;
    group_node_version = old_group_node_version;
    return *this;
  }

  static GlobalAddress Null() {
    static GlobalAddress zero;
    assert(zero.val == 0);
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
  os << "[" << (int)obj.nodeID << ", " << obj.offset << ", "
     << obj.group_node_version << "]";
  return os;
}

#endif /* __GLOBALADDRESS_H__ */
