#ifndef __DIRECTORY_H__
#define __DIRECTORY_H__

#include <thread>

#include <unordered_map>

#include "Common.h"

#include "connection.h"
#include "RawMessageConnection.h"
#include "GlobalAllocator.h"

struct DirectoryConnection;

class Directory {
 public:
  Directory(DirectoryConnection *dCon, uint16_t dirID, uint16_t nodeID);

  ~Directory();

 private:
  DirectoryConnection *dCon;

  // uint32_t machineNR;
  uint16_t dirID;
  uint16_t nodeID;

  std::thread *dirTh;

  GlobalAllocator *chunckAlloc;

  void dirThread();

  // void sendData2App(const RawMessage *m);

  void process_message(const RawMessage *m);
};

#endif /* __DIRECTORY_H__ */
