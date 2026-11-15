#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "common/types.h"
#include "kv/kv_store.h"

namespace raftkv {

// One cluster member. Peers talk Raft on `peer_port`; clients use `client_port`.
struct NodeAddr {
  NodeId id = kNoNode;
  std::string host;
  uint16_t peer_port = 0;
  uint16_t client_port = 0;
};

// Parses "1=127.0.0.1:7001:8001,2=127.0.0.1:7002:8002,..." (id=host:peer:client).
std::vector<NodeAddr> ParseCluster(std::string_view spec);

// Client <-> server wire format (inside a length-prefixed frame).
// Request payload is an encoded Command (see kv_store.h).
struct ClientResponse {
  Status status = Status::Ok;
  NodeId leader_hint = kNoNode;  // set with NotLeader when the leader is known
  std::string value;
};

std::string EncodeResponse(const ClientResponse& r);
ClientResponse DecodeResponse(std::string_view data);  // throws DecodeError

// Socket helpers. All throw std::runtime_error on failure.
int ListenTcp(const std::string& host, uint16_t port);
// Starts a non-blocking connect. Returns the fd; the connect may still be in progress.
int ConnectTcpNonBlocking(const std::string& host, uint16_t port);
// Blocking connect with send/receive timeouts (used by the client library).
int ConnectTcpBlocking(const std::string& host, uint16_t port, int timeout_ms);
void SetNonBlocking(int fd);
void SetNoDelay(int fd);

}  // namespace raftkv
