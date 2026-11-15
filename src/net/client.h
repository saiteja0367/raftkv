#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "kv/kv_store.h"
#include "net/net_util.h"

namespace raftkv {

// Client library. Routes each key to its shard, finds that shard's leader
// (following NotLeader hints and caching the result per shard), keeps one
// connection per server, and retries on timeouts, connection errors, and
// leader changes until the request succeeds or the deadline passes.
//
// Every request carries (client_id, seq). Retries reuse the same seq, and the
// state machine deduplicates on it, so a PUT retried after an ambiguous
// failure is still applied exactly once.
class Client {
 public:
  explicit Client(std::vector<NodeAddr> cluster, uint32_t shards = 1, int request_timeout_ms = 1000,
                  int deadline_ms = 10000);
  ~Client();

  Client(const Client&) = delete;
  Client& operator=(const Client&) = delete;

  ApplyResult Put(const std::string& key, const std::string& value);
  ApplyResult Get(const std::string& key);
  ApplyResult Delete(const std::string& key);

  uint64_t client_id() const { return client_id_; }

 private:
  ApplyResult Execute(OpType op, const std::string& key, const std::string& value);
  std::optional<ClientResponse> TryOnce(size_t node, const std::string& frame);
  void Disconnect(size_t node);

  std::vector<NodeAddr> cluster_;
  uint32_t shards_;
  int request_timeout_ms_;
  int deadline_ms_;
  uint64_t client_id_;
  uint64_t next_seq_ = 1;
  std::vector<size_t> leader_guess_;  // per shard: index into cluster_
  std::map<size_t, int> fds_;         // per node: open connection
};

}  // namespace raftkv
