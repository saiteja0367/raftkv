#pragma once

#include <atomic>
#include <chrono>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "consensus/consensus.h"
#include "kv/kv_store.h"
#include "net/net_util.h"
#include "paxos/paxos_node.h"
#include "paxos/paxos_storage.h"
#include "raft/raft_node.h"
#include "storage/file_storage.h"
#include "storage/node_wal.h"

namespace raftkv {

struct ServerOptions {
  std::string protocol = "raft";  // "raft" or "paxos"
  uint32_t shards = 1;            // number of independent consensus groups
  // "shared": one WAL per node, one fdatasync per loop iteration for all groups.
  // "per-group": a separate log file (and fsync) per group; kept for comparison.
  std::string wal = "shared";
  // Log compaction: snapshot a group's KV state every this many applied entries.
  uint64_t snapshot_every = 10000;
  uint64_t wal_checkpoint_bytes = 64ull << 20;  // shared WAL garbage-collection threshold
  // Serve Gets with ReadIndex (Raft only): confirm leadership with one
  // heartbeat round instead of writing the read to the log.
  bool read_index = true;
  int tick_ms = 10;  // one tick; election timeout = 15..30 ticks = 150..300 ms
  RaftConfig raft{.election_timeout_min = 15,
                  .election_timeout_max = 30,
                  .heartbeat_interval = 5,
                  .max_entries_per_append = 256};
  size_t max_peer_buffer = 8 * 1024 * 1024;  // drop consensus messages past this
};

// One raftkv node: a single-threaded, poll()-based event loop hosting
// `shards` independent consensus groups (multi-Raft / multi-Paxos).
//
//   - Keys are hash-partitioned across groups; each group is a separate
//     replicated state machine with its own log, storage directory, leader,
//     and KV state. Leaders of different groups land on different nodes, so
//     write load spreads across the cluster instead of funnelling through one
//     leader.
//   - All groups share the node's TCP connections; every consensus message
//     carries its group id and is dispatched to that group.
//   - Client requests are routed to their key's group and group-committed:
//     everything for one group that arrives in one loop iteration is proposed
//     with a single durable write.
class Server {
 public:
  Server(NodeId id, std::vector<NodeAddr> cluster, const std::string& data_dir,
         ServerOptions opts = {});
  ~Server();

  void Run(const std::atomic<bool>& stop);

 private:
  enum class ConnKind { PeerIn, PeerOut, Client };
  struct Conn {
    int fd = -1;
    uint64_t gen = 0;  // unique per connection, so a reused fd never gets a stale reply
    ConnKind kind = ConnKind::Client;
    NodeId peer = kNoNode;  // PeerOut only
    bool connecting = false;
    std::string rbuf;
    std::string wbuf;
  };
  struct ClientRef {
    int fd = -1;
    uint64_t gen = 0;
  };
  struct Pending {
    ClientRef client;
    uint64_t epoch = 0;
  };
  // One consensus group (shard) hosted on this node.
  struct Group {
    uint32_t id = 0;
    std::unique_ptr<Storage> raft_storage;
    std::unique_ptr<PaxosStorage> paxos_storage;
    std::unique_ptr<ConsensusNode> node;
    KvStore kv;
    std::vector<std::pair<ClientRef, std::string>> batch;  // this iteration's requests
    std::map<Index, Pending> pending;                      // log index -> waiting client
    // ReadIndex reads: registered (ctx -> client, key), then confirmed reads
    // waiting for the state machine to catch up to their read index.
    std::vector<std::pair<ClientRef, std::string>> read_batch;
    std::map<uint64_t, std::pair<ClientRef, std::string>> reads;
    std::vector<ConsensusNode::ReadState> confirmed_reads;
    bool last_leader = false;
    uint64_t last_epoch = 0;
  };

  void AcceptAll(int listen_fd, ConnKind kind);
  void OnReadable(Conn& c);
  void OnWritable(Conn& c);
  void Close(int fd);
  void HandleFrame(Conn& c, const std::string& payload);
  void ConnectPeer(NodeId peer);
  void SendToPeer(const Message& m);
  void Reply(ClientRef to, const ClientResponse& r);
  void ProposeBatch(Group& g);
  void ServeReads(Group& g);
  void FailReads(Group& g);
  void ProcessOutput(Group& g);
  void LogRoleChange(Group& g);

  NodeId id_;
  std::vector<NodeAddr> cluster_;
  ServerOptions opts_;
  std::unique_ptr<NodeWal> wal_;  // shared mode only; must outlive groups_
  std::vector<std::unique_ptr<Group>> groups_;

  int peer_listen_fd_ = -1;
  int client_listen_fd_ = -1;
  std::map<int, Conn> conns_;
  std::map<NodeId, int> peer_out_;  // peer id -> outbound fd
  uint64_t next_gen_ = 1;
  uint64_t next_read_ctx_ = 1;
};

}  // namespace raftkv
