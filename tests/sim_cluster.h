#pragma once

// Deterministic cluster simulator for testing Raft under faults.
//
// Runs N RaftNodes in one process with a simulated network that can delay,
// reorder, drop, and duplicate messages, partition the cluster, and crash and
// restart nodes (a crash discards all volatile state but keeps the node's
// Storage, exactly like a real process restart). Everything is driven by a
// seeded PRNG, so any failure is reproducible from its seed.
//
// While it runs, it continuously checks Raft's core safety properties:
//   - Election safety: at most one leader per term.
//   - State machine safety: no two nodes ever commit different entries at the
//     same log index (checked against a global record of committed entries).
// Each node also applies its committed entries to its own KvStore so tests can
// verify that all replicas converge to identical key-value state.

#include <algorithm>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "common/codec.h"
#include "kv/kv_store.h"
#include "paxos/paxos_node.h"
#include "raft/raft_node.h"

namespace raftkv::sim {

class SafetyViolation : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

struct NetConfig {
  double drop_rate = 0.0;       // probability a message is lost
  double duplicate_rate = 0.0;  // probability a message is delivered twice
  int min_delay = 1;            // ticks
  int max_delay = 1;            // ticks
};

enum class Protocol { Raft, Paxos };

inline const char* ProtocolName(Protocol p) { return p == Protocol::Raft ? "raft" : "paxos"; }

class SimCluster {
 public:
  SimCluster(int n, uint64_t seed, Protocol protocol = Protocol::Raft, RaftConfig cfg = {})
      : n_(n), protocol_(protocol), cfg_(cfg), rng_(seed) {
    for (int i = 0; i < n; ++i) {
      storage_.push_back(std::make_unique<MemoryStorage>());
      paxos_storage_.push_back(std::make_unique<MemoryPaxosStorage>());
      kv_.push_back(std::make_unique<KvStore>());
      nodes_.push_back(nullptr);
      group_.push_back(0);
      Start(Id(i));
    }
  }

  NetConfig net;

  // Log compaction: when > 0, each node snapshots its KV state and compacts
  // its log every `compact_every` applied entries. Small values make the
  // simulator exercise snapshot transfer to lagging and restarted nodes.
  int compact_every = 0;
  uint64_t snapshots_installed() const { return snapshots_installed_; }

  // ReadIndex: called when a read registered on `node` with RequestRead is
  // confirmed AND the node has applied up to its read index, i.e. the moment
  // a server would answer it from its state machine.
  std::function<void(NodeId node, uint64_t ctx)> on_read;

  bool RequestRead(NodeId id, uint64_t ctx) {
    if (!alive(id)) return false;
    bool ok = node(id)->RequestRead(ctx);
    Collect();
    return ok;
  }

  // Called whenever any node applies a committed client command.
  std::function<void(NodeId node, Index index, const Command& cmd, const ApplyResult& result)> on_apply;

  static NodeId Id(int i) { return static_cast<NodeId>(i + 1); }
  int size() const { return n_; }
  uint64_t now() const { return now_; }
  ConsensusNode* node(NodeId id) { return nodes_[id - 1].get(); }
  RaftNode* raft(NodeId id) { return dynamic_cast<RaftNode*>(nodes_[id - 1].get()); }
  Protocol protocol() const { return protocol_; }
  bool alive(NodeId id) const { return nodes_[id - 1] != nullptr; }
  const KvStore& kv(NodeId id) const { return *kv_[id - 1]; }
  size_t committed_count() const { return committed_.size(); }
  size_t leaders_elected() const { return leaders_by_term_.size(); }

  // --- Fault injection ----------------------------------------------------

  void Crash(NodeId id) {
    nodes_[id - 1].reset();
    waiting_reads_[id - 1].clear();
  }

  void Restart(NodeId id) {
    if (!alive(id)) Start(id);
  }

  // Splits the cluster: nodes in `side` can only talk to each other, the rest
  // only to each other.
  void Partition(const std::vector<NodeId>& side) {
    std::fill(group_.begin(), group_.end(), 0);
    for (NodeId id : side) group_[id - 1] = 1;
  }

  void Heal() { std::fill(group_.begin(), group_.end(), 0); }

  // --- Driving ------------------------------------------------------------

  void Tick() {
    ++now_;
    Deliver();
    for (int i = 0; i < n_; ++i) {
      if (nodes_[i]) nodes_[i]->Tick();
    }
    Collect();
  }

  void Run(int ticks) {
    for (int i = 0; i < ticks; ++i) Tick();
  }

  // Ticks until pred() holds; returns false if `max_ticks` pass first.
  template <typename Pred>
  bool RunUntil(Pred pred, int max_ticks) {
    for (int i = 0; i < max_ticks; ++i) {
      if (pred()) return true;
      Tick();
    }
    return pred();
  }

  // The leader with the highest epoch (term/ballot) among live nodes, if any.
  std::optional<NodeId> Leader() const {
    std::optional<NodeId> best;
    uint64_t best_epoch = 0;
    for (int i = 0; i < n_; ++i) {
      if (nodes_[i] && nodes_[i]->IsLeader() && nodes_[i]->epoch() >= best_epoch) {
        best = Id(i);
        best_epoch = nodes_[i]->epoch();
      }
    }
    return best;
  }

  // Every live node that currently believes it is leader (there can be more
  // than one: a deposed leader in a minority partition doesn't know yet).
  std::vector<NodeId> Leaders() const {
    std::vector<NodeId> out;
    for (int i = 0; i < n_; ++i) {
      if (nodes_[i] && nodes_[i]->IsLeader()) out.push_back(Id(i));
    }
    return out;
  }

  // Proposes a KV command to `id`. Returns the log index, or nullopt.
  std::optional<Index> Propose(NodeId id, const Command& c) {
    if (!alive(id)) return std::nullopt;
    auto idx = node(id)->Propose(EncodeCommand(c));
    Collect();
    return idx;
  }

  // True once every live node has committed and applied at least `index`.
  bool AllCommitted(Index index) const {
    for (int i = 0; i < n_; ++i) {
      if (!nodes_[i] || nodes_[i]->commit_index() < index) return false;
    }
    return true;
  }

  // Throws SafetyViolation unless all replicas hold identical KV state.
  void CheckReplicasConverged() const {
    for (int i = 1; i < n_; ++i) {
      if (kv_[i]->data() != kv_[0]->data()) {
        throw SafetyViolation("replica " + std::to_string(i + 1) + " diverged from replica 1");
      }
    }
  }

 private:
  struct InFlight {
    uint64_t deliver_at;
    uint64_t order;  // random tiebreak => reordering within a tick
    Message msg;
  };

  void Start(NodeId id) {
    // A restarted node rebuilds its state machine by re-applying the log
    // from index 1 as it re-learns the commit index.
    kv_[id - 1] = std::make_unique<KvStore>();
    if (protocol_ == Protocol::Raft) {
      nodes_[id - 1] = std::make_unique<RaftNode>(id, Peers(id), *storage_[id - 1], rng_(), cfg_);
    } else {
      PaxosConfig pc{cfg_.election_timeout_min, cfg_.election_timeout_max, cfg_.heartbeat_interval,
                     cfg_.max_entries_per_append};
      nodes_[id - 1] = std::make_unique<PaxosNode>(id, Peers(id), *paxos_storage_[id - 1], rng_(), pc);
    }
  }

  std::vector<NodeId> Peers(NodeId self) const {
    std::vector<NodeId> p;
    for (int i = 0; i < n_; ++i) {
      if (Id(i) != self) p.push_back(Id(i));
    }
    return p;
  }

  bool Chance(double p) { return std::uniform_real_distribution<double>(0, 1)(rng_) < p; }

  void Enqueue(const Message& m) {
    std::uniform_int_distribution<int> delay(net.min_delay, net.max_delay);
    inflight_.push_back({now_ + static_cast<uint64_t>(delay(rng_)), rng_(), m});
  }

  void Deliver() {
    std::vector<InFlight> due, later;
    for (auto& f : inflight_) (f.deliver_at <= now_ ? due : later).push_back(std::move(f));
    inflight_ = std::move(later);
    std::sort(due.begin(), due.end(), [](const InFlight& a, const InFlight& b) {
      return a.deliver_at != b.deliver_at ? a.deliver_at < b.deliver_at : a.order < b.order;
    });
    for (auto& f : due) {
      const Message& m = f.msg;
      if (!alive(m.to)) continue;                         // crashed: lost
      if (group_[m.from - 1] != group_[m.to - 1]) continue;  // partitioned: lost
      node(m.to)->Step(m);
    }
  }

  void Collect() {
    for (int i = 0; i < n_; ++i) {
      if (!nodes_[i]) continue;
      ConsensusNode& rn = *nodes_[i];

      // Election safety: at most one leader per epoch (Raft term / Paxos ballot).
      if (rn.IsLeader()) {
        auto [it, inserted] = leaders_by_term_.emplace(rn.epoch(), rn.id());
        if (!inserted && it->second != rn.id()) {
          std::ostringstream os;
          os << "two leaders in epoch " << rn.epoch() << ": " << it->second << " and " << rn.id();
          throw SafetyViolation(os.str());
        }
      }

      for (Message& m : rn.TakeMessages()) {
        if (Chance(net.drop_rate)) continue;
        Enqueue(m);
        if (Chance(net.duplicate_rate)) Enqueue(m);
      }

      if (auto snap = rn.TakeSnapshot()) {
        // Restored on restart or received from the leader: replaces the KV
        // state wholesale. Later entries apply on top of it.
        kv_[i]->Restore(snap->data);
        ++snapshots_installed_;
      }
      for (const LogEntry& e : rn.TakeCommitted()) {
        // State machine safety: one value per index, cluster-wide, forever.
        // (Compare values, not terms/ballots: Paxos may hold the same chosen
        // value under different ballots on different nodes.)
        auto [it, inserted] = committed_.emplace(e.index, e);
        if (!inserted && it->second.data != e.data) {
          std::ostringstream os;
          os << "node " << rn.id() << " committed a different value at index " << e.index
             << " than was committed there before (epoch " << e.term << " vs " << it->second.term << ")";
          throw SafetyViolation(os.str());
        }
        if (!e.data.empty()) {
          Command cmd = DecodeCommand(e.data);
          ApplyResult res = kv_[i]->Apply(cmd);
          if (on_apply) on_apply(rn.id(), e.index, cmd, res);
        }
      }
      for (const auto& rs : rn.TakeReadyReads()) waiting_reads_[i].push_back(rs);
      auto& wr = waiting_reads_[i];
      for (auto it = wr.begin(); it != wr.end();) {
        if (rn.applied_index() >= it->index) {
          if (on_read) on_read(rn.id(), it->ctx);
          it = wr.erase(it);
        } else {
          ++it;
        }
      }
      if (compact_every > 0 && rn.applied_index() >= rn.snapshot_index() + static_cast<Index>(compact_every)) {
        rn.Compact(rn.applied_index(), kv_[i]->Serialize());
      }
    }
  }

  int n_;
  Protocol protocol_;
  RaftConfig cfg_;
  std::mt19937_64 rng_;
  uint64_t now_ = 0;

  std::vector<std::unique_ptr<MemoryStorage>> storage_;
  std::vector<std::unique_ptr<MemoryPaxosStorage>> paxos_storage_;
  std::vector<std::unique_ptr<ConsensusNode>> nodes_;
  std::vector<std::unique_ptr<KvStore>> kv_;
  std::vector<int> group_;
  std::vector<InFlight> inflight_;

  uint64_t snapshots_installed_ = 0;
  std::map<int, std::vector<ConsensusNode::ReadState>> waiting_reads_;
  std::map<Term, NodeId> leaders_by_term_;
  std::map<Index, LogEntry> committed_;
};

}  // namespace raftkv::sim
