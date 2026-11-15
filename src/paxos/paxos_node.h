#pragma once

#include <map>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include "consensus/consensus.h"
#include "paxos/paxos_storage.h"

namespace raftkv {

struct PaxosConfig {
  int election_timeout_min = 10;  // ticks
  int election_timeout_max = 20;  // ticks (exclusive)
  int heartbeat_interval = 2;     // ticks
  size_t max_entries_per_accept = 128;
};

// Multi-Paxos with a stable leader (a "distinguished proposer"), as in Lamport's
// "Paxos Made Simple" section 3 and the Multi-Paxos refinements used in
// production systems.
//
// Every node is proposer, acceptor, and learner.
//
// Phase 1 (once per leadership, not per command): a node that stops hearing
// from a leader picks a ballot higher than any it has seen and sends
// Prepare(ballot, from_slot) for every slot it does not yet know is chosen.
// Acceptors promise to ignore lower ballots and report every value they have
// accepted at or after from_slot. With a majority of promises, the new leader
// re-proposes, for each slot, the value accepted with the highest ballot (the
// rule that makes Paxos safe), and fills empty slots with no-ops.
//
// Phase 2 (per command): the leader sends Accept(ballot, slot, value) and a
// slot is chosen once a majority accepts it at that ballot. Commands are
// streamed in contiguous ranges with per-follower progress tracking, so one
// Accept round can choose many slots (batching), and the leader's commit index
// is piggybacked on Accepts (and on empty-Accept heartbeats) so followers learn.
//
// Unlike Raft, Paxos has no "log matching" invariant and no restriction on who
// may lead: any node can become leader, and correctness comes entirely from
// phase-1 value recovery. Slots are independent and may be overwritten by a
// higher ballot (always with the value that may already have been chosen).
class PaxosNode : public ConsensusNode {
 public:
  PaxosNode(NodeId id, std::vector<NodeId> peers, PaxosStorage& storage, uint64_t seed,
            PaxosConfig config = {});

  void Tick() override;
  void Step(const Message& m) override;
  std::optional<Index> ProposeBatch(std::vector<std::string> batch) override;
  std::vector<Message> TakeMessages() override;
  std::optional<Snapshot> TakeSnapshot() override;
  std::vector<LogEntry> TakeCommitted() override;
  void Compact(Index index, std::string data) override;
  Index snapshot_index() const override { return storage_.snapshot_index(); }
  Index applied_index() const override { return applied_; }

  NodeId id() const override { return id_; }
  bool IsLeader() const override { return mode_ == Mode::Leading; }
  NodeId leader() const override { return leader_; }
  uint64_t epoch() const override { return mode_ == Mode::Follower ? storage_.promised() : ballot_; }
  Index commit_index() const override { return commit_; }
  const char* protocol() const override { return "paxos"; }
  std::string StateString() const override;

  Ballot ballot() const { return ballot_; }
  bool IsPreparing() const { return mode_ == Mode::Preparing; }
  Ballot promised() const { return storage_.promised(); }

 private:
  enum class Mode { Follower, Preparing, Leading };

  void StartPrepare();
  void BecomeLeader();
  void StepDown(Ballot seen);

  void HandlePrepare(const Message& m);
  void HandlePromise(const Message& m);
  void HandleAccept(const Message& m);
  void HandleAccepted(const Message& m);
  void HandleInstallSnapshot(const Message& m);

  void SendAccept(NodeId peer);
  void BroadcastAccept();
  void AdvanceCommit();
  void ResetElectionTimer();
  void Send(Message m);
  size_t Quorum() const { return (peers_.size() + 1) / 2 + 1; }

  const NodeId id_;
  const std::vector<NodeId> peers_;
  PaxosStorage& storage_;
  const PaxosConfig config_;
  std::mt19937_64 rng_;

  Mode mode_ = Mode::Follower;
  Ballot ballot_ = 0;         // our ballot while Preparing/Leading
  NodeId leader_ = kNoNode;
  uint64_t max_round_seen_ = 0;

  Index commit_ = 0;   // all slots <= commit_ are known chosen
  Index applied_ = 0;  // slots handed out by TakeCommitted()
  // Follower: every slot <= stable_ is either known chosen (<= commit_) or was
  // accepted at the current promised ballot from the current leader. Reset to
  // commit_ whenever the promised ballot changes. Volatile.
  Index stable_ = 0;

  int election_elapsed_ = 0;
  int election_timeout_ = 0;
  int heartbeat_elapsed_ = 0;

  // Preparing.
  struct PromiseInfo {
    std::vector<LogEntry> entries;  // accepted values at slots >= prepare_from_
    Index snapshot_index = 0;       // acceptor's snapshot, if it reaches prepare_from_
    std::string snapshot;
  };
  Index prepare_from_ = 1;
  std::map<NodeId, PromiseInfo> promises_;

  // Leading.
  Index next_slot_ = 1;
  std::map<NodeId, Index> next_;
  std::map<NodeId, Index> match_;
  std::map<NodeId, int> snapshot_cooldown_;  // ticks until a snapshot may be resent

  std::vector<Message> outbox_;
};

}  // namespace raftkv
