#pragma once

#include <map>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "common/types.h"
#include "consensus/consensus.h"
#include "raft/storage.h"

namespace raftkv {

enum class Role { Follower, PreCandidate, Candidate, Leader };

const char* RoleName(Role r);

// Timing is measured in logical ticks so the core is deterministic and can be
// driven by a real clock (server) or a simulated one (tests).
struct RaftConfig {
  int election_timeout_min = 10;  // ticks
  int election_timeout_max = 20;  // ticks (exclusive upper bound of randomization)
  int heartbeat_interval = 2;     // ticks
  size_t max_entries_per_append = 128;
  // Pre-vote: before starting an election, ask whether peers WOULD vote,
  // without bumping the term. Stops a node that was partitioned away (and
  // kept timing out) from forcing a needless election when it reconnects.
  bool pre_vote = true;
  // Check-quorum: a leader that hasn't heard from a majority within an
  // election timeout steps down instead of lingering as a stale leader.
  bool check_quorum = true;
};

// A single Raft participant, implemented as a pure state machine:
//
//   - inputs:  Tick(), Step(message), Propose(command)
//   - outputs: TakeMessages() (to send), TakeCommitted() (to apply)
//
// It performs no I/O and owns no threads or timers. All side effects go
// through the Storage interface (persisted before any message that depends
// on them is emitted) or the outbox. This makes the protocol logic fully
// testable under a deterministic, fault-injecting network simulator.
class RaftNode : public ConsensusNode {
 public:
  RaftNode(NodeId id, std::vector<NodeId> peers, Storage& storage, uint64_t seed,
           RaftConfig config = {});

  // Advances logical time by one tick (drives elections and heartbeats).
  void Tick() override;

  // Handles one incoming message from a peer.
  void Step(const Message& m) override;

  // Group commit: appends several commands with a single storage write (one
  // fsync) and a single replication round. Returns the index of the first.
  std::optional<Index> ProposeBatch(std::vector<std::string> batch) override;

  // Messages produced since the last call, ready to be sent.
  std::vector<Message> TakeMessages() override;

  std::optional<Snapshot> TakeSnapshot() override;
  // Entries that became committed since the last call, in log order.
  std::vector<LogEntry> TakeCommitted() override;
  void Compact(Index index, std::string data) override;
  Index snapshot_index() const override { return storage_.snapshot_index(); }
  Index applied_index() const override { return applied_; }

  bool RequestRead(uint64_t ctx) override;
  std::vector<ReadState> TakeReadyReads() override;

  NodeId id() const override { return id_; }
  bool IsLeader() const override { return role_ == Role::Leader; }
  NodeId leader() const override { return leader_; }
  uint64_t epoch() const override { return term(); }
  Index commit_index() const override { return commit_; }
  const char* protocol() const override { return "raft"; }
  std::string StateString() const override;

  Role role() const { return role_; }
  Term term() const { return storage_.hard_state().term; }
  Index last_index() const { return storage_.last_index(); }
  Term TermAt(Index i) const;

 private:
  void BecomeFollower(Term term, NodeId leader);
  void BecomePreCandidate();
  void BecomeCandidate();
  void BecomeLeader();

  void HandleRequestVote(const Message& m);
  void HandleRequestVoteResp(const Message& m);
  void HandleAppendEntries(const Message& m);
  void HandleAppendEntriesResp(const Message& m);
  void HandleInstallSnapshot(const Message& m);
  void HandlePreVote(const Message& m);
  void HandlePreVoteResp(const Message& m);
  void StartReadRound(std::vector<uint64_t> ctxs);
  void ConfirmReads();
  void ClearLeaderState();

  void SendAppend(NodeId peer);
  void BroadcastAppend();
  void MaybeAdvanceCommit();
  void ResetElectionTimer();
  void Send(Message m);
  size_t Quorum() const { return (peers_.size() + 1) / 2 + 1; }

  const NodeId id_;
  const std::vector<NodeId> peers_;  // excludes self
  Storage& storage_;
  const RaftConfig config_;
  std::mt19937_64 rng_;

  Role role_ = Role::Follower;
  NodeId leader_ = kNoNode;
  Index commit_ = 0;   // volatile: highest index known committed
  Index applied_ = 0;  // volatile: highest index handed to TakeCommitted()

  int election_elapsed_ = 0;
  int election_timeout_ = 0;
  int heartbeat_elapsed_ = 0;

  std::set<NodeId> votes_;              // candidate only
  std::map<NodeId, Index> next_index_;  // leader only
  std::map<NodeId, Index> match_index_; // leader only
  std::map<NodeId, int> snapshot_cooldown_;  // leader only: ticks until a snapshot may be resent

  // Pre-vote (pre-candidate only).
  std::set<NodeId> prevotes_;

  // Check-quorum (leader only): peers heard from during the current window.
  std::set<NodeId> active_;
  int quorum_check_elapsed_ = 0;

  // ReadIndex (leader only).
  struct PendingRead {
    uint64_t ctx;
    Index index;     // commit index when the read was registered
    uint64_t round;  // heartbeat round that must be acknowledged by a quorum
  };
  uint64_t read_round_ = 0;
  std::vector<uint64_t> reads_awaiting_term_commit_;  // before our first commit in this term
  std::vector<PendingRead> reads_pending_;
  std::map<NodeId, uint64_t> peer_round_;  // highest round each peer has echoed this term
  std::vector<ReadState> ready_reads_;

  std::vector<Message> outbox_;
};

}  // namespace raftkv
