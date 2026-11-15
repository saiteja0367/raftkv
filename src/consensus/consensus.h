#pragma once

#include <optional>
#include <string>
#include <vector>

#include "common/types.h"

namespace raftkv {

// A state machine snapshot covering every log index up to `index`.
struct Snapshot {
  Index index = 0;
  std::string data;
};

// The contract every consensus protocol in this project implements.
//
// A ConsensusNode is a pure, deterministic state machine: it performs no I/O,
// owns no threads or clocks, and talks to the outside world only through
//   inputs:  Tick(), Step(message), ProposeBatch(commands)
//   outputs: TakeMessages() (to send), TakeCommitted() (to apply, in order)
// plus a protocol-specific durable storage object passed at construction.
//
// Because the server and the deterministic simulator depend only on this
// interface, Raft and Multi-Paxos run on the same networking code and are
// verified by the same fault-injection and linearizability tests.
class ConsensusNode {
 public:
  virtual ~ConsensusNode() = default;

  virtual void Tick() = 0;
  virtual void Step(const Message& m) = 0;

  // Appends commands to the replicated log if this node is the leader, with a
  // single durable write. Returns the log index (slot) of the first command.
  virtual std::optional<Index> ProposeBatch(std::vector<std::string> batch) = 0;

  std::optional<Index> Propose(std::string data) {
    std::vector<std::string> b;
    b.push_back(std::move(data));
    return ProposeBatch(std::move(b));
  }

  virtual std::vector<Message> TakeMessages() = 0;

  // If the node's state was replaced by a snapshot (on restart, or one
  // received from the leader), returns it once: the application must restore
  // its state machine from it BEFORE applying TakeCommitted() entries.
  virtual std::optional<Snapshot> TakeSnapshot() = 0;

  // Newly committed (chosen) entries, contiguous and in index order. An
  // entry with empty data is a protocol no-op and must be skipped.
  virtual std::vector<LogEntry> TakeCommitted() = 0;

  // Log compaction: the application hands over a snapshot of its state
  // machine after applying everything up to `index` (<= applied index); the
  // node discards the log prefix it covers. Lagging followers that need the
  // discarded entries are sent the snapshot instead.
  virtual void Compact(Index index, std::string data) = 0;
  virtual Index snapshot_index() const = 0;
  virtual Index applied_index() const = 0;

  // Linearizable reads without a log write (the ReadIndex protocol).
  // RequestRead registers a read under an application-chosen `ctx`; once the
  // leader has confirmed it is still leader, TakeReadyReads() returns
  // {ctx, index}: the application may serve the read from its state machine
  // as soon as it has applied up to `index`. Returns false if this node can't
  // serve reads this way (not leader, or the protocol doesn't support it);
  // the caller then falls back to replicating the read through the log.
  struct ReadState {
    uint64_t ctx = 0;
    Index index = 0;
  };
  virtual bool RequestRead(uint64_t /*ctx*/) { return false; }
  virtual std::vector<ReadState> TakeReadyReads() { return {}; }

  virtual NodeId id() const = 0;
  virtual bool IsLeader() const = 0;
  virtual NodeId leader() const = 0;  // best guess, kNoNode if unknown
  // Monotonic epoch of the current leadership: Raft term or Paxos ballot.
  virtual uint64_t epoch() const = 0;
  virtual Index commit_index() const = 0;
  virtual const char* protocol() const = 0;
  virtual std::string StateString() const = 0;  // for logs and debugging
};

}  // namespace raftkv
