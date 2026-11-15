#pragma once

#include <map>
#include <string>
#include <vector>

#include "common/types.h"

namespace raftkv {

// Paxos ballots are globally unique and totally ordered: (round, node id)
// packed into one integer, so two proposers can never use the same ballot.
using Ballot = uint64_t;
constexpr Ballot MakeBallot(uint64_t round, NodeId node) { return (round << 16) | (node & 0xFFFF); }
constexpr uint64_t BallotRound(Ballot b) { return b >> 16; }
constexpr NodeId BallotNode(Ballot b) { return static_cast<NodeId>(b & 0xFFFF); }

// Durable acceptor state for Multi-Paxos:
//   - the highest ballot promised (never decreases),
//   - for each slot, the last (ballot, value) accepted.
//
// Unlike a Raft log, slots are independent: a later ballot may overwrite one
// slot without touching the others. Both must be durable before the acceptor
// replies, or a crash could make it break a promise or forget an acceptance
// that helped choose a value.
//
// Accepted entries are LogEntry{term = accepted ballot, index = slot, data}.
//
// Snapshots: slots up to snapshot_index() are chosen and their effect is
// captured in snapshot_data(); their individual acceptances are discarded.
class PaxosStorage {
 public:
  virtual ~PaxosStorage() = default;

  virtual Ballot promised() const = 0;
  virtual const std::map<Index, LogEntry>& accepted() const = 0;
  virtual Index snapshot_index() const = 0;
  virtual const std::string& snapshot_data() const = 0;

  virtual void SetPromised(Ballot b) = 0;
  // Records acceptances (overwriting earlier ones for the same slots).
  virtual void Accept(const std::vector<LogEntry>& entries) = 0;
  // Replaces all slots <= index (all chosen) with a state machine snapshot.
  // Used both for local compaction and for snapshots received from a leader.
  virtual void SetSnapshot(Index index, std::string data) = 0;

  Index max_slot() const { return accepted().empty() ? 0 : accepted().rbegin()->first; }
};

// Shared in-memory image used by every PaxosStorage implementation.
struct PaxosImage {
  Ballot promised = 0;
  std::map<Index, LogEntry> accepted;
  Index snap_index = 0;
  std::string snap_data;

  void Accept(const std::vector<LogEntry>& entries) {
    for (const auto& e : entries) {
      if (e.index > snap_index) accepted[e.index] = e;
    }
  }
  void SetSnapshot(Index index, std::string data) {
    if (index <= snap_index) return;
    accepted.erase(accepted.begin(), accepted.upper_bound(index));
    snap_index = index;
    snap_data = std::move(data);
  }
};

class MemoryPaxosStorage : public PaxosStorage {
 public:
  Ballot promised() const override { return img_.promised; }
  const std::map<Index, LogEntry>& accepted() const override { return img_.accepted; }
  Index snapshot_index() const override { return img_.snap_index; }
  const std::string& snapshot_data() const override { return img_.snap_data; }
  void SetPromised(Ballot b) override { img_.promised = b; }
  void Accept(const std::vector<LogEntry>& entries) override { img_.Accept(entries); }
  void SetSnapshot(Index index, std::string data) override { img_.SetSnapshot(index, std::move(data)); }

 private:
  PaxosImage img_;
};

// File-backed acceptor state for one group.
//   <dir>/promised   promised ballot, replaced atomically (tmp + fsync + rename)
//   <dir>/snapshot   snapshot index + state, replaced atomically
//   <dir>/accepted   append-only records [u32 len][u32 crc][u64 ballot][u64 slot][str value];
//                    on replay the last record for a slot wins. Rewritten on
//                    compaction, which also drops superseded records.
// Torn or corrupt tails are detected (length/CRC) and truncated on open.
class FilePaxosStorage : public PaxosStorage {
 public:
  explicit FilePaxosStorage(std::string dir);
  ~FilePaxosStorage() override;

  FilePaxosStorage(const FilePaxosStorage&) = delete;
  FilePaxosStorage& operator=(const FilePaxosStorage&) = delete;

  Ballot promised() const override { return img_.promised; }
  const std::map<Index, LogEntry>& accepted() const override { return img_.accepted; }
  Index snapshot_index() const override { return img_.snap_index; }
  const std::string& snapshot_data() const override { return img_.snap_data; }
  void SetPromised(Ballot b) override;
  void Accept(const std::vector<LogEntry>& entries) override;
  void SetSnapshot(Index index, std::string data) override;

  size_t recovered_tail_bytes() const { return recovered_tail_bytes_; }

 private:
  void RewriteAccepted();

  std::string dir_;
  int fd_ = -1;
  PaxosImage img_;
  size_t recovered_tail_bytes_ = 0;
};

}  // namespace raftkv
