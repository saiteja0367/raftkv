#pragma once

#include <map>
#include <string>
#include <vector>

#include "paxos/paxos_storage.h"
#include "raft/storage.h"

namespace raftkv {

// A single write-ahead log shared by every consensus group on a node.
//
// Why: with one log file per group, a node hosting N groups pays up to N
// fsyncs per event-loop iteration, executed one after another. Measured on
// the dev VM, that made throughput *fall* as shards were added (disk-wait
// bound at ~47% iowait). With a shared WAL, all groups append records to one
// file and the node issues ONE fdatasync per loop iteration, amortizing the
// sync across every group (the same idea as TiKV's raft-engine or
// CockroachDB's shared engine WAL).
//
// Durability contract: group storages buffer their records in memory; the
// owner must call Sync() before any message or client reply that depends on
// those writes leaves the process. The server enforces this with a single
// "durability barrier" per loop iteration.
//
// Record format: [u32 len][u32 crc32(payload)][payload]
//   payload = [u32 group][u8 type][type-specific body]
// Torn or corrupt tails are detected on open and truncated.
class NodeWal {
 public:
  enum RecordType : uint8_t {
    kRaftHardState = 1,  // u64 term, u32 voted_for
    kRaftEntries = 2,    // u32 n, n x (u64 term, u64 index, str data)
    kRaftTruncate = 3,   // u64 from
    kPaxosPromise = 4,   // u64 ballot
    kPaxosAccept = 5,    // u32 n, n x (u64 ballot, u64 slot, str value)
    kRaftSnapshot = 6,   // u64 index, u64 term, str data
    kPaxosSnapshot = 7,  // u64 index, str data
  };

  // A group storage that can re-emit its live state as WAL records; used to
  // garbage-collect the WAL (see MaybeCheckpoint).
  class Participant {
   public:
    virtual ~Participant() = default;
    virtual uint32_t group() const = 0;
    virtual void AppendCheckpoint(std::string& out) const = 0;
  };

  struct Record {
    uint32_t group;
    RecordType type;
    std::string body;
  };

  explicit NodeWal(std::string dir);
  ~NodeWal();

  NodeWal(const NodeWal&) = delete;
  NodeWal& operator=(const NodeWal&) = delete;

  // Records recovered from disk for one group, in write order.
  const std::vector<Record>& Recovered(uint32_t group) const;

  // Buffers a record (not yet durable).
  void Append(uint32_t group, RecordType type, const std::string& body);

  // Writes all buffered records and fdatasyncs once. No-op if nothing is buffered.
  void Sync();

  // WAL garbage collection. Records for compacted, truncated, or superseded
  // state stay in the file until it is rewritten. Once the WAL exceeds its
  // threshold, this replaces it (atomically) with just the live state of
  // every registered group, and raises the threshold to 2x the live size.
  // Returns true if a checkpoint was written.
  bool MaybeCheckpoint();
  void set_checkpoint_threshold(uint64_t bytes) { checkpoint_threshold_ = min_threshold_ = bytes; }
  uint64_t checkpoints() const { return checkpoints_; }

  void Register(Participant* p) { participants_.push_back(p); }
  static std::string EncodeRecord(uint32_t group, RecordType type, const std::string& body);

  bool dirty() const { return !pending_.empty(); }
  uint64_t syncs() const { return syncs_; }
  uint64_t size_bytes() const { return size_; }
  size_t recovered_tail_bytes() const { return recovered_tail_bytes_; }

 private:
  std::string dir_;
  int fd_ = -1;
  std::string pending_;
  uint64_t size_ = 0;
  uint64_t syncs_ = 0;
  size_t recovered_tail_bytes_ = 0;
  std::map<uint32_t, std::vector<Record>> recovered_;
  std::vector<Participant*> participants_;
  uint64_t min_threshold_ = 64ull << 20;
  uint64_t checkpoint_threshold_ = 64ull << 20;
  uint64_t checkpoints_ = 0;
};

// Raft storage for one group, backed by the node's shared WAL.
class WalRaftStorage : public Storage, public NodeWal::Participant {
 public:
  WalRaftStorage(NodeWal& wal, uint32_t group);

  HardState hard_state() const override { return img_.hs; }
  const std::vector<LogEntry>& entries() const override { return img_.log; }
  Index snapshot_index() const override { return img_.snap_index; }
  Term snapshot_term() const override { return img_.snap_term; }
  const std::string& snapshot_data() const override { return img_.snap_data; }
  void SetHardState(const HardState& hs) override;
  void Append(const std::vector<LogEntry>& entries) override;
  void TruncateFrom(Index from) override;
  void Compact(Index index, Term term, std::string data) override;
  void InstallSnapshot(Index index, Term term, std::string data) override;

  uint32_t group() const override { return group_; }
  void AppendCheckpoint(std::string& out) const override;

 private:
  NodeWal& wal_;
  uint32_t group_;
  LogImage img_;
};

// Multi-Paxos acceptor storage for one group, backed by the node's shared WAL.
class WalPaxosStorage : public PaxosStorage, public NodeWal::Participant {
 public:
  WalPaxosStorage(NodeWal& wal, uint32_t group);

  Ballot promised() const override { return img_.promised; }
  const std::map<Index, LogEntry>& accepted() const override { return img_.accepted; }
  Index snapshot_index() const override { return img_.snap_index; }
  const std::string& snapshot_data() const override { return img_.snap_data; }
  void SetPromised(Ballot b) override;
  void Accept(const std::vector<LogEntry>& entries) override;
  void SetSnapshot(Index index, std::string data) override;

  uint32_t group() const override { return group_; }
  void AppendCheckpoint(std::string& out) const override;

 private:
  NodeWal& wal_;
  uint32_t group_;
  PaxosImage img_;
};

}  // namespace raftkv
