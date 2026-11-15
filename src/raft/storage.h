#pragma once

#include <stdexcept>
#include <string>
#include <vector>

#include "common/types.h"

namespace raftkv {

// Raft state that must survive a crash: the current term and who we voted
// for in it. Losing either could let a node vote twice in one term.
struct HardState {
  Term term = 0;
  NodeId voted_for = kNoNode;

  bool operator==(const HardState&) const = default;
};

// Durable storage for Raft's persistent state: hard state, a snapshot of the
// state machine covering a compacted prefix of the log, and the log entries
// that follow it.
//
//   indices:  1 ... snapshot_index() | snapshot_index()+1 ... last_index()
//             (compacted, in snapshot)  (entries())
//
// Mutating calls must be durable before anything that depends on them leaves
// the process. Standalone stores make each call durable on return; the shared
// node WAL buffers and the server syncs once per loop iteration, before any
// message or reply is released.
class Storage {
 public:
  virtual ~Storage() = default;

  virtual HardState hard_state() const = 0;
  // Entries after the snapshot: entries()[i] has index snapshot_index() + 1 + i.
  virtual const std::vector<LogEntry>& entries() const = 0;
  virtual Index snapshot_index() const = 0;
  virtual Term snapshot_term() const = 0;
  virtual const std::string& snapshot_data() const = 0;

  virtual void SetHardState(const HardState& hs) = 0;
  // Appends entries that directly follow last_index().
  virtual void Append(const std::vector<LogEntry>& entries) = 0;
  // Removes every entry with index >= `from` (conflict resolution).
  // `from` must be past the snapshot: committed entries are never truncated.
  virtual void TruncateFrom(Index from) = 0;
  // Log compaction: replaces entries up to `index` (inclusive, and already
  // applied) with a snapshot of the state machine at that point.
  virtual void Compact(Index index, Term term, std::string data) = 0;
  // Installs a snapshot received from the leader. If the log holds an entry
  // at `index` with term `term`, the entries after it are kept (they extend
  // the snapshot); otherwise the whole log is discarded.
  virtual void InstallSnapshot(Index index, Term term, std::string data) = 0;

  Index last_index() const { return snapshot_index() + entries().size(); }
  Index first_index() const { return snapshot_index() + 1; }
  const LogEntry& At(Index i) const { return entries()[i - snapshot_index() - 1]; }
};

// Shared in-memory implementation of the log/snapshot bookkeeping, used by
// MemoryStorage and by the file-backed stores as their in-memory image.
class LogImage {
 public:
  HardState hs;
  std::vector<LogEntry> log;
  Index snap_index = 0;
  Term snap_term = 0;
  std::string snap_data;

  Index last_index() const { return snap_index + log.size(); }

  void Append(const std::vector<LogEntry>& entries) {
    for (const auto& e : entries) {
      if (e.index != last_index() + 1) throw std::logic_error("non-contiguous append");
      log.push_back(e);
    }
  }
  void TruncateFrom(Index from) {
    if (from <= snap_index) throw std::logic_error("truncating into the snapshot");
    if (from <= last_index()) log.resize(from - snap_index - 1);
  }
  // Snapshot at (index, term). Keeps the log suffix if it extends the
  // snapshot, else drops the log. Used for both Compact and InstallSnapshot.
  void SetSnapshot(Index index, Term term, std::string data) {
    if (index <= snap_index) return;  // older than what we have
    bool keep_suffix = index <= last_index() && log[index - snap_index - 1].term == term;
    if (keep_suffix) {
      log.erase(log.begin(), log.begin() + static_cast<std::ptrdiff_t>(index - snap_index));
    } else {
      log.clear();
    }
    snap_index = index;
    snap_term = term;
    snap_data = std::move(data);
  }
};

// In-memory storage for tests and the deterministic simulator. A simulated
// "crash" keeps this object and rebuilds the RaftNode on top of it.
class MemoryStorage : public Storage {
 public:
  HardState hard_state() const override { return img_.hs; }
  const std::vector<LogEntry>& entries() const override { return img_.log; }
  Index snapshot_index() const override { return img_.snap_index; }
  Term snapshot_term() const override { return img_.snap_term; }
  const std::string& snapshot_data() const override { return img_.snap_data; }

  void SetHardState(const HardState& hs) override { img_.hs = hs; }
  void Append(const std::vector<LogEntry>& entries) override { img_.Append(entries); }
  void TruncateFrom(Index from) override { img_.TruncateFrom(from == 0 ? 1 : from); }
  void Compact(Index index, Term term, std::string data) override {
    img_.SetSnapshot(index, term, std::move(data));
  }
  void InstallSnapshot(Index index, Term term, std::string data) override {
    img_.SetSnapshot(index, term, std::move(data));
  }

 private:
  LogImage img_;
};

}  // namespace raftkv
