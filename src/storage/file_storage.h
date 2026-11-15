#pragma once

#include <string>
#include <vector>

#include "raft/storage.h"

namespace raftkv {

// Crash-safe Raft storage for one group on the local filesystem.
//
//   <dir>/hardstate   term + voted_for, replaced atomically
//                     (write temp file -> fsync -> rename -> fsync dir)
//   <dir>/snapshot    snapshot index, term and state, replaced atomically
//   <dir>/log         append-only write-ahead log of records:
//                     [u32 payload_len][u32 crc32(payload)][payload]
//                     payload = [u64 term][u64 index][u32 len][data]
//
// Every mutation is fsync'ed before returning. On open, the log is replayed
// and validated record by record; a torn or corrupt tail (e.g. a crash in the
// middle of a write) is detected via length/CRC/index checks and truncated.
// Compaction writes the snapshot first, then rewrites the log without the
// covered prefix; a crash in between is repaired on the next open.
class FileStorage : public Storage {
 public:
  explicit FileStorage(std::string dir);
  ~FileStorage() override;

  FileStorage(const FileStorage&) = delete;
  FileStorage& operator=(const FileStorage&) = delete;

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

  // Number of bytes dropped from a corrupt/torn log tail during recovery.
  size_t recovered_tail_bytes() const { return recovered_tail_bytes_; }

 private:
  void LoadHardState();
  void LoadSnapshot();
  void LoadLog();
  void WriteSnapshotFile();
  void RewriteLog();  // log file := current entries (atomic replace)

  std::string dir_;
  int log_fd_ = -1;
  LogImage img_;
  std::vector<uint64_t> offsets_;  // file offset of each entry in img_.log
  uint64_t log_size_ = 0;
  size_t recovered_tail_bytes_ = 0;
};

}  // namespace raftkv
