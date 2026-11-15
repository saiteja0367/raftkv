#include "storage/file_storage.h"

#include <fcntl.h>
#include <unistd.h>

#include <stdexcept>

#include "common/codec.h"
#include "common/crc32.h"
#include "storage/fs_util.h"

namespace raftkv {

using fsutil::Fail;

namespace {

constexpr size_t kRecordHeader = 8;  // u32 len + u32 crc

std::string EncodeEntryRecord(const LogEntry& e) {
  Writer payload;
  payload.u64(e.term);
  payload.u64(e.index);
  payload.str(e.data);
  return fsutil::Record(payload.data());
}

}  // namespace

FileStorage::FileStorage(std::string dir) : dir_(std::move(dir)) {
  fsutil::MakeDir(dir_);
  LoadHardState();
  LoadSnapshot();
  LoadLog();
}

FileStorage::~FileStorage() {
  if (log_fd_ >= 0) ::close(log_fd_);
}

void FileStorage::LoadHardState() {
  std::string data;
  if (!fsutil::ReadFile(dir_ + "/hardstate", data)) return;  // fresh node
  // Replaced via rename, so it is either fully old or fully new; the CRC
  // guards against media corruption.
  try {
    Reader r(data);
    uint64_t term = r.u64();
    uint32_t voted = r.u32();
    uint32_t crc = r.u32();
    if (crc != crc32(data.data(), 12)) throw DecodeError("bad crc");
    img_.hs = {term, voted};
  } catch (const DecodeError&) {
    throw std::runtime_error("corrupt hardstate file in " + dir_);
  }
}

void FileStorage::SetHardState(const HardState& hs) {
  if (hs == img_.hs) return;
  Writer w;
  w.u64(hs.term);
  w.u32(hs.voted_for);
  w.u32(crc32(w.data().data(), w.data().size()));
  fsutil::AtomicWrite(dir_, "hardstate", w.data());
  img_.hs = hs;
}

void FileStorage::LoadSnapshot() {
  std::string data;
  if (!fsutil::ReadFile(dir_ + "/snapshot", data)) return;
  try {
    Reader r(data);
    uint32_t crc = r.u32();
    if (crc != crc32(data.data() + 4, data.size() - 4)) throw DecodeError("bad crc");
    img_.snap_index = r.u64();
    img_.snap_term = r.u64();
    img_.snap_data = r.str();
  } catch (const DecodeError&) {
    throw std::runtime_error("corrupt snapshot file in " + dir_);
  }
}

void FileStorage::WriteSnapshotFile() {
  Writer body;
  body.u64(img_.snap_index);
  body.u64(img_.snap_term);
  body.str(img_.snap_data);
  Writer w;
  w.u32(crc32(body.data().data(), body.data().size()));
  w.raw(body.data());
  fsutil::AtomicWrite(dir_, "snapshot", w.data());
}

void FileStorage::LoadLog() {
  std::string path = dir_ + "/log";
  log_fd_ = ::open(path.c_str(), O_RDWR | O_CREAT, 0644);
  if (log_fd_ < 0) Fail("open " + path);
  std::string data = fsutil::ReadAll(log_fd_);

  // Parse every valid record (stops at the first torn/corrupt one).
  std::vector<LogEntry> recs;
  uint64_t off = 0;
  while (data.size() - off >= kRecordHeader) {
    Reader hdr(std::string_view(data).substr(off, kRecordHeader));
    uint32_t len = hdr.u32();
    uint32_t crc = hdr.u32();
    if (data.size() - off - kRecordHeader < len) break;  // torn write
    std::string_view payload = std::string_view(data).substr(off + kRecordHeader, len);
    if (crc32(payload.data(), payload.size()) != crc) break;  // corrupt
    LogEntry e;
    try {
      Reader r(payload);
      e.term = r.u64();
      e.index = r.u64();
      e.data = r.str();
      if (!r.done()) break;
    } catch (const DecodeError&) {
      break;
    }
    if (!recs.empty() && e.index != recs.back().index + 1) break;  // must be contiguous
    recs.push_back(std::move(e));
    off += kRecordHeader + len;
  }
  recovered_tail_bytes_ = data.size() - off;

  // Reconcile with the snapshot: records at or below it are covered by it.
  // (After a crash between "write snapshot" and "rewrite log" the log still
  // holds them.) Keep only the contiguous suffix that extends the snapshot.
  bool dropped = false;
  for (auto& e : recs) {
    if (e.index <= img_.snap_index) {
      if (e.index == img_.snap_index && e.term != img_.snap_term) {
        // The snapshot replaced a conflicting log: nothing after it is valid.
        dropped = true;
        img_.log.clear();
        break;
      }
      dropped = true;
      continue;
    }
    if (e.index != img_.last_index() + 1) {
      dropped = true;
      break;
    }
    img_.log.push_back(std::move(e));
  }
  if (!recs.empty() && img_.snap_index > 0 && recs.front().index > img_.snap_index + 1) {
    img_.log.clear();  // gap between snapshot and log: unusable
    dropped = true;
  }

  if (dropped) {
    RewriteLog();
  } else {
    if (recovered_tail_bytes_ > 0) {
      if (::ftruncate(log_fd_, static_cast<off_t>(off)) != 0) Fail("ftruncate log");
      if (::fsync(log_fd_) != 0) Fail("fsync log");
    }
    uint64_t pos = 0;
    for (const auto& e : img_.log) {
      offsets_.push_back(pos);
      pos += EncodeEntryRecord(e).size();
    }
    log_size_ = off;
    if (::lseek(log_fd_, static_cast<off_t>(log_size_), SEEK_SET) < 0) Fail("lseek log");
  }
}

void FileStorage::RewriteLog() {
  std::string buf;
  offsets_.clear();
  for (const auto& e : img_.log) {
    offsets_.push_back(buf.size());
    buf += EncodeEntryRecord(e);
  }
  fsutil::AtomicWrite(dir_, "log", buf);
  if (log_fd_ >= 0) ::close(log_fd_);
  std::string path = dir_ + "/log";
  log_fd_ = ::open(path.c_str(), O_RDWR, 0644);
  if (log_fd_ < 0) Fail("open " + path);
  log_size_ = buf.size();
  if (::lseek(log_fd_, static_cast<off_t>(log_size_), SEEK_SET) < 0) Fail("lseek log");
}

void FileStorage::Append(const std::vector<LogEntry>& entries) {
  if (entries.empty()) return;
  // Batch all records into one write + one fsync.
  std::string buf;
  std::vector<uint64_t> new_offsets;
  uint64_t off = log_size_;
  for (size_t i = 0; i < entries.size(); ++i) {
    if (entries[i].index != img_.last_index() + 1 + i) throw std::logic_error("non-contiguous append");
    std::string rec = EncodeEntryRecord(entries[i]);
    new_offsets.push_back(off);
    off += rec.size();
    buf += rec;
  }
  fsutil::WriteAll(log_fd_, buf);
  if (::fdatasync(log_fd_) != 0) Fail("fdatasync log");

  log_size_ = off;
  offsets_.insert(offsets_.end(), new_offsets.begin(), new_offsets.end());
  img_.log.insert(img_.log.end(), entries.begin(), entries.end());
}

void FileStorage::TruncateFrom(Index from) {
  if (from == 0) from = 1;
  if (from > img_.last_index()) return;
  if (from <= img_.snap_index) throw std::logic_error("truncating into the snapshot");
  size_t pos = from - img_.snap_index - 1;
  uint64_t new_size = offsets_[pos];
  if (::ftruncate(log_fd_, static_cast<off_t>(new_size)) != 0) Fail("ftruncate log");
  if (::fsync(log_fd_) != 0) Fail("fsync log");
  if (::lseek(log_fd_, static_cast<off_t>(new_size), SEEK_SET) < 0) Fail("lseek log");
  log_size_ = new_size;
  img_.log.resize(pos);
  offsets_.resize(pos);
}

void FileStorage::Compact(Index index, Term term, std::string data) {
  InstallSnapshot(index, term, std::move(data));
}

void FileStorage::InstallSnapshot(Index index, Term term, std::string data) {
  if (index <= img_.snap_index) return;
  img_.SetSnapshot(index, term, std::move(data));
  WriteSnapshotFile();  // 1) snapshot durable first
  RewriteLog();         // 2) then drop the covered prefix
}

}  // namespace raftkv
