#include "storage/node_wal.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <stdexcept>

#include "common/codec.h"
#include "common/crc32.h"
#include "storage/fs_util.h"

namespace raftkv {

using fsutil::Fail;

namespace {


std::string EncodeEntries(const std::vector<LogEntry>& entries) {
  Writer w;
  w.u32(static_cast<uint32_t>(entries.size()));
  for (const auto& e : entries) {
    w.u64(e.term);
    w.u64(e.index);
    w.str(e.data);
  }
  return w.take();
}

std::vector<LogEntry> DecodeEntries(Reader& r) {
  uint32_t n = r.u32();
  if (n > r.remaining() / 20) throw DecodeError("entry count exceeds record");
  std::vector<LogEntry> out;
  out.reserve(n);
  for (uint32_t i = 0; i < n; ++i) {
    LogEntry e;
    e.term = r.u64();
    e.index = r.u64();
    e.data = r.str();
    out.push_back(std::move(e));
  }
  return out;
}

}  // namespace

// ---------------------------------------------------------------------------
// NodeWal
// ---------------------------------------------------------------------------

NodeWal::NodeWal(std::string dir) : dir_(std::move(dir)) {
  if (::mkdir(dir_.c_str(), 0755) != 0 && errno != EEXIST) Fail("mkdir " + dir_);
  std::string path = dir_ + "/wal";
  fd_ = ::open(path.c_str(), O_RDWR | O_CREAT, 0644);
  if (fd_ < 0) Fail("open " + path);

  std::string data;
  char buf[1 << 16];
  for (;;) {
    ssize_t n = ::read(fd_, buf, sizeof(buf));
    if (n < 0) {
      if (errno == EINTR) continue;
      Fail("read wal");
    }
    if (n == 0) break;
    data.append(buf, static_cast<size_t>(n));
  }

  size_t off = 0;
  while (data.size() - off >= 8) {
    Reader hdr(std::string_view(data).substr(off, 8));
    uint32_t len = hdr.u32();
    uint32_t crc = hdr.u32();
    if (len < 5 || data.size() - off - 8 < len) break;  // torn write
    std::string_view payload = std::string_view(data).substr(off + 8, len);
    if (crc32(payload.data(), payload.size()) != crc) break;  // corrupt
    Reader r(payload);
    uint32_t group = r.u32();
    uint8_t type = r.u8();
    if (type < kRaftHardState || type > kPaxosSnapshot) break;
    recovered_[group].push_back(
        Record{group, static_cast<RecordType>(type), std::string(payload.substr(5))});
    off += 8 + len;
  }
  recovered_tail_bytes_ = data.size() - off;
  if (recovered_tail_bytes_ > 0) {
    if (::ftruncate(fd_, static_cast<off_t>(off)) != 0) Fail("ftruncate wal");
    if (::fsync(fd_) != 0) Fail("fsync wal");
  }
  size_ = off;
  if (::lseek(fd_, static_cast<off_t>(off), SEEK_SET) < 0) Fail("lseek wal");
}

NodeWal::~NodeWal() {
  if (fd_ >= 0) {
    try {
      Sync();
    } catch (...) {
    }
    ::close(fd_);
  }
}

const std::vector<NodeWal::Record>& NodeWal::Recovered(uint32_t group) const {
  static const std::vector<Record> kEmpty;
  auto it = recovered_.find(group);
  return it == recovered_.end() ? kEmpty : it->second;
}

std::string NodeWal::EncodeRecord(uint32_t group, RecordType type, const std::string& body) {
  Writer payload;
  payload.u32(group);
  payload.u8(type);
  payload.raw(body);
  return fsutil::Record(payload.data());
}

void NodeWal::Append(uint32_t group, RecordType type, const std::string& body) {
  pending_ += EncodeRecord(group, type, body);
}

bool NodeWal::MaybeCheckpoint() {
  if (size_ < checkpoint_threshold_) return false;
  Sync();
  std::string live;
  for (const Participant* p : participants_) p->AppendCheckpoint(live);
  // Atomic replace: a crash leaves either the old WAL or the new one intact.
  fsutil::AtomicWrite(dir_, "wal", live);
  ::close(fd_);
  std::string path = dir_ + "/wal";
  fd_ = ::open(path.c_str(), O_RDWR, 0644);
  if (fd_ < 0) Fail("open " + path);
  if (::lseek(fd_, 0, SEEK_END) < 0) Fail("lseek wal");
  size_ = live.size();
  checkpoint_threshold_ = std::max(min_threshold_, 2 * size_);
  ++checkpoints_;
  return true;
}

void NodeWal::Sync() {
  if (pending_.empty()) return;
  size_t off = 0;
  while (off < pending_.size()) {
    ssize_t n = ::write(fd_, pending_.data() + off, pending_.size() - off);
    if (n < 0) {
      if (errno == EINTR) continue;
      Fail("write wal");
    }
    off += static_cast<size_t>(n);
  }
  if (::fdatasync(fd_) != 0) Fail("fdatasync wal");
  size_ += pending_.size();
  pending_.clear();
  ++syncs_;
}

// ---------------------------------------------------------------------------
// WalRaftStorage
// ---------------------------------------------------------------------------

namespace {

std::string EncodeRaftSnapshot(Index index, Term term, const std::string& data) {
  Writer w;
  w.u64(index);
  w.u64(term);
  w.str(data);
  return w.take();
}

std::string EncodeHardState(const HardState& hs) {
  Writer w;
  w.u64(hs.term);
  w.u32(hs.voted_for);
  return w.take();
}

}  // namespace

WalRaftStorage::WalRaftStorage(NodeWal& wal, uint32_t group) : wal_(wal), group_(group) {
  for (const auto& rec : wal_.Recovered(group_)) {
    Reader r(rec.body);
    switch (rec.type) {
      case NodeWal::kRaftHardState: {
        Term t = r.u64();
        NodeId v = r.u32();
        img_.hs = {t, v};
        break;
      }
      case NodeWal::kRaftEntries:
        for (auto& e : DecodeEntries(r)) {
          if (e.index <= img_.snap_index) continue;
          if (e.index != img_.last_index() + 1) throw std::runtime_error("wal: non-contiguous raft entries");
          img_.log.push_back(std::move(e));
        }
        break;
      case NodeWal::kRaftTruncate: {
        Index from = r.u64();
        if (from > img_.snap_index && from <= img_.last_index()) img_.TruncateFrom(from);
        break;
      }
      case NodeWal::kRaftSnapshot: {
        Index index = r.u64();
        Term term = r.u64();
        img_.SetSnapshot(index, term, r.str());
        break;
      }
      default:
        throw std::runtime_error("wal: paxos record in a raft group");
    }
  }
  wal_.Register(this);
}

void WalRaftStorage::SetHardState(const HardState& hs) {
  if (hs == img_.hs) return;
  wal_.Append(group_, NodeWal::kRaftHardState, EncodeHardState(hs));
  img_.hs = hs;
}

void WalRaftStorage::Append(const std::vector<LogEntry>& entries) {
  if (entries.empty()) return;
  for (size_t i = 0; i < entries.size(); ++i) {
    if (entries[i].index != img_.last_index() + 1 + i) throw std::logic_error("non-contiguous append");
  }
  wal_.Append(group_, NodeWal::kRaftEntries, EncodeEntries(entries));
  img_.log.insert(img_.log.end(), entries.begin(), entries.end());
}

void WalRaftStorage::TruncateFrom(Index from) {
  if (from == 0) from = 1;
  if (from > img_.last_index()) return;
  Writer w;
  w.u64(from);
  wal_.Append(group_, NodeWal::kRaftTruncate, w.data());
  img_.TruncateFrom(from);
}

void WalRaftStorage::Compact(Index index, Term term, std::string data) {
  InstallSnapshot(index, term, std::move(data));
}

void WalRaftStorage::InstallSnapshot(Index index, Term term, std::string data) {
  if (index <= img_.snap_index) return;
  wal_.Append(group_, NodeWal::kRaftSnapshot, EncodeRaftSnapshot(index, term, data));
  img_.SetSnapshot(index, term, std::move(data));
}

void WalRaftStorage::AppendCheckpoint(std::string& out) const {
  if (img_.snap_index > 0) {
    out += NodeWal::EncodeRecord(group_, NodeWal::kRaftSnapshot,
                                 EncodeRaftSnapshot(img_.snap_index, img_.snap_term, img_.snap_data));
  }
  out += NodeWal::EncodeRecord(group_, NodeWal::kRaftHardState, EncodeHardState(img_.hs));
  if (!img_.log.empty()) out += NodeWal::EncodeRecord(group_, NodeWal::kRaftEntries, EncodeEntries(img_.log));
}

// ---------------------------------------------------------------------------
// WalPaxosStorage
// ---------------------------------------------------------------------------

namespace {

std::string EncodePaxosSnapshot(Index index, const std::string& data) {
  Writer w;
  w.u64(index);
  w.str(data);
  return w.take();
}

}  // namespace

WalPaxosStorage::WalPaxosStorage(NodeWal& wal, uint32_t group) : wal_(wal), group_(group) {
  for (const auto& rec : wal_.Recovered(group_)) {
    Reader r(rec.body);
    switch (rec.type) {
      case NodeWal::kPaxosPromise:
        img_.promised = r.u64();
        break;
      case NodeWal::kPaxosAccept:
        img_.Accept(DecodeEntries(r));
        break;
      case NodeWal::kPaxosSnapshot: {
        Index index = r.u64();
        img_.SetSnapshot(index, r.str());
        break;
      }
      default:
        throw std::runtime_error("wal: raft record in a paxos group");
    }
  }
  wal_.Register(this);
}

void WalPaxosStorage::SetPromised(Ballot b) {
  if (b == img_.promised) return;
  Writer w;
  w.u64(b);
  wal_.Append(group_, NodeWal::kPaxosPromise, w.data());
  img_.promised = b;
}

void WalPaxosStorage::Accept(const std::vector<LogEntry>& entries) {
  if (entries.empty()) return;
  wal_.Append(group_, NodeWal::kPaxosAccept, EncodeEntries(entries));
  img_.Accept(entries);
}

void WalPaxosStorage::SetSnapshot(Index index, std::string data) {
  if (index <= img_.snap_index) return;
  wal_.Append(group_, NodeWal::kPaxosSnapshot, EncodePaxosSnapshot(index, data));
  img_.SetSnapshot(index, std::move(data));
}

void WalPaxosStorage::AppendCheckpoint(std::string& out) const {
  if (img_.snap_index > 0) {
    out += NodeWal::EncodeRecord(group_, NodeWal::kPaxosSnapshot, EncodePaxosSnapshot(img_.snap_index, img_.snap_data));
  }
  Writer w;
  w.u64(img_.promised);
  out += NodeWal::EncodeRecord(group_, NodeWal::kPaxosPromise, w.data());
  if (!img_.accepted.empty()) {
    std::vector<LogEntry> acc;
    for (const auto& [slot, e] : img_.accepted) acc.push_back(e);
    out += NodeWal::EncodeRecord(group_, NodeWal::kPaxosAccept, EncodeEntries(acc));
  }
}

}  // namespace raftkv
