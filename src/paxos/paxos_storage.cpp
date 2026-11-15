#include "paxos/paxos_storage.h"

#include <fcntl.h>
#include <unistd.h>

#include <stdexcept>

#include "common/codec.h"
#include "common/crc32.h"
#include "storage/fs_util.h"

namespace raftkv {

using fsutil::Fail;

namespace {

std::string EncodeAccepted(const LogEntry& e) {
  Writer payload;
  payload.u64(e.term);
  payload.u64(e.index);
  payload.str(e.data);
  return fsutil::Record(payload.data());
}

}  // namespace

FilePaxosStorage::FilePaxosStorage(std::string dir) : dir_(std::move(dir)) {
  fsutil::MakeDir(dir_);

  std::string data;
  if (fsutil::ReadFile(dir_ + "/promised", data)) {
    try {
      Reader r(data);
      uint64_t b = r.u64();
      uint32_t crc = r.u32();
      if (crc != crc32(data.data(), 8)) throw DecodeError("crc");
      img_.promised = b;
    } catch (const DecodeError&) {
      throw std::runtime_error("corrupt promised file in " + dir_);
    }
  }
  if (fsutil::ReadFile(dir_ + "/snapshot", data)) {
    try {
      Reader r(data);
      uint32_t crc = r.u32();
      if (crc != crc32(data.data() + 4, data.size() - 4)) throw DecodeError("crc");
      img_.snap_index = r.u64();
      img_.snap_data = r.str();
    } catch (const DecodeError&) {
      throw std::runtime_error("corrupt snapshot file in " + dir_);
    }
  }

  // Accepted records: replay, keeping the latest record per slot. Slots
  // covered by the snapshot are dropped by PaxosImage::Accept.
  std::string apath = dir_ + "/accepted";
  fd_ = ::open(apath.c_str(), O_RDWR | O_CREAT, 0644);
  if (fd_ < 0) Fail("open " + apath);
  data = fsutil::ReadAll(fd_);
  size_t off = 0;
  while (data.size() - off >= 8) {
    Reader hdr(std::string_view(data).substr(off, 8));
    uint32_t len = hdr.u32();
    uint32_t crc = hdr.u32();
    if (data.size() - off - 8 < len) break;
    std::string_view payload = std::string_view(data).substr(off + 8, len);
    if (crc32(payload.data(), payload.size()) != crc) break;
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
    img_.Accept({e});
    off += 8 + len;
  }
  recovered_tail_bytes_ = data.size() - off;
  if (recovered_tail_bytes_ > 0) {
    if (::ftruncate(fd_, static_cast<off_t>(off)) != 0) Fail("ftruncate");
    if (::fsync(fd_) != 0) Fail("fsync");
  }
  if (::lseek(fd_, static_cast<off_t>(off), SEEK_SET) < 0) Fail("lseek");
}

FilePaxosStorage::~FilePaxosStorage() {
  if (fd_ >= 0) ::close(fd_);
}

void FilePaxosStorage::SetPromised(Ballot b) {
  if (b == img_.promised) return;
  Writer w;
  w.u64(b);
  w.u32(crc32(w.data().data(), 8));
  fsutil::AtomicWrite(dir_, "promised", w.data());
  img_.promised = b;
}

void FilePaxosStorage::Accept(const std::vector<LogEntry>& entries) {
  if (entries.empty()) return;
  std::string buf;
  for (const auto& e : entries) buf += EncodeAccepted(e);
  fsutil::WriteAll(fd_, buf);
  if (::fdatasync(fd_) != 0) Fail("fdatasync accepted");
  img_.Accept(entries);
}

void FilePaxosStorage::SetSnapshot(Index index, std::string data) {
  if (index <= img_.snap_index) return;
  img_.SetSnapshot(index, std::move(data));
  Writer body;
  body.u64(img_.snap_index);
  body.str(img_.snap_data);
  Writer w;
  w.u32(crc32(body.data().data(), body.data().size()));
  w.raw(body.data());
  fsutil::AtomicWrite(dir_, "snapshot", w.data());  // 1) snapshot durable first
  RewriteAccepted();                                // 2) then drop covered slots
}

void FilePaxosStorage::RewriteAccepted() {
  std::string buf;
  for (const auto& [slot, e] : img_.accepted) buf += EncodeAccepted(e);
  fsutil::AtomicWrite(dir_, "accepted", buf);
  if (fd_ >= 0) ::close(fd_);
  std::string path = dir_ + "/accepted";
  fd_ = ::open(path.c_str(), O_RDWR, 0644);
  if (fd_ < 0) Fail("open " + path);
  if (::lseek(fd_, 0, SEEK_END) < 0) Fail("lseek");
}

}  // namespace raftkv
