#include "storage/fs_util.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <stdexcept>

#include "common/codec.h"
#include "common/crc32.h"

namespace raftkv::fsutil {

void Fail(const std::string& what) { throw std::runtime_error(what + ": " + std::strerror(errno)); }

void WriteAll(int fd, const std::string& data) {
  size_t off = 0;
  while (off < data.size()) {
    ssize_t n = ::write(fd, data.data() + off, data.size() - off);
    if (n < 0) {
      if (errno == EINTR) continue;
      Fail("write");
    }
    off += static_cast<size_t>(n);
  }
}

std::string ReadAll(int fd) {
  std::string out;
  char buf[1 << 16];
  for (;;) {
    ssize_t n = ::read(fd, buf, sizeof(buf));
    if (n < 0) {
      if (errno == EINTR) continue;
      Fail("read");
    }
    if (n == 0) break;
    out.append(buf, static_cast<size_t>(n));
  }
  return out;
}

void FsyncDir(const std::string& dir) {
  int fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
  if (fd < 0) Fail("open dir " + dir);
  if (::fsync(fd) != 0) {
    ::close(fd);
    Fail("fsync dir");
  }
  ::close(fd);
}

void MakeDir(const std::string& dir) {
  if (::mkdir(dir.c_str(), 0755) != 0 && errno != EEXIST) Fail("mkdir " + dir);
}

void AtomicWrite(const std::string& dir, const std::string& name, const std::string& data) {
  std::string tmp = dir + "/" + name + ".tmp";
  std::string path = dir + "/" + name;
  int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) Fail("open " + tmp);
  WriteAll(fd, data);
  if (::fsync(fd) != 0) Fail("fsync " + tmp);
  ::close(fd);
  if (::rename(tmp.c_str(), path.c_str()) != 0) Fail("rename " + tmp);
  FsyncDir(dir);
}

bool ReadFile(const std::string& path, std::string& out) {
  int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    if (errno == ENOENT) return false;
    Fail("open " + path);
  }
  out = ReadAll(fd);
  ::close(fd);
  return true;
}

std::string Record(const std::string& payload) {
  Writer rec;
  rec.u32(static_cast<uint32_t>(payload.size()));
  rec.u32(crc32(payload.data(), payload.size()));
  rec.raw(payload);
  return rec.take();
}

}  // namespace raftkv::fsutil
