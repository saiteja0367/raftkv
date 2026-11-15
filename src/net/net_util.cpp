#include "net/net_util.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <stdexcept>

#include "common/codec.h"

namespace raftkv {
namespace {

[[noreturn]] void Fail(const std::string& what) {
  throw std::runtime_error(what + ": " + std::strerror(errno));
}

sockaddr_in MakeAddr(const std::string& host, uint16_t port) {
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  std::string h = host == "localhost" ? "127.0.0.1" : host;
  if (::inet_pton(AF_INET, h.c_str(), &addr.sin_addr) != 1) {
    throw std::runtime_error("invalid IPv4 address: " + host);
  }
  return addr;
}

uint16_t ParsePort(std::string_view s) {
  int v = std::stoi(std::string(s));
  if (v <= 0 || v > 65535) throw std::runtime_error("invalid port");
  return static_cast<uint16_t>(v);
}

}  // namespace

std::vector<NodeAddr> ParseCluster(std::string_view spec) {
  std::vector<NodeAddr> out;
  while (!spec.empty()) {
    size_t comma = spec.find(',');
    std::string_view item = spec.substr(0, comma);
    spec = comma == std::string_view::npos ? std::string_view{} : spec.substr(comma + 1);

    size_t eq = item.find('=');
    size_t c1 = item.find(':', eq);
    size_t c2 = item.find(':', c1 + 1);
    if (eq == std::string_view::npos || c1 == std::string_view::npos || c2 == std::string_view::npos) {
      throw std::runtime_error("bad cluster entry (want id=host:peer_port:client_port): " +
                               std::string(item));
    }
    NodeAddr a;
    a.id = static_cast<NodeId>(std::stoul(std::string(item.substr(0, eq))));
    a.host = std::string(item.substr(eq + 1, c1 - eq - 1));
    a.peer_port = ParsePort(item.substr(c1 + 1, c2 - c1 - 1));
    a.client_port = ParsePort(item.substr(c2 + 1));
    if (a.id == kNoNode) throw std::runtime_error("node ids must be >= 1");
    out.push_back(a);
  }
  if (out.empty()) throw std::runtime_error("empty cluster spec");
  return out;
}

std::string EncodeResponse(const ClientResponse& r) {
  Writer w;
  w.u8(static_cast<uint8_t>(r.status));
  w.u32(r.leader_hint);
  w.str(r.value);
  return w.take();
}

ClientResponse DecodeResponse(std::string_view data) {
  Reader rd(data);
  ClientResponse r;
  uint8_t s = rd.u8();
  if (s > static_cast<uint8_t>(Status::Error)) throw DecodeError("unknown status");
  r.status = static_cast<Status>(s);
  r.leader_hint = rd.u32();
  r.value = rd.str();
  if (!rd.done()) throw DecodeError("trailing bytes");
  return r;
}

void SetNonBlocking(int fd) {
  int flags = ::fcntl(fd, F_GETFL, 0);
  if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) Fail("fcntl O_NONBLOCK");
}

void SetNoDelay(int fd) {
  int one = 1;
  ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
}

int ListenTcp(const std::string& host, uint16_t port) {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) Fail("socket");
  int one = 1;
  ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  sockaddr_in addr = MakeAddr(host, port);
  if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    ::close(fd);
    Fail("bind " + host + ":" + std::to_string(port));
  }
  if (::listen(fd, 128) != 0) {
    ::close(fd);
    Fail("listen");
  }
  SetNonBlocking(fd);
  return fd;
}

int ConnectTcpNonBlocking(const std::string& host, uint16_t port) {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) Fail("socket");
  SetNonBlocking(fd);
  SetNoDelay(fd);
  sockaddr_in addr = MakeAddr(host, port);
  if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 && errno != EINPROGRESS) {
    ::close(fd);
    Fail("connect");
  }
  return fd;
}

int ConnectTcpBlocking(const std::string& host, uint16_t port, int timeout_ms) {
  int fd = ConnectTcpNonBlocking(host, port);
  pollfd p{fd, POLLOUT, 0};
  int rc = ::poll(&p, 1, timeout_ms);
  int err = 0;
  socklen_t len = sizeof(err);
  if (rc <= 0 || ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) != 0 || err != 0) {
    ::close(fd);
    errno = rc == 0 ? ETIMEDOUT : (err ? err : errno);
    Fail("connect " + host + ":" + std::to_string(port));
  }
  // Back to blocking mode, with I/O timeouts.
  int flags = ::fcntl(fd, F_GETFL, 0);
  ::fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);
  timeval tv{timeout_ms / 1000, (timeout_ms % 1000) * 1000};
  ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
  return fd;
}

}  // namespace raftkv
