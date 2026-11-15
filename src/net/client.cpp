#include "net/client.h"

#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <random>
#include <thread>

#include "common/codec.h"
#include "common/shard.h"

namespace raftkv {

Client::Client(std::vector<NodeAddr> cluster, uint32_t shards, int request_timeout_ms, int deadline_ms)
    : cluster_(std::move(cluster)),
      shards_(shards == 0 ? 1 : shards),
      request_timeout_ms_(request_timeout_ms),
      deadline_ms_(deadline_ms) {
  std::random_device rd;
  client_id_ = (static_cast<uint64_t>(rd()) << 32) ^ rd();
  if (client_id_ == 0) client_id_ = 1;
  // Start each shard's leader guess at a different node.
  for (uint32_t s = 0; s < shards_; ++s) leader_guess_.push_back(s % cluster_.size());
}

Client::~Client() {
  for (auto& [node, fd] : fds_) ::close(fd);
}

void Client::Disconnect(size_t node) {
  auto it = fds_.find(node);
  if (it == fds_.end()) return;
  ::close(it->second);
  fds_.erase(it);
}

ApplyResult Client::Put(const std::string& key, const std::string& value) {
  return Execute(OpType::Put, key, value);
}
ApplyResult Client::Get(const std::string& key) { return Execute(OpType::Get, key, ""); }
ApplyResult Client::Delete(const std::string& key) { return Execute(OpType::Delete, key, ""); }

std::optional<ClientResponse> Client::TryOnce(size_t node, const std::string& frame) {
  try {
    auto it = fds_.find(node);
    if (it == fds_.end()) {
      int fd = ConnectTcpBlocking(cluster_[node].host, cluster_[node].client_port, request_timeout_ms_);
      it = fds_.emplace(node, fd).first;
    }
    int fd = it->second;
    size_t off = 0;
    while (off < frame.size()) {
      ssize_t n = ::send(fd, frame.data() + off, frame.size() - off, MSG_NOSIGNAL);
      if (n <= 0) {
        Disconnect(node);
        return std::nullopt;
      }
      off += static_cast<size_t>(n);
    }
    std::string buf;
    char tmp[4096];
    for (;;) {
      if (auto payload = TryPopFrame(buf)) return DecodeResponse(*payload);
      ssize_t n = ::recv(fd, tmp, sizeof(tmp), 0);
      if (n <= 0) {  // timeout, reset, or closed: drop the connection so a late
        Disconnect(node);  // reply can never be mistaken for the next request's
        return std::nullopt;
      }
      buf.append(tmp, static_cast<size_t>(n));
    }
  } catch (const std::exception&) {
    Disconnect(node);
    return std::nullopt;
  }
}

ApplyResult Client::Execute(OpType op, const std::string& key, const std::string& value) {
  Command cmd{client_id_, next_seq_++, op, key, value};
  std::string frame = Frame(EncodeCommand(cmd));
  size_t& guess = leader_guess_[ShardFor(key, shards_)];

  auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(deadline_ms_);
  int backoff_ms = 20;
  while (std::chrono::steady_clock::now() < deadline) {
    auto resp = TryOnce(guess, frame);
    if (resp && (resp->status == Status::Ok || resp->status == Status::NotFound ||
                 resp->status == Status::Error)) {
      return {resp->status, resp->value};
    }
    // Follow the leader hint if we got a new one; otherwise rotate nodes.
    bool hinted = false;
    if (resp && resp->leader_hint != kNoNode) {
      for (size_t i = 0; i < cluster_.size(); ++i) {
        if (cluster_[i].id == resp->leader_hint && i != guess) {
          guess = i;
          hinted = true;
        }
      }
    }
    if (!hinted) {
      guess = (guess + 1) % cluster_.size();
      std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
      backoff_ms = std::min(backoff_ms * 2, 200);
    }
  }
  return {Status::Error, "deadline exceeded"};
}

}  // namespace raftkv
