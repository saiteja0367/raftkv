#include "net/server.h"

#include <poll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <stdexcept>

#include "common/codec.h"
#include "common/shard.h"

namespace raftkv {

using Clock = std::chrono::steady_clock;

Server::Server(NodeId id, std::vector<NodeAddr> cluster, const std::string& data_dir,
               ServerOptions opts)
    : id_(id), cluster_(std::move(cluster)), opts_(opts) {
  if (opts_.shards == 0) throw std::runtime_error("--shards must be >= 1");
  if (opts_.protocol != "raft" && opts_.protocol != "paxos") {
    throw std::runtime_error("unknown protocol (want raft or paxos): " + opts_.protocol);
  }
  if (opts_.wal != "shared" && opts_.wal != "per-group") {
    throw std::runtime_error("unknown --wal mode (want shared or per-group): " + opts_.wal);
  }
  std::vector<NodeId> peers;
  const NodeAddr* self = nullptr;
  for (const auto& a : cluster_) {
    if (a.id == id_) {
      self = &a;
    } else {
      peers.push_back(a.id);
    }
  }
  if (!self) throw std::runtime_error("node id not found in cluster spec");
  if (::mkdir(data_dir.c_str(), 0755) != 0 && errno != EEXIST) {
    throw std::runtime_error("cannot create data dir " + data_dir);
  }

  uint64_t base_seed = static_cast<uint64_t>(Clock::now().time_since_epoch().count()) ^ (uint64_t{id_} << 32);
  size_t entries = 0, torn = 0;
  const bool shared = opts_.wal == "shared";
  if (shared) {
    wal_ = std::make_unique<NodeWal>(data_dir);
    wal_->set_checkpoint_threshold(opts_.wal_checkpoint_bytes);
    torn += wal_->recovered_tail_bytes();
  }
  for (uint32_t gid = 0; gid < opts_.shards; ++gid) {
    auto g = std::make_unique<Group>();
    g->id = gid;
    std::string dir = data_dir + "/group-" + std::to_string(gid);
    uint64_t seed = base_seed + 0x9e3779b97f4a7c15ull * (gid + 1);
    if (opts_.protocol == "raft") {
      if (shared) {
        g->raft_storage = std::make_unique<WalRaftStorage>(*wal_, gid);
      } else {
        auto fs = std::make_unique<FileStorage>(dir);
        torn += fs->recovered_tail_bytes();
        g->raft_storage = std::move(fs);
      }
      entries += g->raft_storage->last_index();
      g->node = std::make_unique<RaftNode>(id_, peers, *g->raft_storage, seed, opts_.raft);
    } else {
      if (shared) {
        g->paxos_storage = std::make_unique<WalPaxosStorage>(*wal_, gid);
      } else {
        auto fs = std::make_unique<FilePaxosStorage>(dir);
        torn += fs->recovered_tail_bytes();
        g->paxos_storage = std::move(fs);
      }
      entries += g->paxos_storage->accepted().size();
      PaxosConfig pc{opts_.raft.election_timeout_min, opts_.raft.election_timeout_max,
                     opts_.raft.heartbeat_interval, opts_.raft.max_entries_per_append};
      g->node = std::make_unique<PaxosNode>(id_, peers, *g->paxos_storage, seed, pc);
    }
    g->last_epoch = g->node->epoch();
    groups_.push_back(std::move(g));
  }
  std::fprintf(stderr, "[node %u] %s, %u shard(s), %s WAL: recovered %zu log entries\n", id_,
               opts_.protocol.c_str(), opts_.shards, opts_.wal.c_str(), entries);
  if (torn > 0) {
    std::fprintf(stderr, "[node %u] recovery: discarded %zu bytes of torn/corrupt log tails\n", id_, torn);
  }

  peer_listen_fd_ = ListenTcp(self->host, self->peer_port);
  client_listen_fd_ = ListenTcp(self->host, self->client_port);
  std::fprintf(stderr, "[node %u] listening peers=%s:%u clients=%s:%u\n", id_, self->host.c_str(),
               self->peer_port, self->host.c_str(), self->client_port);
}

Server::~Server() {
  for (auto& [fd, c] : conns_) ::close(fd);
  if (peer_listen_fd_ >= 0) ::close(peer_listen_fd_);
  if (client_listen_fd_ >= 0) ::close(client_listen_fd_);
}

void Server::Run(const std::atomic<bool>& stop) {
  const auto tick = std::chrono::milliseconds(opts_.tick_ms);
  auto next_tick = Clock::now() + tick;

  while (!stop.load()) {
    std::vector<pollfd> pfds;
    pfds.push_back({peer_listen_fd_, POLLIN, 0});
    pfds.push_back({client_listen_fd_, POLLIN, 0});
    for (auto& [fd, c] : conns_) {
      short ev = POLLIN;
      if (c.connecting || !c.wbuf.empty()) ev |= POLLOUT;
      pfds.push_back({fd, ev, 0});
    }

    auto wait = std::chrono::duration_cast<std::chrono::milliseconds>(next_tick - Clock::now());
    int timeout = static_cast<int>(std::max<int64_t>(0, wait.count()));
    int rc = ::poll(pfds.data(), pfds.size(), timeout);
    if (rc < 0 && errno != EINTR) throw std::runtime_error("poll failed");

    if (rc > 0) {
      if (pfds[0].revents & POLLIN) AcceptAll(peer_listen_fd_, ConnKind::PeerIn);
      if (pfds[1].revents & POLLIN) AcceptAll(client_listen_fd_, ConnKind::Client);
      for (size_t i = 2; i < pfds.size(); ++i) {
        int fd = pfds[i].fd;
        short re = pfds[i].revents;
        if (!re) continue;
        auto it = conns_.find(fd);
        if (it == conns_.end()) continue;
        if (re & (POLLERR | POLLHUP | POLLNVAL) && !(re & POLLIN)) {
          Close(fd);
          continue;
        }
        if (re & POLLOUT) OnWritable(it->second);
        it = conns_.find(fd);
        if (it != conns_.end() && (re & POLLIN)) OnReadable(it->second);
      }
    }

    for (auto& g : groups_) ProposeBatch(*g);

    // Logical clock: catch up on any ticks we owe (bounded to avoid spirals).
    for (int n = 0; Clock::now() >= next_tick && n < 10; ++n) {
      for (auto& g : groups_) g->node->Tick();
      next_tick += tick;
    }
    if (Clock::now() >= next_tick) next_tick = Clock::now() + tick;

    // Durability barrier: everything every group wrote this iteration (votes,
    // terms, ballots, log entries) reaches disk with ONE fdatasync, before
    // any message or client reply that depends on it is released below.
    if (wal_) wal_->Sync();

    for (auto& g : groups_) ProcessOutput(*g);

    // Compaction writes (snapshot records) are synced at the next barrier;
    // garbage-collect the shared WAL once it outgrows the live state.
    if (wal_) wal_->MaybeCheckpoint();

    // Opportunistic flush so replies don't wait for the next poll round.
    std::vector<int> fds;
    for (auto& [fd, c] : conns_) {
      if (!c.connecting && !c.wbuf.empty()) fds.push_back(fd);
    }
    for (int fd : fds) {
      auto c = conns_.find(fd);
      if (c != conns_.end()) OnWritable(c->second);
    }
  }
}

void Server::ProposeBatch(Group& g) {
  // ReadIndex reads: register each with the leader (no log write).
  for (auto& [client, key] : g.read_batch) {
    uint64_t ctx = next_read_ctx_++;
    if (g.node->RequestRead(ctx)) {
      g.reads[ctx] = {client, std::move(key)};
    } else {
      Reply(client, {Status::NotLeader, g.node->leader(), ""});
    }
  }
  g.read_batch.clear();

  if (g.batch.empty()) return;
  if (g.node->IsLeader()) {
    std::vector<std::string> cmds;
    cmds.reserve(g.batch.size());
    for (auto& [client, cmd] : g.batch) cmds.push_back(std::move(cmd));
    Index first = *g.node->ProposeBatch(std::move(cmds));  // one durable write
    for (size_t i = 0; i < g.batch.size(); ++i) {
      g.pending[first + i] = Pending{g.batch[i].first, g.node->epoch()};
    }
  } else {
    for (auto& [client, cmd] : g.batch) Reply(client, {Status::NotLeader, g.node->leader(), ""});
  }
  g.batch.clear();
}

void Server::ProcessOutput(Group& g) {
  for (Message& m : g.node->TakeMessages()) {
    m.group = g.id;
    SendToPeer(m);
  }

  if (auto snap = g.node->TakeSnapshot()) {
    // On restart, or a snapshot sent by the leader: replace the KV state.
    g.kv.Restore(snap->data);
    std::fprintf(stderr, "[node %u] group=%u restored snapshot at index %llu (%zu keys)\n", id_, g.id,
                 static_cast<unsigned long long>(snap->index), g.kv.size());
  }

  for (const LogEntry& e : g.node->TakeCommitted()) {
    if (e.data.empty()) continue;  // protocol no-op
    ApplyResult res;
    try {
      res = g.kv.Apply(DecodeCommand(e.data));
    } catch (const DecodeError&) {
      res.status = Status::Error;
    }
    auto it = g.pending.find(e.index);
    if (it != g.pending.end()) {
      // If a different leader overwrote this index, the original request was
      // not committed: tell the client to retry (dedup makes retries safe).
      if (it->second.epoch == e.term) {
        Reply(it->second.client, {res.status, kNoNode, res.value});
      } else {
        Reply(it->second.client, {Status::Retry, g.node->leader(), ""});
      }
      g.pending.erase(it);
    }
  }

  ServeReads(g);

  if (opts_.snapshot_every > 0 &&
      g.node->applied_index() >= g.node->snapshot_index() + opts_.snapshot_every) {
    g.node->Compact(g.node->applied_index(), g.kv.Serialize());
  }

  LogRoleChange(g);
  if (!g.node->IsLeader()) {
    for (auto& [idx, p] : g.pending) Reply(p.client, {Status::Retry, g.node->leader(), ""});
    g.pending.clear();
    FailReads(g);
  }
}

void Server::ServeReads(Group& g) {
  for (const auto& rs : g.node->TakeReadyReads()) g.confirmed_reads.push_back(rs);
  // Leadership was confirmed after each read arrived; answer once the state
  // machine reflects everything committed up to the read's index.
  auto it = g.confirmed_reads.begin();
  while (it != g.confirmed_reads.end()) {
    if (g.node->applied_index() < it->index) {
      ++it;
      continue;
    }
    auto r = g.reads.find(it->ctx);
    if (r != g.reads.end()) {
      const auto& data = g.kv.data();
      auto v = data.find(r->second.second);
      Reply(r->second.first, v == data.end() ? ClientResponse{Status::NotFound, kNoNode, ""}
                                             : ClientResponse{Status::Ok, kNoNode, v->second});
      g.reads.erase(r);
    }
    it = g.confirmed_reads.erase(it);
  }
}

void Server::FailReads(Group& g) {
  for (auto& [ctx, req] : g.reads) Reply(req.first, {Status::Retry, g.node->leader(), ""});
  g.reads.clear();
  g.confirmed_reads.clear();
}

void Server::LogRoleChange(Group& g) {
  bool leader = g.node->IsLeader();
  if (leader != g.last_leader || (leader && g.node->epoch() != g.last_epoch)) {
    // Machine-readable for scripts: "[node N] group=G epoch=E role=leader|follower ..."
    std::fprintf(stderr, "[node %u] group=%u epoch=%llu role=%s %s\n", id_, g.id,
                 static_cast<unsigned long long>(g.node->epoch()), leader ? "leader" : "follower",
                 g.node->StateString().c_str());
  }
  g.last_leader = leader;
  g.last_epoch = g.node->epoch();
}

void Server::AcceptAll(int listen_fd, ConnKind kind) {
  for (;;) {
    int fd = ::accept(listen_fd, nullptr, nullptr);
    if (fd < 0) return;  // EAGAIN or transient error
    SetNonBlocking(fd);
    SetNoDelay(fd);
    Conn c;
    c.fd = fd;
    c.gen = next_gen_++;
    c.kind = kind;
    conns_[fd] = std::move(c);
  }
}

void Server::Close(int fd) {
  auto it = conns_.find(fd);
  if (it == conns_.end()) return;
  if (it->second.kind == ConnKind::PeerOut) peer_out_.erase(it->second.peer);
  ::close(fd);
  conns_.erase(it);
}

void Server::OnReadable(Conn& c) {
  int fd = c.fd;
  char buf[64 * 1024];
  for (;;) {
    ssize_t n = ::read(fd, buf, sizeof(buf));
    if (n > 0) {
      c.rbuf.append(buf, static_cast<size_t>(n));
      continue;
    }
    if (n < 0 && errno == EINTR) continue;
    if (n == 0 || (errno != EAGAIN && errno != EWOULDBLOCK)) {
      Close(fd);
      return;
    }
    break;
  }
  try {
    while (auto frame = TryPopFrame(c.rbuf)) {
      HandleFrame(c, *frame);
      if (conns_.find(fd) == conns_.end()) return;
    }
  } catch (const DecodeError& e) {
    std::fprintf(stderr, "[node %u] closing connection: bad frame (%s)\n", id_, e.what());
    Close(fd);
  }
}

void Server::OnWritable(Conn& c) {
  int fd = c.fd;
  if (c.connecting) {
    int err = 0;
    socklen_t len = sizeof(err);
    if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) != 0 || err != 0) {
      Close(fd);  // peer down; reconnect on the next message
      return;
    }
    c.connecting = false;
  }
  while (!c.wbuf.empty()) {
    ssize_t n = ::send(fd, c.wbuf.data(), c.wbuf.size(), MSG_NOSIGNAL);
    if (n > 0) {
      c.wbuf.erase(0, static_cast<size_t>(n));
      continue;
    }
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return;
    if (n < 0 && errno == EINTR) continue;
    Close(fd);
    return;
  }
}

void Server::HandleFrame(Conn& c, const std::string& payload) {
  switch (c.kind) {
    case ConnKind::PeerIn: {
      Message m = DecodeMessage(payload);
      if (m.to == id_ && m.group < groups_.size()) groups_[m.group]->node->Step(m);
      break;
    }
    case ConnKind::Client: {
      Command cmd = DecodeCommand(payload);  // validate before it reaches a log
      uint32_t gid = ShardFor(cmd.key, opts_.shards);
      if (cmd.op == OpType::Get && opts_.read_index && opts_.protocol == "raft") {
        groups_[gid]->read_batch.emplace_back(ClientRef{c.fd, c.gen}, cmd.key);
      } else {
        groups_[gid]->batch.emplace_back(ClientRef{c.fd, c.gen}, payload);
      }
      break;
    }
    case ConnKind::PeerOut:
      break;  // outbound peer links are write-only
  }
}

void Server::ConnectPeer(NodeId peer) {
  for (const auto& a : cluster_) {
    if (a.id != peer) continue;
    try {
      int fd = ConnectTcpNonBlocking(a.host, a.peer_port);
      Conn c;
      c.fd = fd;
      c.gen = next_gen_++;
      c.kind = ConnKind::PeerOut;
      c.peer = peer;
      c.connecting = true;
      conns_[fd] = std::move(c);
      peer_out_[peer] = fd;
    } catch (const std::exception&) {
      // Peer unreachable; consensus retries via heartbeats / elections.
    }
    return;
  }
}

void Server::SendToPeer(const Message& m) {
  if (!peer_out_.count(m.to)) ConnectPeer(m.to);
  auto it = peer_out_.find(m.to);
  if (it == peer_out_.end()) return;
  Conn& c = conns_[it->second];
  if (c.wbuf.size() > opts_.max_peer_buffer) return;  // slow/dead peer: drop
  c.wbuf += Frame(EncodeMessage(m));
}

void Server::Reply(ClientRef to, const ClientResponse& r) {
  auto it = conns_.find(to.fd);
  if (it == conns_.end() || it->second.gen != to.gen) return;  // client went away
  it->second.wbuf += Frame(EncodeResponse(r));
}

}  // namespace raftkv
