// Jepsen-style linearizability testing.
//
// Simulated clients issue concurrent Put/Get/Delete operations against the
// cluster while the chaos simulator injects faults. Each client sends its
// request to a node that believes it is leader (possibly a stale leader in a
// minority partition), waits for that node to apply it, and retries with the
// same sequence number on timeout or leader change, exactly like the real
// client library. Every operation's invocation and response is recorded, and
// the full history is checked for linearizability at the end.

#include <cstdlib>
#include <iostream>
#include <random>

#include "linearizability.h"
#include "sim_cluster.h"
#include "test_framework.h"

using namespace raftkv;
using sim::SimCluster;

// ---------------------------------------------------------------------------
// The checker itself, on hand-written histories.
// ---------------------------------------------------------------------------

namespace {

lin::Operation Op(OpType t, const std::string& v, Status st, const std::string& out, uint64_t call,
                  uint64_t ret) {
  lin::Operation o;
  o.type = t;
  o.key = "k";
  o.value = v;
  o.status = st;
  o.output = out;
  o.call = call;
  o.ret = ret;
  return o;
}

}  // namespace

TEST(CheckerAcceptsSequentialHistory) {
  std::vector<lin::Operation> h = {
      Op(OpType::Get, "", Status::NotFound, "", 1, 2),
      Op(OpType::Put, "1", Status::Ok, "", 3, 4),
      Op(OpType::Get, "", Status::Ok, "1", 5, 6),
      Op(OpType::Delete, "", Status::Ok, "", 7, 8),
      Op(OpType::Get, "", Status::NotFound, "", 9, 10),
  };
  CHECK(lin::Check(h).ok);
}

TEST(CheckerRejectsReadOfValueNeverWritten) {
  std::vector<lin::Operation> h = {
      Op(OpType::Put, "1", Status::Ok, "", 1, 2),
      Op(OpType::Get, "", Status::Ok, "2", 3, 4),
  };
  CHECK(!lin::Check(h).ok);
}

TEST(CheckerAllowsEitherOrderForConcurrentWrites) {
  // put(1) and put(2) overlap; a later read may see either one.
  for (const char* seen : {"1", "2"}) {
    std::vector<lin::Operation> h = {
        Op(OpType::Put, "1", Status::Ok, "", 1, 4),
        Op(OpType::Put, "2", Status::Ok, "", 2, 3),
        Op(OpType::Get, "", Status::Ok, seen, 5, 6),
    };
    CHECK(lin::Check(h).ok);
  }
}

TEST(CheckerRejectsStaleReadAfterNewerRead) {
  // After concurrent writes finish, reads can't flip back and forth:
  // seeing "2" and then "1" (with no write in between) is not linearizable.
  std::vector<lin::Operation> h = {
      Op(OpType::Put, "1", Status::Ok, "", 1, 4),
      Op(OpType::Put, "2", Status::Ok, "", 2, 3),
      Op(OpType::Get, "", Status::Ok, "2", 5, 6),
      Op(OpType::Get, "", Status::Ok, "1", 7, 8),
  };
  CHECK(!lin::Check(h).ok);
}

TEST(CheckerRejectsReadThatMissesCompletedWrite) {
  // A write that finished before a read began must be visible to it.
  std::vector<lin::Operation> h = {
      Op(OpType::Put, "1", Status::Ok, "", 1, 2),
      Op(OpType::Put, "2", Status::Ok, "", 3, 4),
      Op(OpType::Get, "", Status::Ok, "1", 5, 6),
  };
  CHECK(!lin::Check(h).ok);
}

// ---------------------------------------------------------------------------
// Whole-system linearizability under faults.
// ---------------------------------------------------------------------------

namespace {

struct Options {
  int compact_every = 0;
  // Serve Gets with ReadIndex (leader confirms leadership with a heartbeat
  // quorum, then reads locally) instead of replicating them through the log.
  bool read_index = false;
  // Disables pre-vote and check-quorum, which together act like a leader
  // lease. Without them, only ReadIndex's quorum confirmation keeps a deposed
  // leader from serving stale reads, so this mode tests that step directly.
  bool no_lease = false;
  // Deliberately unsafe read path, used only to prove the checker catches
  // real anomalies: a leader answers Gets from its local state immediately,
  // without going through the log. A deposed leader that doesn't know it has
  // been replaced then serves stale data.
  bool unsafe_local_reads = false;
};

struct ClientState {
  uint64_t id = 0;
  uint64_t seq = 0;
  bool busy = false;
  size_t op = 0;  // index into history
  Command cmd;
  NodeId target = kNoNode;
  Index index = 0;
  int waited = 0;
};

lin::Result RunHistory(sim::Protocol protocol, uint64_t seed, int ticks, Options opt) {
  constexpr int kNodes = 5;
  constexpr int kClients = 6;
  const std::vector<std::string> keys = {"a", "b", "c"};

  RaftConfig cfg;
  cfg.pre_vote = cfg.check_quorum = !opt.no_lease;
  SimCluster c(kNodes, seed, protocol, cfg);
  c.compact_every = opt.compact_every;
  std::mt19937_64 rng(seed * 104729 + 7);
  auto chance = [&](double p) { return std::uniform_real_distribution<double>(0, 1)(rng) < p; };
  auto pick_node = [&] { return SimCluster::Id(static_cast<int>(rng() % kNodes)); };
  c.net = {.drop_rate = 0.03, .duplicate_rate = 0.02, .min_delay = 1, .max_delay = 4};

  std::vector<lin::Operation> history;
  std::vector<ClientState> clients(kClients);
  for (int i = 0; i < kClients; ++i) clients[i].id = 100 + static_cast<uint64_t>(i);
  uint64_t clock = 0;  // logical real time: strictly increasing event counter

  auto complete = [&](ClientState& cl, const ApplyResult& res) {
    lin::Operation& op = history[cl.op];
    op.status = res.status;
    op.output = res.value;
    op.ret = ++clock;
    cl.busy = false;
  };

  c.on_read = [&](NodeId node, uint64_t ctx) {
    for (auto& cl : clients) {
      if (cl.busy && cl.target == node && ctx == (cl.id << 32 | cl.seq) && cl.index == 0) {
        const auto& d = c.kv(node).data();
        auto it = d.find(cl.cmd.key);
        complete(cl, it == d.end() ? ApplyResult{Status::NotFound, ""} : ApplyResult{Status::Ok, it->second});
      }
    }
  };

  c.on_apply = [&](NodeId node, Index index, const Command& cmd, const ApplyResult& res) {
    for (auto& cl : clients) {
      if (cl.busy && cmd.client_id == cl.id && cmd.seq == cl.seq && node == cl.target && index == cl.index) {
        complete(cl, res);
      }
    }
  };

  auto submit = [&](ClientState& cl) {
    cl.waited = 0;
    cl.target = kNoNode;
    auto leaders = c.Leaders();
    if (leaders.empty()) return;
    NodeId to = leaders[rng() % leaders.size()];
    if (opt.unsafe_local_reads && cl.cmd.op == OpType::Get) {
      const auto& d = c.kv(to).data();
      auto it = d.find(cl.cmd.key);
      complete(cl, it == d.end() ? ApplyResult{Status::NotFound, ""} : ApplyResult{Status::Ok, it->second});
      return;
    }
    if (opt.read_index && cl.cmd.op == OpType::Get) {
      cl.target = to;
      cl.index = 0;  // marks "waiting for a ReadIndex read", not a log entry
      if (!c.RequestRead(to, cl.id << 32 | cl.seq)) cl.target = kNoNode;
      return;
    }
    if (auto idx = c.Propose(to, cl.cmd)) {
      cl.target = to;
      cl.index = *idx;
    }
  };

  auto drive_clients = [&](bool new_ops) {
    for (auto& cl : clients) {
      if (!cl.busy) {
        if (!new_ops || !chance(0.15)) continue;
        lin::Operation op;
        op.client = static_cast<int>(cl.id);
        op.key = keys[rng() % keys.size()];
        uint64_t r = rng() % 10;
        op.type = r < 5 ? OpType::Put : r < 9 ? OpType::Get : OpType::Delete;
        if (op.type == OpType::Put) op.value = std::to_string(cl.id) + ":" + std::to_string(cl.seq + 1);
        op.call = ++clock;
        history.push_back(op);
        cl.busy = true;
        cl.op = history.size() - 1;
        cl.cmd = Command{cl.id, ++cl.seq, op.type, op.key, op.value};
        submit(cl);
        continue;
      }
      // Waiting: retry (same seq) if the target died or it is taking too long.
      ++cl.waited;
      if (cl.target == kNoNode ? cl.waited > 5 : (!c.alive(cl.target) || cl.waited > 40)) submit(cl);
    }
  };

  for (int t = 0; t < ticks; ++t) {
    if (chance(0.004)) {
      int down = 0;
      for (int i = 0; i < kNodes; ++i) down += c.alive(SimCluster::Id(i)) ? 0 : 1;
      if (down < 2) c.Crash(pick_node());
    }
    if (chance(0.01)) c.Restart(pick_node());
    if (chance(0.004)) {
      std::vector<NodeId> side;
      for (int i = 0; i < kNodes; ++i) {
        if (chance(0.5)) side.push_back(SimCluster::Id(i));
      }
      c.Partition(side);
    }
    if (chance(0.008)) c.Heal();
    drive_clients(true);
    c.Tick();
  }

  // Heal and let every in-flight operation finish (with retries).
  c.Heal();
  c.net = {.drop_rate = 0, .duplicate_rate = 0, .min_delay = 1, .max_delay = 2};
  for (int i = 0; i < kNodes; ++i) c.Restart(SimCluster::Id(i));
  bool drained = c.RunUntil([&] {
    drive_clients(false);
    for (auto& cl : clients) {
      if (cl.busy) return false;
    }
    return true;
  }, 5000);
  if (!drained) throw std::runtime_error("client operations did not complete after healing");
  return lin::Check(history);
}

void LinearizabilityCampaign(sim::Protocol protocol, int compact_every = 0, bool read_index = false,
                             bool no_lease = false) {
  int seeds = 100;
  if (const char* env = std::getenv("RAFTKV_LIN_SEEDS")) seeds = std::atoi(env);
  size_t ops = 0;
  for (int s = 1; s <= seeds; ++s) {
    lin::Result r = RunHistory(protocol, static_cast<uint64_t>(s), 3000,
                               {.compact_every = compact_every, .read_index = read_index, .no_lease = no_lease});
    if (!r.ok) {
      std::cerr << "  " << sim::ProtocolName(protocol) << " seed " << s
                << ": history NOT linearizable (key " << r.bad_key << ")\n";
      CHECK(r.ok);
    }
    ops += r.operations;
  }
  std::cout << "  " << sim::ProtocolName(protocol) << " linearizability" << (compact_every ? " + compaction" : "")
            << (read_index ? " + ReadIndex reads" : "") << (no_lease ? " (no pre-vote/check-quorum)" : "")
            << ": " << seeds << " seeds, " << ops
            << " client operations under faults, all histories linearizable\n";
}

}  // namespace

TEST(LinearizableUnderFaultsRaft) { LinearizabilityCampaign(sim::Protocol::Raft); }

TEST(LinearizableUnderFaultsPaxos) { LinearizabilityCampaign(sim::Protocol::Paxos); }

TEST(LinearizableWithCompactionRaft) { LinearizabilityCampaign(sim::Protocol::Raft, 25); }

TEST(LinearizableWithCompactionPaxos) { LinearizabilityCampaign(sim::Protocol::Paxos, 25); }

TEST(LinearizableWithReadIndexRaft) { LinearizabilityCampaign(sim::Protocol::Raft, 25, true); }

TEST(LinearizableWithReadIndexRaftNoLeaderLease) {
  LinearizabilityCampaign(sim::Protocol::Raft, 25, true, /*no_lease=*/true);
}

// Proves the checker has teeth: with the unsafe "read locally on the leader"
// shortcut, a deposed leader in a minority partition serves stale reads, and
// the checker must flag a non-linearizable history.
TEST(CheckerCatchesStaleReadsFromUnsafeLocalReadPath) {
  for (auto p : {sim::Protocol::Raft, sim::Protocol::Paxos}) {
    bool caught = false;
    for (int s = 1; s <= 100 && !caught; ++s) {
      caught = !RunHistory(p, static_cast<uint64_t>(s), 3000, {.unsafe_local_reads = true}).ok;
    }
    if (!caught) std::cerr << "  " << sim::ProtocolName(p) << ": stale reads were not detected\n";
    CHECK(caught);
  }
}
