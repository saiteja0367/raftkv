// Randomized fault-injection ("chaos") test.
//
// For each seed, a 5-node cluster runs under a storm of faults: message loss,
// duplication, reordering and delay; network partitions; node crashes and
// restarts; while clients keep writing. The simulator checks election safety
// and state machine safety on every tick. At the end, all faults are healed
// and the test checks liveness (the cluster recovers and commits new writes)
// and that every replica converged to the same key-value state.

#include <cstdlib>
#include <iostream>

#include "sim_cluster.h"
#include "test_framework.h"

using namespace raftkv;
using sim::SimCluster;

namespace {

struct Stats {
  size_t committed = 0;
  size_t snapshots = 0;
  size_t leaders = 0;
  size_t crashes = 0;
  size_t partitions = 0;
};

void RunChaos(sim::Protocol protocol, uint64_t seed, int ticks, Stats& stats, int compact_every = 0) {
  constexpr int kNodes = 5;
  SimCluster c(kNodes, seed, protocol);
  c.compact_every = compact_every;
  std::mt19937_64 rng(seed * 7919 + 1);
  auto chance = [&](double p) { return std::uniform_real_distribution<double>(0, 1)(rng) < p; };
  auto pick = [&] { return SimCluster::Id(static_cast<int>(rng() % kNodes)); };

  c.net = {.drop_rate = 0.05, .duplicate_rate = 0.02, .min_delay = 1, .max_delay = 4};

  uint64_t seq = 0;
  for (int t = 0; t < ticks; ++t) {
    // Faults.
    if (chance(0.004)) {
      int down = 0;
      for (int i = 0; i < kNodes; ++i) down += c.alive(SimCluster::Id(i)) ? 0 : 1;
      NodeId victim = pick();
      if (down < 2 && c.alive(victim)) {
        c.Crash(victim);
        ++stats.crashes;
      }
    }
    if (chance(0.01)) c.Restart(pick());
    if (chance(0.003)) {
      std::vector<NodeId> side;
      for (int i = 0; i < kNodes; ++i) {
        if (chance(0.5)) side.push_back(SimCluster::Id(i));
      }
      c.Partition(side);
      ++stats.partitions;
    }
    if (chance(0.01)) c.Heal();
    if (chance(0.002)) c.net.drop_rate = std::uniform_real_distribution<double>(0, 0.3)(rng);

    // Client writes go to whoever currently thinks it is leader (possibly a
    // stale leader in a minority partition, whose writes must never commit
    // in a way that conflicts with the majority).
    if (chance(0.3)) {
      if (auto leader = c.Leader()) {
        ++seq;
        c.Propose(*leader, Command{1, seq, OpType::Put, "k" + std::to_string(seq % 64),
                                   std::to_string(seq)});
      }
    }
    c.Tick();
  }

  // Heal everything and require the cluster to make progress again.
  c.Heal();
  c.net = {.drop_rate = 0, .duplicate_rate = 0, .min_delay = 1, .max_delay = 2};
  for (int i = 0; i < kNodes; ++i) c.Restart(SimCluster::Id(i));

  // Like a real client: keep (re)submitting the marker write to the current
  // leader until every replica has applied it. Retries reuse the same
  // (client_id, seq), so the state machine applies the write exactly once.
  const Command marker{2, 1, OpType::Put, "marker", "done"};
  std::optional<NodeId> proposer;
  uint64_t proposer_term = 0;
  auto all_applied = [&] {
    for (int i = 0; i < kNodes; ++i) {
      const auto& d = c.kv(SimCluster::Id(i)).data();
      if (!d.count("marker")) return false;
    }
    return true;
  };
  bool live = c.RunUntil([&] {
    if (all_applied()) return true;
    auto leader = c.Leader();
    if (leader && (!proposer || *proposer != *leader || c.node(*leader)->epoch() != proposer_term)) {
      c.Propose(*leader, marker);
      proposer = leader;
      proposer_term = c.node(*leader)->epoch();
    }
    return false;
  }, 3000);
  if (!live) {
    std::cerr << "  " << sim::ProtocolName(protocol) << " seed " << seed
              << ": cluster failed to recover after faults healed\n";
    for (int i = 0; i < kNodes; ++i) {
      auto* n = c.node(SimCluster::Id(i));
      std::cerr << "    node " << i + 1 << " " << (n ? n->StateString() : "down") << "\n";
    }
    CHECK(live);
  }
  c.Run(50);  // let every replica apply up to the marker
  c.CheckReplicasConverged();
  CHECK_EQ(c.kv(1).data().at("marker"), std::string("done"));

  stats.committed += c.committed_count();
  stats.leaders += c.leaders_elected();
  stats.snapshots += c.snapshots_installed();
}

}  // namespace

void ChaosCampaign(sim::Protocol protocol, int compact_every = 0) {
  int seeds = 200;
  if (const char* env = std::getenv("RAFTKV_CHAOS_SEEDS")) seeds = std::atoi(env);
  Stats stats;
  for (int s = 1; s <= seeds; ++s) {
    try {
      RunChaos(protocol, static_cast<uint64_t>(s), 5000, stats, compact_every);
    } catch (const sim::SafetyViolation& e) {
      std::cerr << "  " << sim::ProtocolName(protocol) << " seed " << s << ": SAFETY VIOLATION: " << e.what()
                << "\n";
      CHECK(false);
    }
  }
  std::cout << "  " << sim::ProtocolName(protocol) << " chaos" << (compact_every ? " + compaction" : "")
            << ": " << seeds << " seeds x 5000 ticks, " << stats.crashes << " crashes, " << stats.partitions
            << " partitions, " << stats.leaders << " leader elections, " << stats.committed
            << " entries committed";
  if (compact_every) std::cout << ", " << stats.snapshots << " snapshots restored/installed";
  std::cout << ", 0 safety violations\n";
}

TEST(ChaosRaftSafetyAndLivenessUnderFaults) { ChaosCampaign(sim::Protocol::Raft); }

TEST(ChaosPaxosSafetyAndLivenessUnderFaults) { ChaosCampaign(sim::Protocol::Paxos); }

// Same campaigns with aggressive log compaction (every 40 applied entries):
// restarted and lagging nodes must catch up through snapshot transfer, and
// Paxos leaders must adopt snapshots reported in promises.
TEST(ChaosRaftWithCompaction) { ChaosCampaign(sim::Protocol::Raft, 40); }

TEST(ChaosPaxosWithCompaction) { ChaosCampaign(sim::Protocol::Paxos, 40); }
