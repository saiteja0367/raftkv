// raftkv-cli: command-line client and load generator.
//
//   raftkv-cli --cluster SPEC put <key> <value>
//   raftkv-cli --cluster SPEC get <key>
//   raftkv-cli --cluster SPEC del <key>
//   raftkv-cli --cluster SPEC bench [--ops N] [--clients C] [--value-size B]

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

#include "net/client.h"

namespace {

void Usage() {
  std::fprintf(stderr,
               "usage: raftkv-cli --cluster SPEC [--shards N] <command>\n"
               "  put <key> <value>\n"
               "  get <key>\n"
               "  del <key>\n"
               "  bench [--ops N] [--clients C] [--value-size BYTES] [--reads PERCENT]\n");
}

int Bench(const std::vector<raftkv::NodeAddr>& cluster, uint32_t shards, int ops, int clients,
          int value_size, int read_pct) {
  using Clock = std::chrono::steady_clock;
  std::vector<std::vector<double>> latencies(clients);
  std::atomic<int> errors{0};
  std::string value(static_cast<size_t>(value_size), 'x');

  auto start = Clock::now();
  std::vector<std::thread> threads;
  for (int t = 0; t < clients; ++t) {
    threads.emplace_back([&, t] {
      raftkv::Client c(cluster, shards);
      int n = ops / clients + (t < ops % clients ? 1 : 0);
      latencies[t].reserve(n);
      int written = 0;
      for (int i = 0; i < n; ++i) {
        bool read = written > 0 && static_cast<int>((static_cast<unsigned>(i) * 2654435761u) % 100) < read_pct;
        auto s = Clock::now();
        auto r = read ? c.Get("bench-" + std::to_string(t) + "-" + std::to_string(i % written))
                      : c.Put("bench-" + std::to_string(t) + "-" + std::to_string(written++), value);
        auto e = Clock::now();
        if (r.status != raftkv::Status::Ok) ++errors;
        latencies[t].push_back(std::chrono::duration<double, std::milli>(e - s).count());
      }
    });
  }
  for (auto& th : threads) th.join();
  double secs = std::chrono::duration<double>(Clock::now() - start).count();

  std::vector<double> all;
  for (auto& v : latencies) all.insert(all.end(), v.begin(), v.end());
  std::sort(all.begin(), all.end());
  auto pct = [&](double p) {
    if (all.empty()) return 0.0;
    size_t i = static_cast<size_t>(p * static_cast<double>(all.size() - 1));
    return all[i];
  };
  std::printf("ops=%zu clients=%d shards=%u reads=%d%% value_size=%dB errors=%d\n", all.size(), clients,
              shards, read_pct, value_size, errors.load());
  std::printf("throughput=%.0f ops/s  latency p50=%.2fms p95=%.2fms p99=%.2fms max=%.2fms\n",
              static_cast<double>(all.size()) / secs, pct(0.50), pct(0.95), pct(0.99),
              all.empty() ? 0.0 : all.back());
  return errors.load() == 0 ? 0 : 1;
}

}  // namespace

int main(int argc, char** argv) {
  std::string cluster;
  uint32_t shards = 1;
  std::vector<std::string> args;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "--cluster" && i + 1 < argc) {
      cluster = argv[++i];
    } else if (a == "--shards" && i + 1 < argc) {
      shards = static_cast<uint32_t>(std::stoul(argv[++i]));
    } else {
      args.push_back(a);
    }
  }
  if (cluster.empty() || args.empty()) {
    Usage();
    return 2;
  }

  try {
    auto nodes = raftkv::ParseCluster(cluster);
    const std::string& cmd = args[0];

    if (cmd == "bench") {
      int ops = 10000, clients = 8, value_size = 100, read_pct = 0;
      for (size_t i = 1; i + 1 < args.size(); i += 2) {
        if (args[i] == "--ops") ops = std::stoi(args[i + 1]);
        else if (args[i] == "--clients") clients = std::stoi(args[i + 1]);
        else if (args[i] == "--value-size") value_size = std::stoi(args[i + 1]);
        else if (args[i] == "--reads") read_pct = std::stoi(args[i + 1]);
      }
      return Bench(nodes, shards, ops, std::max(1, clients), value_size, read_pct);
    }

    raftkv::Client client(nodes, shards);
    raftkv::ApplyResult r;
    if (cmd == "put" && args.size() == 3) r = client.Put(args[1], args[2]);
    else if (cmd == "get" && args.size() == 2) r = client.Get(args[1]);
    else if (cmd == "del" && args.size() == 2) r = client.Delete(args[1]);
    else {
      Usage();
      return 2;
    }

    if (r.status == raftkv::Status::Ok) {
      std::printf("%s\n", cmd == "get" ? r.value.c_str() : "OK");
      return 0;
    }
    std::printf("%s%s%s\n", raftkv::StatusName(r.status), r.value.empty() ? "" : ": ", r.value.c_str());
    return r.status == raftkv::Status::NotFound ? 1 : 3;
  } catch (const std::exception& e) {
    std::fprintf(stderr, "error: %s\n", e.what());
    return 2;
  }
}
