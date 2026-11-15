// raftkv-server: runs one node of a raftkv cluster.
//
//   raftkv-server --id 1
//     --cluster 1=127.0.0.1:7001:8001,2=127.0.0.1:7002:8002,3=127.0.0.1:7003:8003
//     --data ./data/node1

#include <atomic>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <string>

#include "net/server.h"

namespace {

std::atomic<bool> g_stop{false};

void OnSignal(int) { g_stop.store(true); }

void Usage() {
  std::fprintf(stderr,
               "usage: raftkv-server --id N --cluster SPEC --data DIR [--protocol raft|paxos] [--shards N] [--wal shared|per-group]\n"
               "                     [--snapshot-every N] [--no-read-index] [--tick-ms MS]\n"
               "  SPEC = id=host:peer_port:client_port,... (e.g. 1=127.0.0.1:7001:8001,...)\n");
}

}  // namespace

int main(int argc, char** argv) {
  uint32_t id = 0;
  std::string cluster, data;
  raftkv::ServerOptions opts;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    auto next = [&]() -> std::string {
      if (i + 1 >= argc) {
        Usage();
        std::exit(2);
      }
      return argv[++i];
    };
    if (a == "--id") id = static_cast<uint32_t>(std::stoul(next()));
    else if (a == "--cluster") cluster = next();
    else if (a == "--data") data = next();
    else if (a == "--tick-ms") opts.tick_ms = std::stoi(next());
    else if (a == "--protocol") opts.protocol = next();
    else if (a == "--shards") opts.shards = static_cast<uint32_t>(std::stoul(next()));
    else if (a == "--wal") opts.wal = next();
    else if (a == "--snapshot-every") opts.snapshot_every = std::stoull(next());
    else if (a == "--no-read-index") opts.read_index = false;
    else {
      Usage();
      return 2;
    }
  }
  if (id == 0 || cluster.empty() || data.empty()) {
    Usage();
    return 2;
  }

  std::signal(SIGINT, OnSignal);
  std::signal(SIGTERM, OnSignal);
  std::signal(SIGPIPE, SIG_IGN);

  try {
    raftkv::Server server(id, raftkv::ParseCluster(cluster), data, opts);
    server.Run(g_stop);
  } catch (const std::exception& e) {
    std::fprintf(stderr, "fatal: %s\n", e.what());
    return 1;
  }
  std::fprintf(stderr, "[node %u] shut down\n", id);
  return 0;
}
