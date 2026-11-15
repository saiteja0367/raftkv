#!/usr/bin/env bash
# Start / stop a local 3-node raftkv cluster.
#
#   scripts/cluster.sh start     # starts nodes 1-3 in the background
#   PROTOCOL=paxos scripts/cluster.sh start   # same, running Multi-Paxos
#   SHARDS=8 scripts/cluster.sh start         # 8 consensus groups (multi-Raft)
#   scripts/cluster.sh stop      # stops them
#   scripts/cluster.sh kill N    # hard-kills node N (SIGKILL, no clean shutdown)
#   scripts/cluster.sh restart N # restarts node N from its data directory
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/build/raftkv-server}"
WORK="${WORK:-$ROOT/.cluster}"
PROTOCOL="${PROTOCOL:-raft}"   # raft or paxos
SHARDS="${SHARDS:-1}"          # consensus groups per node
WAL="${WAL:-shared}"           # shared (one WAL + one fsync per node) or per-group
SNAPSHOT_EVERY="${SNAPSHOT_EVERY:-10000}"  # compact each group's log every N entries
EXTRA_ARGS="${EXTRA_ARGS:-}"   # e.g. --no-read-index
CLUSTER="1=127.0.0.1:7001:8001,2=127.0.0.1:7002:8002,3=127.0.0.1:7003:8003"

start_node() {
  local id=$1
  mkdir -p "$WORK"
  "$BIN" --id "$id" --cluster "$CLUSTER" --data "$WORK/node$id" --protocol "$PROTOCOL" --shards "$SHARDS" --wal "$WAL" --snapshot-every "$SNAPSHOT_EVERY" $EXTRA_ARGS \
    >>"$WORK/node$id.log" 2>&1 &
  echo $! >"$WORK/node$id.pid"
  echo "started node $id (pid $!)"
}

stop_node() {
  local id=$1 sig=${2:-TERM}
  local pidfile="$WORK/node$id.pid"
  if [[ -f "$pidfile" ]]; then
    kill "-$sig" "$(cat "$pidfile")" 2>/dev/null || true
    rm -f "$pidfile"
    echo "stopped node $id ($sig)"
  fi
}

case "${1:-}" in
  start) for i in 1 2 3; do start_node "$i"; done ;;
  stop) for i in 1 2 3; do stop_node "$i"; done ;;
  kill) stop_node "$2" KILL ;;
  restart) start_node "$2" ;;
  spec) echo "$CLUSTER" ;;
  *) echo "usage: $0 {start|stop|kill N|restart N|spec}" >&2; exit 2 ;;
esac
