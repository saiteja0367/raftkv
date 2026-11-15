#!/usr/bin/env bash
# End-to-end failover test on a real 3-process cluster over TCP.
#   PROTOCOL=raft|paxos  SHARDS=N  WAL=shared|per-group  SNAPSHOT_EVERY=N
#
#  1. Start 3 nodes and write 100 keys (spread across all shards).
#  2. SIGKILL the leader of shard 0 (no clean shutdown, like a power loss).
#     With several shards, that node also leads other shards, which fail over too.
#  3. Verify a new leader is elected, all 100 keys are still readable, and
#     new writes succeed with only 2 of 3 nodes up.
#  4. Restart the killed node from its on-disk log and verify it catches up.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CLI="$ROOT/build/raftkv-cli"
export WORK="$ROOT/.failover-test"
export PROTOCOL="${PROTOCOL:-raft}"
export SHARDS="${SHARDS:-1}" WAL="${WAL:-shared}" SNAPSHOT_EVERY="${SNAPSHOT_EVERY:-10000}"
echo "== protocol: $PROTOCOL, shards: $SHARDS, wal: $WAL, snapshot every: $SNAPSHOT_EVERY"
rm -rf "$WORK"
C="$("$ROOT/scripts/cluster.sh" spec)"
kv() { "$CLI" --cluster "$C" --shards "$SHARDS" "$@"; }
trap '"$ROOT/scripts/cluster.sh" stop >/dev/null; ' EXIT

"$ROOT/scripts/cluster.sh" start
sleep 1

echo "== writing 100 keys"
for i in $(seq 1 100); do kv put "key$i" "value$i" >/dev/null; done

# Leader of shard 0 = the node that announced leadership of group 0 in the highest epoch.
leader=$(grep -h "group=0 epoch=[0-9]* role=leader" "$WORK"/node*.log \
  | sed -E 's/.*\[node ([0-9]+)\] group=0 epoch=([0-9]+) role=leader.*/\2 \1/' \
  | sort -n | tail -1 | cut -d' ' -f2)
led=$(grep -h "role=leader" "$WORK/node$leader.log" | sed -E 's/.*group=([0-9]+).*/\1/' | sort -u | wc -l)
echo "== node $leader leads shard 0 (and $led shard(s) in total); killing it with SIGKILL"
"$ROOT/scripts/cluster.sh" kill "$leader"

start=$(date +%s%N)
kv put after-failover yes >/dev/null
end=$(date +%s%N)
echo "== first write after leader crash succeeded in $(( (end - start) / 1000000 )) ms (includes re-election)"

echo "== verifying all 100 keys survived"
for i in $(seq 1 100); do
  got=$(kv get "key$i")
  [[ "$got" == "value$i" ]] || { echo "FAIL: key$i = '$got'"; exit 1; }
done

echo "== writing 300 more keys while node $leader is down"
for i in $(seq 101 400); do kv put "key$i" "value$i" >/dev/null; done

echo "== restarting node $leader from its on-disk state"
"$ROOT/scripts/cluster.sh" restart "$leader"
sleep 2
"$ROOT/scripts/cluster.sh" kill "$(( leader % 3 + 1 ))"  # take down a different node
got=$(kv get after-failover)
[[ "$got" == "yes" ]] || { echo "FAIL: restarted node did not catch up"; exit 1; }
for i in 150 250 400; do
  got=$(kv get "key$i")
  [[ "$got" == "value$i" ]] || { echo "FAIL: key$i = '$got' after catch-up"; exit 1; }
done
snaps=$(grep -c "restored snapshot" "$WORK/node$leader.log" || true)
echo "== restarted node caught up and serves a quorum ($snaps snapshot restore(s) logged on it)"

echo "PASS"
