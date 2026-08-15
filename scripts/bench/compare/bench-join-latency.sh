#!/bin/bash
# bench-join-latency.sh — time from daemon start to the joining node's first
# successful write, and what that join does to the nodes already running.
# etcfs only: a node claims its own arena, so the expected impact on the
# survivors is none, and that claim is what this measures.
#
# GFS2's equivalent has no timing to take — a joining node needs a free journal,
# and journals are allocated at mkfs (bench-gfs2.sh formats with one per node),
# so a node past that count does not join slowly, it does not join at all.
#
# Usage:
#   ./bench-join-latency.sh
set -euo pipefail
export COMPARE_BACKEND="${COMPARE_BACKEND:-etcfs}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"
[[ "$COMPARE_BACKEND_BASE" == "etcfs" ]] || die "bench-join-latency.sh is etcfs-only (see header)"

RUNTIME="${ETCFS_BENCH_RUNTIME:-30}"

compare_begin
compare_mount
[[ "${#BENCH_NODES[@]}" -ge 3 ]] || die "join latency needs three nodes"
JOINER="${BENCH_NODES[-1]}"
SURVIVORS=("${BENCH_NODES[@]:0:$((${#BENCH_NODES[@]} - 1))}")

LOAD_JOB="
[global]
ioengine=psync
direct=1
filename=$MOUNT_PATH/join-load-@NODE@.dat
size=1G
runtime=$RUNTIME
time_based=1
[randwrite-4k]
rw=randwrite
bs=4k
"
baseline=$(compare_parallel_fio "join-baseline" "$RUNTIME" "$LOAD_JOB" "${SURVIVORS[@]}")

compare_etcfs_snapshot_cmdline "$JOINER"
log "Taking $JOINER out cleanly, then timing its rejoin under load..."
compare_kill_node "$JOINER"
sleep "${ETCFS_LEASE_TTL_SECONDS:-15}"

# The survivors' load and the join have to overlap, so the load runs in the
# background and its aggregate is read back off its own JSONs afterwards
# (a subshell's variable would not survive).
compare_parallel_fio "join-during" "$RUNTIME" "$LOAD_JOB" "${SURVIVORS[@]}" >/dev/null &
load_pid=$!
sleep 2
join_s=$(compare_etcfs_start "$JOINER")
wait "$load_pid"
during=$(jq -s 'map(.jobs[0].write.bw // 0) | add / 1024 | . * 100 | round / 100' \
    "$RESULTS_DIR"/join-during-*.json)

compare_headline join-latency join_s "$join_s" s
compare_headline join-latency survivor_baseline_mibps "$baseline" MiB/s
compare_headline join-latency survivor_during_join_mibps "$during" MiB/s
compare_headline join-latency survivor_impact_pct \
    "$(awk -v b="$baseline" -v d="$during" 'BEGIN{ if (b+0 == 0) print "0"; else printf "%.2f", (b-d)/b*100 }')" %
