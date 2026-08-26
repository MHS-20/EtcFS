#!/bin/bash
# bench-rejoin-load.sh — etcfs only. One node leaves cleanly and rejoins, over
# and over, while the rest of the cluster writes. Two things are being watched:
# the survivors' throughput dip during each cycle (a new node claims its own
# arena, so there should be none), and whether arena reclaim keeps up — the
# leaver's arenas must come back, or every cycle leaks capacity.
#
# ETCFS_REJOIN_CYCLES (default 3), ETCFS_BENCH_RUNTIME (per-cycle load, 30),
# ETCFS_REJOIN_SETTLE (default 120) — how long to wait before re-sampling the
# arena count, so a reclaim still in flight when the last cycle ended is told
# apart from capacity the cycles leaked.
#
# Usage:
#   ./bench-rejoin-load.sh
set -euo pipefail
export COMPARE_BACKEND="${COMPARE_BACKEND:-etcfs}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"
[[ "$COMPARE_BACKEND_BASE" == "etcfs" ]] || die "bench-rejoin-load.sh is etcfs-only"

CYCLES="${ETCFS_REJOIN_CYCLES:-3}"
RUNTIME="${ETCFS_BENCH_RUNTIME:-30}"
SETTLE="${ETCFS_REJOIN_SETTLE:-120}"

# etcfuse_arenas_owned, summed over the whole cluster: the leaver's arenas
# should come back to the pool rather than staying charged to a node that is
# gone. Read off the metrics listener bootstrap-cluster.sh already starts on
# :9090, so nothing has to be installed on the nodes for this.
arenas_owned_total() {
    local ip total=0 v
    for ip in "${COMPARE_PRIV_IPS[@]}"; do
        v=$($SSH_CMD "ec2-user@${COMPARE_PUB_IPS[0]}" \
            "curl -s --max-time 5 http://$ip:9090/metrics | awk '/^etcfuse_arenas_owned /{print \$2}'" || echo 0)
        total=$(awk -v a="$total" -v b="${v:-0}" 'BEGIN{printf "%.0f", a+b}')
    done
    echo "$total"
}

compare_begin
compare_mount
[[ "${#BENCH_NODES[@]}" -ge 3 ]] || die "leave/rejoin needs a node to cycle and two to keep writing"
LEAVER="${BENCH_NODES[-1]}"
SURVIVORS=("${BENCH_NODES[@]:0:$((${#BENCH_NODES[@]} - 1))}")

LOAD_JOB="
[global]
ioengine=psync
direct=1
bs=1M
filename=$MOUNT_PATH/rejoin-load-@NODE@.dat
size=1G
runtime=$RUNTIME
time_based=1
group_reporting=1
[seqwrite]
rw=write
"
baseline=$(compare_parallel_fio "rejoin-baseline" "$RUNTIME" "$LOAD_JOB" "${SURVIVORS[@]}")
compare_headline leave-rejoin survivor_baseline_mibps "$baseline" MiB/s
compare_headline leave-rejoin arenas_owned_before "$(arenas_owned_total)" arenas

compare_etcfs_snapshot_cmdline "$LEAVER"
worst="$baseline"
total_join=0
for c in $(seq 1 "$CYCLES"); do
    compare_parallel_fio "rejoin-cycle$c" "$RUNTIME" "$LOAD_JOB" "${SURVIVORS[@]}" > "$RESULTS_DIR/rejoin-cycle$c.mibps" &
    load_pid=$!
    # Clean leave: SIGTERM, so the node releases its lease and locks rather
    # than being fenced — the fenced path is bench-node-kill.sh's scenario.
    $SSH_CMD "ec2-user@$LEAVER" "sudo killall etcfuse-meta etcfuse 2>/dev/null; sudo umount -l $MOUNT_PATH 2>/dev/null; true"
    sleep 5
    join_s=$(compare_etcfs_start "$LEAVER")
    total_join=$(awk -v a="$total_join" -v b="$join_s" 'BEGIN{printf "%.3f", a+b}')
    wait "$load_pid"
    cycle=$(cat "$RESULTS_DIR/rejoin-cycle$c.mibps")
    log "cycle $c: rejoin ${join_s}s, survivors ${cycle} MiB/s"
    worst=$(awk -v w="$worst" -v c="$cycle" 'BEGIN{print (c < w) ? c : w}')
done

compare_headline leave-rejoin mean_rejoin_s "$(compare_div "$total_join" "$CYCLES" 3)" s
compare_headline leave-rejoin worst_survivor_mibps "$worst" MiB/s
compare_headline leave-rejoin worst_survivor_dip_pct \
    "$(awk -v b="$baseline" -v w="$worst" 'BEGIN{ if (b+0 == 0) print "0"; else printf "%.2f", (b-w)/b*100 }')" %
compare_headline leave-rejoin arenas_owned_after "$(arenas_owned_total)" arenas

# Sampled twice, because the first sample is taken the moment the last cycle
# ends and a reclaim started during that cycle may still be in flight. A count
# that comes back down over the settle window is a reclaim that had not
# finished; one that stays up is capacity the cycles genuinely leaked.
sleep "$SETTLE"
compare_headline leave-rejoin settle_seconds "$SETTLE" s
compare_headline leave-rejoin arenas_owned_settled "$(arenas_owned_total)" arenas
