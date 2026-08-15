#!/bin/bash
# bench-etcd-degraded.sh — etcfs with its own etcd cluster hurt two ways, in
# sequence: one member killed (quorum survives, every commit now needs both
# remaining members), then 50 ms of latency injected on the peer port between
# members. Everything gets slower by design; what is worth publishing is by how
# much, and whether reads still serve while writes crawl — a read that hits a
# lock this node already holds should not touch etcd at all.
#
# etcfs only: the other backends have no etcd to degrade, and their equivalent
# (a downed brick, a downed NFS server) is bench-node-kill.sh's business.
#
# The netem qdisc is filtered to etcd's peer port rather than applied to the
# whole interface, so the injected latency lands on Raft traffic and not on the
# data path, the ssh control channel, or this script's own timing.
#
# Usage:
#   ./bench-etcd-degraded.sh
set -euo pipefail
export COMPARE_BACKEND="${COMPARE_BACKEND:-etcfs}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"
[[ "$COMPARE_BACKEND_BASE" == "etcfs" ]] || die "bench-etcd-degraded.sh is etcfs-only (see header)"

RUNTIME="${ETCFS_BENCH_RUNTIME:-30}"
DELAY_MS="${ETCFS_ETCD_DELAY_MS:-50}"

phase() {
    local name="$1"
    run_fio "degraded-$name" "directory=$MOUNT_PATH
filename_format=fio.\$jobname.\$jobnum.\$filenum" 8M psync 4 1 "$RUNTIME"
    compare_summary_row "degraded-$name" "$RESULTS_DIR/degraded-$name.json"
    compare_headline etcd-degraded "${name}_write_iops" \
        "$(jq -r '.jobs[0].write.iops | round' "$RESULTS_DIR/degraded-$name.json")" ops/s
    compare_headline etcd-degraded "${name}_read_iops" \
        "$(jq -r '.jobs[1].read.iops | round' "$RESULTS_DIR/degraded-$name.json")" ops/s
    compare_headline etcd-degraded "${name}_write_p99_us" \
        "$(jq -r '(.jobs[0].write.clat_ns.percentile."99.000000" // 0) / 1000 | round' "$RESULTS_DIR/degraded-$name.json")" us
}

compare_begin
compare_mount
[[ "${#BENCH_NODES[@]}" -ge 3 ]] || die "etcd-degraded needs a three-member etcd cluster"

phase healthy

log "Killing etcd on ${COMPARE_PUB_IPS[-1]} (quorum retained, no spare member left)..."
$SSH_CMD "ec2-user@${COMPARE_PUB_IPS[-1]}" "sudo killall -9 etcd 2>/dev/null; true"
sleep 5
phase one-member-down

log "Injecting ${DELAY_MS}ms on etcd's peer port between the surviving members..."
for ip in "${COMPARE_PUB_IPS[@]:0:2}"; do
    $SSH_CMD "ec2-user@$ip" "set -e
        IFACE=\$(ip route get 8.8.8.8 | awk '{print \$5; exit}')
        sudo tc qdisc add dev \$IFACE root handle 1: prio
        sudo tc qdisc add dev \$IFACE parent 1:3 handle 30: netem delay ${DELAY_MS}ms
        sudo tc filter add dev \$IFACE protocol ip parent 1:0 prio 1 u32 \
            match ip dport 2380 0xffff flowid 1:3
        sudo tc filter add dev \$IFACE protocol ip parent 1:0 prio 1 u32 \
            match ip sport 2380 0xffff flowid 1:3
    "
done
phase "one-down-plus-${DELAY_MS}ms"
