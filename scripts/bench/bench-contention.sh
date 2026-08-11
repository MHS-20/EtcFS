#!/bin/bash
# bench-contention.sh — many writers/readers on the SAME file(s), across
# every compute node at once, for etcfs and for EFS.
#
# This is the honest worst case for a network filesystem: every op on a
# shared path either takes a lock (etcfs's per-inode exclusive/shared lease
# lock) or serializes some other way (EFS's own internal consistency
# protocol). Aggregate throughput and the worst per-node tail latency are
# what a real multi-writer workload feels, which a single-node/single-file
# benchmark can't show.
#
# Requires: create-infra.sh + setup-compute.sh already run. Uses the
# bursting EFS filesystem from bench-efs-throughput.sh if one exists in
# state (bench_efs_id), skips the EFS side otherwise.
#
# Usage:
#   ./bench-contention.sh [jobs_per_node]   # default: 2
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../infra/state.sh"
source "$SCRIPT_DIR/bench-lib.sh"

RESULTS_DIR="$PROJECT_ROOT/benchmark-results/contention"
mkdir -p "$RESULTS_DIR"
RUNTIME="${ETCFS_BENCH_RUNTIME:-30}"
JOBS_PER_NODE="${1:-2}"

mapfile -t PUB_IPS < <(state_get compute_public_ips 2>/dev/null | jq -r '.[]' 2>/dev/null)
[[ "${#PUB_IPS[@]}" -ge 2 ]] || die "Need at least 2 compute nodes in state for a contention test."

for ip in "${PUB_IPS[@]}"; do
    $SSH_CMD "ec2-user@$ip" "sudo dnf install -y fio nfs-utils >/dev/null 2>&1 || sudo yum install -y fio nfs-utils >/dev/null 2>&1"
done

log "=== etcfs: ${#PUB_IPS[@]} nodes x $JOBS_PER_NODE jobs, one shared file ==="
run_fio_contention "etcfs-contention" "$FUSE_MOUNTPOINT/contention.dat" 8M psync "$JOBS_PER_NODE" 1 "$RUNTIME" "${PUB_IPS[@]}"

EFS_ID=$(state_get bench_efs_id 2>/dev/null | tr -d '"' || echo "")
if [[ -n "$EFS_ID" && "$EFS_ID" != "null" ]]; then
    log "=== EFS (bursting): ${#PUB_IPS[@]} nodes x $JOBS_PER_NODE jobs, one shared file ==="
    for ip in "${PUB_IPS[@]}"; do
        efs_mount "$ip" "$EFS_ID" /mnt/bench-efs-burst
    done
    run_fio_contention "efs-contention" /mnt/bench-efs-burst/contention.dat 8M libaio "$JOBS_PER_NODE" 8 "$RUNTIME" "${PUB_IPS[@]}"
else
    log "No bench_efs_id in state (run bench-efs-throughput.sh first) — skipping EFS contention row."
fi

{
    echo "| Target | Nodes x jobs | Nodes reporting | Aggregate write IOPS | Aggregate read IOPS | Worst-node write p99 (us) | Worst-node read p99 (us) | Write errors | Read errors |"
    echo "|---|---|---|---|---|---|---|---|---|"
    jq -r --arg n "${#PUB_IPS[@]}x${JOBS_PER_NODE}" --arg total "${#PUB_IPS[@]}" \
        '"| etcfs | \($n) | \(.nodes_reporting)/\($total) | \(.total_write_iops|round) | \(.total_read_iops|round) | \(.max_write_p99_us|round) | \(.max_read_p99_us|round) | \(.total_write_errors) | \(.total_read_errors) |"' \
        "$RESULTS_DIR/etcfs-contention-summary.json"
    if [[ -f "$RESULTS_DIR/efs-contention-summary.json" ]]; then
        jq -r --arg n "${#PUB_IPS[@]}x${JOBS_PER_NODE}" --arg total "${#PUB_IPS[@]}" \
            '"| EFS (bursting) | \($n) | \(.nodes_reporting)/\($total) | \(.total_write_iops|round) | \(.total_read_iops|round) | \(.max_write_p99_us|round) | \(.max_read_p99_us|round) | \(.total_write_errors) | \(.total_read_errors) |"' \
            "$RESULTS_DIR/efs-contention-summary.json"
    fi
} | tee "$RESULTS_DIR/summary.md"

log "Contention comparison complete. Results in $RESULTS_DIR"
