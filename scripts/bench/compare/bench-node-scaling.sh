#!/bin/bash
# bench-node-scaling.sh — aggregate throughput and metadata operations/second
# against node count, on a shared working set and on disjoint per-node ones.
#
# ETCFS_SCALE_NODES is the sweep (default "2 4 8"). 16 and 32 are supported the
# same way — `ETCFS_SCALE_NODES="2 4 8 16 32" ./bench-node-scaling.sh` — and
# are deliberately not the default: the cluster is provisioned at the largest
# count in the sweep and billed for the whole run.
#
# One cluster, sized to the largest point, with each point driving only the
# first K nodes, rather than a fresh cluster per point. For etcfs that means
# etcd itself stays at the full member count throughout, so this measures the
# *client* curve against a fixed quorum, not the cost of a wider Raft group —
# widening quorum is a separate axis and a separate run.
# GFS2 cannot be swept this way at all past its mkfs journal count, which is
# exactly the structural limit worth showing: compare_mount allocates one
# journal per provisioned node, so a point beyond that count fails to mount.
#
# Usage:
#   COMPARE_BACKEND=etcfs ./bench-node-scaling.sh
set -euo pipefail
export COMPARE_BACKEND="${COMPARE_BACKEND:-etcfs}"
SCALE_NODES="${ETCFS_SCALE_NODES:-2 4 8}"
MAX=0
for n in $SCALE_NODES; do [[ "$n" -gt "$MAX" ]] && MAX="$n"; done

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"

# The widest sweep point has to be reachable in *mounted* nodes, which is one
# fewer than the cluster on the backends that spend node0 on a server.
compare_client_nodes "$MAX"

RUNTIME="${ETCFS_BENCH_RUNTIME:-30}"
FILES="${ETCFS_META_FILES:-500}"

compare_begin
compare_mount

JOB_HEAD="
[global]
ioengine=psync
direct=1
bs=1M
size=1G
runtime=$RUNTIME
time_based=1
group_reporting=1
continue_on_error=all
"

for n in $SCALE_NODES; do
    [[ "$n" -le "${#BENCH_NODES[@]}" ]] || die "sweep point $n exceeds the ${#BENCH_NODES[@]} mounted node(s)"
    nodes=("${BENCH_NODES[@]:0:$n}")

    shared=$(compare_parallel_fio "scale-shared-${n}n" "$RUNTIME" "$JOB_HEAD
filename=$MOUNT_PATH/scale-shared.dat
[seqwrite]
rw=write
" "${nodes[@]}")
    # @NODE@ is substituted per node by compare_parallel_fio: one file each,
    # which is the disjoint working set.
    disjoint=$(compare_parallel_fio "scale-disjoint-${n}n" "$RUNTIME" "$JOB_HEAD
filename=$MOUNT_PATH/scale-@NODE@.dat
[seqwrite]
rw=write
" "${nodes[@]}")

    compare_headline node-scaling "shared_bw_mibps_${n}n" "$shared" MiB/s
    compare_headline node-scaling "disjoint_bw_mibps_${n}n" "$disjoint" MiB/s
    compare_headline node-scaling "meta_ops_per_sec_${n}n" \
        "$(compare_metadata_ops "scale-meta-${n}n" "$MOUNT_PATH/shared-meta" "$FILES" "${nodes[@]}")" ops/s
done
