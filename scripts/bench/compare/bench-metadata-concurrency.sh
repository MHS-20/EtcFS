#!/bin/bash
# bench-metadata-concurrency.sh — parallel create/stat/unlink in ONE shared
# directory, from one node, then two, then every node, so the headline is
# operations/second against node count rather than a single figure.
#
# The shared directory is the whole point: GFS2/OCFS2 bounce that directory's
# DLM lock between nodes on every operation, so their curve should flatten or
# fall as nodes are added; etcfs pays one Raft commit per mutation and no lock
# ping-pong, so its curve should keep climbing until etcd's own commit rate
# binds.
#
# ETCFS_META_FILES (default 500) is files per node per phase.
#
# Usage:
#   COMPARE_BACKEND=etcfs ./bench-metadata-concurrency.sh
set -euo pipefail
export COMPARE_BACKEND="${COMPARE_BACKEND:-etcfs}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"

FILES="${ETCFS_META_FILES:-500}"

compare_begin
compare_mount

for n in $(seq 1 "${#BENCH_NODES[@]}"); do
    ops=$(compare_metadata_ops "meta-$COMPARE_BACKEND-${n}n" "$MOUNT_PATH/shared-meta" \
        "$FILES" "${BENCH_NODES[@]:0:$n}")
    compare_headline metadata-concurrency "ops_per_sec_${n}n" "$ops" ops/s
done
