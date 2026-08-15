#!/bin/bash
# bench-deep-walk.sh — find/du over a large tree, cold and warm. Every LOOKUP
# on etcfs is an etcd read; NFS with attribute caching and GFS2 reading
# metadata off the local device both have less to do, so this is another one
# etcfs is expected to lose — and the cold/warm pair is the interesting shape,
# since the warm number is what any lookup caching would move.
#
# ETCFS_WALK_FILES (default 80000) sizes the tree.
#
# Usage:
#   COMPARE_BACKEND=etcfs ./bench-deep-walk.sh
set -euo pipefail
export COMPARE_BACKEND="${COMPARE_BACKEND:-etcfs}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"

FILES="${ETCFS_WALK_FILES:-80000}"

compare_begin
compare_mount

log "Populating a $FILES-file tree to walk..."
compare_build_tree "${BENCH_NODES[0]}" "$FILES" >/dev/null
compare_untar_tree "${BENCH_NODES[0]}" "$MOUNT_PATH/walk" "$FILES" >/dev/null

read -r cold warm du_s < <(compare_walk_tree "${BENCH_NODES[0]}" "$MOUNT_PATH/walk")
compare_headline deep-walk find_cold_s "$cold" s
compare_headline deep-walk find_warm_s "$warm" s
compare_headline deep-walk du_s "$du_s" s
compare_headline deep-walk lookups_per_sec_cold "$(compare_div "$FILES" "$cold")" lookups/s
