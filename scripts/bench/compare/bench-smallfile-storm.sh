#!/bin/bash
# bench-smallfile-storm.sh — untar a kernel-source-shaped tree (~80k small
# files, two directory levels) onto one node. Every create is a Raft commit on
# etcfs, so this is the scenario it is expected to lose outright; the number
# worth publishing is by how much.
#
# The tree is generated and tarred on the node's local disk first, so what is
# timed is the untar onto the filesystem under test and not the generator.
#
# ETCFS_STORM_FILES (default 80000) — lower it for a smoke run.
#
# Usage:
#   COMPARE_BACKEND=etcfs ./bench-smallfile-storm.sh
set -euo pipefail
export COMPARE_BACKEND="${COMPARE_BACKEND:-etcfs}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"

FILES="${ETCFS_STORM_FILES:-80000}"

compare_begin
compare_mount

compare_build_tree "$N0" "$FILES" >/dev/null
read -r elapsed rate < <(compare_untar_tree "$N0" "$MOUNT_PATH/storm" "$FILES")
compare_headline smallfile-storm untar_s "$elapsed" s
compare_headline smallfile-storm creates_per_sec "$rate" files/s

# The create rate is only half of what this scenario says about etcfs.  The
# other half is whether the invalidation path kept up with it: a run that is
# fast because it stopped yielding locks is not a faster run.
notify_failures=$(compare_etcfs_notify_failures "$N0")
[[ -z "$notify_failures" ]] || compare_headline smallfile-storm notify_failures "$notify_failures" locks
compare_etcfs_snapshot_metrics "$N0" "after-storm"
