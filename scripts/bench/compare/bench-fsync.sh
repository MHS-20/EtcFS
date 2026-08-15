#!/bin/bash
# bench-fsync.sh — O_DSYNC 4 KiB random writes. Write deferral is off, so every
# write costs a device write plus (on etcfs) a Raft commit, against GFS2 whose
# journal absorbs the same pattern locally. Headline is sustained synchronous
# write IOPS with its 99th-percentile latency.
#
# Usage:
#   COMPARE_BACKEND=etcfs ./bench-fsync.sh
set -euo pipefail
export COMPARE_BACKEND="${COMPARE_BACKEND:-etcfs}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"

RUNTIME="${ETCFS_BENCH_RUNTIME:-60}"

compare_begin
compare_mount

# sync=1 opens with O_DSYNC, which is the pattern under test; psync because
# there is nothing for an async engine to overlap once every write is
# synchronous, and libaio + FUSE is a known hang (see bench-juicefs.sh).
json=$(compare_run_job "fsync-$COMPARE_BACKEND" "$N0" "$RUNTIME" "
[global]
ioengine=psync
direct=1
sync=1
bs=4k
filename=$MOUNT_PATH/fsync.dat
size=1G
runtime=$RUNTIME
time_based=1
group_reporting=1
[dsync-randwrite]
rw=randwrite
numjobs=1
iodepth=1
")
compare_headline fsync-small-writes sync_write_iops "$(compare_fio_iops "$json" write)" iops
compare_headline fsync-small-writes write_p99_us \
    "$(jq -r '(.jobs[0].write.clat_ns.percentile."99.000000" // 0) / 1000 | round' "$json")" us
