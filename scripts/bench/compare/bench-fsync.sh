#!/bin/bash
# bench-fsync.sh — 4 KiB writes with O_DSYNC. Write deferral is off by
# definition, so every write costs a device write plus, on etcfs, a Raft
# commit; GFS2 absorbs the same pattern into a local journal. The number is
# sustained sync-write IOPS and the p99 of a single synchronous write.
#
# Usage:
#   COMPARE_BACKEND=etcfs ./bench-fsync.sh
set -euo pipefail
export COMPARE_BACKEND="${COMPARE_BACKEND:-etcfs}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"

RUNTIME="${ETCFS_BENCH_RUNTIME:-60}"

compare_begin
compare_mount 0

# fdatasync=1 issues an fdatasync(2) after every write, which forces the same
# durable-per-write cost O_DSYNC would (every write pays a device write plus,
# on etcfs, a Raft commit) without depending on fio's sync=dsync string form —
# that syntax needs fio 3.20+, and gfs2/gluster's AL2 AMI ships an older fio
# that only parses sync as a bool and fails the job outright ("failed parsing
# sync=dsync"). iodepth 1 and psync keep one write in flight, which is the
# latency this scenario is about — a deeper queue would report a throughput
# number that hides it.
json=$(compare_run_job "fsync-$COMPARE_BACKEND" "$N0" "$RUNTIME" "
[global]
ioengine=psync
direct=0
fdatasync=1
filename=$MOUNT_PATH/dsync.dat
size=1G
runtime=$RUNTIME
time_based=1
[randwrite-4k-dsync]
rw=randwrite
bs=4k
iodepth=1
")

compare_headline fsync sync_write_iops "$(compare_fio_iops "$json" write)" ops/s
compare_headline fsync sync_write_p99_us "$(compare_p99_us "$json" 0 write)" us
