#!/bin/bash
# bench-single-node.sh — one writer, no sharing: the case where the whole
# coordination layer is pure overhead. The ceiling is the raw device, so that
# is measured first — before any backend has touched the volume, which is the
# only moment it can safely be written directly — and the headline is the
# percentage of it the filesystem retains.
#
# Sequential 1 MiB and random 4 KiB, both one job deep: the first says how much
# bandwidth survives, the second how much of the device's IOPS does.
#
# Usage:
#   COMPARE_BACKEND=etcfs ./bench-single-node.sh
set -euo pipefail
export COMPARE_BACKEND="${COMPARE_BACKEND:-etcfs}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"

RUNTIME="${ETCFS_BENCH_RUNTIME:-30}"

compare_begin
RAW_NODE="${COMPARE_PUB_IPS[0]}"
compare_install_fio "$RAW_NODE"
RAW_DEV=$(compare_shared_device "$RAW_NODE")

# Raw first, mkfs/bootstrap after: every backend below either formats this
# device or writes raw extents to it, so this is the last point at which its
# unmediated ceiling can be measured on the same hardware.
log "Raw-device ceiling on $RAW_DEV (before any backend touches it)..."
raw_seq=$(compare_run_job "raw-seq" "$RAW_NODE" "$RUNTIME" "
[global]
ioengine=libaio
direct=1
filename=$RAW_DEV
runtime=$RUNTIME
time_based=1
[seqwrite-1m]
rw=write
bs=1M
iodepth=4
")
raw_rand=$(compare_run_job "raw-rand" "$RAW_NODE" "$RUNTIME" "
[global]
ioengine=libaio
direct=1
filename=$RAW_DEV
runtime=$RUNTIME
time_based=1
[randwrite-4k]
rw=randwrite
bs=4k
iodepth=4
")

compare_mount 0

fs_seq=$(compare_run_job "fs-seq-$COMPARE_BACKEND" "$N0" "$RUNTIME" "
[global]
ioengine=psync
direct=1
filename=$MOUNT_PATH/single.dat
size=4G
runtime=$RUNTIME
time_based=1
[seqwrite-1m]
rw=write
bs=1M
")
fs_rand=$(compare_run_job "fs-rand-$COMPARE_BACKEND" "$N0" "$RUNTIME" "
[global]
ioengine=psync
direct=1
filename=$MOUNT_PATH/single.dat
size=4G
runtime=$RUNTIME
time_based=1
[randwrite-4k]
rw=randwrite
bs=4k
")

raw_bw=$(compare_fio_bw_mibps "$raw_seq" write)
fs_bw=$(compare_fio_bw_mibps "$fs_seq" write)
raw_iops=$(compare_fio_iops "$raw_rand" write)
fs_iops=$(compare_fio_iops "$fs_rand" write)

compare_headline single-node raw_write_mibps "$raw_bw" MiB/s
compare_headline single-node fs_write_mibps "$fs_bw" MiB/s
compare_headline single-node pct_of_raw_bandwidth "$(compare_div "$(compare_div "$fs_bw" "$raw_bw" 4)" 0.01)" %
compare_headline single-node pct_of_raw_iops "$(compare_div "$(compare_div "$fs_iops" "$raw_iops" 4)" 0.01)" %
