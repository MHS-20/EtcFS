#!/bin/bash
# bench-single-node.sh — one writer, no sharing: the whole coordination layer
# is pure overhead here, and a bare device on the same hardware is the ceiling.
# Headline is the percentage of raw device throughput the filesystem retains.
#
# The raw baseline runs BEFORE the backend is set up, straight against the
# unformatted Multi-Attach volume — the only window in which writing to the
# raw device is safe, and the same physical volume every backend is then
# measured on.
#
# Usage:
#   COMPARE_BACKEND=etcfs ./bench-single-node.sh
set -euo pipefail
export COMPARE_BACKEND="${COMPARE_BACKEND:-etcfs}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"

RUNTIME="${ETCFS_BENCH_RUNTIME:-60}"
JOB_TAIL="
size=2G
runtime=$RUNTIME
time_based=1
group_reporting=1
[seqwrite]
rw=write
numjobs=1
iodepth=16
"

compare_begin
RAW_NODE="${COMPARE_PUB_IPS[0]}"
compare_install_fio "$RAW_NODE"
RAW_DEV=$(compare_shared_device "$RAW_NODE")

raw_json=$(compare_run_job "single-node-raw-$COMPARE_BACKEND" "$RAW_NODE" "$RUNTIME" "
[global]
ioengine=libaio
direct=1
bs=1M
filename=$RAW_DEV
$JOB_TAIL")
raw_bw=$(compare_fio_bw_mibps "$raw_json" write)

compare_mount
fs_json=$(compare_run_job "single-node-fs-$COMPARE_BACKEND" "$N0" "$RUNTIME" "
[global]
ioengine=psync
direct=1
bs=1M
filename=$MOUNT_PATH/single.dat
$JOB_TAIL")
fs_bw=$(compare_fio_bw_mibps "$fs_json" write)

compare_headline single-node raw_device_bw_mibps "$raw_bw" MiB/s
compare_headline single-node fs_bw_mibps "$fs_bw" MiB/s
compare_headline single-node pct_of_raw_retained "$(compare_div "$(compare_div "$fs_bw" "$raw_bw" 4)" 0.01 1)" %
