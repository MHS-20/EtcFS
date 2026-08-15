#!/bin/bash
# bench-handoff.sh — cross-node handoff: node A writes an N-byte file and
# fsyncs it, node B reads it immediately, with B's cache dropped first so the
# read is genuinely a handoff and not B re-reading its own memory.
#
# Two numbers per size, both from B: time-to-first-byte (one 4 KiB O_DIRECT
# read of the freshly published file) and sustained read throughput. etcfs and
# gfs2 read the same physical blocks off the shared device, so only the extent
# map crosses the network; nfs and juicefs move every byte (juicefs through
# object storage). The gap should widen with size, which is what the sweep is
# for.
#
# Usage:
#   COMPARE_BACKEND=etcfs ./bench-handoff.sh
#   ETCFS_HANDOFF_SIZES="1M 64M 1G 8G" COMPARE_BACKEND=gfs2 ./bench-handoff.sh
set -euo pipefail
export COMPARE_BACKEND="${COMPARE_BACKEND:-etcfs}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"

SIZES="${ETCFS_HANDOFF_SIZES:-1M 64M 1G 8G}"

compare_begin
compare_mount
[[ "${#BENCH_NODES[@]}" -ge 2 ]] || die "handoff needs two nodes with the filesystem mounted"
A="${BENCH_NODES[0]}"
B="${BENCH_NODES[1]}"

for size in $SIZES; do
    path="$MOUNT_PATH/handoff-$size.dat"
    label="handoff-$COMPARE_BACKEND-$size"

    # psync throughout: direct=1 + libaio against a FUSE mount is a known hang
    # (see bench-juicefs.sh), and 1 MiB sequential I/O does not need an async
    # engine to reach device bandwidth.
    compare_run_job "$label-write" "$A" 300 "
[global]
ioengine=psync
direct=1
bs=1M
filename=$path
size=$size
[seqwrite]
rw=write
end_fsync=1
" >/dev/null

    $SSH_CMD "ec2-user@$B" "sudo sync; echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null"
    ttfb=$(compare_remote_time "$B" "sudo dd if=$path bs=4k count=1 iflag=direct")

    read_json=$(compare_run_job "$label-read" "$B" 300 "
[global]
ioengine=psync
direct=1
bs=1M
filename=$path
size=$size
[seqread]
rw=read
")
    compare_headline handoff "read_bw_mibps_$size" "$(compare_fio_bw_mibps "$read_json" read)" MiB/s
    compare_headline handoff "ttfb_ms_$size" "$(compare_div "$ttfb" 0.001 1)" ms
    $SSH_CMD "ec2-user@$A" "sudo rm -f $path"
done

log "handoff sweep complete. Results in $RESULTS_DIR"
