#!/bin/bash
# bench-juicefs.sh — JuiceFS (Redis metadata + MinIO object storage, both on
# node0) on its own 3-node cluster + a single 1000-IOPS Multi-Attach volume.
#
# JuiceFS's data plane is always an object store, never local disk, so this
# backend needs no NFS relay (unlike bench-gluster.sh) — MinIO on node0 puts
# an S3-compatible endpoint in front of the same Multi-Attach volume every
# other backend gets, and node1 mounts JuiceFS as a normal network client
# against it, same shape as the other three backends' single-client run.
#
# ETCFS_BENCH_DIRECT=0 runs the warm-page-cache variant (see bench-etcfs.sh's
# header for what the two measure).
#
# Usage:
#   ./bench-juicefs.sh
#   ETCFS_BENCH_DIRECT=0 ./bench-juicefs.sh
set -euo pipefail

DIRECT="${ETCFS_BENCH_DIRECT:-1}"
[[ "$DIRECT" == "1" ]] && BACKEND=juicefs || BACKEND=juicefs-pagecache
export COMPARE_BACKEND="$BACKEND"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"

compare_begin
compare_mount 0   # one client is all the IOPS comparison drives

# psync, not libaio: juicefs is a FUSE mount like etcfs (see bench-etcfs.sh),
# and direct=1 + libaio against a FUSE filesystem is a known hang — confirmed
# directly against this exact backend, fio sitting well past its runtime with
# zero output instead of erroring. Single shared file (like bench-nfs.sh/
# bench-gluster.sh), not per-thread directory+filename_format like etcfs: the
# directory form's per-thread file layout was still hanging even under
# psync, and this backend doesn't need numjobs to fan out across files the
# way etcfs's own comparison does.
run_fio "$BACKEND" "filename=$MOUNT_PATH/fio.dat" 8M psync 4 1 "${ETCFS_BENCH_RUNTIME:-30}" "$DIRECT"
compare_finish "$BACKEND"
