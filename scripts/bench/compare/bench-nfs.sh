#!/bin/bash
# bench-nfs.sh — plain self-hosted NFSv4 (not EFS — see bench-efs-throughput.sh
# for the managed comparison) on its own 3-node cluster + Multi-Attach volume:
# node0 formats+serves the volume, node1 benchmarks it as an NFS client. This
# is the natural deployment for this hardware shape and the baseline the
# gluster/juicefs runs' NFS-relayed backing store is measured against.
#
# ETCFS_BENCH_DIRECT=0 runs the warm-page-cache variant (see bench-etcfs.sh's
# header for what the two measure). psync in that mode, not libaio: libaio
# needs O_DIRECT and degrades to synchronous submission without it.
#
# Usage:
#   ./bench-nfs.sh
#   ETCFS_BENCH_DIRECT=0 ./bench-nfs.sh
set -euo pipefail

DIRECT="${ETCFS_BENCH_DIRECT:-1}"
if [[ "$DIRECT" == "1" ]]; then BACKEND=nfs; ENGINE=libaio; else BACKEND=nfs-pagecache; ENGINE=psync; fi
export COMPARE_BACKEND="$BACKEND"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"

compare_begin
compare_mount

run_fio "$BACKEND" "filename=$MOUNT_PATH/fio.dat" 1G "$ENGINE" 4 32 "${ETCFS_BENCH_RUNTIME:-30}" "$DIRECT"
compare_finish "$BACKEND"
