#!/bin/bash
# bench-etcfs.sh — etcfs on its own 3-node cluster + Multi-Attach volume,
# raw block device (no filesystem — this is etcfs's normal mode, see
# scripts/infra/create-infra.sh's header).
#
# ETCFS_BENCH_DIRECT=0 runs the page-cache variant instead: fio without
# direct=1, against the daemon's default --page-cache=true. The default
# (direct=1) measures the device-bound path, where every operation is a genuine
# round trip by design — there is no daemon-side read cache, only the kernel
# page cache the FUSE layer offers through keep_cache, and O_DIRECT bypasses
# exactly that. With O_DIRECT gone, repeated reads of the same small file hit
# the client's page cache rather than the 1000-IOPS device, which is what every
# other backend's read column already did in the direct=1 run. It reports under
# its own backend name so the two never overwrite each other's results.
#
# Usage:
#   ./bench-etcfs.sh
#   ETCFS_BENCH_DIRECT=0 ./bench-etcfs.sh
set -euo pipefail

DIRECT="${ETCFS_BENCH_DIRECT:-1}"
if [[ "$DIRECT" == "1" ]]; then
    BACKEND=etcfs
else
    BACKEND=etcfs-pagecache
fi

export COMPARE_BACKEND="$BACKEND"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"

compare_begin
N0="${COMPARE_PUB_IPS[0]}"

bash "$INFRA_DIR/bootstrap-cluster.sh" "$ETCFS_STATE" || die "bootstrap-cluster.sh failed"
wait_for_fuse_mount "$N0" 60 2 || die "etcfs never mounted on $N0"
compare_install_fio "$N0"

run_fio "$BACKEND" "directory=$FUSE_MOUNTPOINT
filename_format=fio.\$jobname.\$jobnum.\$filenum" 8M psync "${ETCFS_BENCH_JOBS:-4}" 1 "${ETCFS_BENCH_RUNTIME:-30}" "$DIRECT"
compare_finish "$BACKEND"
