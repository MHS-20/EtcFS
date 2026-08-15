#!/bin/bash
# bench-etcfs.sh — etcfs on its own 3-node cluster + Multi-Attach volume,
# raw block device (no filesystem — this is etcfs's normal mode, see
# scripts/infra/create-infra.sh's header).
#
# Usage:
#   ./bench-etcfs.sh
set -euo pipefail
export COMPARE_BACKEND=etcfs
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"

trap compare_destroy EXIT
compare_provision
N0="${COMPARE_PUB_IPS[0]}"

bash "$INFRA_DIR/bootstrap-cluster.sh" "$ETCFS_STATE" || die "bootstrap-cluster.sh failed"
wait_for_fuse_mount "$N0" 60 2 || die "etcfs never mounted on $N0"

run_fio "etcfs" "directory=$FUSE_MOUNTPOINT
filename_format=fio.\$jobname.\$jobnum.\$filenum" 8M psync "${ETCFS_BENCH_JOBS:-4}" 1 "${ETCFS_BENCH_RUNTIME:-30}"
compare_summary_row etcfs "$RESULTS_DIR/etcfs.json"

log "etcfs comparison run complete. Results in $RESULTS_DIR"
