#!/bin/bash
# bench-etcfs-pagecache.sh — same etcfs cluster as bench-etcfs.sh, but fio runs
# WITHOUT direct=1 and the daemon runs with its default --page-cache=true.
#
# bench-etcfs.sh (direct=1) measures the real device-bound path: every op is a
# genuine round trip, by design (see internal/ipc/datapath.go's read path —
# there is no daemon-side read-data cache, only the kernel page cache the FUSE
# layer offers via keep_cache/direct_io, and O_DIRECT bypasses exactly that).
# This script measures the other side of that same design: with O_DIRECT gone,
# repeated reads/writes to the same small file hit the client kernel page
# cache instead of the 1000-IOPS device, the way every other backend's read
# column already did in the direct=1 comparison run.
#
# Not part of run-all.sh/report.sh — this is a one-off investigative variant,
# not a fifth backend to compare against the other four.
#
# Usage:
#   ./bench-etcfs-pagecache.sh
set -euo pipefail
export COMPARE_BACKEND=etcfs-pagecache
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"

trap compare_destroy EXIT
compare_provision
N0="${COMPARE_PUB_IPS[0]}"

bash "$INFRA_DIR/bootstrap-cluster.sh" "$ETCFS_STATE" || die "bootstrap-cluster.sh failed"
wait_for_fuse_mount "$N0" 60 2 || die "etcfs never mounted on $N0"
$SSH_CMD "ec2-user@$N0" "sudo dnf install -y fio >/dev/null 2>&1 || sudo yum install -y fio >/dev/null 2>&1"

# direct=0 (8th arg to run_fio): the whole point of this run.
run_fio "etcfs-pagecache" "directory=$FUSE_MOUNTPOINT
filename_format=fio.\$jobname.\$jobnum.\$filenum" 8M psync "${ETCFS_BENCH_JOBS:-4}" 1 "${ETCFS_BENCH_RUNTIME:-30}" 0
compare_summary_row etcfs-pagecache "$RESULTS_DIR/etcfs-pagecache.json"

log "etcfs page-cache comparison run complete. Results in $RESULTS_DIR"
