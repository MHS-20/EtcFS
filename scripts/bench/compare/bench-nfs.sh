#!/bin/bash
# bench-nfs.sh — plain self-hosted NFSv4 (not EFS — see bench-efs-throughput.sh
# for the managed comparison) on its own 3-node cluster + Multi-Attach volume:
# node0 formats+serves the volume, node1 benchmarks it as an NFS client. This
# is the natural deployment for this hardware shape and the baseline the
# gluster/juicefs runs' NFS-relayed backing store is measured against.
#
# Usage:
#   ./bench-nfs.sh
set -euo pipefail
export COMPARE_BACKEND=nfs
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"

trap compare_destroy EXIT
compare_provision
SERVER_PUB="${COMPARE_PUB_IPS[0]}"
SERVER_PRIV="${COMPARE_PRIV_IPS[0]}"
CLIENT_PUB="${COMPARE_PUB_IPS[1]}"

compare_export_backing "$SERVER_PUB" "$SERVER_PRIV" "$CLIENT_PUB" "${COMPARE_PUB_IPS[2]}"
$SSH_CMD "ec2-user@$CLIENT_PUB" "sudo dnf install -y fio >/dev/null 2>&1 || sudo yum install -y fio >/dev/null 2>&1"

N0="$CLIENT_PUB"
run_fio "nfs" "filename=$BACKING_PATH/fio.dat" 1G libaio 4 32 "${ETCFS_BENCH_RUNTIME:-30}"
compare_summary_row nfs "$RESULTS_DIR/nfs.json"

log "nfs comparison run complete. Results in $RESULTS_DIR"
