#!/bin/bash
# run-all.sh — runs every scenario in scripts/bench/ against an already-live
# cluster, in the order that lets later scenarios reuse earlier ones' setup
# (bench-contention.sh's EFS row needs bench-efs-throughput.sh's bursting
# filesystem to already be in state).
#
# Requires: create-infra.sh + setup-compute.sh already run.
#
# Usage:
#   ./run-all.sh
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

"$SCRIPT_DIR/bench-iops-ceiling.sh"
"$SCRIPT_DIR/bench-efs-throughput.sh"
"$SCRIPT_DIR/bench-contention.sh"

echo "All benchmark scenarios complete. Results under \$PROJECT_ROOT/benchmark-results/{iops-ceiling,efs-throughput,contention}/"
