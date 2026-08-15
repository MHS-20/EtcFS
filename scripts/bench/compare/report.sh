#!/bin/bash
# report.sh — merges every backend's benchmark-results/compare/<backend>/summary.json
# (written by compare-lib.sh's compare_summary_row) into one markdown table.
# Run standalone any time after one or more bench-*.sh runs — it only reads
# already-written results, never touches AWS.
#
# Usage:
#   ./report.sh
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
COMPARE_DIR="$PROJECT_ROOT/benchmark-results/compare"
OUT="$COMPARE_DIR/report.md"

{
    echo "# Filesystem comparison — etcfs vs juicefs vs gluster vs nfs"
    echo
    echo "Each backend ran on its own 3-node cluster with a dedicated 1000-IOPS"
    echo "io2 Multi-Attach EBS volume (see scripts/bench/compare/compare-lib.sh)."
    echo
    echo "| Backend | randwrite IOPS | randwrite p99 (us) | randread IOPS | randread p99 (us) |"
    echo "|---|---|---|---|---|"
    for backend in etcfs juicefs gluster nfs gfs2; do
        summary="$COMPARE_DIR/$backend/summary.json"
        [[ -f "$summary" ]] || continue
        jq -r '.[] | "| \(.label) | \(.write_iops) | \(.write_p99_us) | \(.read_iops) | \(.read_p99_us) |"' "$summary"
    done
} | tee "$OUT"

echo "Report written to $OUT"
