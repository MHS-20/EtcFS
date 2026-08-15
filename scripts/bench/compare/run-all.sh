#!/bin/bash
# run-all.sh — runs every backend's isolated bench-*.sh in sequence (each
# provisions and tears down its own cluster, so nothing here is shared), then
# builds the combined report.
#
# Usage:
#   ./run-all.sh
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

for backend in etcfs juicefs gluster nfs gfs2; do
    "$SCRIPT_DIR/bench-$backend.sh"
done

"$SCRIPT_DIR/report.sh"
