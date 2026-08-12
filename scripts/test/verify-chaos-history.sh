#!/bin/bash
# Runs the Porcupine consistency checks (cmd/verify-history) against the
# operation histories a docker-based chaos run just recorded.
#
# Usage:
#   scripts/test/verify-chaos-history.sh
#
# Every node in deploy/docker/docker-compose.yml is started with
# --history-log writing into the shared history_data volume; this script
# copies the per-node files out of that volume into a temp directory and
# checks them. Run it after a chaos scenario, while the cluster from that
# scenario is still up (or at least the history_data volume still exists) —
# chaos-lib.sh's teardown removes it along with everything else.
#
# See docs/verification/porcupine.md.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

VOLUME="${1:-docker_history_data}"
OUT="$(mktemp -d)"
trap 'rm -rf "$OUT"' EXIT

if ! docker volume inspect "$VOLUME" >/dev/null 2>&1; then
    echo "[verify-history] volume $VOLUME does not exist — was a docker chaos scenario run?" >&2
    exit 1
fi

echo "[verify-history] copying histories out of $VOLUME"
# The daemon writes its history file 0600 as the container's root, so the copy
# has to run as root too; chown the copies to us afterward or nothing outside
# the container can read them back.
docker run --rm -v "$VOLUME:/history" -v "$OUT:/out" alpine:3.20 \
    sh -c "cp /history/history-*.jsonl /out/ 2>/dev/null; chown $(id -u):$(id -g) /out/*.jsonl 2>/dev/null || true"

files=("$OUT"/history-*.jsonl)
if [[ ! -e "${files[0]}" ]]; then
    echo "[verify-history] no history files found in $VOLUME — was --history-log recording?" >&2
    exit 1
fi

joined="$(IFS=,; echo "${files[*]}")"

echo "[verify-history] building cmd/verify-history"
cd "$PROJECT_ROOT"
go build -o "$OUT/verify-history" ./cmd/verify-history

echo "[verify-history] checking: $joined"
"$OUT/verify-history" --files="$joined"
