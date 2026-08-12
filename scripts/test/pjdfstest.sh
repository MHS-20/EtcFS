#!/bin/bash
# Runs the pjdfstest POSIX conformance suite against EtcFS in Docker and
# leaves per-syscall TAP output plus a summary in a results directory.
#
# Usage:
#   scripts/test/pjdfstest.sh                 # full suite
#   ONLY="chmod rename" scripts/test/pjdfstest.sh   # selected syscalls
#
# Results: deploy/docker/pjdfstest-results/{summary.tsv,<syscall>.tap}

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
DOCKER_DIR="$PROJECT_ROOT/deploy/docker"
COMPOSE=(docker compose -f "$DOCKER_DIR/docker-compose.pjdfstest.yml")

# Some hosts cannot create veth pairs (no veth kernel module), which makes
# every bridge-networked container fail to start. Fall back to host
# networking there rather than reporting it as a filesystem failure.
if ! docker run --rm alpine:3.20 true >/dev/null 2>&1; then
    echo "[pjdfstest] bridge networking unavailable — using host networking"
    COMPOSE+=(-f "$DOCKER_DIR/docker-compose.pjdfstest.hostnet.yml")
fi

RESULTS_DIR="$DOCKER_DIR/pjdfstest-results"
mkdir -p "$RESULTS_DIR"
rm -f "$RESULTS_DIR"/*.tap "$RESULTS_DIR/summary.tsv"

# --remove-orphans also collects the one-off container `compose run` creates,
# which a plain `down` leaves behind to keep retrying against a dead etcd.
cleanup() { "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT

echo "[pjdfstest] building images"
"${COMPOSE[@]}" build

echo "[pjdfstest] starting etcd and preparing the block device"
"${COMPOSE[@]}" up -d etcd block-init

echo "[pjdfstest] running suite"
set +e
# `run` rather than `up`: the suite is a one-shot job, and `up` would tear the
# environment down the moment block-init exits successfully.
"${COMPOSE[@]}" run --rm pjdfstest
status=$?
set -e

echo
if [[ -f "$RESULTS_DIR/summary.tsv" ]]; then
    printf '%-12s %8s %8s %12s\n' SYSCALL PASS FAIL EXPECTED-FAIL
    while IFS=$'\t' read -r name ok fail todo; do
        printf '%-12s %8s %8s %12s\n' "$name" "$ok" "$fail" "$todo"
    done <"$RESULTS_DIR/summary.tsv"
else
    echo "[pjdfstest] no summary produced — see container logs above"
fi
echo "[pjdfstest] results in $RESULTS_DIR"

exit "$status"
