#!/bin/bash
# EtcFS end-to-end integration test runner.
#
# Starts the Docker Compose dev environment, runs a battery of tests
# against the EtcFS cluster, and reports results.
#
# Usage:
#   ./test/e2e/run.sh          # run all tests
#   ./test/e2e/run.sh --quick  # basic smoke test only

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
DOCKER_DIR="$PROJECT_ROOT/deploy/docker"
PASS=0; FAIL=0

log()  { echo "[$(date +%T)] $*"; }
pass() { echo "  ✓ $1"; PASS=$((PASS + 1)); }
fail() { echo "  ✗ $1"; FAIL=$((FAIL + 1)); }

cleanup() {
    log "Cleaning up..."
    cd "$DOCKER_DIR" && docker compose down -v 2>/dev/null || true
}
trap cleanup EXIT

QUICK=false
[[ "${1:-}" == "--quick" ]] && QUICK=true

log "=== EtcFS E2E Test Runner ==="

# ---- Start cluster ----
log "Starting dev cluster..."
cd "$DOCKER_DIR"
docker compose up -d etcd1 etcd2 etcd3 block-init

log "Waiting for etcd cluster to be healthy..."
for i in $(seq 1 30); do
    if docker compose exec -T etcd1 etcdctl endpoint health --cluster 2>/dev/null | grep -q "is healthy"; then
        break
    fi
    sleep 2
done

# ---- Test 1: etcd cluster has 3 healthy members ----
log "Test 1: etcd cluster health"
HEALTHY=$(docker compose exec -T etcd1 etcdctl endpoint health --cluster 2>/dev/null | grep -c "is healthy" || echo "0")
if [[ "$HEALTHY" -eq 3 ]]; then
    pass "etcd cluster: 3/3 healthy"
else
    fail "etcd cluster: $HEALTHY/3 healthy"
fi

# ---- Test 2: etcd basic put/get ----
log "Test 2: etcd basic operations"
docker compose exec -T etcd1 etcdctl put /test/key "hello-etcfuse" >/dev/null 2>&1
VAL=$(docker compose exec -T etcd1 etcdctl get /test/key --print-value-only 2>/dev/null || echo "")
if [[ "$VAL" == "hello-etcfuse" ]]; then
    pass "etcd put/get"
else
    fail "etcd put/get (got: $VAL)"
fi
docker compose exec -T etcd1 etcdctl del /test/key >/dev/null 2>&1

# ---- Test 3: etcd transaction ----
log "Test 3: etcd transaction"
RESULT=$(docker compose exec -T etcd1 etcdctl txn <<'TXN' 2>/dev/null | grep -c SUCCESS || echo "0")
compare "version(/test/txn) = 0"

put /test/txn "created"
TXN
if [[ "$RESULT" -eq 1 ]]; then
    pass "etcd transaction (compare-and-create)"
else
    fail "etcd transaction"
fi
docker compose exec -T etcd1 etcdctl del /test/txn >/dev/null 2>&1

# ---- Test 4: etcd lease ----
log "Test 4: etcd lease + expiry"
LEASE_ID=$(docker compose exec -T etcd1 etcdctl lease grant 5 2>/dev/null | grep -o 'lease [a-f0-9]*' | awk '{print $2}')
if [[ -n "$LEASE_ID" ]]; then
    docker compose exec -T etcd1 etcdctl put /test/lease "leased" --lease="$LEASE_ID" >/dev/null 2>&1
    LEASE_VAL=$(docker compose exec -T etcd1 etcdctl get /test/lease --print-value-only 2>/dev/null || echo "")
    if [[ "$LEASE_VAL" == "leased" ]]; then
        pass "etcd lease: key attached"
    else
        fail "etcd lease: key not attached"
    fi

    log "  Waiting for lease expiry..."
    sleep 8
    LEASE_GONE=$(docker compose exec -T etcd1 etcdctl get /test/lease --print-value-only 2>/dev/null || echo "")
    if [[ -z "$LEASE_GONE" ]]; then
        pass "etcd lease: key auto-deleted on expiry"
    else
        fail "etcd lease: key still present after TTL+grace"
    fi
else
    fail "etcd lease: could not grant"
fi

# ---- Test 5: block device accessible ----
log "Test 5: block device accessibility"
BLOCK_OK=$(docker compose run --rm block-init sh -c "test -f /block-device/etcfuse.img && stat -c %s /block-device/etcfuse.img" 2>/dev/null || echo "0")
if [[ "$BLOCK_OK" -gt 0 ]]; then
    pass "block device: $((BLOCK_OK / 1024 / 1024)) MB raw file created"
else
    fail "block device: not accessible"
fi

if $QUICK; then
    log ""
    log "Quick run complete. Passed=$PASS Failed=$FAIL"
    exit $FAIL
fi

# ---- Tests 6-N: require EtcFS binaries (skipped in Phase 0) ----
log "Test 6: EtcFS binaries (skipped — not yet built in Phase 0)"
log "  Build with: make all"
log "  Then re-run this test suite."

# ---- Summary ----
log ""
log "============================================"
log "E2E test suite complete"
log "  Passed: $PASS"
log "  Failed: $FAIL"
log "============================================"

exit $FAIL
