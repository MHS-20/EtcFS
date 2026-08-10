#!/bin/bash
# End-to-end test: seed etcd → start Go backend → mount FUSE → verify reads.
#
# Usage:
#   ETCD_ENDPOINTS=http://localhost:2379 ./test/e2e/run-phase2.sh
#
# Requires etcd running at ETCD_ENDPOINTS.
# Builds both binaries if needed, seeds data, tests read ops, cleans up.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

ENDPOINTS="${ETCD_ENDPOINTS:-http://localhost:2379}"
SOCK="/tmp/etcfuse-phase2.sock"
MNT="/tmp/etcfuse-phase2-mnt"
PASS=0
FAIL=0

log()  { echo "[$(date +%T)] $*"; }
pass() { echo "  ✓ $1"; ((PASS++)); }
fail() { echo "  ✗ $1"; ((FAIL++)); }

cleanup() {
    log "Cleaning up..."
    fusermount3 -u "$MNT" 2>/dev/null || true
    kill "$META_PID" 2>/dev/null || true
    kill "$FUSE_PID" 2>/dev/null || true
    rm -f "$SOCK"
    rmdir "$MNT" 2>/dev/null || true
}
trap cleanup EXIT

# ---- Build ----
log "Building binaries..."
go build -o bin/etcfuse-meta ./cmd/etcfuse-meta
go build -o bin/seed-etcd   ./cmd/seed-etcd
make -C cmd/etcfuse
log "Binaries built."

# ---- Seed ----
log "Seeding etcd..."
ETCD_ENDPOINTS="$ENDPOINTS" bin/seed-etcd
log "Seeded."

# ---- Start Go backend ----
log "Starting Go metadata backend..."
rm -f "$SOCK"
bin/etcfuse-meta \
    --etcd-endpoints="$ENDPOINTS" \
    --listen="$SOCK" \
    --node-id=phase2-test \
    --lease-ttl=60s \
    &>/tmp/etcfuse-phase2-meta.log &
META_PID=$!

for i in $(seq 1 20); do
    test -S "$SOCK" && break
    sleep 0.2
done
log "Go backend ready (PID=$META_PID)"

# ---- Mount FUSE ----
log "Mounting FUSE daemon..."
mkdir -p "$MNT"
bin/etcfuse --socket="$SOCK" --log-level=3 "$MNT" &>/tmp/etcfuse-phase2-fuse.log &
FUSE_PID=$!

for i in $(seq 1 20); do
    mountpoint -q "$MNT" 2>/dev/null && break
    sleep 0.2
done
log "FUSE mounted at $MNT (PID=$FUSE_PID)"
sleep 1

# ---- Tests ----
log "Running read tests..."

# Read entire directory
ENTRIES=$(ls "$MNT" 2>&1) || true
if echo "$ENTRIES" | grep -q "hello.txt"; then
    pass "ls shows hello.txt"
else
    fail "ls shows hello.txt (got: $ENTRIES)"
fi

# Stat a file
if SIZE=$(stat -c %s "$MNT/hello.txt" 2>/dev/null); then
    pass "stat hello.txt (size=$SIZE)"
else
    fail "stat hello.txt"
fi

# Read a symlink
if TARGET=$(readlink "$MNT/link-to-hello" 2>/dev/null); then
    pass "readlink link-to-hello (-> $TARGET)"
else
    fail "readlink link-to-hello"
fi

# List subdirectory
SUBDIR=$(ls "$MNT/subdir" 2>&1) || true
if echo "$SUBDIR" | grep -q "nested.txt"; then
    pass "ls subdir shows nested.txt"
else
    fail "ls subdir shows nested.txt (got: $SUBDIR)"
fi

# Count regular files
FCOUNT=$(find "$MNT" -type f 2>/dev/null | wc -l)
if [ "$FCOUNT" -ge 4 ]; then
    pass "find -type f counted $FCOUNT files"
else
    fail "find -type f counted $FCOUNT files (expected >=4)"
fi

# ---- Results ----
log ""
log "=== Phase 2 Results ==="
log "  Passed: $PASS"
log "  Failed: $FAIL"
log "========================="

exit $FAIL
