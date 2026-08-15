#!/bin/bash
# chaos-ipc-restart.sh — kill the metadata daemon underneath a live mount and
# assert the mount recovers on its own.
#
# Every other chaos script that restarts a node restarts the FUSE daemon with
# it, which hides the question this one asks: the FUSE process caches one IPC
# connection per worker thread, and if it never notices that connection has
# died, the mount answers EIO forever even after the daemon is back. Nothing
# exercised that, and the mount cannot be remounted to paper over it — the
# recovery has to happen inside the running process.
#
# Docker-only on purpose: the two daemons have to be killable separately, and
# only the compose topology puts them in separate containers. On AWS they share
# an instance and restart together.
#
# S1 — restart: kill the metadata container under a live mount, bring it back,
#      and confirm the same mount serves reads and writes again.
# S2 — repeat: do it several times in a row, so a reconnect that works once but
#      leaks or latches is caught.
#
# Frame-level corruption is covered by test/c/test_ops.c, which drives ipc_sync
# against a socketpair directly; there is no way to inject a truncated frame
# between two real daemons without a proxy in the middle.
#
# Usage:
#   ./chaos-ipc-restart.sh [scenario|all]
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
REPORT_DIR="$PROJECT_ROOT/chaos-report-ipc-restart-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$REPORT_DIR"
PASS=0; FAIL=0

MODE=docker
SCENARIO="${1:-all}"

log() { echo "[$(date +%H:%M:%S)] $1" | tee -a "$REPORT_DIR/chaos.log"; }
logerr() {
    echo "[$(date +%H:%M:%S)] $1" >&2
    echo "[$(date +%H:%M:%S)] $1" >> "$REPORT_DIR/chaos.log"
}
pass() { log "  PASS: $1"; PASS=$((PASS+1)); }
fail() { log "  FAIL: $1"; FAIL=$((FAIL+1)); }

source "$SCRIPT_DIR/chaos-lib.sh"

# kill_meta <idx> — SIGKILL the metadata container, leaving the paired FUSE
# container running with its now-dead socket.
kill_meta() { docker kill -s KILL "$(meta_of "$1")" >/dev/null 2>&1; }

# start_meta <idx> — bring it back and wait until it is serving on its socket.
start_meta() {
    docker start "$(meta_of "$1")" >/dev/null 2>&1
    for _ in $(seq 1 30); do
        docker exec "$(meta_of "$1")" sh -c 'test -S /run/etcfuse/etcfuse.sock' >/dev/null 2>&1 && return 0
        sleep 1
    done
    return 1
}

# io_works <idx> <tag> — one write and one read-back through the live mount.
# The mount is never touched, so a pass means the running FUSE process
# re-established its own connection.
io_works() {
    local n want got
    n=$(node_ip "$1")
    want="ipc-restart-$2-$(date +%s%N)"
    writef "$n" "$want" "ipc-restart-$2.txt" || return 1
    got=$(readf "$n" "ipc-restart-$2.txt") || return 1
    [[ "$got" == "$want" ]]
}

# ---- S1: one restart under a live mount ----

scenario_s1() {
    log "======== S1: metadata daemon restart under a live mount ========"

    io_works 1 s1-before || { fail "mount not usable before the kill"; return; }

    log "  killing $(meta_of 1) (FUSE container keeps running)..."
    kill_meta 1
    sleep 2

    # The mount must still be mounted: losing the backend is not a reason for
    # the FUSE process to exit or for the kernel to drop the mount.
    check_mount "$(node_ip 1)" || { fail "mount disappeared when the daemon died"; return; }

    # I/O while the daemon is down is expected to fail; that is not the test.
    io_works 1 s1-down >/dev/null 2>&1 && log "  (I/O still served while down — buffered)"

    log "  restarting $(meta_of 1)..."
    start_meta 1 || { fail "metadata daemon did not come back"; return; }

    # A few attempts: the first request after the restart is allowed to fail,
    # because it is the one that discovers the dead connection. The point is
    # that it recovers without remounting, not that it never errors.
    local ok=1
    for attempt in 1 2 3 4 5; do
        if io_works 1 "s1-after-$attempt"; then ok=0; break; fi
        sleep 2
    done
    if [[ "$ok" -ne 0 ]]; then
        dump_logs "$(node_ip 1)"
        fail "mount still broken after the daemon came back (never reconnected)"
        return
    fi

    # The other nodes must have been unaffected throughout.
    io_works 2 s1-peer || { fail "peer node lost I/O during the restart"; return; }

    pass "mount recovered on its own after a metadata daemon restart"
}

# ---- S2: repeated restarts ----

scenario_s2() {
    log "======== S2: repeated restarts, no cumulative damage ========"

    for round in 1 2 3; do
        log "  round $round: kill + restart $(meta_of 1)"
        kill_meta 1
        sleep 2
        start_meta 1 || { fail "daemon did not come back in round $round"; return; }

        local ok=1
        for attempt in 1 2 3 4 5; do
            if io_works 1 "s2-r$round-$attempt"; then ok=0; break; fi
            sleep 2
        done
        [[ "$ok" -eq 0 ]] || { dump_logs "$(node_ip 1)"; fail "no recovery in round $round"; return; }
    done

    # Data written across the restarts must all still be there and correct.
    local listing
    listing=$(lsf "$(node_ip 2)")
    for round in 1 2 3; do
        grep -q "ipc-restart-s2-r$round-" <<< "$listing" || {
            fail "round $round's file is missing from a peer's view"
            return
        }
    done

    pass "three restarts in a row, every recovery clean"
}

# ---- driver ----

log "IPC restart test: mode=$MODE scenario=$SCENARIO"
provision_cluster || { log "FATAL: provision failed"; teardown_cluster; exit 1; }

case "$SCENARIO" in
    s1)  scenario_s1 ;;
    s2)  scenario_s2 ;;
    all) scenario_s1; scenario_s2 ;;
    *)   log "unknown scenario: $SCENARIO"; teardown_cluster; exit 1 ;;
esac

teardown_cluster

{
    echo "=== IPC Restart Report ($MODE) ==="
    echo "Pass: $PASS  Fail: $FAIL  Total: $((PASS+FAIL))"
    if [[ "$FAIL" -eq 0 ]]; then
        echo "STATUS: ALL PASS"
    else
        echo "STATUS: FAILURES PRESENT"
    fi
} | tee "$REPORT_DIR/summary.txt"

log "Report: $REPORT_DIR/summary.txt"
[[ "$FAIL" -eq 0 ]]
