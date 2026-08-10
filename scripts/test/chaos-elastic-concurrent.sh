#!/bin/bash
# chaos-elastic-concurrent.sh — verify the cluster tolerates two nodes
# joining AT THE SAME TIME, not one after another. chaos-elastic.sh only
# ever adds one node at a time, fully healthy before the next add starts;
# that's not what a real autoscaling group does under a load spike, which
# can fire off several instance launches within the same few seconds.
#
# Concurrency contended per README § Sharding hot structures:
#   - etcd `member add` back to back before the first joiner is healthy —
#     changes quorum size mid-join.
#   - `arena:<node_id>` acquisition — two nodes racing for the free-arena
#     pool / next arena ID (pkg/arena.Allocator.AcquireArena).
#   - inode allocation — a single CAS-retried global counter
#     (metadata.Store.NextCounter), contended directly by concurrent
#     creates from both new nodes.
#
# Usage:
#   ./chaos-elastic-concurrent.sh docker
#   ./chaos-elastic-concurrent.sh aws
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
REPORT_DIR="$PROJECT_ROOT/chaos-report-elastic-concurrent-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$REPORT_DIR"
PASS=0; FAIL=0

MODE="${1:-}"
[[ "$MODE" == "docker" || "$MODE" == "aws" ]] || { echo "usage: $0 docker|aws"; exit 1; }

log() { echo "[$(date +%H:%M:%S)] $1" | tee -a "$REPORT_DIR/chaos.log"; }
logerr() {
    echo "[$(date +%H:%M:%S)] $1" >&2
    echo "[$(date +%H:%M:%S)] $1" >> "$REPORT_DIR/chaos.log"
}

source "$SCRIPT_DIR/chaos-lib.sh"

# Same leak-prevention rationale as chaos-elastic.sh: add_node persists
# per-node info to $REPORT_DIR/node<id>.info so it survives the subshell
# `X=$(add_node N)` forks; a leftover file at exit means its instance (aws)
# is still running and needs cleanup.
cleanup_extras() {
    [[ "$MODE" == "aws" ]] || return
    local f inst ids=()
    for f in "$REPORT_DIR"/node*.info; do
        [[ -f "$f" ]] || continue
        inst=$(cut -d' ' -f3 "$f")
        [[ -n "$inst" ]] && ids+=("$inst")
    done
    [[ "${#ids[@]}" -gt 0 ]] || return
    log "  cleanup: terminating leftover extra AWS instances: ${ids[*]}"
    aws ec2 terminate-instances --instance-ids "${ids[@]}" >/dev/null 2>&1 || true
}
trap cleanup_extras EXIT

ok()  { PASS=$((PASS+1)); log "  PASS: $1"; }
bad() { FAIL=$((FAIL+1)); log "  FAIL: $1"; }

# ============================================================
# MAIN
# ============================================================
if ! provision_cluster; then
    log "FATAL: provision failed"
    teardown_cluster
    exit 1
fi

[[ "$MODE" == "aws" ]] && TAG=$(jq -r '.cluster_name' "$PROJECT_ROOT/$STATE_FILE")

log "======== Baseline (3 nodes) ========"
if writef "$N1" "before-scale" "cs-baseline.txt"; then
    ok "baseline write on n1"
else
    bad "baseline write on n1 did not land"
fi

# ============================================================
# CS1: two nodes join AT THE SAME TIME.
#
# `X=$(add_node N)` normally captures the winning node's identifier via
# command substitution — but backgrounding that with `&` forks the
# subshell before the assignment happens, so the assignment itself would
# be silently lost. Redirect each call's stdout to its own file instead,
# and read the result back after `wait`.
# ============================================================
log "======== CS1: concurrent join — node 4 and node 5 simultaneously ========"
add_node 4 > "$REPORT_DIR/node4-join.out" 2>> "$REPORT_DIR/chaos.log" &
PID4=$!
add_node 5 > "$REPORT_DIR/node5-join.out" 2>> "$REPORT_DIR/chaos.log" &
PID5=$!

RC4=0; RC5=0
wait "$PID4" || RC4=$?
wait "$PID5" || RC5=$?

NODE4=$(cat "$REPORT_DIR/node4-join.out" 2>/dev/null)
NODE5=$(cat "$REPORT_DIR/node5-join.out" 2>/dev/null)

if [[ "$RC4" -eq 0 && -n "$NODE4" ]]; then
    ok "node4 joined concurrently ($NODE4)"
else
    bad "node4 failed to join concurrently (rc=$RC4)"
fi
if [[ "$RC5" -eq 0 && -n "$NODE5" ]]; then
    ok "node5 joined concurrently ($NODE5)"
else
    bad "node5 failed to join concurrently (rc=$RC5)"
fi

# Nothing past this point can assume a joiner actually came up — a failed
# concurrent join is exactly the scenario this script exists to catch, so
# keep going and report against whichever nodes are actually available
# rather than aborting the whole run.

# ============================================================
# CS2: both joiners see data written before either of them existed.
# ============================================================
log "======== CS2: both joiners see pre-join data ========"
if [[ -n "$NODE4" ]]; then
    V=$(readf "$NODE4" "cs-baseline.txt")
    # shellcheck disable=SC2015
    [[ -n "$V" ]] && ok "node4 sees pre-join data: $V" || bad "node4 cannot see pre-join data"
fi
if [[ -n "$NODE5" ]]; then
    V=$(readf "$NODE5" "cs-baseline.txt")
    # shellcheck disable=SC2015
    [[ -n "$V" ]] && ok "node5 sees pre-join data: $V" || bad "node5 cannot see pre-join data"
fi

# ============================================================
# CS3: arena ownership is disjoint. Two nodes racing AcquireArena for the
# first time must not end up owning the same arena — that would mean both
# writing file content into the same disk range.
# ============================================================
log "======== CS3: arena ownership disjoint after concurrent first write ========"
if [[ -n "$NODE4" ]]; then writef "$NODE4" "n4-arena-claim" "cs-n4-arena.txt" >/dev/null 2>&1; fi
if [[ -n "$NODE5" ]]; then writef "$NODE5" "n5-arena-claim" "cs-n5-arena.txt" >/dev/null 2>&1; fi

if [[ -n "$NODE4" && -n "$NODE5" ]]; then
    # Ownership is one key per arena — arena:<node>/<arena_id> — so the node's
    # holdings are a prefix, not a single key.
    ARENA4=$(etcdctl_on get --prefix "arena:n4/" --keys-only 2>/dev/null | grep -c 'arena:n4/')
    ARENA5=$(etcdctl_on get --prefix "arena:n5/" --keys-only 2>/dev/null | grep -c 'arena:n5/')
    SHARED=$(etcdctl_on get --prefix "arena:" --keys-only 2>/dev/null | grep -oE '/[0-9]+$' | sort | uniq -d | tr -d '\n')
    if [[ "$ARENA4" -gt 0 && "$ARENA5" -gt 0 && -z "$SHARED" ]]; then
        ok "n4 holds $ARENA4 arena(s), n5 holds $ARENA5, and no arena ID is claimed twice"
    else
        bad "arena collision or missing arena record — n4 arenas=$ARENA4 n5 arenas=$ARENA5 shared=[$SHARED]"
    fi
else
    log "  SKIP: arena check needs both n4 and n5 to have joined"
fi

# ============================================================
# CS4: concurrent creates from both new nodes at once — the direct stress
# test for metadata.Store.NextCounter's CAS-retry loop, which every inode
# allocation goes through regardless of which node calls it.
# ============================================================
CONCURRENT_FILES=10
log "======== CS4: $CONCURRENT_FILES concurrent creates from each new node ========"
if [[ -n "$NODE4" && -n "$NODE5" ]]; then
    for i in $(seq 1 "$CONCURRENT_FILES"); do
        writef "$NODE4" "n4-$i" "cs-concurrent-n4-$i.txt" >>"$REPORT_DIR/chaos.log" 2>&1 &
        writef "$NODE5" "n5-$i" "cs-concurrent-n5-$i.txt" >>"$REPORT_DIR/chaos.log" 2>&1 &
    done
    wait

    # Count from a node that did NOT do the writing — proves the creates
    # actually reached etcd, not just each node's own write-through cache.
    SURVIVOR="$N1"
    LISTING=$(lsf "$SURVIVOR" 2>/dev/null)
    SEEN=$(echo "$LISTING" | grep -c '^cs-concurrent-n[45]-[0-9]*\.txt$')
    EXPECTED=$((CONCURRENT_FILES * 2))
    if [[ "$SEEN" -eq "$EXPECTED" ]]; then
        ok "all $EXPECTED concurrently-created files visible from n1 — no inode collision"
    else
        bad "expected $EXPECTED concurrent creates visible from n1, saw $SEEN"
    fi
else
    log "  SKIP: concurrent-create stress needs both n4 and n5 to have joined"
fi

# ============================================================
# CS5: original 3 nodes stayed healthy throughout the concurrent join.
# ============================================================
log "======== CS5: original nodes unaffected by the concurrent join ========"
if writef "$N1" "after-concurrent-join" "cs-post-join.txt"; then
    V2=$(readf "$N2" "cs-post-join.txt"); V3=$(readf "$N3" "cs-post-join.txt")
    if [[ -n "$V2" && -n "$V3" ]]; then
        ok "n1/n2/n3 still fully functional after the concurrent join"
    else
        bad "n2 or n3 lost visibility of n1's post-join write (n2='$V2' n3='$V3')"
    fi
else
    bad "n1 write failed after the concurrent join"
fi

# ============================================================
# Scale back down (best-effort; failures here don't invalidate the
# join-side assertions above, but are still worth counting).
# ============================================================
log "======== Scale-in: removing both extra nodes ========"
[[ -n "$NODE5" ]] && remove_node 5
[[ -n "$NODE4" ]] && remove_node 4
sleep 3

V=$(readf "$N1" "cs-baseline.txt")
# shellcheck disable=SC2015
[[ -n "$V" ]] && ok "original baseline data intact after scale-in" || bad "baseline data lost after scale-in"

teardown_cluster

{
    echo "=== Concurrent Scale-Out Chaos Report ($MODE) ==="
    echo "Pass: $PASS  Fail: $FAIL  Total: $((PASS+FAIL))"
    [[ "$FAIL" -eq 0 ]] && echo "STATUS: ALL PASS" || echo "STATUS: $FAIL FAILURES"
} | tee "$REPORT_DIR/summary.txt"
echo "Report: $REPORT_DIR/summary.txt"

[[ "$FAIL" -eq 0 ]]
