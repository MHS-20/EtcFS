#!/bin/bash
# chaos-fencing-namespace.sh — verify the fencing generation guard covers
# namespace mutations, not only extent writes.
#
# Background: the guard was originally applied on the data path only, so a
# fenced node was blocked from writing file bytes while still being able to
# create, delete and rename entries in the shared namespace. Chaos scenario S5
# did not catch this because it asserts only that a *write* is rejected.
#
# Each scenario below fences n1 by bumping gen:n1, then asserts that a specific
# namespace operation is refused, that the namespace is unchanged afterwards,
# and that the surviving nodes are unaffected.
#
# Usage:
#   ./chaos-fencing-namespace.sh docker [scenario|all]
#   ./chaos-fencing-namespace.sh aws    [scenario|all]
#   ./chaos-fencing-namespace.sh both   [scenario|all]   # docker first, then aws
#
# Scenarios: create mkdir unlink rename truncate survivors recover
#
# `both` runs docker first and only continues to AWS if docker passed —
# provisioning EC2 to rediscover a failure Docker already found wastes minutes
# and money.
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

MODE="${1:-}"
SCENARIO="${2:-all}"
[[ "$MODE" == "docker" || "$MODE" == "aws" || "$MODE" == "both" ]] || {
    echo "usage: $0 docker|aws|both [create|mkdir|unlink|rename|truncate|survivors|recover|all]"
    exit 1
}

# --- both: re-invoke self per transport, docker gating aws ------------------
if [[ "$MODE" == "both" ]]; then
    echo "=== Phase 1/2: docker ==="
    if ! "$0" docker "$SCENARIO"; then
        echo "=== docker phase FAILED — not provisioning AWS ==="
        exit 1
    fi
    echo "=== Phase 2/2: aws ==="
    exec "$0" aws "$SCENARIO"
fi

REPORT_DIR="$PROJECT_ROOT/chaos-report-fencing-ns-$MODE-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$REPORT_DIR"
PASS=0; FAIL=0

log() { echo "[$(date +%H:%M:%S)] $1" | tee -a "$REPORT_DIR/chaos.log"; }
logerr() {
    echo "[$(date +%H:%M:%S)] $1" >&2
    echo "[$(date +%H:%M:%S)] $1" >> "$REPORT_DIR/chaos.log"
}

source "$SCRIPT_DIR/chaos-lib.sh"

# ============================================================
# Fencing helpers
# ============================================================

# gen_of <node_key> — current fencing generation, 0 when the key is absent.
gen_of() {
    local v
    v=$(etcdctl_on get "gen:$1" --print-value-only 2>/dev/null | tr -d '[:space:]')
    [[ -n "$v" ]] && echo "$v" || echo 0
}

# fence_node <node_key> — bump the generation the way the fencing controller
# would after dual confirmation. The daemon caches the generation it started
# with, so the bump invalidates every guard it holds without needing a restart.
fence_node() {
    local g n
    g=$(gen_of "$1"); n=$((g + 1))
    log "  fencing $1: gen $g -> $n"
    etcdctl_on put "gen:$1" "$n" >/dev/null 2>&1
    sleep 2
}

# unfence_node <node_key> <gen> — restore the generation the daemon started
# with, so its cached guard matches again.
unfence_node() {
    log "  restoring gen:$1 -> $2"
    etcdctl_on put "gen:$1" "$2" >/dev/null 2>&1
    sleep 2
}

# ok <msg> / bad <msg> — single place for the pass/fail counters.
ok()  { PASS=$((PASS+1)); log "  PASS: $1"; }
bad() { FAIL=$((FAIL+1)); log "  FAIL: $1"; }

# refute <description> <command...> — assert the command FAILS.
# The whole point of these scenarios is that an operation must be refused, so a
# zero exit status is the failure case here.
refute() {
    local what="$1"; shift
    if "$@" >/dev/null 2>&1; then
        bad "$what: operation succeeded on a fenced node"
        return 1
    fi
    ok "$what: rejected"
    return 0
}

# affirm <description> <command...> — assert the command SUCCEEDS.
affirm() {
    local what="$1"; shift
    if "$@" >/dev/null 2>&1; then
        ok "$what"
        return 0
    fi
    bad "$what"
    return 1
}

# runcmd_status <node> <cmd> — run a command and propagate its exit status.
# The shared runcmd() swallows failures into an "ERR:n" string on stdout, which
# refute() cannot act on.
runcmd_status() {
    local out
    out=$(runcmd "$1" "$2")
    [[ "$out" == *"ERR:"* ]] && return 1
    return 0
}

# entry_exists <node> <name> — is <name> listed at the mount root of <node>?
entry_exists() {
    lsf "$1" 2>/dev/null | tr -s ' ' '\n' | grep -qx "$2"
}

# require_baseline — every scenario needs n1 mounted and unfenced to start.
# Returns the generation n1 is currently running with, for later restore.
BASE_GEN=""
require_baseline() {
    check_mount "$N1" || { bad "FUSE mount on n1 not ready"; return 1; }
    BASE_GEN=$(gen_of n1)
    return 0
}

# ============================================================
# Scenarios
# ============================================================

# A fenced node must not be able to create a file.  Before the guard covered
# namespace ops this succeeded, and the created entry was visible cluster-wide.
run_create() {
    log "======== NS1: create rejected while fenced ========"
    require_baseline || return

    affirm "baseline create works before fencing" writef "$N1" "pre" "ns1-pre.txt"

    fence_node n1
    refute "create on fenced node" writef "$N1" "post" "ns1-post.txt"

    # Checked from a survivor, not from n1: n1's own view could be served from
    # a local cache and would not prove the entry never reached etcd.
    if entry_exists "$N2" "ns1-post.txt"; then
        bad "fenced create leaked into the namespace (visible from n2)"
    else
        ok "fenced create left no entry in the namespace"
    fi

    unfence_node n1 "$BASE_GEN"
}

# Same assertion for mkdir — a separate transaction path (AtomicCreateDir).
run_mkdir() {
    log "======== NS2: mkdir rejected while fenced ========"
    require_baseline || return

    fence_node n1
    refute "mkdir on fenced node" mkdirf "$N1" "ns2-dir"

    if entry_exists "$N2" "ns2-dir"; then
        bad "fenced mkdir leaked into the namespace (visible from n2)"
    else
        ok "fenced mkdir left no entry in the namespace"
    fi

    unfence_node n1 "$BASE_GEN"
}

# Deletion is the dangerous direction: a fenced node destroying entries other
# nodes still rely on. The entry must survive the attempt.
run_unlink() {
    log "======== NS3: unlink rejected while fenced, entry survives ========"
    require_baseline || return

    affirm "seed file for deletion" writef "$N1" "keep" "ns3-keeper.txt"

    fence_node n1
    refute "unlink on fenced node" rmf "$N1" "ns3-keeper.txt"

    if entry_exists "$N2" "ns3-keeper.txt"; then
        ok "entry survived the fenced unlink"
    else
        bad "fenced unlink removed the entry — data loss"
    fi

    unfence_node n1 "$BASE_GEN"
}

# Rename mutates two dirents in one transaction; both must be refused, leaving
# the original name in place.
run_rename() {
    log "======== NS4: rename rejected while fenced ========"
    require_baseline || return

    affirm "seed file for rename" writef "$N1" "orig" "ns4-src.txt"

    fence_node n1
    refute "rename on fenced node" mvf "$N1" "ns4-src.txt" "ns4-dst.txt"

    if entry_exists "$N2" "ns4-src.txt" && ! entry_exists "$N2" "ns4-dst.txt"; then
        ok "namespace unchanged after fenced rename"
    else
        bad "fenced rename altered the namespace"
    fi

    unfence_node n1 "$BASE_GEN"
}

# Truncate writes the inode record through Put rather than Txn, so it bypassed
# the guard even after the Txn path was covered.  Regression test for that.
run_truncate() {
    log "======== NS5: truncate rejected while fenced ========"
    require_baseline || return

    affirm "seed file for truncate" writef "$N1" "0123456789" "ns5-trunc.txt"

    fence_node n1
    refute "truncate on fenced node" runcmd_status "$N1" "truncate -s 2 /mnt/etcfuse/ns5-trunc.txt"

    unfence_node n1 "$BASE_GEN"

    # Read back from a survivor after unfencing: the size must be untouched.
    local v
    v=$(readf "$N2" "ns5-trunc.txt")
    if [[ "$v" == "0123456789" ]]; then
        ok "file contents unchanged after fenced truncate"
    else
        bad "fenced truncate altered the file (got '${v}')"
    fi
}

# Fencing one node must not degrade the others. A guard that rejected everyone's
# writes would pass every assertion above while breaking the cluster.
run_survivors() {
    log "======== NS6: survivors unaffected while n1 is fenced ========"
    require_baseline || return

    fence_node n1

    affirm "n2 create while n1 fenced"  writef "$N2" "n2-data" "ns6-n2.txt"
    affirm "n3 create while n1 fenced"  writef "$N3" "n3-data" "ns6-n3.txt"
    affirm "n2 mkdir while n1 fenced"   mkdirf "$N2" "ns6-dir"
    affirm "n2 unlink while n1 fenced"  rmf "$N2" "ns6-n2.txt"

    unfence_node n1 "$BASE_GEN"
}

# Once the generation is restored, the node must become usable again without a
# restart — the guard compares against the value the daemon cached at startup.
run_recover() {
    log "======== NS7: node recovers after generation is restored ========"
    require_baseline || return

    fence_node n1
    refute "create while fenced" writef "$N1" "no" "ns7-blocked.txt"

    unfence_node n1 "$BASE_GEN"

    affirm "create after generation restored" writef "$N1" "yes" "ns7-ok.txt"

    if entry_exists "$N2" "ns7-ok.txt"; then
        ok "post-recovery write visible cluster-wide"
    else
        bad "post-recovery write not visible from n2"
    fi
}

# ============================================================
# MAIN — provision once, run scenarios in sequence, teardown once.
# ============================================================
if ! provision_cluster; then
    log "FATAL: provision failed"
    teardown_cluster
    exit 1
fi

case "$SCENARIO" in
    create)    run_create ;;
    mkdir)     run_mkdir ;;
    unlink)    run_unlink ;;
    rename)    run_rename ;;
    truncate)  run_truncate ;;
    survivors) run_survivors ;;
    recover)   run_recover ;;
    all)       run_create; run_mkdir; run_unlink; run_rename; run_truncate; run_survivors; run_recover ;;
    *)         log "unknown scenario: $SCENARIO" ;;
esac

teardown_cluster

{
    echo "=== Namespace Fencing Guard Report ($MODE) ==="
    echo "Pass: $PASS  Fail: $FAIL  Total: $((PASS+FAIL))"
    [[ "$FAIL" -eq 0 ]] && echo "STATUS: ALL PASS" || echo "STATUS: $FAIL FAILURES"
} | tee "$REPORT_DIR/summary.txt"
echo "Report: $REPORT_DIR/summary.txt"

[[ "$FAIL" -eq 0 ]]
