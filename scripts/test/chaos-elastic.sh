#!/bin/bash
# chaos-elastic.sh — verify graceful scale-out/scale-in: add 2 nodes to a
# running 3-node cluster, confirm they join and serve correctly, then
# remove them gracefully, confirming the cluster keeps working throughout.
# Models aggressive AWS autoscaling (rapid add/remove of compute nodes).
#
# Usage:
#   ./chaos-elastic.sh docker
#   ./chaos-elastic.sh aws
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
REPORT_DIR="$PROJECT_ROOT/chaos-report-elastic-$(date +%Y%m%d-%H%M%S)"
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

# Extra nodes launched outside the base 3-node cluster this run owns; used
# by the exit trap so a failed remove_node doesn't leak AWS instances.
# add_node/remove_node (from chaos-lib.sh) persist per-node info to
# $REPORT_DIR/node<id>.info — command substitution subshells lose in-memory
# state, so the aws add_node writes IP/instance-id to a file instead — and
# remove_node deletes that file once terminated, so any leftover file at
# exit means its instance is still running.
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


# ============================================================
# canary <label> — write from N1, verify readable from N2 and N3 (the
# original, never-removed nodes). Cheap correctness check reused at every
# phase of the scale-out/scale-in sequence.
# ============================================================
canary() {
    local label="$1"
    local fname="elastic-$label.txt"
    if ! writef "$N1" "data-$label" "$fname"; then
        FAIL=$((FAIL+1)); log "  FAIL ($label): write on N1 did not land"; return 1
    fi
    local v2 v3
    v2=$(readf "$N2" "$fname"); v3=$(readf "$N3" "$fname")
    if [[ -n "$v2" && -n "$v3" ]]; then
        PASS=$((PASS+1)); log "  PASS ($label): N2/N3 both read back '$v2'"
    else
        FAIL=$((FAIL+1)); log "  FAIL ($label): N2='$v2' N3='$v3'"
    fi
}

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
canary baseline

log "======== Scale-out: adding node 4 ========"
NODE4=$(add_node 4)
if [[ -n "$NODE4" ]]; then
    PASS=$((PASS+1)); log "  PASS: node4 joined and mounted ($NODE4)"
    # node4 must see data written before it existed.
    V=$(readf "$NODE4" "elastic-baseline.txt")
    # shellcheck disable=SC2015
    [[ -n "$V" ]] && { PASS=$((PASS+1)); log "  PASS: node4 sees pre-join data: $V"; } || { FAIL=$((FAIL+1)); log "  FAIL: node4 cannot see pre-join data"; }
    # a write from node4 must be visible to the original nodes.
    writef "$NODE4" "from-node4" "elastic-node4-write.txt"
    V=$(readf "$N1" "elastic-node4-write.txt")
    # shellcheck disable=SC2015
    [[ -n "$V" ]] && { PASS=$((PASS+1)); log "  PASS: N1 sees node4's write: $V"; } || { FAIL=$((FAIL+1)); log "  FAIL: N1 cannot see node4's write"; }
else
    FAIL=$((FAIL+1)); log "  FAIL: node4 failed to join"
fi
canary after-add-node4

log "======== Scale-out: adding node 5 ========"
NODE5=$(add_node 5)
if [[ -n "$NODE5" ]]; then
    PASS=$((PASS+1)); log "  PASS: node5 joined and mounted ($NODE5)"
    V=$(readf "$NODE5" "elastic-node4-write.txt")
    # shellcheck disable=SC2015
    [[ -n "$V" ]] && { PASS=$((PASS+1)); log "  PASS: node5 sees node4's write: $V"; } || { FAIL=$((FAIL+1)); log "  FAIL: node5 cannot see node4's write"; }
else
    FAIL=$((FAIL+1)); log "  FAIL: node5 failed to join"
fi
canary five-node

log "======== Scale-in: removing node 5 gracefully ========"
remove_node 5
sleep 3
canary after-remove-node5
# shellcheck disable=SC2015
[[ -n "$NODE4" ]] && { V=$(readf "$NODE4" "elastic-baseline.txt"); [[ -n "$V" ]] && { PASS=$((PASS+1)); log "  PASS: node4 still functional after node5 removed"; } || { FAIL=$((FAIL+1)); log "  FAIL: node4 broken after node5 removed"; }; }

log "======== Scale-in: removing node 4 gracefully ========"
remove_node 4
sleep 3
canary after-remove-node4

log "======== Final check: back to 3-node baseline ========"
V=$(readf "$N1" "elastic-baseline.txt")
# shellcheck disable=SC2015
[[ -n "$V" ]] && { PASS=$((PASS+1)); log "  PASS: original baseline data intact: $V"; } || { FAIL=$((FAIL+1)); log "  FAIL: baseline data lost"; }

teardown_cluster

{
    echo "=== Elastic Scale-Out/Scale-In Chaos Report ($MODE) ==="
    echo "Pass: $PASS  Fail: $FAIL  Total: $((PASS+FAIL))"
    [[ "$FAIL" -eq 0 ]] && echo "STATUS: ALL PASS" || echo "STATUS: $FAIL FAILURES"
} | tee "$REPORT_DIR/summary.txt"
echo "Report: $REPORT_DIR/summary.txt"
