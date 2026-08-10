#!/bin/bash
# chaos-test-single-cluster.sh — run all chaos scenarios against ONE cluster,
# back to back, to check the cluster recovers from repeated/sequential faults
# instead of each scenario getting a fresh cluster (see chaos-test.sh).
#
# Usage:
#   ./chaos-test-single-cluster.sh docker [scenario|all]
#   ./chaos-test-single-cluster.sh aws    [scenario|all]
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
REPORT_DIR="$PROJECT_ROOT/chaos-report-single-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$REPORT_DIR"
PASS=0; FAIL=0

MODE="${1:-}"
SCENARIO="${2:-all}"
[[ "$MODE" == "docker" || "$MODE" == "aws" ]] || { echo "usage: $0 docker|aws [scenario|all]"; exit 1; }

log() { echo "[$(date +%H:%M:%S)] $1" | tee -a "$REPORT_DIR/chaos.log"; }
logerr() {
    echo "[$(date +%H:%M:%S)] $1" >&2
    echo "[$(date +%H:%M:%S)] $1" >> "$REPORT_DIR/chaos.log"
}

source "$SCRIPT_DIR/chaos-lib.sh"


# ============================================================
# Scenarios — same assertions as chaos-test.sh S1/S2/S3/S5/S6/S7,
# run back to back against the ONE cluster provisioned above.
# ============================================================

run_s1() {
    log "======== S1: C daemon SIGKILL ========"
    if ! check_mount "$N1"; then FAIL=$((FAIL+1)); log "  FAIL: FUSE mount not ready"; return; fi
    if ! writef "$N1" "s1-data" "s1-hello.txt"; then FAIL=$((FAIL+1)); log "  FAIL: pre-crash write did not land"; dump_logs "$N1"; return; fi
    if [[ "$MODE" == "docker" ]]; then
        local R=$(restart_fuse "$N1")
    else
        runcmd "$N1" "sudo pkill -9 -x etcfuse 2>/dev/null; sleep 1; sudo fusermount -uz /mnt/etcfuse 2>/dev/null; sleep 1; true"
        local R=$(runcmd30 "$N1" "
          sudo nohup /usr/local/bin/etcfuse --socket=/run/etcfuse/etcfuse.sock --node-id=n1 --log-level=1 /mnt/etcfuse > /tmp/fuse.log 2>&1 &
          for i in \$(seq 1 20); do sudo mountpoint -q /mnt/etcfuse 2>/dev/null && echo OK && exit 0; sleep 1; done
          echo FAIL
        ")
    fi
    [[ "$R" == "OK" ]] || log "  WARN: n1 did not remount cleanly ($R)"
    local V=$(readf "$N1" "s1-hello.txt")
    # shellcheck disable=SC2015
    [[ -n "$V" ]] && { PASS=$((PASS+1)); log "  PASS: $V"; } || { FAIL=$((FAIL+1)); log "  FAIL"; }
}

run_s2() {
    log "======== S2: Go daemon SIGKILL ========"
    if ! check_mount "$N1"; then FAIL=$((FAIL+1)); log "  FAIL: FUSE mount not ready"; return; fi
    if ! writef "$N1" "go-data" "s2-hello.txt"; then FAIL=$((FAIL+1)); log "  FAIL: pre-crash write did not land"; dump_logs "$N1"; return; fi
    if [[ "$MODE" == "docker" ]]; then
        local R=$(restart_pair "$M1" "$N1")
    else
        runcmd "$N1" "sudo pkill -9 etcfuse-meta 2>/dev/null; sleep 1; sudo rm -f /run/etcfuse/etcfuse.sock; true"
        runcmd "$N1" "sudo fusermount -uz /mnt/etcfuse 2>/dev/null; sleep 1; true"
        local R=$(restart_daemons "$N1" "n1")
    fi
    [[ "$R" == "OK" ]] || log "  WARN: n1 did not remount cleanly ($R)"
    local V=$(readf "$N1" "s2-hello.txt")
    # shellcheck disable=SC2015
    [[ -n "$V" ]] && { PASS=$((PASS+1)); log "  PASS: $V"; } || { FAIL=$((FAIL+1)); log "  FAIL"; }
}

run_s3() {
    log "======== S3: Network partition ========"
    if ! check_mount "$N1"; then FAIL=$((FAIL+1)); log "  FAIL: FUSE mount not ready"; return; fi
    if ! writef "$N1" "pre-part" "s3-p1.txt"; then FAIL=$((FAIL+1)); log "  FAIL: pre-partition write did not land"; dump_logs "$N1"; return; fi
    writef "$N2" "survivor-write" "s3-p2.txt" || log "  WARN: N2 pre-partition write failed"
    readf "$N3" "s3-p1.txt" > /dev/null

    if [[ "$MODE" == "docker" ]]; then
        log "  Disconnecting N1 (fuse+meta) from network..."
        partition_node "$N1" "$M1"
        sleep 15
        writef "$N2" "during-part" "s3-p3.txt"
        local V2=$(readf "$N3" "s3-p3.txt")
        # shellcheck disable=SC2015
        [[ -n "$V2" ]] && { PASS=$((PASS+1)); log "  PASS: Survivors work: $V2"; } || { FAIL=$((FAIL+1)); log "  FAIL: survivors"; }
        log "  Reconnecting N1..."
        heal_node "$N1" "$M1"
        sleep 20
        log "  Restarting N1 daemons after self-fence..."
        local R=$(restart_pair "$M1" "$N1")
    else
        local SG=$(jq -r '.sg_id' "$PROJECT_ROOT/$STATE_FILE")
        local VPC=$(jq -r '.vpc_id' "$PROJECT_ROOT/$STATE_FILE")
        local N1_INST=$(jq -r '.compute_instance_ids[0]' "$PROJECT_ROOT/$STATE_FILE")
        local N1_ENI=$(aws ec2 describe-instances --instance-ids $N1_INST \
            --query 'Reservations[0].Instances[0].NetworkInterfaces[0].NetworkInterfaceId' --output text 2>/dev/null)
        local MY_IP=$(curl -s http://checkip.amazonaws.com 2>/dev/null || echo "0.0.0.0")
        local TEMP_SG=$(aws ec2 create-security-group --group-name "chaos-temp-$$" --description "Temp partition SG" --vpc-id "$VPC" --query 'GroupId' --output text 2>/dev/null)
        aws ec2 authorize-security-group-ingress --group-id "$TEMP_SG" --protocol tcp --port 22 --cidr "${MY_IP}/32" 2>/dev/null || true
        log "  Swapping N1 to TEMP_SG=$TEMP_SG (no etcd ports)..."
        aws ec2 modify-network-interface-attribute --network-interface-id "$N1_ENI" --groups "$TEMP_SG" 2>/dev/null
        sleep 15
        writef "$N2" "during-part" "s3-p3.txt"
        local V2=$(readf "$N3" "s3-p3.txt")
        # shellcheck disable=SC2015
        [[ -n "$V2" ]] && { PASS=$((PASS+1)); log "  PASS: Survivors work: $V2"; } || { FAIL=$((FAIL+1)); log "  FAIL: survivors"; }
        log "  Restoring N1 to original SG..."
        aws ec2 modify-network-interface-attribute --network-interface-id "$N1_ENI" --groups "$SG" "$TEMP_SG" 2>/dev/null || true
        sleep 5
        aws ec2 modify-network-interface-attribute --network-interface-id "$N1_ENI" --groups "$SG" 2>/dev/null || true
        aws ec2 delete-security-group --group-id "$TEMP_SG" 2>/dev/null || true
        sleep 20
        log "  Restarting N1 daemons after self-fence..."
        local R=$(restart_daemons "$N1" "n1")
    fi
    [[ "$R" == "OK" ]] || log "  WARN: n1 did not remount cleanly ($R)"
    local V3=$(readf "$N1" "s3-p3.txt")
    # shellcheck disable=SC2015
    [[ -n "$V3" ]] && { PASS=$((PASS+1)); log "  PASS: N1 reads survivor: $V3"; } || { FAIL=$((FAIL+1)); log "  FAIL: N1 restore"; }
}

run_s5() {
    log "======== S5: Generation bump ========"
    if ! check_mount "$N1"; then FAIL=$((FAIL+1)); log "  FAIL: FUSE mount not ready"; return; fi
    local GEN
    if [[ "$MODE" == "docker" ]]; then
        GEN=$(etcdctl_on get gen:n1 --print-value-only 2>/dev/null || echo "1")
    else
        GEN=$(runcmd "$N1" 'sudo ETCDCTL_API=3 /usr/local/bin/etcdctl --endpoints=http://127.0.0.1:2379 get gen:n1 --print-value-only' 2>/dev/null || echo "1")
    fi
    [[ "$GEN" =~ ^[0-9]+$ ]] || GEN=1
    local NEWGEN=$((GEN + 1))
    log "  bumping gen:n1 $GEN -> $NEWGEN"
    if [[ "$MODE" == "docker" ]]; then
        etcdctl_on put gen:n1 "$NEWGEN" 2>/dev/null
    else
        runcmd "$N1" "sudo ETCDCTL_API=3 /usr/local/bin/etcdctl --endpoints=http://127.0.0.1:2379 put gen:n1 $NEWGEN" 2>/dev/null
    fi
    writef "$N1" "post-fence" "s5-fence.txt"
    local V=$(readf "$N1" "s5-fence.txt")
    # shellcheck disable=SC2015
    [[ -z "$V" ]] && { PASS=$((PASS+1)); log "  PASS: write blocked"; } || { FAIL=$((FAIL+1)); log "  FAIL: write succeeded"; }
    # Restore gen so later scenarios on this SAME cluster aren't fenced forever.
    if [[ "$MODE" == "docker" ]]; then
        etcdctl_on put gen:n1 "$GEN" 2>/dev/null
        local R=$(restart_pair "$M1" "$N1")
    else
        runcmd "$N1" "sudo ETCDCTL_API=3 /usr/local/bin/etcdctl --endpoints=http://127.0.0.1:2379 put gen:n1 $GEN" 2>/dev/null
        local R=$(restart_daemons "$N1" "n1")
    fi
    [[ "$R" == "OK" ]] || log "  WARN: n1 did not remount cleanly after un-fencing ($R)"
}

run_s6() {
    log "======== S6: All 3 crash ========"
    if ! check_mount "$N1"; then FAIL=$((FAIL+1)); log "  FAIL: FUSE mount not ready"; return; fi
    for i in 1 2 3; do
        eval "ip=\$N$i"
        # shellcheck disable=SC2154
        if ! writef "$ip" "data-n$i" "s6-ac$i.txt"; then FAIL=$((FAIL+1)); log "  FAIL: pre-crash write on n$i did not land"; dump_logs "$ip"; return; fi
    done
    if [[ "$MODE" == "docker" ]]; then
        docker kill -s KILL "$N1" "$N2" "$N3" "$M1" "$M2" "$M3" >/dev/null 2>&1
        sleep 3
        for pair in "$M1 $N1" "$M2 $N2" "$M3 $N3"; do
            docker start ${pair% *} >/dev/null 2>&1
        done
        sleep 3
        for pair in "$M1 $N1" "$M2 $N2" "$M3 $N3"; do
            docker start ${pair#* } >/dev/null 2>&1
        done
    else
        for i in 1 2 3; do
            eval "ip=\$N$i"
            runcmd "$ip" "sudo pkill -9 etcfuse-meta etcfuse 2>/dev/null; sudo umount -l /mnt/etcfuse 2>/dev/null; true"
        done
        sleep 3
        for i in 1 2 3; do
            eval "ip=\$N$i"
            local R=$(restart_daemons "$ip" "n$i")
            [[ "$R" == "OK" ]] || log "  WARN: n$i did not remount cleanly ($R)"
        done
    fi
    local V ALL=0
    for i in 1 2 3; do
        eval "ip=\$N$i"
        for k in $(seq 1 20); do check_mount "$ip" && break; sleep 1; done
        V=$(readf "$ip" "s6-ac$i.txt")
        [[ -n "$V" ]] && ALL=$((ALL+1))
    done
    # shellcheck disable=SC2015
    [[ "$ALL" -ge 3 ]] && { PASS=$((PASS+1)); log "  PASS: $ALL/3 survived"; } || { FAIL=$((FAIL+1)); log "  FAIL: $ALL/3"; }
}

run_s7() {
    log "======== S7: Mid-write crash ========"
    if ! check_mount "$N1"; then FAIL=$((FAIL+1)); log "  FAIL: FUSE mount not ready"; return; fi
    for f in a b c; do
        if ! writef "$N1" "wal-$f" "s7-w$f.txt"; then FAIL=$((FAIL+1)); log "  FAIL: pre-crash write w$f.txt did not land"; dump_logs "$N1"; return; fi
    done
    if [[ "$MODE" == "docker" ]]; then
        local R=$(restart_pair "$M1" "$N1")
    else
        runcmd "$N1" "sudo pkill -9 etcfuse-meta etcfuse 2>/dev/null; sudo umount -l /mnt/etcfuse 2>/dev/null; sleep 2; true"
        local R=$(restart_daemons "$N1" "n1")
    fi
    [[ "$R" == "OK" ]] || log "  WARN: n1 did not remount cleanly ($R)"
    local S=0
    for f in wa wb wc; do local V=$(readf "$N1" "s7-$f.txt"); [[ -n "$V" ]] && S=$((S+1)); done
    # shellcheck disable=SC2015
    [[ "$S" -ge 1 ]] && { PASS=$((PASS+1)); log "  PASS: $S/3 survived"; } || { FAIL=$((FAIL+1)); log "  FAIL: 0/3"; }
}

# ============================================================
# MAIN — provision ONCE, run scenarios in sequence, teardown ONCE.
# ============================================================
if ! provision_cluster; then
    log "FATAL: provision failed"
    teardown_cluster
    exit 1
fi

case "$SCENARIO" in
    1) run_s1 ;; 2) run_s2 ;; 3|4) run_s3 ;; 5) run_s5 ;; 6) run_s6 ;; 7) run_s7 ;;
    all) run_s1; run_s2; run_s3; run_s5; run_s6; run_s7 ;;
    *) log "unknown scenario: $SCENARIO" ;;
esac

teardown_cluster

{
    echo "=== Single-Cluster Chaos Test Report ($MODE) ==="
    echo "Pass: $PASS  Fail: $FAIL  Total: $((PASS+FAIL))"
    [[ "$FAIL" -eq 0 ]] && echo "STATUS: ALL PASS" || echo "STATUS: $FAIL FAILURES"
} | tee "$REPORT_DIR/summary.txt"
echo "Report: $REPORT_DIR/summary.txt"
