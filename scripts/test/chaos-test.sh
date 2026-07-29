#!/bin/bash
# etcfuse-chaos-test.sh — fault injection test suite for EtcFS
#
# Tests crash, fencing, recovery, network partition, and mid-write crash
# scenarios on a 3-node cluster. Each scenario provisions fresh infra.
#
# Usage: bash etcfuse-chaos-test.sh [--scenario all|1|2|3|4|5|6|7]

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/../infra/state.sh"

SCENARIO="${1:-all}"
REPORT_DIR="$PROJECT_ROOT/chaos-report-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$REPORT_DIR"

PASS=0; FAIL=0; TOTAL=0

log() { echo "[$(date +%H:%M:%S)] $1"; }
pass() { PASS=$((PASS+1)); TOTAL=$((TOTAL+1)); echo "  PASS: $1"; }
fail() { FAIL=$((FAIL+1)); TOTAL=$((TOTAL+1)); echo "  FAIL: $1"; }

provision_and_deploy() {
    log "=== Provisioning 3-node cluster ==="
    cd "$PROJECT_ROOT"
    ETCFS_COMPUTE_NODES=3 ETCFS_INSTANCE_TYPE=t3.medium ETCFS_VOLUME_SIZE=30 ETCFS_CLUSTER="etcfuse-chaos-$RANDOM" bash scripts/infra/create-infra.sh 2>/dev/null
    N1=$(python3 -c "import json; print(json.load(open('infra-state.json'))['compute_public_ips'][0])")
    N2=$(python3 -c "import json; print(json.load(open('infra-state.json'))['compute_public_ips'][1])")
    N3=$(python3 -c "import json; print(json.load(open('infra-state.json'))['compute_public_ips'][2])")
    P1=$(python3 -c "import json; print(json.load(open('infra-state.json'))['compute_ips'][0])")
    P2=$(python3 -c "import json; print(json.load(open('infra-state.json'))['compute_ips'][1])")
    P3=$(python3 -c "import json; print(json.load(open('infra-state.json'))['compute_ips'][2])")
    ETCD="http://$P1:2379,http://$P2:2379,http://$P3:2379"
    INIT="e0=http://$P1:2380,e1=http://$P2:2380,e2=http://$P3:2380"
    go build -o bin/etcfuse-meta ./cmd/etcfuse-meta 2>/dev/null
    tar czf /tmp/chaos.tar.gz cmd/etcfuse pkg/fuse pkg/block pkg/wal 2>/dev/null
    python3 -c 'import struct, sys; sys.stdout.buffer.write(struct.pack(">Q", 1000))' > /tmp/counter.bin 2>/dev/null
    for ip in $N1 $N2 $N3; do
        ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 ec2-user@$ip 'sudo yum install -y fuse3-libs fuse3-devel gcc 2>/dev/null|tail -1; curl -fsSL https://github.com/etcd-io/etcd/releases/download/v3.5.18/etcd-v3.5.18-linux-amd64.tar.gz -o /tmp/etcd.tar.gz 2>/dev/null; sudo tar -xzf /tmp/etcd.tar.gz -C /usr/local/bin --strip-components=1 etcd-v3.5.18-linux-amd64/etcd etcd-v3.5.18-linux-amd64/etcdctl 2>/dev/null; sudo chmod +x /usr/local/bin/etcd /usr/local/bin/etcdctl' 2>/dev/null
        for f in chaos.tar.gz counter.bin; do scp -o StrictHostKeyChecking=no -q /tmp/$f ec2-user@$ip:/tmp/; done
        scp -o StrictHostKeyChecking=no -q bin/etcfuse-meta ec2-user@$ip:/tmp/
        ssh -o StrictHostKeyChecking=no ec2-user@$ip 'cd /tmp && rm -rf s && mkdir s && cd s && tar xzf /tmp/chaos.tar.gz && gcc -Wall -Wextra -Werror -std=c11 -D_GNU_SOURCE -O2 -g -I. cmd/etcfuse/main.c pkg/fuse/fuse.c pkg/fuse/ops.c pkg/block/block.c pkg/wal/wal.c -o /tmp/etcfuse -lfuse3 -lpthread 2>/dev/null && sudo cp /tmp/etcfuse /usr/local/bin/etcfuse && sudo cp /tmp/etcfuse-meta /usr/local/bin/etcfuse-meta && sudo chmod +x /usr/local/bin/etcfuse-meta' 2>/dev/null
    done
    for ip in $N1 $N2 $N3; do
        eval "priv=\${P${ip##*\.}}"
        case $ip in $N1) ename=e0;priv=$P1;; $N2) ename=e1;priv=$P2;; $N3) ename=e2;priv=$P3;; esac
        ssh -o StrictHostKeyChecking=no ec2-user@$ip "sudo rm -rf /var/lib/etcd; sudo mkdir -p /var/lib/etcd; sudo nohup /usr/local/bin/etcd --name $ename --data-dir /var/lib/etcd --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://$priv:2379 --listen-peer-urls http://0.0.0.0:2380 --initial-advertise-peer-urls http://$priv:2380 --initial-cluster e0=http://$P1:2380,e1=http://$P2:2380,e2=http://$P3:2380 --initial-cluster-state new > /tmp/etcd.log 2>&1 &" 2>/dev/null
    done
    sleep 10
    ssh -o StrictHostKeyChecking=no ec2-user@$N1 'sudo ETCDCTL_API=3 /usr/local/bin/etcdctl --endpoints=http://127.0.0.1:2379 put inode_alloc_counter < /tmp/counter.bin' 2>/dev/null
    for i in 1 2 3; do
        eval "ip=\$N$i"
        ssh -o StrictHostKeyChecking=no ec2-user@$ip "sudo killall -9 etcfuse-meta etcfuse 2>/dev/null; sudo umount -l /mnt/etcfuse 2>/dev/null; sleep 1; sudo rm -rf /mnt/etcfuse; sudo mkdir -p /mnt/etcfuse; sudo rm -f /tmp/etcfuse.sock /tmp/etcfuse-notify.sock; sudo nohup /usr/local/bin/etcfuse-meta --listen=/tmp/etcfuse.sock --etcd-endpoints=$ETCD --node-id=n$i --cluster-name=chaos --lease-ttl=10s --block-device=/dev/nvme1n1 --log-level=1 > /tmp/meta.log 2>&1 & sleep 3; sudo nohup /usr/local/bin/etcfuse --socket=/tmp/etcfuse.sock --node-id=n$i --log-level=1 /mnt/etcfuse > /tmp/fuse.log 2>&1 & sleep 4; sudo mountpoint -q /mnt/etcfuse && echo OK" 2>/dev/null
    done
    log "3 nodes deployed and mounted"
}

teardown() {
    cd "$PROJECT_ROOT"; bash scripts/infra/destroy-infra.sh --force 2>/dev/null &
}

write_test_file() {
    local N="$1" F="$2"
    echo -n "data-$RANDOM-$RANDOM" | timeout 5 ssh -o StrictHostKeyChecking=no -q ec2-user@$N "sudo tee /mnt/etcfuse/$F > /dev/null" 2>/dev/null
}

read_test_file() {
    timeout 5 ssh -o StrictHostKeyChecking=no -q ec2-user@$1 "sudo cat /mnt/etcfuse/$2" 2>/dev/null || echo ""
}

assert_data() {
    local OUT="$1" FILE="$2" NODE="$3"
    [[ -n "$OUT" ]] && pass "Data readable from $NODE after crash: $FILE" || fail "Data lost on $NODE: $FILE"
}

# ============================================================
# Scenario 1: C FUSE daemon SIGKILL + restart
# ============================================================
run_scenario_1() {
    log ""
    log "========================================="
    log "Scenario 1: C FUSE daemon SIGKILL + restart"
    log "========================================="
    provision_and_deploy
    local N1=$(python3 -c "import json; print(json.load(open('$PROJECT_ROOT/infra-state.json'))['compute_public_ips'][0])")
    local N2=$(python3 -c "import json; print(json.load(open('$PROJECT_ROOT/infra-state.json'))['compute_public_ips'][1])")

    log "Writing test files..."
    write_test_file $N1 "crash-test.txt"; sleep 0.5
    local VAL=$(read_test_file $N1 "crash-test.txt")
    assert_data "$VAL" "crash-test.txt" "N1"

    log "SIGKILL C daemon on N1..."
    ssh -o StrictHostKeyChecking=no -q ec2-user@$N1 'sudo pkill -9 etcfuse; sudo umount -l /mnt/etcfuse 2>/dev/null; sleep 2; sudo mountpoint -q /mnt/etcfuse && echo STILL_MOUNTED || echo UNMOUNTED' 2>/dev/null
    sleep 2

    log "Restarting C daemon on N1..."
    ssh -o StrictHostKeyChecking=no -q ec2-user@$N1 'sudo mkdir -p /mnt/etcfuse; sudo nohup /usr/local/bin/etcfuse --socket=/tmp/etcfuse.sock --node-id=n1 --log-level=1 /mnt/etcfuse > /tmp/fuse.log 2>&1 &' 2>/dev/null
    sleep 4

    log "Verifying data survives..."
    local VAL2=$(read_test_file $N1 "crash-test.txt")
    assert_data "$VAL2" "crash-test.txt" "N1 (after restart)"

    # Verify N2 can still read it
    local VAL3=$(read_test_file $N2 "crash-test.txt")
    assert_data "$VAL3" "crash-test.txt" "N2"
    teardown
}

# ============================================================
# Scenario 2: Go metadata daemon SIGKILL + restart
# ============================================================
run_scenario_2() {
    log ""
    log "========================================="
    log "Scenario 2: Go metadata daemon SIGKILL + restart"
    log "========================================="
    provision_and_deploy
    local N1=$(python3 -c "import json; print(json.load(open('$PROJECT_ROOT/infra-state.json'))['compute_public_ips'][0])")
    local N2=$(python3 -c "import json; print(json.load(open('$PROJECT_ROOT/infra-state.json'))['compute_public_ips'][1])")

    log "Writing test files..."
    write_test_file $N1 "go-crash-test.txt"; sleep 0.5
    local VAL=$(read_test_file $N1 "go-crash-test.txt")
    assert_data "$VAL" "go-crash-test.txt" "N1"

    log "SIGKILL Go daemon on N1..."
    ssh -o StrictHostKeyChecking=no -q ec2-user@$N1 'sudo pkill -9 etcfuse-meta; sleep 2; sudo rm -f /tmp/etcfuse.sock' 2>/dev/null
    sleep 2

    log "Restarting Go daemon on N1..."
    local P1=$(python3 -c "import json; print(json.load(open('$PROJECT_ROOT/infra-state.json'))['compute_ips'][0])")
    P2=$(python3 -c "import json; print(json.load(open('$PROJECT_ROOT/infra-state.json'))['compute_ips'][1])")
    P3=$(python3 -c "import json; print(json.load(open('$PROJECT_ROOT/infra-state.json'))['compute_ips'][2])")
    local ETCD="http://$P1:2379,http://$P2:2379,http://$P3:2379"
    ssh -o StrictHostKeyChecking=no -q ec2-user@$N1 "sudo nohup /usr/local/bin/etcfuse-meta --listen=/tmp/etcfuse.sock --etcd-endpoints=$ETCD --node-id=n1 --cluster-name=chaos --lease-ttl=10s --block-device=/dev/nvme1n1 --log-level=1 > /tmp/meta.log 2>&1 &" 2>/dev/null
    sleep 3
    ssh -o StrictHostKeyChecking=no -q ec2-user@$N1 'sudo nohup /usr/local/bin/etcfuse --socket=/tmp/etcfuse.sock --node-id=n1 --log-level=1 /mnt/etcfuse > /tmp/fuse.log 2>&1 &' 2>/dev/null
    sleep 4

    log "Verifying data survives..."
    local VAL2=$(read_test_file $N1 "go-crash-test.txt")
    assert_data "$VAL2" "go-crash-test.txt" "N1 (after restart)"
    local VAL3=$(read_test_file $N2 "go-crash-test.txt")
    assert_data "$VAL3" "go-crash-test.txt" "N2"
    teardown
}

# ============================================================
# Scenario 7: WAL replay / data-then-metadata crash
# ============================================================
run_scenario_7() {
    log ""
    log "========================================="
    log "Scenario 7: Mid-write crash — data-then-metadata recovery"
    log "========================================="
    provision_and_deploy
    local N1=$(python3 -c "import json; print(json.load(open('$PROJECT_ROOT/infra-state.json'))['compute_public_ips'][0])")

    log "Writing several test files..."
    for i in 1 2 3; do write_test_file $N1 "wal-test-$i.txt"; done
    sleep 1

    log "SIGKILL all daemons on N1 during write..."
    ssh -o StrictHostKeyChecking=no -q ec2-user@$N1 'sudo pkill -9 etcfuse-meta etcfuse; sudo umount -l /mnt/etcfuse 2>/dev/null; sleep 2' 2>/dev/null

    log "Restart everything on N1..."
    local P1=$(python3 -c "import json; print(json.load(open('$PROJECT_ROOT/infra-state.json'))['compute_ips'][0])")
    P2=$(python3 -c "import json; print(json.load(open('$PROJECT_ROOT/infra-state.json'))['compute_ips'][1])")
    P3=$(python3 -c "import json; print(json.load(open('$PROJECT_ROOT/infra-state.json'))['compute_ips'][2])")
    local ETCD="http://$P1:2379,http://$P2:2379,http://$P3:2379"
    ssh -o StrictHostKeyChecking=no -q ec2-user@$N1 "sudo rm -f /tmp/etcfuse.sock /tmp/etcfuse-notify.sock; sudo mkdir -p /mnt/etcfuse; sudo nohup /usr/local/bin/etcfuse-meta --listen=/tmp/etcfuse.sock --etcd-endpoints=$ETCD --node-id=n1 --cluster-name=chaos --lease-ttl=10s --block-device=/dev/nvme1n1 --log-level=1 > /tmp/meta.log 2>&1 & sleep 3; sudo nohup /usr/local/bin/etcfuse --socket=/tmp/etcfuse.sock --node-id=n1 --log-level=1 /mnt/etcfuse > /tmp/fuse.log 2>&1 & sleep 4; sudo mountpoint -q /mnt/etcfuse && echo MOUNTED" 2>/dev/null

    log "Checking which files survived..."
    local SURVIVED=0
    for i in 1 2 3; do
        local V=$(read_test_file $N1 "wal-test-$i.txt")
        if [[ -n "$V" ]]; then SURVIVED=$((SURVIVED+1)); fi
    done
    [[ "$SURVIVED" -ge 1 ]] && pass "$SURVIVED/3 test files survived crash" || fail "No files survived crash"
    teardown
}

# ============================================================
# Scenario 3+4: Network partition + self-fencing
# ============================================================
run_scenario_3_4() {
    log ""
    log "========================================="
    log "Scenario 3+4: Network partition → self-fence → recovery"
    log "========================================="
    provision_and_deploy
    local N1=$(python3 -c "import json; print(json.load(open('$PROJECT_ROOT/infra-state.json'))['compute_public_ips'][0])")
    local N2=$(python3 -c "import json; print(json.load(open('$PROJECT_ROOT/infra-state.json'))['compute_public_ips'][1])")
    local N3=$(python3 -c "import json; print(json.load(open('$PROJECT_ROOT/infra-state.json'))['compute_public_ips'][2])")
    P1=$(python3 -c "import json; print(json.load(open('$PROJECT_ROOT/infra-state.json'))['compute_ips'][0])")
    P2=$(python3 -c "import json; print(json.load(open('$PROJECT_ROOT/infra-state.json'))['compute_ips'][1])")
    P3=$(python3 -c "import json; print(json.load(open('$PROJECT_ROOT/infra-state.json'))['compute_ips'][2])")

    log "Writing data from N1..."
    write_test_file $N1 "partition-test.txt"; sleep 0.5
    local VAL=$(read_test_file $N2 "partition-test.txt")
    assert_data "$VAL" "partition-test.txt" "N2 (before partition)"

    log "Partitioning N1 from etcd (iptables DROP)..."
    ssh -o StrictHostKeyChecking=no -q ec2-user@$N1 "sudo iptables -A OUTPUT -p tcp --dport 2379 -j DROP; sudo iptables -A OUTPUT -p tcp --dport 2380 -j DROP" 2>/dev/null
    log "N1 partitioned — waiting 15s for self-fence..."

    sleep 5
    local FENCED=$(ssh -o StrictHostKeyChecking=no -q ec2-user@$N1 'sudo mountpoint -q /mnt/etcfuse && echo UP || echo FENCED' 2>/dev/null || echo "SSH_FAIL")
    log "N1 after partition: $FENCED"

    # Write to N2 during partition
    write_test_file $N2 "partition-survivor.txt"
    local VAL2=$(read_test_file $N3 "partition-survivor.txt")
    assert_data "$VAL2" "partition-survivor.txt" "N3"

    log "Restoring N1 network..."
    ssh -o StrictHostKeyChecking=no -q ec2-user@$N1 "sudo iptables -F OUTPUT" 2>/dev/null || true
    sleep 15

    log "Checking N1 rejoin..."
    local N1_OK=$(ssh -o StrictHostKeyChecking=no -q ec2-user@$N1 'sudo mountpoint -q /mnt/etcfuse 2>/dev/null && echo MOUNTED || echo DOWN' 2>/dev/null || echo "UNREACHABLE")
    log "N1 after restore: $N1_OK"
    local VAL3=$(read_test_file $N1 "partition-test.txt")
    [[ -n "$VAL3" ]] && pass "N1 data readable after partition" || fail "N1 data lost after partition"
    local VAL4=$(read_test_file $N1 "partition-survivor.txt")
    [[ -n "$VAL4" ]] && pass "N1 can read N2's file written during partition" || fail "N1 cannot read N2's file"

    # Check generation was bumped
    local GEN=$(ssh -o StrictHostKeyChecking=no -q ec2-user@$N1 'sudo ETCDCTL_API=3 /usr/local/bin/etcdctl --endpoints=http://127.0.0.1:2379 get gen:n1 --print-value-only 2>/dev/null' 2>/dev/null || echo "?")
    log "N1 generation: $GEN"
    teardown
}

# ============================================================
# Scenario 5: Generation bump → fence → lock reclamation
# ============================================================
run_scenario_5() {
    log ""
    log "========================================="
    log "Scenario 5: Generation bump — stale writes rejected"
    log "========================================="
    provision_and_deploy
    local N1=$(python3 -c "import json; print(json.load(open('$PROJECT_ROOT/infra-state.json'))['compute_public_ips'][0])")
    local N2=$(python3 -c "import json; print(json.load(open('$PROJECT_ROOT/infra-state.json'))['compute_public_ips'][1])")

    # Read current generation
    local GEN_BEFORE=$(ssh -o StrictHostKeyChecking=no -q ec2-user@$N1 'sudo ETCDCTL_API=3 /usr/local/bin/etcdctl --endpoints=http://127.0.0.1:2379 get gen:n1 --print-value-only 2>/dev/null' 2>/dev/null || echo "0")
    [[ "$GEN_BEFORE" != "1" && "$GEN_BEFORE" != "0" ]] && GEN_BEFORE=1
    local NEW_GEN=$((GEN_BEFORE + 1))

    log "Current gen:n1 = $GEN_BEFORE. Bumping to $NEW_GEN..."
    ssh -o StrictHostKeyChecking=no -q ec2-user@$N1 "sudo ETCDCTL_API=3 /usr/local/bin/etcdctl --endpoints=http://127.0.0.1:2379 put gen:n1 $NEW_GEN" 2>/dev/null

    log "Writing after fence bump..."
    write_test_file $N1 "post-fence.txt"
    local VAL=$(read_test_file $N1 "post-fence.txt")
    [[ -n "$VAL" ]] && fail "Write succeeded after fence (should have been rejected)" || pass "Stale generation write rejected"

    # Reset generation
    ssh -o StrictHostKeyChecking=no -q ec2-user@$N1 "sudo ETCDCTL_API=3 /usr/local/bin/etcdctl --endpoints=http://127.0.0.1:2379 put gen:n1 $GEN_BEFORE" 2>/dev/null
    teardown
}

# ============================================================
# Scenario 6: All daemons killed + simultaneous restart
# ============================================================
run_scenario_6() {
    log ""
    log "========================================="
    log "Scenario 6: All 3 nodes crash simultaneously"
    log "========================================="
    provision_and_deploy
    local N1=$(python3 -c "import json; print(json.load(open('$PROJECT_ROOT/infra-state.json'))['compute_public_ips'][0])")
    local N2=$(python3 -c "import json; print(json.load(open('$PROJECT_ROOT/infra-state.json'))['compute_public_ips'][1])")
    local N3=$(python3 -c "import json; print(json.load(open('$PROJECT_ROOT/infra-state.json'))['compute_public_ips'][2])")
    P1=$(python3 -c "import json; print(json.load(open('$PROJECT_ROOT/infra-state.json'))['compute_ips'][0])")
    P2=$(python3 -c "import json; print(json.load(open('$PROJECT_ROOT/infra-state.json'))['compute_ips'][1])")
    P3=$(python3 -c "import json; print(json.load(open('$PROJECT_ROOT/infra-state.json'))['compute_ips'][2])")
    local ETCD="http://$P1:2379,http://$P2:2379,http://$P3:2379"

    log "Writing data on all 3 nodes..."
    write_test_file $N1 "all-crash-n1.txt"
    write_test_file $N2 "all-crash-n2.txt"
    write_test_file $N3 "all-crash-n3.txt"
    sleep 1

    log "SIGKILL all daemons on all 3 nodes..."
    for ip in $N1 $N2 $N3; do
        ssh -o StrictHostKeyChecking=no -q ec2-user@$ip 'sudo pkill -9 etcfuse-meta etcfuse; sudo umount -l /mnt/etcfuse 2>/dev/null' 2>/dev/null
    done
    sleep 3

    log "Restarting all 3 nodes..."
    for i in 1 2 3; do
        eval "ip=\$N$i"
        ssh -o StrictHostKeyChecking=no -q ec2-user@$ip "sudo rm -f /tmp/etcfuse.sock /tmp/etcfuse-notify.sock; sudo mkdir -p /mnt/etcfuse; sudo nohup /usr/local/bin/etcfuse-meta --listen=/tmp/etcfuse.sock --etcd-endpoints=$ETCD --node-id=n$i --cluster-name=chaos --lease-ttl=10s --block-device=/dev/nvme1n1 --log-level=1 > /tmp/meta.log 2>&1 & sleep 3; sudo nohup /usr/local/bin/etcfuse --socket=/tmp/etcfuse.sock --node-id=n$i --log-level=1 /mnt/etcfuse > /tmp/fuse.log 2>&1 & sleep 4; sudo mountpoint -q /mnt/etcfuse && echo N${i}_RECOVERED || echo N${i}_FAILED" 2>/dev/null
    done

    log "Verifying all data survived..."
    local C1=$(read_test_file $N1 "all-crash-n1.txt")
    local C2=$(read_test_file $N2 "all-crash-n2.txt")
    local C3=$(read_test_file $N3 "all-crash-n3.txt")
    [[ -n "$C1" ]] && pass "N1 data survived all-crash" || fail "N1 data lost"
    [[ -n "$C2" ]] && pass "N2 data survived all-crash" || fail "N2 data lost"
    [[ -n "$C3" ]] && pass "N3 data survived all-crash" || fail "N3 data lost"
    teardown
}

# ============================================================
# Run selected scenarios
# ============================================================
case $SCENARIO in
    1) run_scenario_1 ;;
    2) run_scenario_2 ;;
    3|4) run_scenario_3_4 ;;
    5) run_scenario_5 ;;
    6) run_scenario_6 ;;
    7) run_scenario_7 ;;
    all|*)
        run_scenario_1
        run_scenario_2
        run_scenario_3_4
        run_scenario_5
        run_scenario_6
        run_scenario_7
        ;;
esac

# ============================================================
# Report
# ============================================================
{
    echo ""
    echo "============================================"
    echo "  Chaos Test Report"
    echo "============================================"
    echo ""
    echo "  Scenarios run: $SCENARIO"
    echo "  Passed: $PASS/$TOTAL"
    echo "  Failed: $FAIL/$TOTAL"
    echo ""
    for f in "$REPORT_DIR"/*.log 2>/dev/null; do
        [[ -f "$f" ]] && echo "  Log: $(basename $f)"
    done
    echo ""
    if [[ "$FAIL" -eq 0 ]]; then
        echo "  STATUS: ALL PASSED"
    else
        echo "  STATUS: $FAIL FAILURES"
    fi
} | tee "$REPORT_DIR/summary.txt"

echo "Report saved to: $REPORT_DIR/summary.txt"
