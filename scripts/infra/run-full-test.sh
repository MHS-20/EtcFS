#!/bin/bash
# run-full-test.sh — end-to-end validation of an EtcFS cluster.
#
# Runs a battery of tests across all compute nodes after setup-compute.sh.
# Tests POSIX filesystem operations through the FUSE mount, etcd health,
# and cross-node visibility.  No block-device-level checks yet — those are
# still future work.
#
# Usage:
#   ./run-full-test.sh                    # run all tests
#   ./run-full-test.sh --skip-fs          # skip filesystem tests (no daemon binary yet)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/state.sh"

mapfile -t PUB_IPS < <(state_get compute_public_ips 2>/dev/null | jq -r '.[]' 2>/dev/null)
mapfile -t PRIV_IPS < <(state_get compute_ips 2>/dev/null | jq -r '.[]' 2>/dev/null)
COUNT=${#PUB_IPS[@]}

if [[ "$COUNT" -lt 1 ]]; then
    die "No compute nodes in state. Run create-infra.sh and setup-compute.sh first."
fi

SKIP_FS=false
for arg in "$@"; do
    case "$arg" in --skip-fs) SKIP_FS=true ;; esac
done

TESTDIR="/mnt/etcfuse/run-full-test-$$"
PASS=0; FAIL=0

# Arithmetic assignment, not post-increment: under `set -e`, `((PASS++))`
# evaluates to the *old* value as its exit status, and `((0))` is false — so
# incrementing PASS from 0 on the very first pass() call killed the whole
# script right there. This ran to completion for the first time only once
# that was fixed.
pass() { echo "  PASS: $1"; PASS=$((PASS + 1)); }
fail() { echo "  FAIL: $1"; FAIL=$((FAIL + 1)); }

log "=== EtcFS Full Validation ==="
log "Nodes:     $COUNT"
log "Mount:     $FUSE_MOUNTPOINT"
log ""

# ---- Test 1: SSH reachable on all nodes ----
log "--- Test 1: SSH reachable on all $COUNT nodes ---"
for ip in "${PUB_IPS[@]}"; do
    if ssh $SSH_OPTS "ec2-user@$ip" "echo ok" &>/dev/null; then
        pass "SSH to $ip"
    else
        fail "SSH to $ip"
    fi
done

# ---- Test 2: etcd installed on all nodes ----
log "--- Test 2: etcd installed on all nodes ---"
for ip in "${PUB_IPS[@]}"; do
    if $SSH_CMD "ec2-user@$ip" "test -f /usr/local/bin/etcd" 2>/dev/null; then
        pass "etcd binary on $ip"
    else
        fail "etcd binary on $ip"
    fi
done

# ---- Test 3: etcd cluster health ----
log "--- Test 3: etcd cluster health ---"
FIRST_IP="${PUB_IPS[0]}"
HEALTHY=$(etcdctl_cmd "$FIRST_IP" "endpoint health --cluster" 2>/dev/null | grep -c "is healthy" || echo "0")
if [[ "$HEALTHY" -eq "$COUNT" ]]; then
    pass "etcd cluster: all $COUNT members healthy"
else
    fail "etcd cluster: got $HEALTHY healthy, expected $COUNT"
    etcdctl_cmd "$FIRST_IP" "endpoint health --cluster" || true
fi

# ---- Test 4: etcd membership count ----
log "--- Test 4: etcd membership count ---"
MEMBER_COUNT=$(etcdctl_cmd "$FIRST_IP" "member list" 2>/dev/null | wc -l)
if [[ "$MEMBER_COUNT" -ge "$COUNT" ]]; then
    pass "etcd members: $MEMBER_COUNT (>= $COUNT nodes)"
else
    fail "etcd members: $MEMBER_COUNT (< $COUNT nodes)"
fi

# ---- Tests 5-8: FUSE filesystem tests ----
if $SKIP_FS; then
    log "--- Filesystem tests SKIPPED (--skip-fs) ---"
else
    # ---- Test 5: FUSE mount on all nodes ----
    log "--- Test 5: FUSE mount present on all nodes ---"
    for ip in "${PUB_IPS[@]}"; do
        if $SSH_CMD "ec2-user@$ip" "mountpoint -q $FUSE_MOUNTPOINT 2>/dev/null" 2>/dev/null; then
            pass "FUSE mounted on $ip"
        else
            fail "FUSE mounted on $ip (daemon may not be running yet — this is expected in design phase)"
        fi
    done

    # ---- Test 6: Single-node file write & read ----
    log "--- Test 6: Single-node file write & read ---"
    N0="${PUB_IPS[0]}"
    if $SSH_CMD "ec2-user@$N0" "mountpoint -q $FUSE_MOUNTPOINT 2>/dev/null" 2>/dev/null; then
        rm -rf "$TESTDIR"
        $SSH_CMD "ec2-user@$N0" "
set -e
sudo mkdir -p $TESTDIR
echo 'hello-from-node0' | sudo tee $TESTDIR/write-test.txt > /dev/null
"
        CONTENT=$($SSH_CMD "ec2-user@$N0" "sudo cat $TESTDIR/write-test.txt" 2>/dev/null || echo "")
        if [[ "$CONTENT" == "hello-from-node0" ]]; then
            pass "single-node write & read"
        else
            fail "single-node write & read (got: '$CONTENT')"
        fi
    else
        fail "single-node write — mount not available on node0"
    fi

    # ---- Test 7: Cross-node visibility ----
    log "--- Test 7: Cross-node write/read visibility ---"
    if [[ "$COUNT" -ge 2 ]]; then
        N1="${PUB_IPS[1]}"
        N0_MOUNTED=$($SSH_CMD "ec2-user@$N0" "mountpoint -q $FUSE_MOUNTPOINT && echo yes || echo no" 2>/dev/null)
        N1_MOUNTED=$($SSH_CMD "ec2-user@$N1" "mountpoint -q $FUSE_MOUNTPOINT && echo yes || echo no" 2>/dev/null)
        if [[ "$N0_MOUNTED" == "yes" && "$N1_MOUNTED" == "yes" ]]; then
            $SSH_CMD "ec2-user@$N0" "echo 'cross-node-test' | sudo tee ${TESTDIR}/cross-node.txt > /dev/null"
            # Force cache invalidation by using a fresh stat
            sleep 2
            N1_CONTENT=$($SSH_CMD "ec2-user@$N1" "sudo cat ${TESTDIR}/cross-node.txt" 2>/dev/null || echo "")
            if [[ "$N1_CONTENT" == "cross-node-test" ]]; then
                pass "cross-node write (node0→node1 read)"
            else
                fail "cross-node write (node1 sees: '$N1_CONTENT')"
            fi
        else
            fail "cross-node visibility — mounts not available on both nodes"
        fi
    else
        log "  (skipped — need >= 2 nodes with FUSE mounts)"
    fi

    # ---- Test 8: Concurrent file creation (unique names, no locking) ----
    log "--- Test 8: Concurrent file creation ---"
    if [[ "$COUNT" -ge 2 ]]; then
        CONCUR_DIR="${TESTDIR}/concurrent-$$"
        $SSH_CMD "ec2-user@$N0" "sudo mkdir -p $CONCUR_DIR" 2>/dev/null
        # Each node creates a uniquely-named file
        for i in "${!PUB_IPS[@]}"; do
            $SSH_CMD "ec2-user@${PUB_IPS[$i]}" \
                "echo 'node$i' | sudo tee ${CONCUR_DIR}/from-node$i.txt > /dev/null" 2>/dev/null || true &
        done
        wait
        sleep 2
        LISTING=$($SSH_CMD "ec2-user@$N0" "sudo ls $CONCUR_DIR" 2>/dev/null || echo "")
        LIST_COUNT=$(echo "$LISTING" | wc -w)
        if [[ "$LIST_COUNT" -ge "$COUNT" ]]; then
            pass "concurrent creates: $LIST_COUNT files visible ($COUNT expected)"
        else
            fail "concurrent creates: only $LIST_COUNT/$COUNT files visible"
        fi
    else
        log "  (skipped — need >= 2 nodes)"
    fi

    # ---- Test 9: Directory listing consistency ----
    log "--- Test 9: Directory listing consistency ---"
    LIST0=$($SSH_CMD "ec2-user@$N0" "sudo ls $FUSE_MOUNTPOINT 2>/dev/null | sort" 2>/dev/null || echo "")
    if [[ "$COUNT" -ge 2 ]]; then
        N1_MOUNTED=$($SSH_CMD "ec2-user@$N1" "mountpoint -q $FUSE_MOUNTPOINT && echo yes || echo no" 2>/dev/null)
        if [[ "$N1_MOUNTED" == "yes" ]]; then
            LIST1=$($SSH_CMD "ec2-user@$N1" "sudo ls $FUSE_MOUNTPOINT 2>/dev/null | sort" 2>/dev/null || echo "")
            if [[ "$LIST0" == "$LIST1" ]]; then
                pass "directory listing consistent across nodes"
            else
                fail "directory listing inconsistent (node0: '$LIST0' vs node1: '$LIST1')"
            fi
        fi
    fi

    # ---- Test 10: EtcFS daemon restart ----
    # No systemd here (see bootstrap-cluster.sh): both daemons are nohup'd
    # background processes, so "restart" means kill them and start them
    # again over SSH, the same way bootstrap-cluster.sh started them.
    log "--- Test 10: EtcFS daemon restart survival ---"
    TARGET="${PUB_IPS[0]}"
    TARGET_NODE_ID="n1"
    ENDPOINTS=""
    for p in "${PRIV_IPS[@]}"; do
        [[ -n "$ENDPOINTS" ]] && ENDPOINTS+=","
        ENDPOINTS+="http://$p:2379"
    done
    VOL_ID=$(state_get volume_id 2>/dev/null | tr -d '"')
    CLUSTER_NAME=$(state_get cluster_name 2>/dev/null | tr -d '"')
    $SSH_CMD "ec2-user@$TARGET" "
        sudo killall -9 etcfuse-meta etcfuse 2>/dev/null; sudo umount -l $FUSE_MOUNTPOINT 2>/dev/null; sleep 1
        sudo rm -f /run/etcfuse/etcfuse.sock /run/etcfuse/etcfuse-notify.sock
        sudo nohup /usr/local/bin/etcfuse-meta --listen=/run/etcfuse/etcfuse.sock \
            --etcd-endpoints=$ENDPOINTS --node-id=$TARGET_NODE_ID --cluster-name=$CLUSTER_NAME \
            --block-device=/dev/nvme1n1 --log-level=1 > /tmp/meta.log 2>&1 &
        sleep 4
        sudo nohup /usr/local/bin/etcfuse --socket=/run/etcfuse/etcfuse.sock \
            --node-id=$TARGET_NODE_ID --log-level=1 $FUSE_MOUNTPOINT > /tmp/fuse.log 2>&1 &
    " 2>/dev/null || true
    sleep 8
    if $SSH_CMD "ec2-user@$TARGET" "mountpoint -q $FUSE_MOUNTPOINT 2>/dev/null" 2>/dev/null; then
        pass "daemon restarted, mount survived"
    else
        fail "daemon restart — mount did not come back"
    fi

    # Cleanup
    $SSH_CMD "ec2-user@$N0" "sudo rm -rf $TESTDIR" 2>/dev/null || true
fi

# ---- Summary ----
log ""
log "============================================"
log "EtcFS test suite complete"
log "  Passed: $PASS"
log "  Failed: $FAIL"
log "============================================"

exit $FAIL
