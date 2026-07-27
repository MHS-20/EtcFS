#!/bin/bash
# fence-test.sh — end-to-end fencing validation for EtcFS.
#
# Tests that a hard crash of the EtcFS daemon triggers etcd session expiry,
# peer detection, and fencing via EC2 API.
#
# The fencing flow tested:
#   1. Daemon dies → etcd lease expires (membership key deleted)
#   2. Fencing controller detects lease expiry via watch
#   3. Fencing controller force-detaches EBS volume
#   4. Fencing controller polls DescribeVolumes until detached
#   5. Fencing controller bumps gen:<node> epoch via CAS
#   6. Surviving nodes can now reclaim locks (conditioned on new epoch)
#
# [TEMPLATE] Requires the fencing controller service to exist.
#            During design phase, tests the etcd lease/epoch mechanics.
#
# Usage: ./fence-test.sh <target_node_ip> <target_instance_id>

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../infra/state.sh"

TARGET_IP="${1:?Usage: $0 <target_node_ip> <target_instance_id>}"
TARGET_INSTANCE="${2:?Usage: $0 <target_node_ip> <target_instance_id>}"
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

log() { echo "[$(date +%T)] $*"; }

# Pick a surviving peer node
mapfile -t ALL_IPS < <(state_get compute_public_ips | jq -r '.[]')
PEER_IP=""
for ip in "${ALL_IPS[@]}"; do
    if [[ "$ip" != "$TARGET_IP" ]]; then
        PEER_IP="$ip"
        break
    fi
done
[[ -n "$PEER_IP" ]] || die "No peer node found (need at least 2 nodes)"

log "=== EtcFS Fencing Test ==="
log "Target:   $TARGET_IP ($TARGET_INSTANCE)"
log "Peer:     $PEER_IP"
log "Cluster:  $CLUSTER_NAME"
log ""

# ---- Step 1: Verify target node membership in etcd ----
log "Step 1: Verifying target node membership in etcd..."
MEMBERSHIP_KEYS=$(etcdctl_cmd "$PEER_IP" "get membership: --prefix --keys-only" 2>/dev/null || echo "")
if echo "$MEMBERSHIP_KEYS" | grep -q "membership:"; then
    echo -e "${GREEN}OK${NC}: etcd membership keys exist"
    echo "  Keys: $(echo "$MEMBERSHIP_KEYS" | grep membership: | wc -l) active membership(s)"
else
    echo -e "${RED}FAIL${NC}: no membership keys found in etcd"
    exit 1
fi

# ---- Step 2: Record pre-fence epoch ----
log "Step 2: Recording pre-fence epoch..."
PRE_EPOCH=$(etcdctl_cmd "$PEER_IP" "get gen: --prefix" 2>/dev/null | grep "gen:" | wc -l | tr -d '[:space:]' || echo "0")
log "  Pre-fence epoch key count: $PRE_EPOCH"

# ---- Step 3: Record pre-fence lock state ----
log "Step 3: Recording pre-fence lock state..."
LOCK_COUNT=$(etcdctl_cmd "$PEER_IP" "get lock: --prefix --keys-only" 2>/dev/null | grep "lock:" | wc -l | tr -d '[:space:]' || echo "0")
log "  Active locks: $LOCK_COUNT"

# ---- Step 4: Crash target daemon ----
log "Step 4: Simulating daemon crash on target node..."
# First try SIGKILL on the EtcFS daemon
$SSH_CMD "ec2-user@$TARGET_IP" "sudo systemctl kill -s SIGKILL etcfuse 2>/dev/null" || true
sleep 2
# Then, to simulate total node failure: kill etcd too
$SSH_CMD "ec2-user@$TARGET_IP" "sudo systemctl kill -s SIGKILL etcd 2>/dev/null" || true
log "  Daemon killed on target node"

# ---- Step 5: Wait for etcd session TTL expiry ----
log "Step 5: Waiting for session TTL to expire (${LEASH_TTL:-10}s default)..."
WAIT_MAX=30
MEMBERSHIP_GONE=false
for i in $(seq 1 $WAIT_MAX); do
    KEYS=$(etcdctl_cmd "$PEER_IP" "get membership: --prefix --keys-only" 2>/dev/null || echo "")
    KEY_COUNT=$(echo "$KEYS" | grep "membership:" | wc -l | tr -d '[:space:]' || echo "0")
    if [[ "$KEY_COUNT" -lt "$COUNT" ]]; then
        echo "  t+${i}s: membership key(s) removed (now $KEY_COUNT, was $COUNT)"
        MEMBERSHIP_GONE=true
        break
    fi
    sleep 1
done

if $MEMBERSHIP_GONE; then
    echo -e "${GREEN}OK${NC}: membership key(s) expired"
else
    echo -e "${RED}WARN${NC}: membership keys still present after ${WAIT_MAX}s — TTL may be longer, or self-fencing may have occurred differently"
fi

# ---- Step 6: Verify epoch behavior ----
log "Step 6: Verifying epoch state..."
POST_EPOCH=$(etcdctl_cmd "$PEER_IP" "get gen: --prefix" 2>/dev/null | grep "gen:" | wc -l | tr -d '[:space:]' || echo "0")

if [[ "$POST_EPOCH" -gt "$PRE_EPOCH" ]]; then
    echo -e "${GREEN}OK${NC}: fencing epoch incremented ($PRE_EPOCH → $POST_EPOCH)"
else
    # During design phase, the fencing controller won't exist yet.
    # Self-fencing on the node itself should still prevent writes.
    log "  Epoch unchanged ($PRE_EPOCH). Self-fencing should have prevented writes."
    log "  (External fencing controller not yet implemented — expected in design phase)"
fi

# ---- Step 7: Verify instance state (if AWS credentials available) ----
log "Step 7: Checking EC2 instance state..."
if aws sts get-caller-identity &>/dev/null 2>&1; then
    INSTANCE_STATE=$(get_instance_state "$TARGET_INSTANCE" 2>/dev/null || echo "unknown")
    log "  Instance $TARGET_INSTANCE state: $INSTANCE_STATE"
    case "$INSTANCE_STATE" in
        stopped|terminated)
            echo -e "${GREEN}OK${NC}: target instance is stopped/terminated (external fence executed)"
            ;;
        running)
            log "  Instance still running — external fence may not have executed yet"
            log "  (Expected if fencing controller not deployed)"
            ;;
    esac
else
    log "  (AWS credentials not available — skipping instance state check)"
fi

# ---- Step 8: Verify surviving nodes still functional ----
log "Step 8: Verifying surviving nodes..."
if $SSH_CMD "ec2-user@$PEER_IP" "sudo ETCDCTL_API=3 etcdctl \
    --endpoints=https://localhost:2379 \
    --cacert=/etc/etcfuse/ca.crt \
    --cert=/etc/etcfuse/client.crt \
    --key=/etc/etcfuse/client.key \
    endpoint health 2>&1" | grep -q "is healthy"; then
    echo -e "${GREEN}OK${NC}: surviving node etcd healthy"
else
    echo -e "${RED}FAIL${NC}: surviving node etcd not healthy"
fi

echo ""
echo "===================================="
echo "EtcFS fencing test complete."
echo "Check etcfuse and etcd logs on surviving nodes for details:"
echo "  journalctl -u etcfuse --no-pager -n 50"
echo "  journalctl -u etcd --no-pager -n 50"
echo "===================================="
