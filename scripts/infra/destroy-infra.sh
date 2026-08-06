#!/bin/bash
# destroy-infra.sh — tear down all EtcFS infrastructure.
#
# Reads state from $ETCFS_STATE and destroys everything created
# by create-infra.sh.  Use --force to skip confirmation.
#
# Cleanup order:
#   1. Terminate compute instances
#   2. Detach + delete EBS volume
#   3. Delete security group
#   4. Remove state file

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/state.sh"

FORCE="${1:-}"

log "=== EtcFS Infrastructure Teardown ==="

# ---- Read state ----

VOL_ID=$(state_get volume_id 2>/dev/null || echo "")
SG_ID=$(state_get sg_id 2>/dev/null || echo "")
CLUSTER_NAME=$(state_get cluster_name 2>/dev/null || echo "")
COMPUTE_IDS=$(state_get compute_instance_ids 2>/dev/null | jq -r '.[]' 2>/dev/null || echo "")

# Nodes launched by add_node (scripts/test/chaos-lib.sh, used by every chaos
# script's elastic-join scenarios) are never written back into this state
# file's compute_instance_ids — that list is fixed at create-infra.sh time.
# A scenario that adds a node and doesn't call remove_node on it before
# teardown (or fails partway through one that does) leaves it running forever,
# invisible to the loop below, still billing. Caught this directly: a real
# 2026-08-06 AWS run left one add_node instance running after "teardown
# complete" reported success. A tag sweep is the actual fix — it finds every
# instance belonging to this cluster regardless of whether create-infra.sh
# ever knew about it, not just the ones on the original list.
EXTRA_IDS=""
if [[ -n "$CLUSTER_NAME" && "$CLUSTER_NAME" != "null" ]]; then
    ALL_TAGGED=$(aws ec2 describe-instances \
        --filters "Name=tag:ClusterName,Values=$CLUSTER_NAME" "Name=instance-state-name,Values=pending,running,stopping,stopped" \
        --query 'Reservations[].Instances[].InstanceId' --output text 2>/dev/null || echo "")
    for id in $ALL_TAGGED; do
        if ! grep -qx "$id" <<< "$COMPUTE_IDS"; then
            EXTRA_IDS="$EXTRA_IDS $id"
        fi
    done
    EXTRA_IDS="${EXTRA_IDS# }"
fi

log "Volume:        ${VOL_ID:-none}"
log "Security Grp:  ${SG_ID:-none}"
log "Nodes:         $(echo "$COMPUTE_IDS" | wc -w)"
[[ -n "$EXTRA_IDS" ]] && log "Extra nodes:   $(echo "$EXTRA_IDS" | wc -w) untracked, found by ClusterName tag ($EXTRA_IDS)"

if [[ "$FORCE" != "--force" ]]; then
    echo ""
    read -p "Destroy all EtcFS resources? [y/N] " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        die "aborted"
    fi
fi

# ---- Step 1: Terminate all instances ----

ALL_IDS="$COMPUTE_IDS $EXTRA_IDS"
if [[ -n "${ALL_IDS// }" ]]; then
    log "Terminating instances..."
    for id in $ALL_IDS; do
        [[ -z "$id" || "$id" == "null" ]] && continue
        aws ec2 terminate-instances --instance-ids "$id" 2>/dev/null || true
    done

    log "Waiting for termination..."
    for id in $ALL_IDS; do
        [[ -z "$id" || "$id" == "null" ]] && continue
        aws ec2 wait instance-terminated --instance-ids "$id" 2>/dev/null || true
    done
    log "Instances terminated"
fi

# ---- Step 2: Delete EBS volume ----

if [[ -n "$VOL_ID" && "$VOL_ID" != "null" ]]; then
    log "Waiting for volume to detach..."
    for i in $(seq 1 30); do
        STATE=$(aws ec2 describe-volumes --volume-ids "$VOL_ID" \
            --query 'Volumes[0].State' --output text 2>/dev/null || echo "unknown")
        ATTACHMENTS=$(aws ec2 describe-volumes --volume-ids "$VOL_ID" \
            --query 'Volumes[0].Attachments | length(@)' --output text 2>/dev/null || echo "0")
        if [[ "$STATE" == "available" || "$ATTACHMENTS" == "0" ]]; then
            break
        fi
        sleep 2
    done

    aws ec2 delete-volume --volume-id "$VOL_ID" 2>/dev/null || true
    log "Volume $VOL_ID deletion requested"

    for i in $(seq 1 15); do
        STATE=$(aws ec2 describe-volumes --volume-ids "$VOL_ID" \
            --query 'Volumes[0].State' --output text 2>/dev/null || echo "deleted")
        if [[ "$STATE" == "deleted" || "$STATE" == "None" || "$STATE" == "null" ]]; then
            log "Volume deleted"
            break
        fi
        sleep 2
    done
fi

# ---- Step 3: Delete security group ----

if [[ -n "$SG_ID" && "$SG_ID" != "null" ]]; then
    log "Deleting security group $SG_ID..."
    for attempt in 1 2 3; do
        if aws ec2 delete-security-group --group-id "$SG_ID" 2>/dev/null; then
            log "Security group $SG_ID deleted"
            break
        fi
        log "  SG delete attempt $attempt failed, retrying in 5s..."
        sleep 5
    done
fi

# ---- Clean up state ----

rm -f "$ETCFS_STATE"
log ""
log "=== EtcFS teardown complete ==="
