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
COMPUTE_IDS=$(state_get compute_instance_ids 2>/dev/null | jq -r '.[]' 2>/dev/null || echo "")

log "Volume:        ${VOL_ID:-none}"
log "Security Grp:  ${SG_ID:-none}"
log "Nodes:         $(echo "$COMPUTE_IDS" | wc -w)"

if [[ "$FORCE" != "--force" ]]; then
    echo ""
    read -p "Destroy all EtcFS resources? [y/N] " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        die "aborted"
    fi
fi

# ---- Step 1: Terminate all instances ----

if [[ -n "$COMPUTE_IDS" ]]; then
    log "Terminating instances..."
    for id in $COMPUTE_IDS; do
        [[ -z "$id" || "$id" == "null" ]] && continue
        aws ec2 terminate-instances --instance-ids "$id" 2>/dev/null || true
    done

    log "Waiting for termination..."
    for id in $COMPUTE_IDS; do
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
