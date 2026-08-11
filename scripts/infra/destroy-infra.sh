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
#   4. Remove state file — only if every step above was confirmed, not just
#      attempted; otherwise the state file is kept and the script exits 1,
#      so a re-run has something to retry against instead of an orphaned
#      resource nothing knows about anymore.

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
BENCH_VOL_ID=$(state_get bench_scratch_volume_id 2>/dev/null | grep -v '^null$' || echo "")
BENCH_EFS_ID=$(state_get bench_efs_id 2>/dev/null | grep -v '^null$' || echo "")

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
#
# FAILED tracks every step that didn't confirm success. Every prior version
# of this loop logged "deletion requested" / "deleted" unconditionally and
# fell through to "teardown complete" on a timeout with no error at all —
# the volume (and the security group, below) kept billing/existing while the
# operator believed teardown had succeeded. Observed directly: a 2026-08-09
# run left both an "available" volume and a security group behind, silently.

FAILED=""

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

    VOL_DELETED=false
    for attempt in 1 2 3; do
        if aws ec2 delete-volume --volume-id "$VOL_ID" 2>/dev/null; then
            log "Volume $VOL_ID deletion requested"
            for i in $(seq 1 15); do
                STATE=$(aws ec2 describe-volumes --volume-ids "$VOL_ID" \
                    --query 'Volumes[0].State' --output text 2>/dev/null || echo "deleted")
                if [[ "$STATE" == "deleted" || "$STATE" == "None" || "$STATE" == "null" ]]; then
                    log "Volume deleted"
                    VOL_DELETED=true
                    break
                fi
                sleep 2
            done
            [[ "$VOL_DELETED" == "true" ]] && break
            log "  Volume delete requested but never confirmed deleted (still: $STATE), retrying..."
        else
            log "  Volume delete attempt $attempt failed (often means still attached), retrying in 5s..."
            sleep 5
        fi
    done
    if [[ "$VOL_DELETED" != "true" ]]; then
        log "ERROR: volume $VOL_ID was not confirmed deleted — still billing, check manually"
        FAILED="$FAILED volume:$VOL_ID"
    fi
fi

# ---- Step 2b: Delete benchmark.sh's scratch volume and EFS filesystem ----
# Own state keys, not create-infra.sh's — never covered by step 2 above.

if [[ -n "$BENCH_VOL_ID" ]]; then
    log "Detaching/deleting benchmark scratch volume $BENCH_VOL_ID..."
    aws ec2 detach-volume --volume-id "$BENCH_VOL_ID" 2>/dev/null || true
    aws ec2 wait volume-available --volume-ids "$BENCH_VOL_ID" 2>/dev/null || true
    if aws ec2 delete-volume --volume-id "$BENCH_VOL_ID" 2>/dev/null; then
        log "Benchmark scratch volume deleted"
    else
        log "ERROR: benchmark scratch volume $BENCH_VOL_ID was not deleted — check manually"
        FAILED="$FAILED bench-volume:$BENCH_VOL_ID"
    fi
fi

if [[ -n "$BENCH_EFS_ID" ]]; then
    log "Deleting benchmark EFS filesystem $BENCH_EFS_ID..."
    MT_IDS=$(aws efs describe-mount-targets --file-system-id "$BENCH_EFS_ID" \
        --query 'MountTargets[].MountTargetId' --output text 2>/dev/null || echo "")
    for mt in $MT_IDS; do
        aws efs delete-mount-target --mount-target-id "$mt" 2>/dev/null || true
    done
    for i in $(seq 1 15); do
        REMAINING=$(aws efs describe-mount-targets --file-system-id "$BENCH_EFS_ID" \
            --query 'MountTargets | length(@)' --output text 2>/dev/null || echo "0")
        [[ "$REMAINING" == "0" ]] && break
        sleep 3
    done
    if aws efs delete-file-system --file-system-id "$BENCH_EFS_ID" 2>/dev/null; then
        log "Benchmark EFS filesystem deleted"
    else
        log "ERROR: benchmark EFS filesystem $BENCH_EFS_ID was not deleted — check manually"
        FAILED="$FAILED bench-efs:$BENCH_EFS_ID"
    fi
fi

# ---- Step 3: Delete security group ----

if [[ -n "$SG_ID" && "$SG_ID" != "null" ]]; then
    log "Deleting security group $SG_ID..."
    SG_DELETED=false
    for attempt in 1 2 3; do
        if aws ec2 delete-security-group --group-id "$SG_ID" 2>/dev/null; then
            log "Security group $SG_ID deleted"
            SG_DELETED=true
            break
        fi
        log "  SG delete attempt $attempt failed, retrying in 5s..."
        sleep 5
    done
    if [[ "$SG_DELETED" != "true" ]]; then
        log "ERROR: security group $SG_ID was not deleted — check manually (a lingering ENI from a just-terminated instance is the usual cause; retrying this script after a minute often clears it)"
        FAILED="$FAILED sg:$SG_ID"
    fi
fi

# ---- Report ----

if [[ -n "$FAILED" ]]; then
    log ""
    log "=== EtcFS teardown INCOMPLETE — resources still exist:$FAILED ==="
    log "State file kept at $ETCFS_STATE so a re-run can retry these."
    exit 1
fi

rm -f "$ETCFS_STATE"
log ""
log "=== EtcFS teardown complete ==="
