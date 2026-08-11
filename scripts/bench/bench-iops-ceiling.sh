#!/bin/bash
# bench-iops-ceiling.sh — does EtcFS's own IOPS scale with the ceiling AWS
# provisions, or is the daemon itself the bottleneck once the device isn't?
#
# Sweeps the live cluster's data volume (and a same-tier raw scratch volume,
# as the device-ceiling reference) across IOPS tiers via `aws ec2
# modify-volume`, which io2 supports without detaching — one cluster, no
# per-tier recreation. fio runs psync/numjobs=4/directory+filename_format
# against etcfs (see benchmark.sh for why: a shared filename does not split
# across numjobs threads, so per-thread files are what makes numjobs the
# concurrency knob it's meant to be), libaio/numjobs=4 against the raw
# reference.
#
# Requires: create-infra.sh + setup-compute.sh already run.
#
# Usage:
#   ./bench-iops-ceiling.sh [tier1 tier2 ...]   # default: 100 1000 4000
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../infra/state.sh"
source "$SCRIPT_DIR/bench-lib.sh"

RESULTS_DIR="$PROJECT_ROOT/benchmark-results/iops-ceiling"
mkdir -p "$RESULTS_DIR"
RUNTIME="${ETCFS_BENCH_RUNTIME:-30}"
TIERS=(100 1000 4000)
[[ $# -gt 0 ]] && TIERS=("$@")

mapfile -t PUB_IPS < <(state_get compute_public_ips 2>/dev/null | jq -r '.[]' 2>/dev/null)
[[ "${#PUB_IPS[@]}" -ge 1 ]] || die "No compute nodes in state. Run create-infra.sh and setup-compute.sh first."
N0="${PUB_IPS[0]}"

DATA_VOL_ID=$(state_get volume_id | tr -d '"')
[[ -n "$DATA_VOL_ID" && "$DATA_VOL_ID" != "null" ]] || die "no volume_id in state"

SCRATCH_VOL_ID=$(state_get bench_scratch_volume_id 2>/dev/null | tr -d '"' || echo "")
if [[ -z "$SCRATCH_VOL_ID" || "$SCRATCH_VOL_ID" == "null" ]]; then
    log "Creating scratch io2 volume..."
    SCRATCH_VOL_ID=$(aws ec2 create-volume --volume-type io2 --size "${ETCFS_BENCH_SCRATCH_SIZE:-$VOLUME_SIZE_GB}" \
        --iops "${TIERS[0]}" --availability-zone "$AZ" \
        --tag-specifications "ResourceType=volume,Tags=[{Key=ClusterName,Value=$CLUSTER_NAME},{Key=Name,Value=${CLUSTER_NAME}-bench-scratch}]" \
        --query 'VolumeId' --output text)
    state_put bench_scratch_volume_id "\"$SCRATCH_VOL_ID\""
    aws ec2 wait volume-available --volume-ids "$SCRATCH_VOL_ID" 2>/dev/null || true
fi
N0_ID=$(aws ec2 describe-instances --filters "Name=ip-address,Values=$N0" \
    --query 'Reservations[0].Instances[0].InstanceId' --output text)
ATTACHED=$(aws ec2 describe-volumes --volume-ids "$SCRATCH_VOL_ID" \
    --query "Volumes[0].Attachments[?InstanceId=='$N0_ID'] | length(@)" --output text)
if [[ "$ATTACHED" == "0" ]]; then
    aws ec2 attach-volume --volume-id "$SCRATCH_VOL_ID" --instance-id "$N0_ID" --device /dev/sdg >/dev/null
    aws ec2 wait volume-in-use --volume-ids "$SCRATCH_VOL_ID" 2>/dev/null || true
fi
SCRATCH_DEV=$($SSH_CMD "ec2-user@$N0" "for d in /dev/nvme2n1 /dev/sdg /dev/xvdg; do [[ -b \$d ]] && echo \$d && break; done")
[[ -n "$SCRATCH_DEV" ]] || die "scratch device not visible on $N0"

$SSH_CMD "ec2-user@$N0" "sudo dnf install -y fio >/dev/null 2>&1 || sudo yum install -y fio >/dev/null 2>&1"

DONE_TIERS=()
for tier in "${TIERS[@]}"; do
    log "=== Tier: ${tier} IOPS ==="
    if ! modify_volume_iops "$DATA_VOL_ID" "$tier" || ! modify_volume_iops "$SCRATCH_VOL_ID" "$tier"; then
        log "Skipping ${tier} IOPS tier — AWS's per-volume modification-rate limit (see above)."
        continue
    fi

    run_fio "raw-${tier}iops" "filename=$SCRATCH_DEV" "${ETCFS_BENCH_SCRATCH_SIZE:-$VOLUME_SIZE_GB}G" libaio 4 32 "$RUNTIME"
    run_fio "etcfs-${tier}iops" "directory=$FUSE_MOUNTPOINT
filename_format=fio.\$jobname.\$jobnum.\$filenum" 8M psync "${ETCFS_BENCH_JOBS:-4}" 1 "$RUNTIME"
    DONE_TIERS+=("$tier")
done
[[ "${#DONE_TIERS[@]}" -gt 0 ]] || die "every tier was rate-limited — no results to report"

{
    echo "| Tier (IOPS) | Target | randwrite IOPS | randwrite p99 (us) | randread IOPS | randread p99 (us) |"
    echo "|---|---|---|---|---|---|"
    for tier in "${DONE_TIERS[@]}"; do
        fio_row "${tier} | raw" "$RESULTS_DIR/raw-${tier}iops.json"
        fio_row "${tier} | etcfs" "$RESULTS_DIR/etcfs-${tier}iops.json"
    done
} | tee "$RESULTS_DIR/summary.md"

log "Restoring data/scratch volumes to ${TIERS[0]} IOPS (best-effort — a rate-limited volume is torn down by destroy-infra.sh regardless of its current IOPS)..."
modify_volume_iops "$DATA_VOL_ID" "${TIERS[0]}" || true
modify_volume_iops "$SCRATCH_VOL_ID" "${TIERS[0]}" || true

log "IOPS ceiling sweep complete. Results in $RESULTS_DIR"
