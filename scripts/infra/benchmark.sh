#!/bin/bash
# benchmark.sh — IOPS/latency benchmark of EtcFS vs. its own device ceiling
# and vs. EFS, on node0 of an already-provisioned cluster.
#
# Runs the same fio job (4k random write, 4k random read, 128k sequential
# write) against four targets on node0:
#   1. raw   — a dedicated scratch io2 volume, no filesystem (the device
#              ceiling; provisioned separately from the live EtcFS data
#              volume so this never writes over real filesystem data)
#   2. ext4  — the same scratch volume, formatted (cost of a local FS)
#   3. efs   — an EFS filesystem (generalPurpose), mounted over NFS
#   4. etcfs — $FUSE_MOUNTPOINT on the live cluster (the subject)
#
# FSx for Lustre is deliberately not included — provisioning cost/time is
# an order of magnitude higher for one comparison row; add it the same way
# if it's ever needed.
#
# Requires: create-infra.sh + setup-compute.sh already run (state file has
# compute nodes and a mounted EtcFS). Prereqs on node0 (fio, nfs-utils, the
# scratch volume, the EFS mount target) are created idempotently here.
#
# Usage:
#   ./benchmark.sh                    # 30s per job (default)
#   ETCFS_BENCH_RUNTIME=60 ./benchmark.sh
#
# Output: JSON fio results in $PROJECT_ROOT/benchmark-results/, and a
# Markdown summary table printed to stdout (also saved as summary.md).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/state.sh"

RUNTIME="${ETCFS_BENCH_RUNTIME:-30}"
SCRATCH_SIZE_GB="${ETCFS_BENCH_SCRATCH_SIZE:-$VOLUME_SIZE_GB}"
SCRATCH_IOPS="${ETCFS_BENCH_SCRATCH_IOPS:-$VOLUME_IOPS}"
RESULTS_DIR="$PROJECT_ROOT/benchmark-results"
mkdir -p "$RESULTS_DIR"

mapfile -t PUB_IPS < <(state_get compute_public_ips 2>/dev/null | jq -r '.[]' 2>/dev/null)
[[ "${#PUB_IPS[@]}" -ge 1 ]] || die "No compute nodes in state. Run create-infra.sh and setup-compute.sh first."
N0="${PUB_IPS[0]}"
N0_ID=$(aws ec2 describe-instances --filters "Name=ip-address,Values=$N0" \
    --query 'Reservations[0].Instances[0].InstanceId' --output text)

log "=== EtcFS benchmark ==="
log "Node:        $N0 ($N0_ID)"
log "Runtime:     ${RUNTIME}s per job"
log "Scratch vol: ${SCRATCH_SIZE_GB}GB io2, ${SCRATCH_IOPS} IOPS (provisioned ceiling to compare against)"
log ""

# ---- Step 1: scratch io2 volume (raw + ext4 baselines) ----

SCRATCH_VOL_ID=$(state_get bench_scratch_volume_id 2>/dev/null || echo "")
if [[ -z "$SCRATCH_VOL_ID" || "$SCRATCH_VOL_ID" == "null" ]]; then
    log "Creating scratch io2 volume..."
    SCRATCH_VOL_ID=$(aws ec2 create-volume \
        --volume-type io2 --size "$SCRATCH_SIZE_GB" --iops "$SCRATCH_IOPS" \
        --availability-zone "$AZ" \
        --tag-specifications "ResourceType=volume,Tags=[{Key=ClusterName,Value=$CLUSTER_NAME},{Key=Name,Value=${CLUSTER_NAME}-bench-scratch}]" \
        --query 'VolumeId' --output text)
    state_put bench_scratch_volume_id "\"$SCRATCH_VOL_ID\""
    # Only a freshly created volume can be in "creating" state; an
    # already-attached one is "in-use", and waiting on "available" for that
    # polls until the waiter's own timeout (~10 min) since it never happens.
    aws ec2 wait volume-available --volume-ids "$SCRATCH_VOL_ID" 2>/dev/null || true
fi

ATTACHED=$(aws ec2 describe-volumes --volume-ids "$SCRATCH_VOL_ID" \
    --query "Volumes[0].Attachments[?InstanceId=='$N0_ID'] | length(@)" --output text)
if [[ "$ATTACHED" == "0" ]]; then
    log "Attaching scratch volume to $N0_ID..."
    aws ec2 attach-volume --volume-id "$SCRATCH_VOL_ID" --instance-id "$N0_ID" --device /dev/sdg
    aws ec2 wait volume-in-use --volume-ids "$SCRATCH_VOL_ID"
    sleep 5
fi

# ---- Step 2: EFS filesystem + mount target ----
# Best-effort: the IAM identity running this may lack elasticfilesystem:*.
# If so, skip the EFS row rather than aborting the raw/ext4/etcfs baselines.

EFS_ENABLED=true
EFS_ID=$(state_get bench_efs_id 2>/dev/null || echo "")
if [[ -z "$EFS_ID" || "$EFS_ID" == "null" ]]; then
    log "Creating EFS filesystem (generalPurpose)..."
    if ! EFS_ID=$(aws efs create-file-system \
        --performance-mode generalPurpose --throughput-mode bursting \
        --tags "Key=ClusterName,Value=$CLUSTER_NAME" \
        --query 'FileSystemId' --output text 2>/tmp/efs-err); then
        log "WARNING: EFS creation failed, skipping EFS baseline: $(cat /tmp/efs-err)"
        EFS_ENABLED=false
    else
        state_put bench_efs_id "\"$EFS_ID\""
    fi
fi

if $EFS_ENABLED; then
    aws efs wait file-system-available --file-system-id "$EFS_ID" 2>/dev/null || true

    # NFS (2049) within the cluster SG — reuses the SG create-infra.sh already made.
    aws ec2 authorize-security-group-ingress \
        --group-id "$SG_ID" --protocol tcp --port 2049 --source-group "$SG_ID" 2>/dev/null || true

    MT_EXISTS=$(aws efs describe-mount-targets --file-system-id "$EFS_ID" \
        --query "MountTargets[?SubnetId=='$SUBNET_ID'] | length(@)" --output text 2>/dev/null || echo "0")
    if [[ "$MT_EXISTS" == "0" ]]; then
        log "Creating EFS mount target in $SUBNET_ID..."
        aws efs create-mount-target --file-system-id "$EFS_ID" \
            --subnet-id "$SUBNET_ID" --security-groups "$SG_ID" >/dev/null
    fi
    log "Waiting for EFS mount target..."
    for i in $(seq 1 30); do
        STATE=$(aws efs describe-mount-targets --file-system-id "$EFS_ID" \
            --query "MountTargets[?SubnetId=='$SUBNET_ID'].LifeCycleState | [0]" --output text 2>/dev/null || echo "")
        [[ "$STATE" == "available" ]] && break
        sleep 5
    done
    EFS_DNS="${EFS_ID}.efs.${REGION}.amazonaws.com"
fi

# ---- Step 3: prep node0 (packages, scratch device, mounts, fio job file) ----

log "Preparing $N0 (fio, nfs-utils, mounts)..."
$SSH_CMD "ec2-user@$N0" "sudo dnf install -y fio nfs-utils e2fsprogs >/dev/null 2>&1 || sudo yum install -y fio nfs-utils e2fsprogs >/dev/null 2>&1"

SCRATCH_DEV=$($SSH_CMD "ec2-user@$N0" "
for d in /dev/nvme2n1 /dev/sdg /dev/xvdg; do [[ -b \$d ]] && echo \$d && break; done
")
[[ -n "$SCRATCH_DEV" ]] || die "scratch device not visible on $N0"
log "Scratch device: $SCRATCH_DEV"

$SSH_CMD "ec2-user@$N0" "sudo mkdir -p /mnt/bench-ext4 /mnt/bench-efs; sudo umount /mnt/bench-ext4 2>/dev/null || true"

# ---- Step 4: run fio against each target, pull results back ----
#
# filename/size are per-target, so each gets its own job file rather than
# trying to append a --name= job on fio's command line (that creates a
# *separate* job alongside the ones in the ini file instead of setting
# their filename/size, and the ini-defined jobs then fail with "you need
# to specify size=").

run_fio() {
    local label="$1" filename="$2" size="$3"
    # etcfs is a single-threaded, synchronous-IPC daemon end to end (see
    # docs/NEXT_STEPS.md item 24) — one shared fd, no concurrent request
    # processing. Driving it at iodepth=32/numjobs=4 doesn't measure the
    # daemon's IOPS, it measures how fast a 128-deep queue drains through a
    # single serialized path, which produced a run that hadn't finished
    # after 19 minutes for what should be ~90s of time_based work. QD=1
    # measures the thing that's actually true of this target: per-op
    # latency with no concurrency to hide it.
    local depth=32 jobs=4 sdepth=16 engine=libaio
    # libaio needs native filesystem AIO support, which FUSE does not
    # reliably provide — against etcfs it produced a job that hadn't
    # finished after 19 minutes at iodepth=32 nor after 90s at iodepth=1,
    # while a plain O_DIRECT dd against the same mount completed each 4k
    # write in ~20ms. psync (one syscall, one thread) is what actually
    # exercises the daemon instead of fio's AIO plumbing.
    if [[ "$label" == "etcfs" ]]; then depth=1; jobs=1; sdepth=1; engine=psync; fi
    log "--- fio: $label ---"
    local job="
[global]
ioengine=$engine
direct=1
filename=$filename
size=$size
runtime=$RUNTIME
time_based=1
group_reporting=1
[randwrite-4k]
rw=randwrite
bs=4k
numjobs=$jobs
iodepth=$depth
[randread-4k]
rw=randread
bs=4k
numjobs=$jobs
iodepth=$depth
stonewall
[seqwrite-128k]
rw=write
bs=128k
numjobs=1
iodepth=$sdepth
stonewall
"
    echo "$job" | $SSH_CMD "ec2-user@$N0" "cat > /tmp/${label}.fio"
    $SSH_CMD "ec2-user@$N0" \
        "sudo fio /tmp/${label}.fio --output-format=json --output=/tmp/${label}.json"
    scp $SSH_OPTS "ec2-user@$N0:/tmp/${label}.json" "$RESULTS_DIR/${label}.json" >/dev/null
}

# Raw device first, before anything formats or mounts it.
run_fio "raw" "$SCRATCH_DEV" "${SCRATCH_SIZE_GB}G"

$SSH_CMD "ec2-user@$N0" "
set -e
sudo mkfs.ext4 -F -q $SCRATCH_DEV
sudo mount $SCRATCH_DEV /mnt/bench-ext4
"
run_fio "ext4" "/mnt/bench-ext4/fio.dat" "1G"

if $EFS_ENABLED; then
    $SSH_CMD "ec2-user@$N0" "
    sudo umount /mnt/bench-efs 2>/dev/null || true
    sudo mount -t nfs4 -o nfsvers=4.1 $EFS_DNS:/ /mnt/bench-efs
    "
    run_fio "efs" "/mnt/bench-efs/fio.dat" "1G"
fi

run_fio "etcfs" "$FUSE_MOUNTPOINT/fio.dat" "256M"

$SSH_CMD "ec2-user@$N0" "sudo rm -f /mnt/bench-ext4/fio.dat /mnt/bench-efs/fio.dat $FUSE_MOUNTPOINT/fio.dat 2>/dev/null || true"

# ---- Step 5: summarize ----

SUMMARY="$RESULTS_DIR/summary.md"
{
    echo "| Target | randwrite-4k IOPS | randwrite-4k p99 (us) | randread-4k IOPS | randread-4k p99 (us) | seqwrite-128k MiB/s |"
    echo "|---|---|---|---|---|---|"
    LABELS="raw ext4 etcfs"
    $EFS_ENABLED && LABELS="raw ext4 efs etcfs"
    for label in $LABELS; do
        f="$RESULTS_DIR/${label}.json"
        [[ -f "$f" ]] || continue
        wiops=$(jq -r '.jobs[0].write.iops // 0 | floor' "$f")
        wp99=$(jq -r '.jobs[0].write.clat_ns.percentile."99.000000" // 0 | . / 1000 | floor' "$f")
        riops=$(jq -r '.jobs[1].read.iops // 0 | floor' "$f")
        rp99=$(jq -r '.jobs[1].read.clat_ns.percentile."99.000000" // 0 | . / 1000 | floor' "$f")
        seqbw=$(jq -r '.jobs[2].write.bw // 0 | . / 1024 | floor' "$f")
        echo "| $label | $wiops | $wp99 | $riops | $rp99 | $seqbw |"
    done
    echo ""
    echo "Provisioned ceiling for the scratch/data volumes: ${SCRATCH_IOPS} IOPS (io2)."
} | tee "$SUMMARY"

log ""
log "============================================"
log "Benchmark complete. Raw fio JSON + summary in $RESULTS_DIR"
log "============================================"
