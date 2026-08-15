#!/bin/bash
# bench-juicefs.sh — JuiceFS (Redis metadata + MinIO object storage, both on
# node0) on its own 3-node cluster + a single 1000-IOPS Multi-Attach volume.
#
# JuiceFS's data plane is always an object store, never local disk, so this
# backend needs no NFS relay (unlike bench-gluster.sh) — MinIO on node0 puts
# an S3-compatible endpoint in front of the same Multi-Attach volume every
# other backend gets, and node1 mounts JuiceFS as a normal network client
# against it, same shape as the other three backends' single-client run.
#
# Usage:
#   ./bench-juicefs.sh
set -euo pipefail
export COMPARE_BACKEND=juicefs
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"

trap compare_destroy EXIT
compare_provision
N0="${COMPARE_PUB_IPS[0]}"
N1="${COMPARE_PUB_IPS[1]}"
P0="${COMPARE_PRIV_IPS[0]}"

dev=$(detect_ebs_dev "$N0")
[[ -n "$dev" ]] || die "bench-juicefs: no EBS device found on $N0"

log "Setting up Redis (metadata) + MinIO (object storage, on the shared volume) on $N0..."
$SSH_CMD "ec2-user@$N0" "
    sudo mkfs.ext4 -q -F $dev
    sudo mkdir -p /mnt/juicefs-minio-data
    sudo mount $dev /mnt/juicefs-minio-data

    sudo dnf install -y redis fio >/dev/null 2>&1 || sudo yum install -y redis fio >/dev/null 2>&1
    sudo systemctl enable --now redis >/dev/null 2>&1 || sudo systemctl enable --now redis6 >/dev/null 2>&1

    curl -fsSL https://dl.min.io/server/minio/release/linux-amd64/minio -o /tmp/minio
    sudo install -m 755 /tmp/minio /usr/local/bin/minio
    sudo useradd -r minio-user 2>/dev/null || true
    sudo chown -R minio-user:minio-user /mnt/juicefs-minio-data
    sudo -u minio-user nohup /usr/local/bin/minio server /mnt/juicefs-minio-data --address :9000 \
        > /tmp/minio.log 2>&1 &
    sleep 3

    curl -fsSL https://d.juicefs.com/install.sh | sh -
"

log "Formatting JuiceFS volume compare-vol (redis meta + minio storage)..."
$SSH_CMD "ec2-user@$N0" "
    sudo juicefs format --storage minio \
        --bucket http://127.0.0.1:9000/juicefs-bucket \
        --access-key minioadmin --secret-key minioadmin \
        redis://127.0.0.1:6379/1 compare-vol
"

log "Mounting JuiceFS client on $N1..."
$SSH_CMD "ec2-user@$N1" "
    curl -fsSL https://d.juicefs.com/install.sh | sh -
    sudo dnf install -y fio >/dev/null 2>&1 || sudo yum install -y fio >/dev/null 2>&1
    sudo mkdir -p /mnt/compare-juicefs
    sudo juicefs mount -d redis://$P0:6379/1 /mnt/compare-juicefs
"
sleep 3

N0="$N1"
run_fio "juicefs" "filename=/mnt/compare-juicefs/fio.dat" 1G libaio 4 32 "${ETCFS_BENCH_RUNTIME:-30}"
compare_summary_row juicefs "$RESULTS_DIR/juicefs.json"

log "juicefs comparison run complete. Results in $RESULTS_DIR"
