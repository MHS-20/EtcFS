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

compare_begin
N0="${COMPARE_PUB_IPS[0]}"
N1="${COMPARE_PUB_IPS[1]}"
P0="${COMPARE_PRIV_IPS[0]}"

compare_open_port 6379  # redis (metadata)
compare_open_port 9000  # minio (object storage)

dev=$(compare_shared_device "$N0")

log "Setting up Redis (metadata) + MinIO (object storage, on the shared volume) on $N0..."
$SSH_CMD "ec2-user@$N0" "set -e
    sudo mkfs.ext4 -q -F $dev
    sudo mkdir -p /mnt/juicefs-minio-data
    sudo mount $dev /mnt/juicefs-minio-data

    sudo dnf install -y redis6 fio || sudo dnf install -y redis fio || sudo yum install -y redis fio
    # Not systemctl enable --now: the packaged unit's default redis*.conf
    # binds 127.0.0.1 only, which refuses the N1 client's cross-node connect
    # below — run it directly with the bind/protected-mode flags that matter
    # for this instead of hunting the package's actual conf path.
    REDIS_BIN=\$(command -v redis-server || command -v redis6-server)
    sudo nohup \"\$REDIS_BIN\" --bind 0.0.0.0 --protected-mode no > /tmp/redis.log 2>&1 &
    sleep 2

    curl -fsSL https://dl.min.io/server/minio/release/linux-amd64/minio -o /tmp/minio
    sudo install -m 755 /tmp/minio /usr/local/bin/minio
    sudo useradd -r -m -d /home/minio-user minio-user 2>/dev/null || true
    sudo chown -R minio-user:minio-user /mnt/juicefs-minio-data
    # Without MINIO_ROOT_USER/PASSWORD set, MinIO mints random root
    # credentials on every startup instead of accepting minioadmin/minioadmin
    # — juicefs format below still succeeds (bucket creation tolerates it),
    # but every actual data object write then fails auth, which surfaces
    # through FUSE as a plain I/O error with no hint it was ever about auth.
    sudo -u minio-user MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \
        nohup /usr/local/bin/minio server /mnt/juicefs-minio-data --address :9000 \
        > /tmp/minio.log 2>&1 &
    sleep 3

    curl -fsSL https://d.juicefs.com/install | sudo sh -
"

log "Formatting JuiceFS volume compare-vol (redis meta + minio storage)..."
$SSH_CMD "ec2-user@$N0" "set -e
    # Not 127.0.0.1 for the bucket: format's storage endpoint is stored in
    # Redis and read back by every future mount, including N1's — a
    # localhost URL is only ever correct from N0's own point of view, and N1
    # reading it back means N1 trying to reach MinIO on N1's own localhost,
    # which doesn't exist there. Every juicefs write hung retrying that
    # instead of erroring, confirmed directly.
    sudo juicefs format --storage minio \
        --bucket http://$P0:9000/juicefs-bucket \
        --access-key minioadmin --secret-key minioadmin \
        redis://127.0.0.1:6379/1 compare-vol
"

log "Mounting JuiceFS client on $N1..."
$SSH_CMD "ec2-user@$N1" "set -e
    curl -fsSL https://d.juicefs.com/install | sudo sh -
    sudo mkdir -p /mnt/compare-juicefs
    sudo juicefs mount -d redis://$P0:6379/1 /mnt/compare-juicefs
"
sleep 3
compare_install_fio "$N1"

N0="$N1"
# psync, not libaio: juicefs is a FUSE mount like etcfs (see bench-etcfs.sh),
# and direct=1 + libaio against a FUSE filesystem is a known hang — confirmed
# directly against this exact backend, fio sitting well past its runtime with
# zero output instead of erroring. Single shared file (like bench-nfs.sh/
# bench-gluster.sh), not per-thread directory+filename_format like etcfs: the
# directory form's per-thread file layout was still hanging even under
# psync, and this backend doesn't need numjobs to fan out across files the
# way etcfs's own comparison does.
run_fio "juicefs" "filename=/mnt/compare-juicefs/fio.dat" 8M psync 4 1 "${ETCFS_BENCH_RUNTIME:-30}"
compare_finish juicefs
