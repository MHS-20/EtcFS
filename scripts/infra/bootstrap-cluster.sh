#!/bin/bash
# bootstrap-cluster.sh — install etcd + both EtcFS binaries on every node in
# a state file, and bring up a fresh cluster.
#
# This is the single node-software gate every path that needs a *working*
# cluster goes through: setup-compute.sh (a persistent, manually-poked test
# cluster) and scripts/test/chaos-lib.sh (an ephemeral per-run cluster) both
# call this instead of each maintaining their own copy of the bootstrap.
#
# The model — no TLS, no systemd, etcd started fresh on every node
# simultaneously, daemons as nohup'd background processes — is deliberately
# the one scripts/test/chaos-lib.sh's AWS path already used, not a new one.
# An earlier TLS+systemd version of this (in setup-compute.sh) had four
# separate bugs around re-running against a partially-up cluster: a cert-
# reuse check that never matched and silently re-signed a new CA on every
# run, systemd `start` being a no-op on an already-active unit so neither a
# new binary nor new certs were ever picked up on a re-run, a killed FUSE
# daemon leaving a stale mount that made the next `mkdir -p` fail with
# ENOTCONN, and — the one that mattered most — no `etcd member add` step
# before starting a joining node, so a staged multi-node bootstrap could
# never actually reach quorum.
#
# Sidestepping the join case entirely (every node starts as part of one
# fresh cluster, together) removes that whole bug class rather than fixing
# it: there is no "already up" state to reconcile against, because nothing
# here is left running between invocations that this script itself doesn't
# tear down and restart. A node added *after* the cluster already exists is
# a different, later operation — see add-compute-node.sh — and does need
# `etcd member add`, which it already does correctly.
#
# Usage:
#   bootstrap-cluster.sh <state-file> [node-id-prefix]
#
# <state-file> must already have compute_ips, compute_public_ips,
# compute_instance_ids, volume_id and cluster_name — i.e. create-infra.sh
# has already run against it. node-id-prefix defaults to "n" (nodes become
# n1, n2, ...); pass something else to avoid a node-ID collision when
# layering a second bootstrap onto infra an earlier one hasn't torn down.
#
# Not `set -e`: remote setup steps are individually retried or tolerated
# (`|| true`) throughout, the same way chaos-lib.sh's proven version was —
# a transient SSH hiccup during package install must not abort the whole
# bootstrap when the retry immediately after it would have succeeded.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

STATE_FILE="${1:?usage: bootstrap-cluster.sh <state-file> [node-id-prefix]}"
[[ "$STATE_FILE" = /* ]] || STATE_FILE="$PROJECT_ROOT/$STATE_FILE"
PREFIX="${2:-n}"

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=10"
log() { echo "[$(date +%T)] $*"; }

[[ -f "$STATE_FILE" ]] || { echo "ERROR: state file not found: $STATE_FILE (run create-infra.sh first)" >&2; exit 1; }

wait_ssh() {
    local ip="$1" max="${2:-12}"
    for _ in $(seq 1 "$max"); do
        timeout 5 ssh $SSH_OPTS -o ConnectTimeout=5 "ec2-user@$ip" "echo ok" 2>/dev/null && return 0
        sleep 5
    done
    return 1
}
ssh_retry() {
    local ip="$1"; shift
    wait_ssh "$ip" 12
    timeout 120 ssh $SSH_OPTS "ec2-user@$ip" "$@" 2>&1
}
check_mount() {
    local M
    M=$(timeout 10 ssh $SSH_OPTS -q "ec2-user@$1" "sudo mountpoint -q /mnt/etcfuse && echo OK || echo FAIL" 2>/dev/null)
    [[ "$M" == "OK" ]]
}
dump_logs() {
    local out
    out=$(timeout 20 ssh $SSH_OPTS -q "ec2-user@$1" \
        "echo '--- meta.log ---'; sudo tail -40 /tmp/meta.log 2>/dev/null; echo '--- fuse.log ---'; sudo tail -20 /tmp/fuse.log 2>/dev/null" 2>&1)
    log "  ---- daemon logs from $1 ----"
    while IFS= read -r line; do log "    $line"; done <<< "$out"
    log "  ---- end daemon logs ----"
}

mapfile -t PUB_IPS < <(jq -r '.compute_public_ips[]' "$STATE_FILE")
mapfile -t PRIV_IPS < <(jq -r '.compute_ips[]' "$STATE_FILE")
mapfile -t INST_IDS < <(jq -r '.compute_instance_ids[]' "$STATE_FILE")
VOL_ID=$(jq -r '.volume_id // empty' "$STATE_FILE")
TAG=$(jq -r '.cluster_name' "$STATE_FILE")
COUNT=${#PUB_IPS[@]}

[[ "$COUNT" -ge 1 ]] || { echo "ERROR: no compute_public_ips in $STATE_FILE" >&2; exit 1; }

log "Bootstrapping $COUNT-node cluster '$TAG' (state=$STATE_FILE)"

# ---- Build once locally, ship to every node ----
cd "$PROJECT_ROOT" || exit 1
go build -o "$PROJECT_ROOT/bin/etcfuse-meta" "$PROJECT_ROOT/cmd/etcfuse-meta/" 2>&1 | tail -1
tar czf /tmp/etcfs-bootstrap-src.tar.gz -C "$PROJECT_ROOT" cmd/etcfuse pkg/fuse pkg/block

# ---- Per-node: packages, etcd binary, both EtcFS binaries, compiled on-node
# (not cross-compiled) so there is no libc/libfuse mismatch against the AMI
# to fall back from. ----
for ip in "${PUB_IPS[@]}"; do
    wait_ssh "$ip" 12
    ssh_retry "$ip" "sudo dnf install -y fuse3-libs fuse3-devel gcc 2>&1 | tail -2" || true
    wait_ssh "$ip" 6
    ssh_retry "$ip" "
        curl -fsSL https://github.com/etcd-io/etcd/releases/download/v3.5.18/etcd-v3.5.18-linux-amd64.tar.gz -o /tmp/etcd.tar.gz && \
        sudo tar -xzf /tmp/etcd.tar.gz -C /usr/local/bin --strip-components=1 etcd-v3.5.18-linux-amd64/etcd etcd-v3.5.18-linux-amd64/etcdctl && \
        sudo chmod +x /usr/local/bin/etcd /usr/local/bin/etcdctl
    " || true
    wait_ssh "$ip" 3
    scp $SSH_OPTS -q "$PROJECT_ROOT/bin/etcfuse-meta" "ec2-user@$ip:/tmp/" 2>/dev/null || true
    ssh_retry "$ip" "sudo cp /tmp/etcfuse-meta /usr/local/bin/etcfuse-meta && sudo chmod +x /usr/local/bin/etcfuse-meta" || true
    scp $SSH_OPTS -q /tmp/etcfs-bootstrap-src.tar.gz "ec2-user@$ip:/tmp/" 2>/dev/null || true
    ssh_retry "$ip" "
        rm -rf /tmp/s && mkdir /tmp/s && cd /tmp/s && tar xzf /tmp/etcfs-bootstrap-src.tar.gz && \
        gcc -I. -Wall -Wextra -Werror -std=c11 -D_GNU_SOURCE -O2 -g \
            cmd/etcfuse/main.c pkg/fuse/fuse.c pkg/fuse/ops.c pkg/block/block.c \
            -o /tmp/etcfuse -lfuse3 -lpthread 2>&1 && \
        sudo cp /tmp/etcfuse /usr/local/bin/etcfuse && sudo chmod +x /usr/local/bin/etcfuse
    " || true
    ssh_retry "$ip" "sudo mkdir -p /mnt/etcfuse /run/etcfuse" || true
done

# ---- Start etcd on every node simultaneously as one fresh cluster.  Plain
# http, no TLS: this is a test cluster reachable only inside the VPC's
# security group, and TLS here was the majority of the bug surface this
# script exists to avoid (see the file header). ----
log "Starting etcd cluster..."
INITIAL_CLUSTER=""
for i in "${!PRIV_IPS[@]}"; do
    [[ -n "$INITIAL_CLUSTER" ]] && INITIAL_CLUSTER+=","
    INITIAL_CLUSTER+="e${i}=http://${PRIV_IPS[$i]}:2380"
done
for i in "${!PUB_IPS[@]}"; do
    ip="${PUB_IPS[$i]}"; priv="${PRIV_IPS[$i]}"; ename="e${i}"
    ssh $SSH_OPTS "ec2-user@$ip" "
        sudo rm -rf /var/lib/etcd && sudo mkdir -p /var/lib/etcd
        sudo nohup /usr/local/bin/etcd --name $ename --data-dir /var/lib/etcd \
            --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://$priv:2379 \
            --listen-peer-urls http://0.0.0.0:2380 --initial-advertise-peer-urls http://$priv:2380 \
            --initial-cluster $INITIAL_CLUSTER --initial-cluster-state new \
            --auto-compaction-mode revision --auto-compaction-retention 100000 \
            --quota-backend-bytes 8589934592 > /tmp/etcd.log 2>&1 &
    " 2>/dev/null
done
sleep 10
log "etcd cluster running"

# ---- Start the EtcFS daemons on every node.  ETCFS_FENCE_MODE=nvme selects
# device-enforced fencing instead of the EBS-detach control-plane fallback;
# unset/ebs is the default and needs the fencing IAM instance profile
# create-infra.sh attaches by default. ----
log "Starting daemons..."
ENDPOINTS=""
for priv in "${PRIV_IPS[@]}"; do
    [[ -n "$ENDPOINTS" ]] && ENDPOINTS+=","
    ENDPOINTS+="http://$priv:2379"
done

for i in "${!PUB_IPS[@]}"; do
    ip="${PUB_IPS[$i]}"
    node_id="${PREFIX}$((i+1))"
    inst_id="${INST_IDS[$i]:-}"
    fence_flags=""
    dev_flag="--block-device=/dev/nvme1n1"
    if [[ "${ETCFS_FENCE_MODE:-ebs}" == "nvme" ]]; then
        fence_flags="--nvme-reservations"
        [[ -n "$VOL_ID" ]] && dev_flag="--volume-id=$VOL_ID"
    elif [[ -n "$VOL_ID" && -n "$inst_id" ]]; then
        fence_flags="--ebs-volume-id=$VOL_ID --ec2-instance-id=$inst_id"
    fi
    ssh $SSH_OPTS "ec2-user@$ip" "
        sudo killall -9 etcfuse-meta etcfuse 2>/dev/null; sudo umount -l /mnt/etcfuse 2>/dev/null; sleep 1
        sudo rm -f /run/etcfuse/etcfuse.sock /run/etcfuse/etcfuse-notify.sock
        sudo nohup /usr/local/bin/etcfuse-meta --listen=/run/etcfuse/etcfuse.sock \
            --etcd-endpoints=$ENDPOINTS --node-id=$node_id --cluster-name=$TAG \
            --lease-ttl=${ETCFS_LEASE_TTL:-10s} $dev_flag --log-level=1 $fence_flags \
            --metrics-addr=:9090 ${ETCFS_META_EXTRA_ARGS:-} > /tmp/meta.log 2>&1 &
        sleep 4
        sudo nohup /usr/local/bin/etcfuse --socket=/run/etcfuse/etcfuse.sock \
            --node-id=$node_id --log-level=1 /mnt/etcfuse > /tmp/fuse.log 2>&1 &
        sleep 7
        sudo mountpoint -q /mnt/etcfuse && echo OK || echo FAIL
    " 2>/dev/null
done

MOUNT_OK=0
for ip in "${PUB_IPS[@]}"; do
    if check_mount "$ip"; then
        MOUNT_OK=$((MOUNT_OK+1))
    else
        log "  ERROR: FUSE mount on $ip FAILED!"
        dump_logs "$ip"
    fi
done
log "Daemons running ($MOUNT_OK/$COUNT mounts OK)"
[[ "$MOUNT_OK" -eq "$COUNT" ]]
