#!/bin/bash
# add-compute-node.sh — add one compute node to an already-running EtcFS
# cluster (created by create-infra.sh + bootstrap-cluster.sh).
#
# Unlike bootstrap-cluster.sh (every node starts together as one fresh
# cluster), this node is joining a cluster that already has quorum, so it
# has to go through etcd's real join protocol: `etcdctl member add` against
# the running cluster first, then start the new member with
# --initial-cluster-state existing and the initial-cluster string that
# member add returns. Skipping that step (starting the new member with a
# self-assembled node list instead) is exactly the bug that made the old,
# TLS+systemd version of this cluster's initial bootstrap never work — see
# bootstrap-cluster.sh's header.
#
# Same no-TLS, no-systemd, nohup'd-background-process model as
# bootstrap-cluster.sh and scripts/test/chaos-lib.sh's proven AWS add_node,
# for the same reason: one model, everywhere, instead of two that drift.
#
# Mutates the state file (adds this node to compute_ips/compute_public_ips/
# compute_instance_ids) so it's picked up by later runs and other scripts.
#
# Usage: add-compute-node.sh

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/state.sh"

log "=== Adding EtcFS compute node ==="

EXISTING_PUB_IP=$(state_get compute_public_ips | jq -r '.[0]')
[[ -n "$EXISTING_PUB_IP" && "$EXISTING_PUB_IP" != "null" ]] || die "No existing nodes in state."
wait_for_ssh "$EXISTING_PUB_IP" || die "SSH not available on existing node."

CLUSTER=$(state_get cluster_name)
AMI=$(state_get ami_id)
SG=$(state_get sg_id)
SUBNET=$(state_get subnet_id)
KEY=$(state_get key_name)
VOL=$(state_get volume_id)
NEW_NODE_NUM=$(($(state_get compute_ips | jq 'length') + 1))
ENAME="e$((NEW_NODE_NUM - 1))"
NODE_ID="${CLUSTER}-node-${NEW_NODE_NUM}"

# ---- Launch instance + attach the shared volume ----
log "Launching instance #${NEW_NODE_NUM}..."
IAM_PROFILE=$("$SCRIPT_DIR/fencing-iam.sh" profile-name)
INSTANCE_ID=$(aws ec2 run-instances \
    --image-id "$AMI" \
    --instance-type "$INSTANCE_TYPE" \
    --key-name "$KEY" \
    --security-group-ids "$SG" \
    --subnet-id "$SUBNET" \
    --associate-public-ip-address \
    --iam-instance-profile "Name=$IAM_PROFILE" \
    --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":20,"VolumeType":"gp3"}}]' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=${CLUSTER}-compute-${NEW_NODE_NUM}},{Key=ClusterName,Value=$CLUSTER}]" \
    --query 'Instances[0].InstanceId' --output text)

aws ec2 wait instance-running --instance-ids "$INSTANCE_ID"
PRIV_IP=$(get_instance_private_ip "$INSTANCE_ID")
PUB_IP=$(get_instance_public_ip "$INSTANCE_ID")
log "  $INSTANCE_ID -> $PUB_IP / $PRIV_IP"

log "Attaching EBS volume $VOL..."
aws ec2 attach-volume --volume-id "$VOL" --instance-id "$INSTANCE_ID" --device /dev/sdf 2>/dev/null
sleep 8

state_append compute_ips "\"$PRIV_IP\""
state_append compute_public_ips "\"$PUB_IP\""
state_append compute_instance_ids "\"$INSTANCE_ID\""

# ---- Packages, etcd, both EtcFS binaries — compiled on-node, matching
# bootstrap-cluster.sh, so there's no cross-compile/libc mismatch to guard
# against. ----
wait_for_ssh "$PUB_IP" 20 5
log "Installing packages..."
ssh $SSH_OPTS "ec2-user@$PUB_IP" "sudo dnf install -y fuse3-libs fuse3-devel gcc 2>&1 | tail -2" || true
ssh $SSH_OPTS "ec2-user@$PUB_IP" "
    curl -fsSL https://github.com/etcd-io/etcd/releases/download/v3.5.18/etcd-v3.5.18-linux-amd64.tar.gz -o /tmp/etcd.tar.gz && \
    sudo tar -xzf /tmp/etcd.tar.gz -C /usr/local/bin --strip-components=1 etcd-v3.5.18-linux-amd64/etcd etcd-v3.5.18-linux-amd64/etcdctl && \
    sudo chmod +x /usr/local/bin/etcd /usr/local/bin/etcdctl
" || true

log "Building and deploying EtcFS binaries..."
cd "$PROJECT_ROOT" || die "cannot cd to $PROJECT_ROOT"
go build -o "$PROJECT_ROOT/bin/etcfuse-meta" "$PROJECT_ROOT/cmd/etcfuse-meta/" 2>&1 | tail -1
tar czf /tmp/etcfs-addnode-src.tar.gz -C "$PROJECT_ROOT" cmd/etcfuse pkg/fuse pkg/block
scp $SSH_OPTS -q "$PROJECT_ROOT/bin/etcfuse-meta" "ec2-user@$PUB_IP:/tmp/"
ssh $SSH_OPTS "ec2-user@$PUB_IP" "sudo cp /tmp/etcfuse-meta /usr/local/bin/etcfuse-meta && sudo chmod +x /usr/local/bin/etcfuse-meta"
scp $SSH_OPTS -q /tmp/etcfs-addnode-src.tar.gz "ec2-user@$PUB_IP:/tmp/"
ssh $SSH_OPTS "ec2-user@$PUB_IP" "
    rm -rf /tmp/s && mkdir /tmp/s && cd /tmp/s && tar xzf /tmp/etcfs-addnode-src.tar.gz && \
    gcc -I. -Wall -Wextra -Werror -std=c11 -D_GNU_SOURCE -O2 -g \
        cmd/etcfuse/main.c pkg/fuse/fuse.c pkg/fuse/ops.c pkg/block/block.c \
        -o /tmp/etcfuse -lfuse3 -lpthread && \
    sudo cp /tmp/etcfuse /usr/local/bin/etcfuse && sudo chmod +x /usr/local/bin/etcfuse
"
ssh $SSH_OPTS "ec2-user@$PUB_IP" "sudo mkdir -p /mnt/etcfuse /run/etcfuse"

# ---- etcd member add: the running cluster has to know about this node
# before it starts, or its own self-assembled member list will never match
# what the cluster actually has ("member count is unequal"). ----
log "Adding etcd member $ENAME (peer-urls=http://$PRIV_IP:2380)..."
ADDOUT=""
INITIAL_CLUSTER=""
for _ in $(seq 1 10); do
    ADDOUT=$(ssh $SSH_OPTS "ec2-user@$EXISTING_PUB_IP" \
        "sudo ETCDCTL_API=3 /usr/local/bin/etcdctl --endpoints=http://127.0.0.1:2379 member add $ENAME --peer-urls=http://$PRIV_IP:2380" 2>&1)
    INITIAL_CLUSTER=$(echo "$ADDOUT" | grep '^ETCD_INITIAL_CLUSTER=' | cut -d= -f2- | tr -d '"')
    [[ -n "$INITIAL_CLUSTER" ]] && break
    # An add that landed but couldn't be read back (e.g. the cluster reported
    # unhealthy on a later retry) leaves the member in place, and every
    # further attempt fails with "Peer URLs already exists" — recover the
    # initial-cluster string from the member list instead of giving up.
    if echo "$ADDOUT" | grep -q "Peer URLs already exists"; then
        INITIAL_CLUSTER=$(ssh $SSH_OPTS "ec2-user@$EXISTING_PUB_IP" \
            "sudo ETCDCTL_API=3 /usr/local/bin/etcdctl --endpoints=http://127.0.0.1:2379 member list" 2>/dev/null |
            awk -F', ' '$3 != "" && $4 != "" {print $3 "=" $4}' | paste -sd, -)
        [[ -n "$INITIAL_CLUSTER" ]] && break
    fi
    sleep 2
done
[[ -n "$INITIAL_CLUSTER" ]] || die "could not parse ETCD_INITIAL_CLUSTER after 10 attempts: $ADDOUT"

# ---- Start etcd as the joining member ----
log "Starting etcd on new node..."
ssh $SSH_OPTS "ec2-user@$PUB_IP" "
    sudo mkdir -p /var/lib/etcd
    sudo nohup /usr/local/bin/etcd --name $ENAME --data-dir /var/lib/etcd \
        --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://$PRIV_IP:2379 \
        --listen-peer-urls http://0.0.0.0:2380 --initial-advertise-peer-urls http://$PRIV_IP:2380 \
        --initial-cluster '$INITIAL_CLUSTER' --initial-cluster-state existing \
        --auto-compaction-mode revision --auto-compaction-retention 100000 \
        --quota-backend-bytes 8589934592 > /tmp/etcd.log 2>&1 &
" 2>/dev/null
sleep 8

# ---- Start the EtcFS daemons ----
log "Starting EtcFS daemons..."
mapfile -t ALL_PRIV_IPS < <(state_get compute_ips | jq -r '.[]')
ENDPOINTS=""
for p in "${ALL_PRIV_IPS[@]}"; do
    [[ -n "$ENDPOINTS" ]] && ENDPOINTS+=","
    ENDPOINTS+="http://$p:2379"
done
INST_ID="$INSTANCE_ID"
FENCE_FLAGS=""
DEV_FLAG="--block-device=/dev/nvme1n1"
if [[ "${ETCFS_FENCE_MODE:-ebs}" == "nvme" ]]; then
    FENCE_FLAGS="--nvme-reservations"
    [[ -n "$VOL" ]] && DEV_FLAG="--volume-id=$VOL"
elif [[ -n "$VOL" && -n "$INST_ID" ]]; then
    FENCE_FLAGS="--ebs-volume-id=$VOL --ec2-instance-id=$INST_ID"
fi
ssh $SSH_OPTS "ec2-user@$PUB_IP" "
    sudo nohup /usr/local/bin/etcfuse-meta --listen=/run/etcfuse/etcfuse.sock \
        --etcd-endpoints=$ENDPOINTS --node-id=$NODE_ID --cluster-name=$CLUSTER \
        --lease-ttl=${ETCFS_LEASE_TTL:-10s} $DEV_FLAG --log-level=1 $FENCE_FLAGS > /tmp/meta.log 2>&1 &
    sleep 4
    sudo nohup /usr/local/bin/etcfuse --socket=/run/etcfuse/etcfuse.sock \
        --node-id=$NODE_ID --log-level=1 /mnt/etcfuse > /tmp/fuse.log 2>&1 &
    sleep 7
" 2>/dev/null

MOUNTED=0
for _ in $(seq 1 20); do
    M=$(ssh $SSH_OPTS -q "ec2-user@$PUB_IP" "sudo mountpoint -q /mnt/etcfuse && echo OK || echo FAIL" 2>/dev/null)
    [[ "$M" == "OK" ]] && { MOUNTED=1; break; }
    sleep 1
done
if [[ "$MOUNTED" -ne 1 ]]; then
    log "ERROR: EtcFS FUSE never mounted on new node"
    ssh $SSH_OPTS -q "ec2-user@$PUB_IP" "echo '--- meta.log ---'; sudo tail -30 /tmp/meta.log 2>/dev/null; echo '--- fuse.log ---'; sudo tail -20 /tmp/fuse.log 2>/dev/null" || true
    die "node add failed"
fi

log "=== Node added: $PUB_IP ($ENAME / $NODE_ID) ==="
log "EtcFS cluster now has $NEW_NODE_NUM nodes"
