#!/bin/bash
# add-compute-node.sh — add a compute node to a running EtcFS cluster.
#
# Adds a single EC2 instance to an existing cluster:
#   1. Launch instance + attach shared EBS volume
#   2. Generate TLS certs for the new node
#   3. Add to existing etcd cluster (etcdctl member add)
#   4. Install etcd + both EtcFS binaries (etcfuse-meta, etcfuse — see
#      setup-compute.sh's header for the two-binary split)
#   5. Join etcd cluster, start etcfuse-meta then etcfuse, mount FUSE
#
# The EtcFS daemon handles inode-range reservation and arena acquisition
# automatically on first write. No manual rebalancing step needed.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/state.sh"

log "=== Adding EtcFS compute node ==="

# Pick an existing node to talk to for etcdctl member add
EXISTING_PUB_IP=$(state_get compute_public_ips | jq -r '.[0]')
EXISTING_PRIV_IP=$(state_get compute_ips | jq -r '.[0]')
[[ -n "$EXISTING_PUB_IP" && "$EXISTING_PUB_IP" != "null" ]] || die "No existing nodes in state."
wait_for_ssh "$EXISTING_PUB_IP" || die "SSH not available on existing node."

CERT_DIR="$PROJECT_ROOT/certs"
[[ -f "$CERT_DIR/ca.key" ]] || die "CA key not found at $CERT_DIR/ca.key — run setup-compute.sh first?"

CLUSTER=$(state_get cluster_name)
AMI=$(state_get ami_id)
SG=$(state_get sg_id)
SUBNET=$(state_get subnet_id)
KEY=$(state_get key_name)
VOL=$(state_get volume_id)
AZ=$(state_get az)
NEW_NODE_NUM=$(($(state_get compute_ips | jq 'length') + 1))

# ---- Step 1: Launch instance ----
log "Launching instance #${NEW_NODE_NUM}..."
INSTANCE_ID=$(aws ec2 run-instances \
    --image-id "$AMI" \
    --instance-type "$INSTANCE_TYPE" \
    --key-name "$KEY" \
    --security-group-ids "$SG" \
    --subnet-id "$SUBNET" \
    --associate-public-ip-address \
    --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":20,"VolumeType":"gp3"}}]' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=${CLUSTER}-compute-${NEW_NODE_NUM}},{Key=ClusterName,Value=$CLUSTER}]" \
    --query 'Instances[0].InstanceId' --output text)

aws ec2 wait instance-running --instance-ids "$INSTANCE_ID"
PRIV_IP=$(get_instance_private_ip "$INSTANCE_ID")
PUB_IP=$(get_instance_public_ip "$INSTANCE_ID")
log "  $INSTANCE_ID → $PUB_IP / $PRIV_IP"

# ---- Step 2: Attach shared EBS volume ----
log "Attaching EBS volume $VOL..."
aws ec2 attach-volume --volume-id "$VOL" --instance-id "$INSTANCE_ID" --device /dev/sdf 2>/dev/null
sleep 5

# Update state
state_append compute_ips "\"$PRIV_IP\""
state_append compute_public_ips "\"$PUB_IP\""
state_append compute_instance_ids "\"$INSTANCE_ID\""

# ---- Step 3: Install packages ----
wait_for_ssh "$PUB_IP" 60 5
log "Installing packages..."
$SSH_CMD "ec2-user@$PUB_IP" <<PACKAGES
set -e
sudo dnf install -y gcc make git rsync fuse3-libs fuse3-devel 2>&1 | tail -3
sudo dnf install -y kernel-headers 2>&1 | tail -3 || true
PACKAGES

# ---- Step 4: Install etcd ----
log "Installing etcd..."
ETCD_VER="${ETCD_VER:-v3.5.18}"
ETCD_URL="https://github.com/etcd-io/etcd/releases/download/${ETCD_VER}/etcd-${ETCD_VER}-linux-amd64.tar.gz"
$SSH_CMD "ec2-user@$PUB_IP" <<ETCDINST
set -e
sudo mkdir -p /etc/etcd/tls /var/lib/etcd
sudo chown ec2-user:ec2-user /var/lib/etcd
curl -sLo /tmp/etcd.tar.gz '${ETCD_URL}'
tar xzf /tmp/etcd.tar.gz -C /tmp
sudo mv /tmp/etcd-${ETCD_VER}-linux-amd64/etcd /tmp/etcd-${ETCD_VER}-linux-amd64/etcdctl /usr/local/bin/
sudo chmod 755 /usr/local/bin/etcd /usr/local/bin/etcdctl
rm -rf /tmp/etcd*
ETCDINST

# ---- Step 5: Generate TLS certs for the new node ----
ETCD_NAME="etcd-$((NEW_NODE_NUM - 1))"
PEER_URL="https://${PRIV_IP}:2380"
log "Generating TLS certs for $ETCD_NAME ($PRIV_IP)..."

cp "$CERT_DIR/ca.crt" /tmp/ca.crt
cp "$CERT_DIR/ca.key" /tmp/ca.key

for t in server peer; do
    openssl req -new -newkey rsa:2048 -nodes -subj "/CN=${ETCD_NAME}" \
        -keyout /tmp/${t}.key -out /tmp/${t}.csr \
        -addext "subjectAltName=IP:${PRIV_IP},IP:127.0.0.1"
    openssl x509 -req -days 3650 -in /tmp/${t}.csr -CA /tmp/ca.crt -CAkey /tmp/ca.key \
        -CAcreateserial -out /tmp/${t}.crt \
        -extfile <(echo "subjectAltName=IP:${PRIV_IP},IP:127.0.0.1")
done
rm -f /tmp/server.csr /tmp/peer.csr

# ---- Step 6: etcd member add ----
log "Adding etcd member $ETCD_NAME..."
etcdctl_cmd "$EXISTING_PUB_IP" "member add $ETCD_NAME --peer-urls=$PEER_URL" 2>&1 | head -3

# Build initial-cluster string including the new node
INITIAL_CLUSTER=$(etcdctl_cmd "$EXISTING_PUB_IP" "member list" 2>/dev/null | \
    sed 's/: peerURLs=/=/' | sed 's/,clientURLs.*//' | tr '\n' ',' | sed 's/,$//')
INITIAL_CLUSTER="${INITIAL_CLUSTER},${ETCD_NAME}=${PEER_URL}"

# ---- Step 7: Upload files ----
log "Uploading certs and binaries..."
scp $SSH_OPTS /tmp/ca.crt "$CERT_DIR/client.crt" "$CERT_DIR/client.key" "ec2-user@$PUB_IP:/tmp/"
scp $SSH_OPTS /tmp/server.crt /tmp/server.key /tmp/peer.crt /tmp/peer.key "ec2-user@$PUB_IP:/tmp/"

# EtcFS is two binaries — etcfuse-meta (Go, all etcd ops) and etcfuse (C,
# FUSE + block I/O, talks to etcfuse-meta over a Unix socket). Built for the
# AWS target, not the operator's local toolchain — see build_etcfuse_binary
# in state.sh for why (libfuse3/glibc mismatch against the AMI otherwise).
if DAEMON_BIN=$(build_etcfuse_binary); then
    :
else
    log "WARNING: Docker build failed, falling back to bin/etcfuse"
    DAEMON_BIN="$PROJECT_ROOT/bin/etcfuse"
fi
if META_BIN=$(build_etcfuse_meta_binary); then
    :
else
    log "WARNING: etcfuse-meta build failed, falling back to bin/etcfuse-meta"
    META_BIN="$PROJECT_ROOT/bin/etcfuse-meta"
fi
SOCKET_PATH="/run/etcfuse.sock"
if [[ -f "$DAEMON_BIN" ]]; then
    gzip -c "$DAEMON_BIN" > /tmp/etcfuse.gz
    scp $SSH_OPTS /tmp/etcfuse.gz "ec2-user@$PUB_IP:/tmp/"
    rm -f /tmp/etcfuse.gz
    HAS_BINARY=true
else
    HAS_BINARY=false
fi
if [[ -f "$META_BIN" ]]; then
    gzip -c "$META_BIN" > /tmp/etcfuse-meta.gz
    scp $SSH_OPTS /tmp/etcfuse-meta.gz "ec2-user@$PUB_IP:/tmp/"
    rm -f /tmp/etcfuse-meta.gz
    HAS_META_BINARY=true
else
    HAS_META_BINARY=false
fi

rm -f /tmp/ca.crt /tmp/ca.key /tmp/server.* /tmp/peer.*

# ---- Step 8: Setup node ----
log "Configuring node..."
$SSH_CMD "ec2-user@$PUB_IP" bash <<SETUP
set -e
# Cert dirs
sudo mkdir -p /etc/etcfuse /etc/etcd/tls
sudo cp /tmp/ca.crt /tmp/client.crt /tmp/client.key /etc/etcfuse/
sudo mv /tmp/server.crt /etc/etcd/tls/server-${ETCD_NAME}.crt
sudo mv /tmp/server.key /etc/etcd/tls/server-${ETCD_NAME}.key
sudo mv /tmp/peer.crt /etc/etcd/tls/peer-${ETCD_NAME}.crt
sudo mv /tmp/peer.key /etc/etcd/tls/peer-${ETCD_NAME}.key
sudo chown -R root:root /etc/etcfuse /etc/etcd/tls
sudo chmod 600 /etc/etcfuse/*.key /etc/etcd/tls/*.key 2>/dev/null || true

# Daemon binaries
if $HAS_BINARY; then
    gzip -d -f /tmp/etcfuse.gz
    sudo mv /tmp/etcfuse /usr/local/bin/etcfuse
    sudo chmod 755 /usr/local/bin/etcfuse
fi
if $HAS_META_BINARY; then
    gzip -d -f /tmp/etcfuse-meta.gz
    sudo mv /tmp/etcfuse-meta /usr/local/bin/etcfuse-meta
    sudo chmod 755 /usr/local/bin/etcfuse-meta
fi

# etcd args
sudo mkdir -p /etc/etcd /etc/systemd/system/etcd.service.d
sudo tee /etc/etcd/etcd.args > /dev/null <<'ARGS'
--name ${ETCD_NAME}
--data-dir /var/lib/etcd
--listen-client-urls https://0.0.0.0:2379
--advertise-client-urls https://${PRIV_IP}:2379
--listen-peer-urls https://0.0.0.0:2380
--initial-advertise-peer-urls ${PEER_URL}
--initial-cluster ${INITIAL_CLUSTER}
--initial-cluster-state existing
--initial-cluster-token ${CLUSTER}
--client-cert-auth
--trusted-ca-file /etc/etcd/tls/ca.crt
--cert-file /etc/etcd/tls/server-${ETCD_NAME}.crt
--key-file /etc/etcd/tls/server-${ETCD_NAME}.key
--peer-client-cert-auth
--peer-trusted-ca-file /etc/etcd/tls/ca.crt
--peer-cert-file /etc/etcd/tls/peer-${ETCD_NAME}.crt
--peer-key-file /etc/etcd/tls/peer-${ETCD_NAME}.key
ARGS

sudo tee /etc/systemd/system/etcd.service.d/etcfuse.conf > /dev/null <<'DROPIN'
[Service]
ExecStart=
ExecStart=/bin/sh -c 'exec /usr/local/bin/etcd \$(cat /etc/etcd/etcd.args)'
Restart=always
DROPIN

# etcd systemd base unit
sudo tee /etc/systemd/system/etcd.service > /dev/null <<'UNIT'
[Unit]
Description=etcd (EtcFS colocated)
After=network-online.target
Wants=network-online.target
[Service]
Type=notify
ExecStart=/bin/true
Restart=always
RestartSec=5
LimitNOFILE=65536
[Install]
WantedBy=multi-user.target
UNIT
SETUP

# ---- Step 9: Create EtcFS daemon systemd units + start ----
log "Creating EtcFS daemon systemd units..."

ETCD_ENDPOINTS=$(state_get etcd_endpoints)
# Append new node's endpoint
ETCD_ENDPOINTS="${ETCD_ENDPOINTS},https://${PRIV_IP}:2379"
NODE_ID="${CLUSTER}-node-${NEW_NODE_NUM}"

# See setup-compute.sh for why the split: etcfuse-meta owns every etcd/cert/
# lease-ttl flag, etcfuse only talks FUSE + raw block I/O to it over
# $SOCKET_PATH. No --peer-url/--initial-cluster/--mount-point/--az/
# --heartbeat-interval flag exists on either binary.
$SSH_CMD "ec2-user@$PUB_IP" "sudo tee /etc/systemd/system/etcfuse-meta.service" <<METAUNIT
[Unit]
Description=EtcFS metadata backend (Go)
After=network-online.target etcd.service
Wants=network-online.target etcd.service

[Service]
Type=simple
ExecStart=/usr/local/bin/etcfuse-meta \\
  --listen=${SOCKET_PATH} \\
  --node-id=${NODE_ID} \\
  --etcd-endpoints=${ETCD_ENDPOINTS} \\
  --etcd-cert=/etc/etcfuse/client.crt \\
  --etcd-key=/etc/etcfuse/client.key \\
  --etcd-ca=/etc/etcfuse/ca.crt \\
  --cluster-name=${CLUSTER} \\
  --lease-ttl=${LEASH_TTL}s \\
  --volume-id=${VOL}
Restart=on-failure
RestartSec=5
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
METAUNIT

$SSH_CMD "ec2-user@$PUB_IP" "sudo tee /etc/systemd/system/etcfuse.service" <<DAEMONUNIT
[Unit]
Description=EtcFS FUSE frontend (C)
After=network-online.target etcfuse-meta.service
Wants=network-online.target etcfuse-meta.service

[Service]
Type=simple
ExecStartPre=/usr/bin/mkdir -p ${FUSE_MOUNTPOINT}
ExecStart=/usr/local/bin/etcfuse \\
  --socket=${SOCKET_PATH} \\
  --volume-id=${VOL} \\
  --node-id=${NODE_ID} \\
  --log-level=2 \\
  ${FUSE_MOUNTPOINT}
Restart=on-failure
RestartSec=5
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
DAEMONUNIT

$SSH_CMD "ec2-user@$PUB_IP" "sudo systemctl daemon-reload"
$SSH_CMD "ec2-user@$PUB_IP" "sudo systemctl enable etcd etcfuse-meta etcfuse"

# ---- Step 10: Start etcd + daemon ----
log "Starting etcd on new node..."
$SSH_CMD "ec2-user@$PUB_IP" "sudo systemctl start etcd"
if ! wait_for_etcd "$PUB_IP" 60; then
    log "ERROR: etcd did not become healthy on new node"
    exit 1
fi
log "etcd healthy on new node"

# Give etcd membership time to propagate
sleep 3

if $HAS_BINARY && $HAS_META_BINARY; then
    log "Starting EtcFS daemons (etcfuse-meta, then etcfuse)..."
    $SSH_CMD "ec2-user@$PUB_IP" "sudo mkdir -p $FUSE_MOUNTPOINT && sudo systemctl start etcfuse-meta"
    sleep 3
    $SSH_CMD "ec2-user@$PUB_IP" "sudo systemctl start etcfuse"
    if ! wait_for_daemon_ready "$PUB_IP" 60 2; then
        log "ERROR: EtcFS daemons did not become ready on new node"
        $SSH_CMD "ec2-user@$PUB_IP" "sudo journalctl -u etcfuse-meta --no-pager -n 30" || true
        $SSH_CMD "ec2-user@$PUB_IP" "sudo journalctl -u etcfuse --no-pager -n 30" || true
        exit 1
    fi
    log "EtcFS FUSE mounted at $FUSE_MOUNTPOINT on new node"
    $SSH_CMD "ec2-user@$PUB_IP" "df -h $FUSE_MOUNTPOINT"
else
    log "  (missing binary — skipping daemon start)"
fi

# ---- Done ----
log "=== Node added: $PUB_IP ($ETCD_NAME) ==="
log "EtcFS cluster now has $NEW_NODE_NUM nodes"
