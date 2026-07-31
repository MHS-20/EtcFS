#!/bin/bash
# create-infra.sh — provision EtcFS infrastructure on AWS.
#
# Creates:
#   1. EC2 key pair (if not existing)
#   2. Security group (etcd ports + intra-cluster)
#   3. EBS io2 Multi-Attach volume (raw block device for file data)
#   4. Compute nodes (etcd colocated, 3 minimum for quorum)
#   5. Attaches EBS volume to all compute nodes
#
# Etcd is colocated on compute nodes — no dedicated etcd instances or NLB.
# The EBS volume is used raw by EtcFS (no filesystem format on it).
#
# Idempotent: skips already-created resources.
#
# Usage:
#   ETCFS_COMPUTE_NODES=3 ETCFS_VOLUME_SIZE=100 ./create-infra.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/state.sh"

ETCD_URL="https://github.com/etcd-io/etcd/releases/download/${ETCD_VER}/etcd-${ETCD_VER}-linux-amd64.tar.gz"

log "=== EtcFS Infrastructure Provisioning ==="
log "Region:      $REGION"
log "AZ:          $AZ"
log "Cluster:     $CLUSTER_NAME"
log "Nodes:       $COMPUTE_NODES (etcd colocated)"
log "Volume:      ${VOLUME_SIZE_GB}GB io2, ${VOLUME_IOPS} IOPS"
log "State file:  $ETCFS_STATE"
log "Mount point: $FUSE_MOUNTPOINT"
log ""

# ---- Step 1: Key Pair ----

REAL_PEM="${PEM_PATH/#\~/$HOME}"

if [[ -z "$KEY_NAME" ]]; then
    die "ETCFS_KEY_NAME must be set (e.g. ETCFS_KEY_NAME=etcfuse-keypair)"
fi

if [[ ! -f "$REAL_PEM" ]]; then
    die "Private key not found at $REAL_PEM. Create one with: ssh-keygen -t ed25519 -f $REAL_PEM"
fi

log "Using existing key: $REAL_PEM (key pair: $KEY_NAME)"

if aws ec2 describe-key-pairs --key-names "$KEY_NAME" &>/dev/null; then
    log "Key pair '$KEY_NAME' already exists in AWS"
else
    log "Importing local public key as AWS key pair '$KEY_NAME'..."
    aws ec2 import-key-pair \
        --key-name "$KEY_NAME" \
        --public-key-material "fileb://${REAL_PEM}.pub"
    log "Imported."
fi

state_put key_name "\"$KEY_NAME\""
state_put region "\"$REGION\""
state_put az "\"$AZ\""
state_put cluster_name "\"$CLUSTER_NAME\""

# ---- Step 2: Security Group ----

VPC_ID=$(discover_vpc)
log "VPC: $VPC_ID"

SG_ID=$(state_get sg_id 2>/dev/null || echo "")
if [[ -n "$SG_ID" && "$SG_ID" != "null" && "$SG_ID" != "" ]]; then
    if aws ec2 describe-security-groups --group-ids "$SG_ID" &>/dev/null; then
        log "Security group already exists: $SG_ID"
    else
        SG_ID=""
    fi
fi

if [[ -z "$SG_ID" ]]; then
    SG_NAME="${CLUSTER_NAME}-sg"
    SG_ID=$(aws ec2 describe-security-groups \
        --filters "Name=group-name,Values=$SG_NAME" "Name=vpc-id,Values=$VPC_ID" \
        --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || echo "")

    if [[ -n "$SG_ID" && "$SG_ID" != "None" ]]; then
        log "Security group already exists: $SG_ID"
    else
        log "Creating security group: $SG_NAME"
        SG_ID=$(aws ec2 create-security-group \
            --group-name "$SG_NAME" \
            --description "EtcFS cluster $CLUSTER_NAME" \
            --vpc-id "$VPC_ID" \
            --tag-specifications "ResourceType=security-group,Tags=[{Key=ClusterName,Value=$CLUSTER_NAME}]" \
            --query 'GroupId' --output text)

        MY_IP=$(curl -s https://checkip.amazonaws.com 2>/dev/null || echo "0.0.0.0/32")
        aws ec2 authorize-security-group-ingress \
            --group-id "$SG_ID" --protocol tcp --port 22 --cidr "${MY_IP}/32" 2>/dev/null || \
        aws ec2 authorize-security-group-ingress \
            --group-id "$SG_ID" --protocol tcp --port 22 --cidr 0.0.0.0/0

        # etcd client port (2379) and peer port (2380) within the cluster
        aws ec2 authorize-security-group-ingress \
            --group-id "$SG_ID" --protocol tcp --port 2379 --source-group "$SG_ID"
        aws ec2 authorize-security-group-ingress \
            --group-id "$SG_ID" --protocol tcp --port 2380 --source-group "$SG_ID"

        # Full intra-cluster communication (no GFS2 kernel-level ports needed)
        # EtcFS is userspace-only via FUSE + etcd. Keep all ports open within SG for
        # future tooling (metrics, health checks, debug).
        aws ec2 authorize-security-group-ingress \
            --group-id "$SG_ID" --protocol tcp --port 9100 --source-group "$SG_ID" 2>/dev/null || true
        aws ec2 authorize-security-group-ingress \
            --group-id "$SG_ID" --protocol tcp --port 9090 --source-group "$SG_ID" 2>/dev/null || true

        log "Security group created: $SG_ID"
    fi
fi

state_put vpc_id "\"$VPC_ID\""
state_put subnet_id "\"$SUBNET_ID\""
state_put sg_id "\"$SG_ID\""

# ---- Step 3: AMI ----

AMI_ID=$(discover_ami)
log "AMI: $AMI_ID"
state_put ami_id "\"$AMI_ID\""

# ---- Step 4: EBS Multi-Attach volume (raw, no filesystem) ----

VOL_ID=$(state_get volume_id 2>/dev/null || echo "")
if [[ -n "$VOL_ID" && "$VOL_ID" != "null" && "$VOL_ID" != "" ]]; then
    VOL_STATE=$(aws ec2 describe-volumes --volume-ids "$VOL_ID" \
        --query 'Volumes[0].State' --output text 2>/dev/null || echo "deleted")
    if [[ "$VOL_STATE" != "deleted" && "$VOL_STATE" != "None" && "$VOL_STATE" != "null" ]]; then
        log "EBS volume already exists: $VOL_ID ($VOL_STATE)"
    else
        VOL_ID=""
    fi
fi

if [[ -z "$VOL_ID" ]]; then
    log "Creating EBS io2 Multi-Attach volume (${VOLUME_SIZE_GB}GB, ${VOLUME_IOPS} IOPS)..."
    VOL_ID=$(aws ec2 create-volume \
        --volume-type io2 \
        --size "$VOLUME_SIZE_GB" \
        --iops "$VOLUME_IOPS" \
        --multi-attach-enabled \
        --availability-zone "$AZ" \
        --tag-specifications "ResourceType=volume,Tags=[{Key=ClusterName,Value=$CLUSTER_NAME},{Key=Name,Value=${CLUSTER_NAME}-data}]" \
        --query 'VolumeId' --output text)
    log "Volume: $VOL_ID"
fi

state_put volume_id "\"$VOL_ID\""

# ---- Step 5: Compute nodes (etcd colocated) ----

EXISTING_IDS=$(state_get compute_instance_ids 2>/dev/null | jq -r '. | length' 2>/dev/null || echo "0")
if [[ "$EXISTING_IDS" -ge "$COMPUTE_NODES" ]]; then
    ALL_RUNNING=true
    mapfile -t CHECK_IDS < <(state_get compute_instance_ids | jq -r '.[]')
    for cid in "${CHECK_IDS[@]}"; do
        CSTATE=$(aws ec2 describe-instances --instance-ids "$cid" \
            --query 'Reservations[0].Instances[0].State.Name' --output text 2>/dev/null || echo "terminated")
        if [[ "$CSTATE" != "running" ]]; then
            ALL_RUNNING=false
            break
        fi
    done
    if $ALL_RUNNING; then
        log "Compute nodes already exist and are running"
    else
        log "Some compute nodes are not running, will re-launch"
        EXISTING_IDS=0
    fi
fi

if [[ "$EXISTING_IDS" -lt "$COMPUTE_NODES" ]]; then
    log "=== Creating compute nodes ($COMPUTE_NODES nodes) ==="

    COMPUTE_IDS=()
    COMPUTE_IPS=()
    COMPUTE_PUB_IPS=()

    for i in $(seq 1 $COMPUTE_NODES); do
        NAME="${CLUSTER_NAME}-compute-${i}"
        log "Launching $NAME..."
        INST_ID=$(aws ec2 run-instances \
            --image-id "$AMI_ID" \
            --instance-type "$INSTANCE_TYPE" \
            --key-name "$KEY_NAME" \
            --security-group-ids "$SG_ID" \
            --subnet-id "$SUBNET_ID" \
            --associate-public-ip-address \
            --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":20,"VolumeType":"gp3"}}]' \
            --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$NAME},{Key=ClusterName,Value=$CLUSTER_NAME}]" \
            --query 'Instances[0].InstanceId' --output text)

        COMPUTE_IDS+=("$INST_ID")
        log "  Instance: $INST_ID"
    done

    log "Waiting for compute nodes..."
    for id in "${COMPUTE_IDS[@]}"; do
        wait_for_instance "$id"
        ip=$(get_instance_private_ip "$id")
        pub=$(get_instance_public_ip "$id")
        COMPUTE_IPS+=("$ip")
        COMPUTE_PUB_IPS+=("$pub")
        log "  $id → $pub / $ip"
    done

    state_save_array compute_instance_ids "${COMPUTE_IDS[@]}"
    state_save_array compute_ips "${COMPUTE_IPS[@]}"
    state_save_array compute_public_ips "${COMPUTE_PUB_IPS[@]}"
else
    log "Using existing compute nodes"
    mapfile -t COMPUTE_IPS < <(state_get compute_ips | jq -r '.[]')
    mapfile -t COMPUTE_IDS < <(state_get compute_instance_ids | jq -r '.[]')
fi

# ---- Step 6: etcd endpoints ----

log ""
log "=== Building etcd endpoint list ==="

ETCD_ENDPOINTS=""
for ip in "${COMPUTE_IPS[@]}"; do
    [[ -n "$ETCD_ENDPOINTS" ]] && ETCD_ENDPOINTS+=","
    ETCD_ENDPOINTS+="https://${ip}:2379"
done

log "Etcd endpoints: $ETCD_ENDPOINTS"
state_put etcd_endpoints "\"$ETCD_ENDPOINTS\""

# ---- Step 7: Attach EBS to compute nodes ----

log ""
log "=== Attaching EBS to compute nodes ==="

for i in "${!COMPUTE_IDS[@]}"; do
    id="${COMPUTE_IDS[$i]}"
    log "Attaching $VOL_ID to $id..."
    ATTACH_ERR=$(aws ec2 attach-volume \
        --volume-id "$VOL_ID" \
        --instance-id "$id" \
        --device /dev/sdf 2>&1 >/dev/null) || log "  attach-volume error: $ATTACH_ERR"
done

log "Waiting for all attachments..."
for i in $(seq 1 30); do
    ATTACH_COUNT=$(aws ec2 describe-volumes --volume-ids "$VOL_ID" \
        --query 'Volumes[0].Attachments | length(@)' --output text 2>/dev/null || echo "0")
    if [[ "$ATTACH_COUNT" -ge "${#COMPUTE_IDS[@]}" ]]; then
        log "All $ATTACH_COUNT attachments complete"
        break
    fi
    sleep 2
done

if [[ "$ATTACH_COUNT" -lt "${#COMPUTE_IDS[@]}" ]]; then
    die "only $ATTACH_COUNT/${#COMPUTE_IDS[@]} attachments after 60s — infra left up for inspection, run destroy-infra.sh to tear down"
fi

# ---- Done ----

state_put created_at "\"$(date +%Y-%m-%dT%H:%M:%S%z)\""

log ""
log "============================================"
log "EtcFS infrastructure provisioned successfully!"
log "============================================"
log ""
log "State file: $ETCFS_STATE"
log "Compute nodes: $COMPUTE_NODES (etcd colocated)"
log "EBS volume:   $VOL_ID (${VOLUME_SIZE_GB}GB io2 Multi-Attach, raw)"
log ""
log "Next steps:"
log "  1. Setup nodes:    ./scripts/infra/setup-compute.sh"
log "  2. Run tests:      ./scripts/infra/run-full-test.sh"
