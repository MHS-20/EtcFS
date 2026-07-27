#!/bin/bash
# state.sh — EtcFS infrastructure state management.
#
# Sources:
#   $ETCFS_STATE   — path to state JSON file (default: infra-state.json)
#
# Provides helpers: state_get, state_put, state_save, state_init,
#   discover_*, wait_for_*, SSH_CMD, log, die.
#
# Adapted from QAttach infra scripts. Replaces cluster-agent references
# with etcfuse daemon references. Same state-file + etcd colocation pattern.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Default state file
ETCFS_STATE="${ETCFS_STATE:-$PROJECT_ROOT/infra-state.json}"

# ---- Defaults (overridable via env) ----
REGION="${AWS_DEFAULT_REGION:-eu-west-1}"
AZ="${ETCFS_AZ:-eu-west-1a}"
CLUSTER_NAME="${ETCFS_CLUSTER:-etcfuse}"
KEY_NAME="${ETCFS_KEY_NAME:-etcfuse-keypair}"
PEM_PATH="${ETCFS_PEM_PATH:-$HOME/.ssh/id_ed25519}"
COMPUTE_NODES="${ETCFS_COMPUTE_NODES:-3}"
INSTANCE_TYPE="${ETCFS_INSTANCE_TYPE:-m7i.large}"
VOLUME_SIZE_GB="${ETCFS_VOLUME_SIZE:-100}"
VOLUME_IOPS="${ETCFS_VOLUME_IOPS:-5000}"
SUBNET_ID="${ETCFS_SUBNET:-}"
SECURITY_GROUP_ID="${ETCFS_SG:-}"
ETCD_VER="${ETCFS_ETCD_VERSION:-v3.5.18}"
LEASH_TTL="${ETCFS_LEASE_TTL:-10}"
HEARTBEAT_INTERVAL="${ETCFS_HEARTBEAT:-3}"
FUSE_MOUNTPOINT="${ETCFS_MOUNTPOINT:-/mnt/etcfuse}"

export AWS_DEFAULT_REGION="$REGION"

# Expand ~ in PEM_PATH
PEM="${PEM_PATH/#\~/$HOME}"
SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=10 -o BatchMode=yes -i $PEM"
SSH_CMD="ssh $SSH_OPTS"

log()  { echo "[$(date +%T)] $*"; }
die()  { echo "ERROR: $*" >&2; exit 1; }

# ---- State file helpers ----

_state_file() {
    # Reload state when the JSON file changes on disk
    cat "$ETCFS_STATE" 2>/dev/null || echo "{}"
}

state_init() {
    if [[ ! -f "$ETCFS_STATE" ]]; then
        cat > "$ETCFS_STATE" <<'JSON'
{
  "cluster_name": "",
  "region": "",
  "az": "",
  "key_name": "",
  "vpc_id": "",
  "sg_id": "",
  "subnet_id": "",
  "ami_id": "",
  "volume_id": "",
  "compute_ips": [],
  "compute_public_ips": [],
  "compute_instance_ids": [],
  "etcd_endpoints": "",
  "created_at": ""
}
JSON
    fi
}

state_get() {
    local key="$1"
    jq -r ".$key" "$ETCFS_STATE"
}

state_put() {
    local key="$1" value="$2"
    local tmp
    tmp="$(mktemp)"
    jq ".$key = $value" "$ETCFS_STATE" > "$tmp" && mv "$tmp" "$ETCFS_STATE"
}

state_append() {
    local key="$1" value="$2"
    local tmp
    tmp="$(mktemp)"
    jq ".$key += [$value]" "$ETCFS_STATE" > "$tmp" && mv "$tmp" "$ETCFS_STATE"
}

state_save_array() {
    local key="$1"; shift
    local json
    json=$(printf '%s\n' "$@" | jq -R . | jq -s .)
    state_put "$key" "$json"
}

# ---- AWS discovery ----

discover_vpc() {
    if [[ -n "$SUBNET_ID" ]]; then
        aws ec2 describe-subnets --subnet-ids "$SUBNET_ID" \
            --query 'Subnets[0].VpcId' --output text
    else
        aws ec2 describe-vpcs \
            --filters Name=isDefault,Values=true \
            --query 'Vpcs[0].VpcId' --output text
    fi
}

discover_subnet() {
    if [[ -n "$SUBNET_ID" ]]; then
        echo "$SUBNET_ID"
    else
        aws ec2 describe-subnets \
            --filters "Name=vpc-id,Values=$(discover_vpc)" "Name=availability-zone,Values=$AZ" \
            --query 'Subnets[0].SubnetId' --output text
    fi
}

discover_ami() {
    aws ec2 describe-images \
        --owners amazon \
        --filters \
            "Name=name,Values=al2023-ami-2023*" \
            "Name=architecture,Values=x86_64" \
            "Name=state,Values=available" \
        --query 'sort_by(Images,&CreationDate)[-1].ImageId' --output text
}

# ---- Instance helpers ----

wait_for_instance() {
    local id="$1"
    log "waiting for instance $id to be running..."
    aws ec2 wait instance-running --instance-ids "$id"
}

get_instance_private_ip() {
    local id="$1"
    aws ec2 describe-instances --instance-ids "$id" \
        --query 'Reservations[0].Instances[0].PrivateIpAddress' --output text
}

get_instance_public_ip() {
    local id="$1"
    aws ec2 describe-instances --instance-ids "$id" \
        --query 'Reservations[0].Instances[0].PublicIpAddress' --output text
}

get_instance_state() {
    local id="$1"
    aws ec2 describe-instances --instance-ids "$id" \
        --query 'Reservations[0].Instances[0].State.Name' --output text
}

# ---- SSH / health-check helpers ----

wait_for_ssh() {
    local ip="$1"
    local max_retries="${2:-60}"
    local delay="${3:-5}"
    local i=0
    while [[ $i -lt $max_retries ]]; do
        if ssh $SSH_OPTS "ec2-user@${ip}" "echo ok" &>/dev/null; then
            return 0
        fi
        i=$((i + 1))
        sleep "$delay"
    done
    return 1
}

wait_for_inactive() {
    local ip="$1" svc="$2"
    local max_retries="${3:-30}"
    local delay="${4:-2}"
    local i=0
    while [[ $i -lt $max_retries ]]; do
        local state
        state=$(ssh $SSH_OPTS "ec2-user@${ip}" "systemctl is-active $svc 2>/dev/null || true" 2>/dev/null)
        if [[ "$state" != "active" ]]; then
            return 0
        fi
        i=$((i + 1))
        sleep "$delay"
    done
    return 1
}

wait_for_active() {
    local ip="$1" svc="$2"
    local max_retries="${3:-30}"
    local delay="${4:-2}"
    local i=0
    while [[ $i -lt $max_retries ]]; do
        local state
        state=$(ssh $SSH_OPTS "ec2-user@${ip}" "systemctl is-active $svc 2>/dev/null || true" 2>/dev/null)
        if [[ "$state" == "active" ]]; then
            return 0
        fi
        i=$((i + 1))
        sleep "$delay"
    done
    return 1
}

# wait_for_etcd <ip> [max_retries]
# Waits for local etcd endpoint to respond healthy.
wait_for_etcd() {
    local ip="$1"
    local max_retries="${2:-30}"
    local delay=2
    local i=0
    while [[ $i -lt $max_retries ]]; do
        if ssh $SSH_OPTS "ec2-user@${ip}" \
            "sudo ETCDCTL_API=3 etcdctl \
              --endpoints=https://localhost:2379 \
              --cacert=/etc/etcfuse/ca.crt \
              --cert=/etc/etcfuse/client.crt \
              --key=/etc/etcfuse/client.key \
              endpoint health 2>&1" | grep -q "is healthy"; then
            return 0
        fi
        i=$((i + 1))
        sleep "$delay"
    done
    return 1
}

# wait_for_fuse_mount <ip> [max_retries]
# Waits for the EtcFS FUSE mount to appear.
wait_for_fuse_mount() {
    local ip="$1"
    local max_retries="${2:-60}"
    local delay="${3:-2}"
    local i=0
    while [[ $i -lt $max_retries ]]; do
        if ssh $SSH_OPTS "ec2-user@${ip}" "mountpoint -q $FUSE_MOUNTPOINT 2>/dev/null" 2>/dev/null; then
            return 0
        fi
        i=$((i + 1))
        sleep "$delay"
    done
    return 1
}

# wait_for_daemon_ready <ip> [max_retries] [delay_secs]
# Waits for the EtcFS FUSE daemon to be fully ready:
#   1. systemd unit is active (process running)
#   2. local etcd is healthy
#   3. FUSE mount is present and responsive
wait_for_daemon_ready() {
    local ip="$1"
    local max_retries="${2:-60}"
    local delay="${3:-2}"
    local i=0
    while [[ $i -lt $max_retries ]]; do
        # 1. systemd unit active
        local active
        active=$(ssh $SSH_OPTS "ec2-user@${ip}" "systemctl is-active etcfuse 2>/dev/null || true" 2>/dev/null)
        if [[ "$active" != "active" ]]; then
            i=$((i + 1))
            sleep "$delay"
            continue
        fi

        # 2. etcd healthy
        local etcd_ok
        etcd_ok=$(ssh $SSH_OPTS "ec2-user@${ip}" \
            "sudo ETCDCTL_API=3 etcdctl \
              --endpoints=https://localhost:2379 \
              --cacert=/etc/etcfuse/ca.crt \
              --cert=/etc/etcfuse/client.crt \
              --key=/etc/etcfuse/client.key \
              endpoint health 2>&1 | grep -c 'is healthy' | tr -d '[:space:]'" 2>/dev/null || echo 0)
        if [[ "$etcd_ok" != "1" ]]; then
            i=$((i + 1))
            sleep "$delay"
            continue
        fi

        # 3. FUSE mount responsive
        if ! ssh $SSH_OPTS "ec2-user@${ip}" "test -d $FUSE_MOUNTPOINT && ls $FUSE_MOUNTPOINT >/dev/null 2>&1" 2>/dev/null; then
            i=$((i + 1))
            sleep "$delay"
            continue
        fi

        return 0
    done
    return 1
}

# detect_ebs_dev <ip> — find the shared EBS block device on a node
detect_ebs_dev() {
    local ip="$1"
    local vol_id
    vol_id=$(state_get volume_id)
    $SSH_CMD "ec2-user@${ip}" "
VOL_SERIAL=\${vol_id//-/}
EBS_DEV=\$(lsblk -J -o NAME,SERIAL 2>/dev/null | python3 -c \"
import sys, json
data = json.load(sys.stdin)
for dev in data.get('blockdevices', []):
    if dev.get('serial','') == '\$VOL_SERIAL':
        print('/dev/' + dev['name'])
        break
\" 2>/dev/null || true)
if [[ -z \"\$EBS_DEV\" ]]; then
    for dev in /dev/nvme1n1 /dev/sdf /dev/xvdf; do
        [[ -b \$dev ]] && EBS_DEV=\$dev && break
    done
fi
echo \"\$EBS_DEV\"
"
}

# etcdctl_on <ip> <args...> — run etcdctl on a node
etcdctl_cmd() {
    local ip="$1"; shift
    ssh $SSH_OPTS "ec2-user@${ip}" -- \
        "sudo ETCDCTL_API=3 etcdctl \
          --endpoints=https://localhost:2379 \
          --cacert=/etc/etcfuse/ca.crt \
          --cert=/etc/etcfuse/client.crt \
          --key=/etc/etcfuse/client.key $*" 2>/dev/null
}

# remote_cmd <ip> <cmd...> — run command on a node
remote_cmd() {
    local ip="$1"; shift
    ssh $SSH_OPTS "ec2-user@${ip}" -- "$*"
}

# ---- Init ----
state_init
