#!/bin/bash
# bench-gluster.sh — GlusterFS replica-3 on its own 3-node cluster, each node
# backed by its own independent 1000-IOPS io2 volume as a true local brick —
# Gluster's real deployment model (it replicates across independent per-node
# storage; it does not take one shared block device). The cluster's own
# Multi-Attach volume from compare_provision is left unused here for the same
# reason: mounting it would mean either only one node ever formatting it, or
# relaying every brick over NFS, neither of which is how Gluster runs in
# production and both of which would benchmark the relay, not Gluster.
#
# Runs on Amazon Linux 2, not AL2023 (see ETCFS_AMI_NAME_FILTER in
# scripts/infra/state.sh): AL2023 has no glusterfs-server package at all.
# AL2's own amzn2-core repo ships GlusterFS *client* packages (cli/fuse/
# client-xlators) but not glusterfs-server — that needs the CentOS Storage
# SIG's el7 build (vault.centos.org, no gpgcheck: ephemeral infra, torn down
# at the end of this same script) plus EPEL's userspace-rcu — the one runtime
# lib el7's gluster9 build needs that AL2 base doesn't carry at a new enough
# ABI (AL2 ships its own older userspace-rcu under the same package name, and
# yum's priorities plugin hides EPEL's behind it; --disableplugin=priorities
# on just that one install is what gets the newer one).
#
# Usage:
#   ./bench-gluster.sh
set -euo pipefail
export COMPARE_BACKEND=gluster
export ETCFS_AMI_NAME_FILTER="amzn2-ami-hvm-*-x86_64-gp2"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"

trap 'compare_destroy_local_volumes; compare_destroy' EXIT
compare_provision
N0="${COMPARE_PUB_IPS[0]}"
N1="${COMPARE_PUB_IPS[1]}"
N2="${COMPARE_PUB_IPS[2]}"
P0="${COMPARE_PRIV_IPS[0]}"; P1="${COMPARE_PRIV_IPS[1]}"; P2="${COMPARE_PRIV_IPS[2]}"
INST0=$(state_get compute_instance_ids | jq -r '.[0]')
INST1=$(state_get compute_instance_ids | jq -r '.[1]')
INST2=$(state_get compute_instance_ids | jq -r '.[2]')

# glusterd management (24007) + brick daemons (49152-49251, one per brick —
# 3 bricks here, wide range is glusterd's own default).
compare_open_port 24007
compare_open_port 49152 49251

log "Creating one local 1000-IOPS brick volume per node..."
mapfile -t BRICK_DEVS < <(compare_create_local_volumes "$ETCFS_VOLUME_IOPS" \
    "$N0:$P0:$INST0" "$N1:$P1:$INST1" "$N2:$P2:$INST2")

log "Installing glusterd + formatting local brick on each node..."
IPS=("$N0" "$N1" "$N2")
for i in 0 1 2; do
    $SSH_CMD "ec2-user@${IPS[$i]}" "set -e
        sudo amazon-linux-extras install -y epel
        printf '%s\n' '[gluster9]' 'name=Gluster 9' \
            'baseurl=https://vault.centos.org/centos/7/storage/\$basearch/gluster-9/' \
            'gpgcheck=0' 'enabled=1' | sudo tee /etc/yum.repos.d/gluster9.repo >/dev/null
        sudo yum install -y --disableplugin=priorities userspace-rcu
        sudo yum install -y xfsprogs ncurses-compat-libs
        sudo yum install -y --disablerepo=amzn2-core --disableplugin=priorities glusterfs-server
        sudo mkfs.xfs -f ${BRICK_DEVS[$i]}
        sudo mkdir -p /mnt/gluster-brick
        sudo mount ${BRICK_DEVS[$i]} /mnt/gluster-brick
        sudo mkdir -p /mnt/gluster-brick/brick
        sudo systemctl enable --now glusterd
    "
done

$SSH_CMD "ec2-user@$N0" "sudo gluster peer probe $P1 && sudo gluster peer probe $P2"
sleep 5
$SSH_CMD "ec2-user@$N0" "set -e
    sudo gluster volume create compare-vol replica 3 \
        $P0:/mnt/gluster-brick/brick $P1:/mnt/gluster-brick/brick $P2:/mnt/gluster-brick/brick force
    sudo gluster volume start compare-vol
"
sleep 5

$SSH_CMD "ec2-user@$N1" "set -e
    sudo yum install -y glusterfs-fuse fio >/dev/null 2>&1
    sudo mkdir -p /mnt/compare-gluster
    sudo mount -t glusterfs $P0:/compare-vol /mnt/compare-gluster
"

N0="$N1"
run_fio "gluster" "filename=/mnt/compare-gluster/fio.dat" 1G libaio 4 32 "${ETCFS_BENCH_RUNTIME:-30}"
compare_summary_row gluster "$RESULTS_DIR/gluster.json"

log "gluster comparison run complete. Results in $RESULTS_DIR"
