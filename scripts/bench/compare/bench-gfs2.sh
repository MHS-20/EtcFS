#!/bin/bash
# bench-gfs2.sh — GFS2 (Red Hat's shared-disk cluster filesystem) on its own
# 3-node cluster, all three mounting the cluster's raw Multi-Attach volume
# directly — the actual same-shared-device model etcfs uses, unlike
# Gluster/JuiceFS which federate independent per-node/object storage. This is
# the closest real competitor to etcfs's own deployment shape in this suite.
#
# Runs on Amazon Linux 2, not AL2023 (see ETCFS_AMI_NAME_FILTER in
# scripts/infra/state.sh): AL2023 dropped the gfs2-utils/dlm/corosync
# packages GFS2 needs, AL2 still carries them.
#
# GFS2 requires cluster-wide locking (DLM) backed by a quorum service
# (corosync) before the shared device can be mounted from more than one node
# at once — mounting without it is the one thing this script cannot skip or
# simplify away, since an unfenced concurrent mount is exactly the corruption
# case Multi-Attach's block-level guarantee does NOT extend to a filesystem.
#
# Usage:
#   ./bench-gfs2.sh
set -euo pipefail
export COMPARE_BACKEND=gfs2
export ETCFS_AMI_NAME_FILTER="amzn2-ami-hvm-*-x86_64-gp2"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"

compare_begin
N0="${COMPARE_PUB_IPS[0]}"; N1="${COMPARE_PUB_IPS[1]}"; N2="${COMPARE_PUB_IPS[2]}"
P0="${COMPARE_PRIV_IPS[0]}"; P1="${COMPARE_PRIV_IPS[1]}"; P2="${COMPARE_PRIV_IPS[2]}"
IPS=("$N0" "$N1" "$N2")
PRIVS=("$P0" "$P1" "$P2")

compare_open_port_udp 5404 5405  # corosync totem
compare_open_port 21064          # dlm_controld

log "Installing corosync/dlm/gfs2-utils on all nodes..."
for ip in "${IPS[@]}"; do
    $SSH_CMD "ec2-user@$ip" "set -e; sudo yum install -y corosync dlm gfs2-utils"
done

log "Writing corosync.conf (3-node quorum) on all nodes..."
# crypto_hash/crypto_cipher: none — crypto needs an authkey (corosync-keygen)
# this harness never generates; without one corosync's default crypto
# refuses to start at all. Off is fine: trusted VPC-internal traffic,
# ephemeral infra.
#
# nodelist entries are written one key per line, not the compact
# `node { ring0_addr: x; nodeid: n; }` single-line form: corosync.conf's
# parser is line-oriented, not brace-matching, and the compact form fails
# with a bare "Missing closing brace" — confirmed directly against this file.
COROSYNC_CONF="
totem {
    version: 2
    cluster_name: comparegfs2
    transport: udpu
    crypto_hash: none
    crypto_cipher: none
}
nodelist {
$(for i in 0 1 2; do printf '    node {\n        ring0_addr: %s\n        nodeid: %d\n    }\n' "${PRIVS[$i]}" "$((i+1))"; done)
}
quorum {
    provider: corosync_votequorum
}
logging {
    to_syslog: yes
}
"
for ip in "${IPS[@]}"; do
    $SSH_CMD "ec2-user@$ip" "sudo mkdir -p /etc/corosync"
    echo "$COROSYNC_CONF" | $SSH_CMD "ec2-user@$ip" "sudo tee /etc/corosync/corosync.conf >/dev/null"
    $SSH_CMD "ec2-user@$ip" "echo '--- corosync.conf on '\$(hostname)' ---'; sudo cat -A /etc/corosync/corosync.conf"
    $SSH_CMD "ec2-user@$ip" "sudo systemctl enable --now corosync || { sudo timeout 5 corosync -f 2>&1; sudo journalctl -xeu corosync --no-pager | tail -30; exit 1; }"
done

log "Waiting for corosync quorum..."
for i in $(seq 1 30); do
    $SSH_CMD "ec2-user@$N0" "sudo corosync-quorumtool -s 2>/dev/null | grep -q 'Quorate:.*Yes'" && break
    sleep 2
done

log "Starting DLM on all nodes..."
for ip in "${IPS[@]}"; do
    $SSH_CMD "ec2-user@$ip" "sudo systemctl enable --now dlm"
done
sleep 5

dev=$(compare_shared_device "$N0")

log "Formatting $dev as GFS2 (3 journals, lock_dlm) from $N0..."
$SSH_CMD "ec2-user@$N0" "sudo mkfs.gfs2 -O -p lock_dlm -t comparegfs2:vol1 -j 3 $dev"

log "Mounting on all nodes..."
for ip in "${IPS[@]}"; do
    $SSH_CMD "ec2-user@$ip" "sudo mkdir -p /mnt/compare-gfs2 && sudo mount -t gfs2 -o noatime $dev /mnt/compare-gfs2"
done
compare_install_fio "$N0"

run_fio "gfs2" "filename=/mnt/compare-gfs2/fio.dat" 1G libaio 4 32 "${ETCFS_BENCH_RUNTIME:-30}"
compare_finish gfs2
