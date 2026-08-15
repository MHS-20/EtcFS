#!/bin/bash
# compare-lib.sh — shared provisioning for the etcfs-vs-juicefs-vs-gluster-vs-nfs
# comparison suite (scripts/bench/compare/bench-*.sh).
#
# Each bench-*.sh in this directory owns a fully isolated 3-node cluster + one
# io2 Multi-Attach EBS volume (1000 IOPS by default), created and destroyed by
# itself — never shared with another backend's run, unlike scripts/bench/'s
# other scenarios which all reuse one already-up etcfs cluster. That isolation
# is what create-infra.sh/destroy-infra.sh already give for free (both are
# backend-agnostic: they provision compute + a raw EBS volume, nothing
# etcfs-specific), driven through a private ETCFS_STATE file per backend.
#
# Requires: scripts/infra/create-infra.sh, scripts/infra/destroy-infra.sh.
# Source this after setting COMPARE_BACKEND (e.g. "juicefs") — it derives an
# isolated state file and cluster name from it.

COMPARE_BACKEND="${COMPARE_BACKEND:?set COMPARE_BACKEND before sourcing compare-lib.sh}"
COMPARE_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPARE_PROJECT_ROOT="$(cd "$COMPARE_SCRIPT_DIR/../../.." && pwd)"
INFRA_DIR="$COMPARE_PROJECT_ROOT/scripts/infra"

export ETCFS_STATE="$COMPARE_PROJECT_ROOT/infra-state-compare-${COMPARE_BACKEND}.json"
export ETCFS_CLUSTER="compare-${COMPARE_BACKEND}"
export ETCFS_COMPUTE_NODES="${ETCFS_COMPUTE_NODES:-3}"
export ETCFS_VOLUME_IOPS="${ETCFS_VOLUME_IOPS:-1000}"
export ETCFS_VOLUME_SIZE="${ETCFS_VOLUME_SIZE:-20}"

source "$INFRA_DIR/state.sh"
source "$COMPARE_SCRIPT_DIR/../bench-lib.sh"

RESULTS_DIR="$PROJECT_ROOT/benchmark-results/compare/$COMPARE_BACKEND"
mkdir -p "$RESULTS_DIR"

compare_provision() {
    log "=== [$COMPARE_BACKEND] provisioning 3-node cluster + ${ETCFS_VOLUME_IOPS}-IOPS Multi-Attach volume ==="
    bash "$INFRA_DIR/create-infra.sh"
    mapfile -t COMPARE_PUB_IPS < <(state_get compute_public_ips | jq -r '.[]')
    mapfile -t COMPARE_PRIV_IPS < <(state_get compute_ips | jq -r '.[]')
    COMPARE_VOL_ID=$(state_get volume_id | tr -d '"')
    for ip in "${COMPARE_PUB_IPS[@]}"; do
        wait_for_ssh "$ip" 40 5 || die "compare_provision: $ip never came up over SSH"
    done
}

# compare_destroy — always run from a `trap ... EXIT`, best-effort: a failed
# benchmark run must not leave billing infra behind (see chaos-lib.sh's
# teardown_cluster for the same "async is not worth the orphan risk here"
# call, made the other way — this one blocks, since bench-*.sh scripts run
# one at a time from a shell, not from a chaos loop that needs its next
# iteration unblocked).
compare_destroy() {
    log "=== [$COMPARE_BACKEND] tearing down ==="
    bash "$INFRA_DIR/destroy-infra.sh" --force || log "WARNING: [$COMPARE_BACKEND] teardown reported leftover resources — check AWS console for ClusterName=$ETCFS_CLUSTER"
}

# compare_export_backing <server_pub_ip> <server_priv_ip> <client_pub_ips...>
# — format the shared Multi-Attach volume as ext4 on the server node only
# (Multi-Attach guarantees safe concurrent *block* access, not a concurrent
# filesystem — mounting ext4 from two nodes at once corrupts it) and NFS-export
# it to the rest of the cluster, mounted at an identical path everywhere.
#
# nfs backend benchmarks this mount directly. gluster and juicefs use it as
# their shared backing store instead of local per-node disks, because a
# single 1000-IOPS Multi-Attach volume — the resource shape this suite was
# asked to give every backend — has no per-node-local storage to hand out;
# an NFS re-export is the closest same-shaped substitute, not how either
# product would be deployed in production. Their own reports say so.
BACKING_PATH=/mnt/compare-backing
compare_export_backing() {
    local server_pub="$1" server_priv="$2"
    shift 2
    local client_pubs=("$@")

    local dev
    dev=$(detect_ebs_dev "$server_pub")
    [[ -n "$dev" ]] || die "compare_export_backing: no EBS device found on $server_pub"

    $SSH_CMD "ec2-user@$server_pub" "
        sudo mkfs.ext4 -q -F $dev
        sudo mkdir -p $BACKING_PATH
        sudo mount $dev $BACKING_PATH
        sudo dnf install -y nfs-utils >/dev/null 2>&1 || sudo yum install -y nfs-utils >/dev/null 2>&1
        echo '$BACKING_PATH *(rw,sync,no_subtree_check,no_root_squash)' | sudo tee /etc/exports >/dev/null
        sudo systemctl enable --now nfs-server >/dev/null 2>&1
        sudo exportfs -ra
    "
    for ip in "${client_pubs[@]}"; do
        $SSH_CMD "ec2-user@$ip" "
            sudo dnf install -y nfs-utils >/dev/null 2>&1 || sudo yum install -y nfs-utils >/dev/null 2>&1
            sudo mkdir -p $BACKING_PATH
            sudo mount -t nfs4 $server_priv:$BACKING_PATH $BACKING_PATH
        "
    done
}

# compare_create_local_volumes <iops> <pub_ip>:<priv_ip>:<instance_id> ...
# — one independent io2 volume per node, attached and formatted as that
# node's own local disk. This is for backends that replicate across
# independent per-node storage (Gluster) rather than one device shared by
# every node (etcfs's raw Multi-Attach volume, or NFS/JuiceFS's single
# server-side disk behind a network protocol) — giving them the *shared*
# volume instead would mean either corrupting it (concurrent ext4 mounts on
# one Multi-Attach device) or relaying every brick through NFS, which is not
# how the product is deployed and would benchmark the relay, not the product.
# Prints one device path per node, in input order. Volume IDs are appended to
# COMPARE_LOCAL_VOL_IDS (global array) so compare_destroy_local_volumes can
# find them again.
COMPARE_LOCAL_VOL_IDS=()
compare_create_local_volumes() {
    local iops="$1"
    shift
    local entry pub inst vol_id dev
    for entry in "$@"; do
        IFS=: read -r pub _ inst <<< "$entry"
        vol_id=$(aws ec2 create-volume --volume-type io2 --size "$ETCFS_VOLUME_SIZE" \
            --iops "$iops" --availability-zone "$AZ" \
            --tag-specifications "ResourceType=volume,Tags=[{Key=ClusterName,Value=$ETCFS_CLUSTER},{Key=Name,Value=${ETCFS_CLUSTER}-local}]" \
            --query 'VolumeId' --output text)
        aws ec2 wait volume-available --volume-ids "$vol_id" 2>/dev/null || true
        aws ec2 attach-volume --volume-id "$vol_id" --instance-id "$inst" --device /dev/sdg >/dev/null
        aws ec2 wait volume-in-use --volume-ids "$vol_id" 2>/dev/null || true
        COMPARE_LOCAL_VOL_IDS+=("$vol_id")
        dev=$($SSH_CMD "ec2-user@$pub" "for d in /dev/nvme2n1 /dev/sdg /dev/xvdg; do [[ -b \$d ]] && echo \$d && break; done")
        [[ -n "$dev" ]] || die "compare_create_local_volumes: device not visible on $pub"
        echo "$dev"
    done
}

compare_destroy_local_volumes() {
    local vol_id
    for vol_id in "${COMPARE_LOCAL_VOL_IDS[@]}"; do
        aws ec2 detach-volume --volume-id "$vol_id" >/dev/null 2>&1 || true
        aws ec2 wait volume-available --volume-ids "$vol_id" 2>/dev/null || true
        aws ec2 delete-volume --volume-id "$vol_id" >/dev/null 2>&1 \
            || log "WARNING: local volume $vol_id was not deleted — check manually"
    done
}

# compare_summary_row <label> <fio_json> — appends one row to this backend's
# own summary.json (an array of {label, write_iops, write_p99_us, read_iops,
# read_p99_us}), the shape report.sh reads back across every backend.
compare_summary_row() {
    local label="$1" file="$2"
    local row
    row=$(jq --arg l "$label" '{
        label: $l,
        write_iops: (.jobs[0].write.iops // 0 | round),
        write_p99_us: ((.jobs[0].write.clat_ns.percentile."99.000000" // 0) / 1000 | round),
        read_iops: (.jobs[1].read.iops // 0 | round),
        read_p99_us: ((.jobs[1].read.clat_ns.percentile."99.000000" // 0) / 1000 | round)
    }' "$file")
    local summary="$RESULTS_DIR/summary.json"
    [[ -f "$summary" ]] || echo "[]" > "$summary"
    jq --argjson r "$row" '. + [$r]' "$summary" > "$summary.tmp" && mv "$summary.tmp" "$summary"
}
