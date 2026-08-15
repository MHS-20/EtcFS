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

# Not $PROJECT_ROOT: state.sh derives that from $0, which here is bench-*.sh
# (three directories deeper than scripts/infra/*.sh, where state.sh assumes
# it's invoked from), so it resolves one level too high once state.sh is
# sourced from this directory instead of run as a top-level script itself.
RESULTS_DIR="$COMPARE_PROJECT_ROOT/benchmark-results/compare/$COMPARE_BACKEND"
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

# compare_destroy — always run from a `trap ... EXIT`. Fire-and-forget, same
# pattern as scripts/test/chaos-lib.sh's own teardown_cluster: nohup+disown so
# the calling bench-*.sh's own exit isn't gated on instance termination /
# volume deletion actually completing, which is what lets independent
# backends run back-to-back (or in parallel, in separate shells) without one
# backend's teardown blocking the next backend's provisioning. destroy-infra.sh
# underneath is still the same synchronous, confirms-every-step script — this
# just moves the wait off the critical path, it doesn't skip it.
compare_destroy() {
    log "=== [$COMPARE_BACKEND] tearing down (async) ==="
    nohup bash "$INFRA_DIR/destroy-infra.sh" --force \
        > "$COMPARE_PROJECT_ROOT/teardown-${COMPARE_BACKEND}.log" 2>&1 &
    disown
}

# compare_begin [extra_teardown] — install the teardown trap and provision.
# Every bench-*.sh opens with this pair; extra_teardown is a command run before
# compare_destroy, for a backend that created resources of its own.
compare_begin() {
    local extra="${1:-}"
    if [[ -n "$extra" ]]; then
        # Expanded now on purpose: extra is the caller's literal command, and
        # the local holding it is gone by the time the trap fires.
        # shellcheck disable=SC2064
        trap "$extra; compare_destroy" EXIT
    else
        trap compare_destroy EXIT
    fi
    compare_provision
}

# compare_install_fio <pub_ip>... — install fio wherever the run will drive it.
# dnf or yum: this suite spans AL2023 and AL2, and the backends that need AL2
# say so in their own headers.
compare_install_fio() {
    local ip
    for ip in "$@"; do
        $SSH_CMD "ec2-user@$ip" \
            "sudo dnf install -y fio >/dev/null 2>&1 || sudo yum install -y fio >/dev/null 2>&1"
    done
}

# compare_shared_device <pub_ip> — the cluster's Multi-Attach volume as seen by
# that node, failing loudly rather than letting a later mkfs run against an
# empty path.
compare_shared_device() {
    local dev
    dev=$(detect_ebs_dev "$1")
    [[ -n "$dev" ]] || die "$COMPARE_BACKEND: no shared EBS device found on $1"
    echo "$dev"
}

# compare_finish <label> — record the run's summary row and say where it went.
compare_finish() {
    compare_summary_row "$1" "$RESULTS_DIR/$1.json"
    log "$1 comparison run complete. Results in $RESULTS_DIR"
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
# compare_open_port <from_port> [to_port] — self-referencing SG ingress rule,
# same pattern as create-infra.sh's own etcd rules. create-infra.sh only opens
# etcd's own ports (2379/2380) plus 9090/9100 — every backend here needs its
# own protocol's port opened before its first cross-node connection, or that
# connection hangs until TCP's own timeout instead of failing fast.
compare_open_port() {
    local from="$1" to="${2:-$1}"
    local sg; sg=$(state_get sg_id | tr -d '"')
    aws ec2 authorize-security-group-ingress --group-id "$sg" \
        --protocol tcp --port "${from}-${to}" --source-group "$sg" >/dev/null 2>&1 || true
}

# compare_open_port_udp — same, for corosync's totem transport (5405/udp).
compare_open_port_udp() {
    local from="$1" to="${2:-$1}"
    local sg; sg=$(state_get sg_id | tr -d '"')
    aws ec2 authorize-security-group-ingress --group-id "$sg" \
        --protocol udp --port "${from}-${to}" --source-group "$sg" >/dev/null 2>&1 || true
}

BACKING_PATH=/mnt/compare-backing
compare_export_backing() {
    local server_pub="$1" server_priv="$2"
    shift 2
    local client_pubs=("$@")

    compare_open_port 2049
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
# Prints one device path per node, in input order. Every volume is tagged
# ClusterName=$ETCFS_CLUSTER,Name=<cluster>-local so compare_destroy_local_volumes
# can find them again by tag instead of by an in-memory list — this function
# is normally called as `mapfile ... < <(compare_create_local_volumes ...)`,
# and process substitution runs the function in a subshell, so any plain
# variable/array it set would never be visible to the caller once it returns.
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
        dev=$($SSH_CMD "ec2-user@$pub" "for d in /dev/nvme2n1 /dev/sdg /dev/xvdg; do [[ -b \$d ]] && echo \$d && break; done")
        [[ -n "$dev" ]] || die "compare_create_local_volumes: device not visible on $pub"
        echo "$dev"
    done
}

# compare_destroy_local_volumes — sweeps by tag rather than an in-memory list,
# so it also catches volumes from an earlier attempt of this same backend
# that failed before reaching its own teardown (this suite's iterate-until-
# green workflow leaves exactly that behind otherwise — every failed retry's
# local volumes, orphaned and still billing). Async, same as compare_destroy:
# force-detach (--force, no wait for "available") and delete immediately —
# a delete that lands before the detach is fully processed just fails and
# retries a few times in the background, off the caller's critical path.
compare_destroy_local_volumes() {
    (
        for vol_id in $(aws ec2 describe-volumes \
            --filters "Name=tag:ClusterName,Values=$ETCFS_CLUSTER" "Name=tag:Name,Values=${ETCFS_CLUSTER}-local" \
            --query 'Volumes[].VolumeId' --output text 2>/dev/null); do
            aws ec2 detach-volume --volume-id "$vol_id" --force >/dev/null 2>&1 || true
            for _ in 1 2 3 4 5; do
                aws ec2 delete-volume --volume-id "$vol_id" >/dev/null 2>&1 && break
                sleep 5
            done
        done
    ) > "$COMPARE_PROJECT_ROOT/teardown-${COMPARE_BACKEND}-local-volumes.log" 2>&1 &
    disown
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
