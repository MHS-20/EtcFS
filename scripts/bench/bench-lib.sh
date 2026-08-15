#!/bin/bash
# bench-lib.sh — shared fio/AWS helpers for the scripts/bench/ scenario suite.
#
# Mirrors scripts/test/chaos-lib.sh's role for the chaos suite: source this
# after setting RESULTS_DIR and N0 (node0's public IP), on top of an already
# `source`d scripts/infra/state.sh (SSH_CMD, log, die, state_get/put, AZ,
# CLUSTER_NAME come from there).
#
# Every scenario here runs against an already-provisioned, already-mounted
# cluster (create-infra.sh + setup-compute.sh) — this library never creates or
# tears down compute nodes, only the extra EBS/EFS resources a scenario needs.

# ---- generic fio runner ----
#
# One shared file per invocation unless unique_filename=1: fio's default is to
# open exactly the path given, identically, in every thread of every numjobs —
# it does NOT split a shared `filename=` into one file per thread. That default
# is what a contention scenario wants (many threads on one inode) and exactly
# what a throughput scenario does NOT want (see benchmark.sh's fix for the
# etcfs numjobs case) — callers choose which by passing target_spec as either
# `filename=$path` (shared) or `directory=$dir` (fio names one file per thread).
run_fio() {
    local label="$1" target_spec="$2" size="$3" engine="$4" jobs="$5" depth="$6" runtime="$7"
    log "--- fio: $label ---"
    local job="
[global]
ioengine=$engine
direct=1
$target_spec
size=$size
runtime=$runtime
time_based=1
group_reporting=1
[randwrite-4k]
rw=randwrite
bs=4k
numjobs=$jobs
iodepth=$depth
[randread-4k]
rw=randread
bs=4k
numjobs=$jobs
iodepth=$depth
stonewall
"
    echo "$job" | $SSH_CMD "ec2-user@$N0" "cat > /tmp/${label}.fio"
    # timeout, generously above the job's own two-stanza runtime budget: a
    # backend whose I/O errors out on every op (auth failure, dead backing
    # store) can leave fio retrying indefinitely instead of failing fast —
    # seen directly against a misconfigured MinIO, where fio sat for 25+
    # minutes past its 30s runtime instead of erroring out.
    timeout "$((runtime * 4 + 120))" $SSH_CMD "ec2-user@$N0" \
        "sudo fio /tmp/${label}.fio --output-format=json --output=/tmp/${label}.json"
    scp $SSH_OPTS "ec2-user@$N0:/tmp/${label}.json" "$RESULTS_DIR/${label}.json" >/dev/null
}

# ---- EBS: live IOPS modification ----
#
# io2 and gp3 both support modify-volume without detaching, which is what lets
# one already-provisioned cluster sweep several IOPS tiers instead of a fresh
# cluster per tier — the volume the etcfs data path is actually reading and
# writing changes ceiling underneath it, live.
# Returns 0 and logs on success. Returns 1 (not fatal — callers must check)
# and logs a WARNING on AWS's own per-volume modification-rate limit, which
# is a real operational constraint worth surfacing rather than working
# around: AWS enforces a cooldown after a volume's IOPS is changed, and it is
# long enough that a single benchmarking session can hit it after just two
# changes to the same volume in quick succession — a rate limit large enough
# to know about before relying on live IOPS retuning in production, not just
# in a benchmark harness.
modify_volume_iops() {
    local vol_id="$1" iops="$2"
    log "Setting $vol_id to $iops IOPS..."

    # The new IOPS target takes effect as soon as modify-volume is accepted —
    # "optimizing" is background rebalancing of existing data at the new
    # performance level, not a block on serving I/O at it. It IS a block on
    # the *next* modify-volume call, though (rejected with
    # IncorrectModificationState until AWS reports "completed"), and
    # "completed" on a small volume can sit at a stalled-looking progress
    # percentage for a long time — not worth blocking a benchmark sweep on.
    # So: retry the call itself against that rejection, rather than
    # pre-emptively waiting for a state this method doesn't actually need.
    local deadline=$((SECONDS + 600)) out
    while [[ $SECONDS -lt $deadline ]]; do
        if out=$(aws ec2 modify-volume --volume-id "$vol_id" --iops "$iops" 2>&1); then
            log "$vol_id set to $iops IOPS"
            return 0
        fi
        if [[ "$out" == *VolumeModificationRateExceeded* ]]; then
            log "WARNING: $vol_id hit AWS's modification-rate limit, cannot retune this session: $out"
            return 1
        fi
        if [[ "$out" != *IncorrectModificationState* ]]; then
            die "modify_volume_iops: $vol_id: $out"
        fi
        sleep 10
    done
    die "modify_volume_iops: $vol_id still rejecting modify-volume after 10 minutes: $out"
}

# ---- EFS ----
#
# throughput_mode: bursting | provisioned. mibps is only used (and required)
# for provisioned — bursting scales with stored size and burst credits, which
# is the whole point of comparing the two: it has no fixed number to pass.
efs_create() {
    local throughput_mode="$1" mibps="${2:-}" sg_id="$3" subnet_id="$4" name_tag="$5"
    local args=(--performance-mode generalPurpose --throughput-mode "$throughput_mode"
        --tags "Key=ClusterName,Value=$CLUSTER_NAME" "Key=Name,Value=$name_tag")
    [[ "$throughput_mode" == "provisioned" ]] && args+=(--provisioned-throughput-in-mibps "$mibps")

    local efs_id
    efs_id=$(aws efs create-file-system "${args[@]}" --query 'FileSystemId' --output text)
    # NFS (2049) within the cluster SG, self-referencing — idempotent, may
    # already exist from an earlier EFS filesystem's setup.
    # Both streams discarded, not just stderr: this runs inside a function
    # whose own stdout is captured by the caller via command substitution —
    # any stdout leaking out here (e.g. the rule JSON on success) becomes
    # part of the caller's variable, corrupting it silently.
    aws ec2 authorize-security-group-ingress \
        --group-id "$sg_id" --protocol tcp --port 2049 --source-group "$sg_id" >/dev/null 2>&1 || true
    aws efs wait file-system-available --file-system-id "$efs_id" 2>/dev/null || true

    # `wait file-system-available` returning is not proof the mount-target API
    # accepts the filesystem yet — a fresh filesystem can still answer
    # IncorrectFileSystemLifeCycleState for a few seconds after. Retry rather
    # than fail once on a transient race.
    local mt_deadline=$((SECONDS + 60)) mt_out
    while [[ $SECONDS -lt $mt_deadline ]]; do
        if mt_out=$(aws efs create-mount-target --file-system-id "$efs_id" \
            --subnet-id "$subnet_id" --security-groups "$sg_id" 2>&1); then
            break
        fi
        [[ "$mt_out" == *IncorrectFileSystemLifeCycleState* ]] || die "efs_create: create-mount-target: $mt_out"
        sleep 5
    done

    local mt_state deadline=$((SECONDS + 120))
    while [[ $SECONDS -lt $deadline ]]; do
        mt_state=$(aws efs describe-mount-targets --file-system-id "$efs_id" \
            --query 'MountTargets[0].LifeCycleState' --output text 2>/dev/null || echo "")
        [[ "$mt_state" == "available" ]] && break
        sleep 5
    done
    echo "$efs_id"
}

# efs_mount <node_ip> <efs_id> <local_path> — mounts by the mount target's
# own IP address, not its DNS name.
#
# EFS's documented DNS name (fs-id.efs.region.amazonaws.com) is the normal
# way to mount,
# but its private-hosted-zone record can lag mount-target creation by more
# than a few minutes in practice (seen firsthand: still NXDOMAIN 10+ minutes
# after the mount target reported "available", on a VPC with both
# enableDnsSupport and enableDnsHostnames already on — so it isn't a
# misconfigured VPC, just propagation latency this benchmark shouldn't be
# blocked on). The mount target's IP is available the moment
# describe-mount-targets reports it, no propagation involved, and NFS over
# an IP literal works identically to NFS over its resolved name.
efs_mount() {
    local ip="$1" efs_id="$2" path="$3" mt_ip out

    mt_ip=$(aws efs describe-mount-targets --file-system-id "$efs_id" \
        --query 'MountTargets[0].IpAddress' --output text)
    [[ -n "$mt_ip" && "$mt_ip" != "None" ]] || die "efs_mount: no mount target IP for $efs_id"

    $SSH_CMD "ec2-user@$ip" "sudo mkdir -p $path; sudo umount $path 2>/dev/null || true"
    if ! out=$($SSH_CMD "ec2-user@$ip" "sudo mount -t nfs4 -o nfsvers=4.1 $mt_ip:/ $path" 2>&1); then
        die "efs_mount: $ip:$path ($mt_ip): $out"
    fi
}

efs_destroy() {
    local efs_id="$1"
    local mt_ids
    mt_ids=$(aws efs describe-mount-targets --file-system-id "$efs_id" \
        --query 'MountTargets[].MountTargetId' --output text 2>/dev/null || echo "")
    for mt in $mt_ids; do aws efs delete-mount-target --mount-target-id "$mt" 2>/dev/null || true; done
    local deadline=$((SECONDS + 60))
    while [[ $SECONDS -lt $deadline ]]; do
        [[ -z "$(aws efs describe-mount-targets --file-system-id "$efs_id" --query 'MountTargets' --output text 2>/dev/null)" ]] && break
        sleep 5
    done
    aws efs delete-file-system --file-system-id "$efs_id" 2>/dev/null || true
}

# ---- contention: many nodes, one file ----
#
# One fio invocation per node, all pointed at the identical path, launched
# together and waited on afterward — real cross-node contention on one inode,
# not per-node isolated throughput. group_reporting only aggregates within a
# single fio process, so the per-node JSONs are summed here rather than relying
# on fio to do it across hosts.
run_fio_contention() {
    local label="$1" shared_path="$2" size="$3" engine="$4" jobs_per_node="$5" depth="$6" runtime="$7"
    shift 7
    local ips=("$@")
    log "--- contention: $label (${#ips[@]} nodes x $jobs_per_node jobs on $shared_path) ---"

    # continue_on_error: many independent writers on one file/one lock is
    # exactly the case where a lock acquisition can legitimately exhaust its
    # retry budget and return EAGAIN (internal/ipc/datapath.go's lockInode
    # path) — that is contention behavior this scenario exists to measure,
    # not a fault. fio's default is to abort the whole job on the first I/O
    # error, which would report almost no data from the busiest run instead
    # of the error rate under contention; this keeps it going and counts
    # errors as their own metric.
    local job="
[global]
ioengine=$engine
direct=1
filename=$shared_path
size=$size
runtime=$runtime
time_based=1
group_reporting=1
unique_filename=0
continue_on_error=all
[randrw-4k]
rw=randrw
rwmixread=50
bs=4k
numjobs=$jobs_per_node
iodepth=$depth
"
    local pids=()
    for ip in "${ips[@]}"; do
        echo "$job" | $SSH_CMD "ec2-user@$ip" "cat > /tmp/${label}.fio"
        (
            # fio exits nonzero whenever continue_on_error let it run through
            # any error at all — that's expected under contention and not a
            # reason to lose the JSON it still wrote. This subshell inherits
            # set -e, so without the `|| true` here it would exit on that
            # nonzero status and never reach the scp below.
            $SSH_CMD "ec2-user@$ip" \
                "sudo fio /tmp/${label}.fio --output-format=json --output=/tmp/${label}.json" || true
            scp $SSH_OPTS "ec2-user@$ip:/tmp/${label}.json" "$RESULTS_DIR/${label}-$ip.json" >/dev/null
        ) &
        pids+=($!)
    done
    # A node's fio process can exit nonzero (e.g. every op on it errored) and
    # still have produced a partial JSON worth aggregating — one node's
    # failure must not throw away every other node's real data, and a script
    # under set -e must not abort mid-loop before every node has been waited
    # on and every JSON copied back.
    local failed=0
    for pid in "${pids[@]}"; do wait "$pid" || failed=$((failed + 1)); done
    [[ "$failed" -eq 0 ]] || log "WARNING: $failed/${#ips[@]} node(s) hit a fio error during $label — see per-node JSON"

    shopt -s nullglob
    local files=("$RESULTS_DIR/${label}"-*.json)
    shopt -u nullglob
    [[ "${#files[@]}" -gt 0 ]] || die "run_fio_contention: no results came back for $label (every node failed before writing JSON)"

    jq -s '{
        total_write_iops: (map(.jobs[0].write.iops) | add),
        total_read_iops: (map(.jobs[0].read.iops) | add),
        max_write_p99_us: (map(.jobs[0].write.clat_ns.percentile."99.000000" // 0) | max / 1000),
        max_read_p99_us: (map(.jobs[0].read.clat_ns.percentile."99.000000" // 0) | max / 1000),
        total_write_errors: (map(.jobs[0].write.total_err // 0) | add),
        total_read_errors: (map(.jobs[0].read.total_err // 0) | add),
        nodes_reporting: (. | length)
    }' "${files[@]}" > "$RESULTS_DIR/${label}-summary.json"
    cat "$RESULTS_DIR/${label}-summary.json"
}

# ---- summarizing one run_fio() result into a markdown row ----
#
# jobs[0] is randwrite-4k, jobs[1] is randread-4k (see run_fio's job file) —
# each job's own read/write side is zero, since it never issued the other
# op, so this must read the write side of jobs[0] and the read side of
# jobs[1], not both sides of the same job.
fio_row() {
    local label="$1" file="$2"
    jq -r --arg l "$label" '
        (.jobs[0].write.iops) as $wi |
        (.jobs[1].read.iops) as $ri |
        (.jobs[0].write.clat_ns.percentile."99.000000" // 0) as $wp |
        (.jobs[1].read.clat_ns.percentile."99.000000" // 0) as $rp |
        "| \($l) | \($wi | round) | \(($wp/1000) | round) | \($ri | round) | \(($rp/1000) | round) |"
    ' "$file"
}
