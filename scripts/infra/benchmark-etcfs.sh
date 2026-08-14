#!/bin/bash
# benchmark-etcfs.sh — IOPS/latency benchmark of EtcFS only, on node0 of an
# already-provisioned cluster.
#
# Same fio job as benchmark.sh (4k random write, 4k random read, 128k
# sequential write), but against $FUSE_MOUNTPOINT alone — no raw/ext4/EFS
# scratch volume is provisioned or mounted. Use this instead of
# benchmark.sh when the raw/ext4/EFS comparison rows aren't wanted, e.g.
# re-measuring after a data-path change with the cluster's own io2 volume
# (set ETCFS_VOLUME_IOPS=1000 in create-infra.sh) as the only target.
#
# Requires: create-infra.sh + setup-compute.sh already run (state file has
# compute nodes and a mounted EtcFS).
#
# Usage:
#   ./benchmark-etcfs.sh                    # 30s per job (default)
#   ETCFS_BENCH_RUNTIME=60 ./benchmark-etcfs.sh
#   ETCFS_BENCH_READONLY=1 ./benchmark-etcfs.sh   # randread-4k only
#
# Output: JSON fio results + Markdown summary in
# $PROJECT_ROOT/benchmark-results/.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/state.sh"

RUNTIME="${ETCFS_BENCH_RUNTIME:-30}"
RESULTS_DIR="$PROJECT_ROOT/benchmark-results"
mkdir -p "$RESULTS_DIR"

mapfile -t PUB_IPS < <(state_get compute_public_ips 2>/dev/null | jq -r '.[]' 2>/dev/null)
[[ "${#PUB_IPS[@]}" -ge 1 ]] || die "No compute nodes in state. Run create-infra.sh and setup-compute.sh first."
N0="${PUB_IPS[0]}"

log "=== EtcFS-only benchmark ==="
log "Node:        $N0"
log "Runtime:     ${RUNTIME}s per job"
log "Target:      $FUSE_MOUNTPOINT (${VOLUME_IOPS} IOPS io2 data volume)"
log ""

$SSH_CMD "ec2-user@$N0" "sudo dnf install -y fio >/dev/null 2>&1 || sudo yum install -y fio >/dev/null 2>&1"

# Refuse to run against a mountpoint that is not mounted.  fio's directory= is
# an ordinary path, so an EtcFS mount that failed to come up leaves it writing
# to a plain directory on the instance's root volume — and reporting that
# volume's numbers as EtcFS's.  A root gp3's 3000-IOPS baseline is well above
# the io2 data volume's provisioning, so the failure reads as a spectacular
# result rather than as a broken run.  The most common cause is a node that was
# fenced (and so had the shared volume detached) since it was last bootstrapped.
if ! $SSH_CMD "ec2-user@$N0" "mountpoint -q $FUSE_MOUNTPOINT" 2>/dev/null; then
    die "$FUSE_MOUNTPOINT is not mounted on $N0 — benchmarking it would measure the root volume, not EtcFS. Re-run bootstrap-cluster.sh and check the daemon logs."
fi

# Matches run_fio's etcfs branch in benchmark.sh: psync (not libaio — FUSE
# doesn't reliably support AIO), one file per thread via directory= +
# filename_format= so numjobs actually exercises concurrent FUSE workers on
# separate inodes instead of serializing on one shared file's lock. 8M size
# keeps fio's own file-layout pass short at etcfs's QD1 write rate.
JOBS="${ETCFS_BENCH_JOBS:-4}"
READONLY="${ETCFS_BENCH_READONLY:-0}"
JOB="
[global]
ioengine=psync
direct=1
directory=$FUSE_MOUNTPOINT
filename_format=fio.\$jobname.\$jobnum.\$filenum
size=8M
runtime=$RUNTIME
time_based=1
group_reporting=1
[randread-4k]
rw=randread
bs=4k
numjobs=$JOBS
iodepth=1
"
if [[ "$READONLY" != "1" ]]; then
JOB="$JOB
[randwrite-4k]
rw=randwrite
bs=4k
numjobs=$JOBS
iodepth=1
stonewall
[seqwrite-128k]
rw=write
bs=128k
numjobs=1
iodepth=1
stonewall
"
fi
echo "$JOB" | $SSH_CMD "ec2-user@$N0" "cat > /tmp/etcfs.fio"
$SSH_CMD "ec2-user@$N0" \
    "sudo fio /tmp/etcfs.fio --output-format=json --output=/tmp/etcfs.json"
scp $SSH_OPTS "ec2-user@$N0:/tmp/etcfs.json" "$RESULTS_DIR/etcfs.json" >/dev/null
$SSH_CMD "ec2-user@$N0" "sudo rm -f $FUSE_MOUNTPOINT/fio.* 2>/dev/null || true"

SUMMARY="$RESULTS_DIR/summary-etcfs.md"
f="$RESULTS_DIR/etcfs.json"
{
    if [[ "$READONLY" == "1" ]]; then
        echo "| Target | randread-4k IOPS | randread-4k p99 (us) |"
        echo "|---|---|---|"
        riops=$(jq -r '.jobs[0].read.iops // 0 | floor' "$f")
        rp99=$(jq -r '.jobs[0].read.clat_ns.percentile."99.000000" // 0 | . / 1000 | floor' "$f")
        echo "| etcfs | $riops | $rp99 |"
    else
        echo "| Target | randwrite-4k IOPS | randwrite-4k p99 (us) | randread-4k IOPS | randread-4k p99 (us) | seqwrite-128k MiB/s |"
        echo "|---|---|---|---|---|---|"
        riops=$(jq -r '.jobs[0].read.iops // 0 | floor' "$f")
        rp99=$(jq -r '.jobs[0].read.clat_ns.percentile."99.000000" // 0 | . / 1000 | floor' "$f")
        wiops=$(jq -r '.jobs[1].write.iops // 0 | floor' "$f")
        wp99=$(jq -r '.jobs[1].write.clat_ns.percentile."99.000000" // 0 | . / 1000 | floor' "$f")
        seqbw=$(jq -r '.jobs[2].write.bw // 0 | . / 1024 | floor' "$f")
        echo "| etcfs | $wiops | $wp99 | $riops | $rp99 | $seqbw |"
    fi
    echo ""
    echo "Data volume: ${VOLUME_IOPS} IOPS (io2)."
} | tee "$SUMMARY"

log ""
log "============================================"
log "Benchmark complete. Raw fio JSON + summary in $RESULTS_DIR"
log "============================================"
