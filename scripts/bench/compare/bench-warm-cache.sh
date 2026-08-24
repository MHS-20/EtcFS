#!/bin/bash
# bench-warm-cache.sh — repeated reads of a working set that fits in RAM, with
# the page cache left on.
#
# Every other read number in this suite is measured with direct=1, which
# bypasses the page cache by definition. That is the right default for
# comparing the coordination overhead — it makes each operation a genuine round
# trip — but it means nothing in the suite has ever measured the path a
# read-mostly workload actually takes, and etcfs's page cache is the one that
# has to justify a coherence protocol the other backends do not pay for.
#
# So this is the same read twice: once against a dropped cache, once against a
# warm one. The cold number should be unremarkable; the warm number is the
# whole point, and the ratio between them is what says whether a lock-scoped
# page cache is competitive with backends that cache with no coherence
# obligation at all.
#
# The working set is deliberately small enough to fit in page cache with room
# to spare — a set that does not fit measures eviction policy instead.
#
# ETCFS_WARM_SET (default 512M) sizes the working set.
# ETCFS_WARM_RUNTIME (default 30) is the seconds per pass.
#
# Usage:
#   COMPARE_BACKEND=etcfs ./bench-warm-cache.sh
set -euo pipefail
export COMPARE_BACKEND="${COMPARE_BACKEND:-etcfs}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"

SET_SIZE="${ETCFS_WARM_SET:-512M}"
RUNTIME="${ETCFS_WARM_RUNTIME:-30}"
SET_MB=$(( $(numfmt --from=iec "$SET_SIZE") / 1048576 ))

compare_begin
compare_mount

job() {
    cat <<EOF
[warm]
directory=$MOUNT_PATH
filename=warmset.dat
rw=randread
bs=4k
size=$SET_SIZE
direct=0
ioengine=psync
numjobs=1
time_based=1
runtime=$RUNTIME
group_reporting=1
EOF
}

# Lay the file down and drop every cache, so the first pass genuinely reads from
# the device rather than from what writing it just left behind.
log "Laying down a $SET_SIZE working set..."
$SSH_CMD "ec2-user@$N0" \
    "sudo dd if=/dev/urandom of=$MOUNT_PATH/warmset.dat bs=1M count=$SET_MB status=none"
compare_drop_caches "$N0"

cold_json=$(compare_run_job "warm-cache-cold" "$N0" "$RUNTIME" "$(job)")
cold_iops=$(compare_fio_iops "$cold_json" read)

# No drop this time: the pass above is what warmed it.
warm_json=$(compare_run_job "warm-cache-warm" "$N0" "$RUNTIME" "$(job)")
warm_iops=$(compare_fio_iops "$warm_json" read)

compare_headline warm-cache read_iops_cold "$cold_iops" iops
compare_headline warm-cache read_iops_warm "$warm_iops" iops
compare_headline warm-cache warm_speedup "$(compare_div "$warm_iops" "$cold_iops")" x
