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
# to spare — a set that does not fit measures eviction policy instead — and the
# whole of it is read sequentially between the two passes.
#
# That pre-warm is not a nicety. Random 4 KiB reads at a device-bound ~1000 IOPS
# touch only ~124 MiB in a 30 s pass, so with a 512 MiB set the "warm" pass was
# still reading blocks that had never been cached and the result came out at
# exactly 1.00x. Reading the file end to end guarantees the set really is
# resident.
#
# The pre-warm only survives because of the two fio settings below, and both
# are off fio's defaults:
#
#   invalidate=0   fio otherwise calls posix_fadvise(POSIX_FADV_DONTNEED) over
#                  the whole file as it opens it, which throws away the cache
#                  the pre-warm just built -- every time, immediately before the
#                  pass that exists to measure it.
#   fadvise_hint=0 fio otherwise declares POSIX_FADV_RANDOM, which sets the
#                  file's readahead to zero. With the pre-warm discarded, that
#                  leaves the pass rebuilding the cache one 4 KiB page per
#                  device I/O, and at ~1000 IOPS it never catches up inside
#                  30 s.
#
# Together they made this scenario report a working page cache as an inert one:
# warm equal to cold, every read reaching the daemon, on a filesystem that
# serves a genuinely warm 256 MiB set at over a million IOPS. Anything measuring
# a *cache* has to leave the cache alone; the O_DIRECT scenarios elsewhere in
# the suite are the ones that want a cold read, and they ask for it with
# direct=1 rather than by discarding what another step deliberately built.
#
# ETCFS_WARM_SET (default 256M) sizes the working set.
# ETCFS_WARM_RUNTIME (default 30) is the seconds per pass.
#
# Usage:
#   COMPARE_BACKEND=etcfs ./bench-warm-cache.sh
set -euo pipefail
export COMPARE_BACKEND="${COMPARE_BACKEND:-etcfs}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"

SET_SIZE="${ETCFS_WARM_SET:-256M}"
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
invalidate=0
fadvise_hint=0
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

# Read the whole file so the set is genuinely resident, rather than relying on
# whatever fraction the random cold pass happened to touch.
log "Pre-warming the page cache with a full sequential read..."
$SSH_CMD "ec2-user@$N0" "sudo dd if=$MOUNT_PATH/warmset.dat of=/dev/null bs=1M status=none"

# Count the READ requests that actually reach the daemon across the warm pass.
# This is what makes the result interpretable rather than merely a number: if
# the kernel is serving from its page cache the daemon sees almost none, and if
# it is not, the daemon sees one per read. Without it, "warm equals cold" cannot
# be told apart from "the cache works but something keeps dropping it".
reads_before=$(compare_etcfs_read_ops "$N0")

warm_json=$(compare_run_job "warm-cache-warm" "$N0" "$RUNTIME" "$(job)")
warm_iops=$(compare_fio_iops "$warm_json" read)

reads_after=$(compare_etcfs_read_ops "$N0")

# Re-checked after the pass, not only at mount time: the state that matters is
# the one the measurement ran under, and the client can be lost in between.
cache_state=$(compare_etcfs_page_cache "$N0")
[[ "$cache_state" == "up" ]] || log "WARNING page cache was '$cache_state' after the warm pass; the numbers below are device-bound"
if [[ -n "$reads_before" && -n "$reads_after" ]]; then
    compare_headline warm-cache daemon_reads_during_warm "$((reads_after - reads_before))" reads
fi

compare_headline warm-cache read_iops_cold "$cold_iops" iops
compare_headline warm-cache read_iops_warm "$warm_iops" iops
compare_headline warm-cache warm_speedup "$(compare_div "$warm_iops" "$cold_iops")" x
