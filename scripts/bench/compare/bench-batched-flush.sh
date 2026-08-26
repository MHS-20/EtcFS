#!/bin/bash
# bench-batched-flush.sh — etcfs only. Many files open at once on every node,
# written to slowly and never closed, so their buffered extents are published
# by the interval sweep rather than by close(). That is the one workload the
# cross-inode flush batch was written for: the small-file storm cannot show it,
# because there close() publishes each file before the sweep ever sees it.
#
# The headline is inodes published per batched transaction. One means the batch
# is a single flush under another name and buys nothing; anything above it is
# the commit count the sweep saved.
#
# ETCFS_FLUSH_FILES (default 256) — files held open per node.
# ETCFS_FLUSH_RUNTIME (default 300) — seconds of writing.
#
# Usage:
#   ./bench-batched-flush.sh
set -euo pipefail
export COMPARE_BACKEND="${COMPARE_BACKEND:-etcfs}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"
[[ "$COMPARE_BACKEND_BASE" == "etcfs" ]] || die "bench-batched-flush.sh is etcfs-only"

FILES="${ETCFS_FLUSH_FILES:-256}"
RUNTIME="${ETCFS_FLUSH_RUNTIME:-300}"

compare_begin
compare_mount

# One writer process per node, holding every file open for the whole run: a
# python process because the shell has no way to keep hundreds of descriptors
# open and write across them in a loop. Writes are small and spread over all
# the files so that every sweep finds many inodes dirty at once, which is the
# state the batch exists to handle.
WRITER=$(cat <<'PY'
import os, sys, time
d, n, secs = sys.argv[1], int(sys.argv[2]), float(sys.argv[3])
os.makedirs(d, exist_ok=True)
fds = [os.open(os.path.join(d, "f%d" % i), os.O_CREAT | os.O_WRONLY, 0o644) for i in range(n)]
end, off = time.time() + secs, 0
payload = b"x" * 4096
while time.time() < end:
    for fd in fds:
        os.pwrite(fd, payload, off)
    off += 4096
    time.sleep(0.5)
for fd in fds:
    os.close(fd)
PY
)

for i in "${!BENCH_NODES[@]}"; do
    $SSH_CMD "ec2-user@${BENCH_NODES[$i]}" \
        "cat > /tmp/flushwriter.py <<'EOF2'
$WRITER
EOF2
sudo dnf install -y python3 >/dev/null 2>&1 || true"
done

compare_etcfs_snapshot_metrics "$N0" "before-batched-flush"
for i in "${!BENCH_NODES[@]}"; do
    $SSH_CMD -n -f "ec2-user@${BENCH_NODES[$i]}" \
        "sudo python3 /tmp/flushwriter.py $MOUNT_PATH/flushbatch-$i $FILES $RUNTIME >/dev/null 2>&1" &
done
wait
sleep 10
compare_etcfs_snapshot_metrics "$N0" "after-batched-flush"

# Read the two counters' deltas off the snapshots this node just took, rather
# than the absolute values, so a daemon that had done other work before the run
# does not count towards the ratio.
counter() { awk -v k="$2" '$1 == k {v=$2} END {printf "%.0f", v+0}' "$RESULTS_DIR/metrics-$1.txt"; }
batches=$(( $(counter after-batched-flush etcfuse_metadata_flush_batch_total) \
          - $(counter before-batched-flush etcfuse_metadata_flush_batch_total) ))
inodes=$(( $(counter after-batched-flush etcfuse_metadata_flush_batch_inodes_total) \
         - $(counter before-batched-flush etcfuse_metadata_flush_batch_inodes_total) ))

compare_headline batched-flush files_open_per_node "$FILES" files
compare_headline batched-flush flush_batches "$batches" txns
compare_headline batched-flush inodes_flushed "$inodes" inodes
compare_headline batched-flush inodes_per_batch "$(compare_div "$inodes" "${batches:-0}" 3)" inodes/txn
