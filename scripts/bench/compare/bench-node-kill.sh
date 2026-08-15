#!/bin/bash
# bench-node-kill.sh — kill a node that is holding locks under load and measure
# how long until a surviving node's I/O to those same inodes resumes. The
# flagship number of this suite.
#
# Both nodes hammer one shared file so the victim genuinely owns its lock at
# the moment it dies; the survivor's probe log then gives the interval from
# the kill to its next successful write. What is being compared:
#   etcfs    lease expiry plus a generation guard — no reboot, no replay
#   gfs2     the survivors fence the dead node and replay its journal
#   gluster  the replica set drops to two copies
#   nfs      the server IS the filesystem: killing it is a total outage for
#            every client, not a partial degradation. Reported anyway, as
#            that is exactly the difference worth publishing.
#   juicefs  same shape as nfs — redis + minio on node0 are the single point.
#
# ETCFS_KILL_SETTLE (default 20) is how long the load runs before the kill.
#
# Usage:
#   COMPARE_BACKEND=etcfs ./bench-node-kill.sh
set -euo pipefail
export COMPARE_BACKEND="${COMPARE_BACKEND:-etcfs}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"

SETTLE="${ETCFS_KILL_SETTLE:-20}"
WAIT_AFTER="${ETCFS_KILL_WAIT:-120}"

compare_begin
compare_mount
[[ "${#BENCH_NODES[@]}" -ge 2 ]] || die "node-kill needs two nodes with the filesystem mounted"
SURVIVOR="${BENCH_NODES[0]}"
VICTIM="$(compare_failure_target)"
SHARED="$MOUNT_PATH/kill-target.dat"

log "victim=$VICTIM survivor=$SURVIVOR shared inode=$SHARED"
compare_probe_start "$VICTIM" "$SHARED"
compare_probe_start "$SURVIVOR" "$SHARED"
sleep "$SETTLE"

T0=$(compare_epoch)
log "killing $VICTIM at $T0"
compare_kill_node "$VICTIM"

# Long enough for the slowest recovery in the field: a lease expiry is seconds,
# a journal replay after fencing is tens of seconds.
sleep "$WAIT_AFTER"
compare_probe_stop "$SURVIVOR"

read -r resume stall errs < <(compare_probe_recovery "$SURVIVOR" "$T0")
[[ "$resume" != "-1" ]] || die "survivor $SURVIVOR never completed a write after the kill (>${WAIT_AFTER}s)"

compare_headline node-kill resume_s "$resume" s
compare_headline node-kill max_stall_s "$stall" s
compare_headline node-kill failed_ops_after_kill "$errs" ops
