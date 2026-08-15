#!/bin/bash
# bench-volume-growth.sh — etcfs only. Fill the filesystem until writes fail
# for space, grow the shared EBS volume underneath the running cluster, and
# time how long until the new space is actually allocatable — with no daemon
# restart anywhere, which is the claim being tested.
#
# The daemon re-reads the device size when an arena acquisition fails for space
# and retries the allocation once if the volume grew (Device.RefreshSize,
# internal/ipc/service.go), so the writer that hit ENOSPC is itself what
# notices the growth: the poll loop below is the measurement, not a workaround.
#
# GFS2 by contrast needs gfs2_grow plus a mount that notices; NFS grows
# server-side transparently. Neither is run here — this is about the shared
# raw-device path.
#
# Usage:
#   ./bench-volume-growth.sh
set -euo pipefail
export COMPARE_BACKEND="${COMPARE_BACKEND:-etcfs}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"
[[ "$COMPARE_BACKEND_BASE" == "etcfs" ]] || die "bench-volume-growth.sh is etcfs-only"

GROW_BY="${ETCFS_GROW_BY_GIB:-10}"
POLL_TIMEOUT="${ETCFS_GROW_TIMEOUT:-600}"

compare_begin
compare_mount

log "Filling $MOUNT_PATH until a write fails for space..."
$SSH_CMD "ec2-user@$N0" "
    i=0
    while sudo dd if=/dev/zero of=$MOUNT_PATH/fill-\$i.dat bs=1M count=512 oflag=direct >/dev/null 2>&1; do
        i=\$((i + 1))
    done
    echo \"filled with \$i x 512MiB files\"
"

NEW_SIZE=$((ETCFS_VOLUME_SIZE + GROW_BY))
log "Growing $COMPARE_VOL_ID to ${NEW_SIZE} GiB"
T0=$(compare_epoch)
aws ec2 modify-volume --volume-id "$COMPARE_VOL_ID" --size "$NEW_SIZE" >/dev/null

# The write that succeeds is the definition of "allocatable": statfs numbers
# can move for reasons other than the new space being usable.
$SSH_CMD "ec2-user@$N0" "
    deadline=\$(( \$(date +%s) + $POLL_TIMEOUT ))
    until sudo dd if=/dev/zero of=$MOUNT_PATH/post-growth.dat bs=1M count=64 oflag=direct >/dev/null 2>&1; do
        [[ \$(date +%s) -lt \$deadline ]] || exit 1
        sleep 1
    done
" || die "no write succeeded within ${POLL_TIMEOUT}s of the volume growing"
T1=$(compare_epoch)

compare_headline volume-growth allocatable_after_s \
    "$(awk -v a="$T0" -v b="$T1" 'BEGIN{printf "%.3f", b-a}')" s
compare_headline volume-growth grown_by_gib "$GROW_BY" GiB
