#!/bin/bash
# bench-storm-ab.sh — the small-file storm run as a paired A/B on ONE cluster.
#
# Every other storm run provisions its own cluster, and five runs on one day
# spanned 1159-1440 s with four of them the same build. Wall clock at n=1
# therefore cannot resolve anything below roughly 20%, which is larger than any
# change measured so far. This alternates the two builds on the same three
# instances — A, B, A, B, ... — so the host, the volume and the etcd cluster
# are shared by both sides and only the daemon differs.
#
# ETCFS_AB_REV_A / ETCFS_AB_REV_B are git revisions (default: HEAD and HEAD~1).
# ETCFS_AB_ROUNDS (default 2) is how many untars each build does.
# ETCFS_STORM_FILES (default 20000) — smaller than the published 80k run
# because this does 2*ROUNDS of them back to back.
#
# Only the Go daemon is swapped between rounds. The C frontend is rebuilt on
# the node at bootstrap and stays at whatever revision the cluster came up on,
# so a revision pair that differs in cmd/etcfuse or pkg/fuse is not measurable
# this way; every perf change this exists to compare has been Go-side.
#
# Usage:
#   ETCFS_AB_REV_B=8821cf9 ./bench-storm-ab.sh
set -euo pipefail
export COMPARE_BACKEND="${COMPARE_BACKEND:-etcfs}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"
[[ "$COMPARE_BACKEND_BASE" == "etcfs" ]] || die "bench-storm-ab.sh is etcfs-only"

REV_A="${ETCFS_AB_REV_A:-HEAD}"
REV_B="${ETCFS_AB_REV_B:-HEAD~1}"
ROUNDS="${ETCFS_AB_ROUNDS:-2}"
FILES="${ETCFS_STORM_FILES:-20000}"

# Built from detached worktrees rather than by checking the tree out back and
# forth: the run is long, and a checkout would leave the working tree on
# whichever revision the run happened to die on.
build_rev() {
    local rev="$1" out="$2" wt
    wt=$(mktemp -d)
    git -C "$COMPARE_PROJECT_ROOT" worktree add -q --detach "$wt" "$rev" || die "no such revision: $rev"
    (cd "$wt" && go build -o "$out" ./cmd/etcfuse-meta/) || die "build failed for $rev"
    git -C "$COMPARE_PROJECT_ROOT" worktree remove --force "$wt"
}

BIN_A="$RESULTS_DIR/etcfuse-meta-A"
BIN_B="$RESULTS_DIR/etcfuse-meta-B"
build_rev "$REV_A" "$BIN_A"
build_rev "$REV_B" "$BIN_B"
SHA_A=$(git rev-parse --short "$REV_A")
SHA_B=$(git rev-parse --short "$REV_B")
log "A=$REV_A ($SHA_A)  B=$REV_B ($SHA_B)  ${ROUNDS} round(s) each, $FILES files"

compare_begin
compare_mount
compare_build_tree "$N0" "$FILES" >/dev/null

# The daemons' own command lines, captured while they are still running:
# compare_etcfs_start relaunches from /tmp/meta.cmd and /tmp/fuse.cmd, and
# nothing writes those files until something asks for them. Without this the
# first build swap kills the daemons and then has no command to bring them
# back with.
for ip in "${COMPARE_PUB_IPS[@]}"; do
    compare_etcfs_snapshot_cmdline "$ip"
done

# deploy_and_restart <binary> — put this build on every node and bring the
# daemons back on it. The mount has to come back everywhere, not just on the
# node the untar runs on, because a node left without one is a peer that
# cannot yield a lock.
deploy_and_restart() {
    local bin="$1" ip
    for ip in "${COMPARE_PUB_IPS[@]}"; do
        $SSH_CMD "ec2-user@$ip" "sudo killall -9 etcfuse-meta etcfuse 2>/dev/null; sudo umount -l $FUSE_MOUNTPOINT 2>/dev/null; true"
        # SSH_OPTS is a word list, not one argument.
        # shellcheck disable=SC2086
        scp -q $SSH_OPTS "$bin" "ec2-user@$ip:/tmp/etcfuse-meta"
        $SSH_CMD "ec2-user@$ip" "sudo cp /tmp/etcfuse-meta /usr/local/bin/etcfuse-meta && sudo chmod +x /usr/local/bin/etcfuse-meta"
    done
    for ip in "${COMPARE_PUB_IPS[@]}"; do
        compare_etcfs_start "$ip" >/dev/null
    done
}

# One untar per round into a directory of its own: a second untar over the same
# names would be measuring overwrite, not create.
round() {
    local side="$1" bin="$2" r="$3" elapsed rate
    deploy_and_restart "$bin"
    compare_etcfs_snapshot_metrics "$N0" "ab-$side-$r-before"
    read -r elapsed rate < <(compare_untar_tree "$N0" "$MOUNT_PATH/storm-$side-$r" "$FILES")
    compare_etcfs_snapshot_metrics "$N0" "ab-$side-$r-after"
    compare_headline storm-ab "untar_s_${side}_r${r}" "$elapsed" s
    echo "$elapsed"
}

sum_a=0 sum_b=0
for r in $(seq 1 "$ROUNDS"); do
    a=$(round A "$BIN_A" "$r")
    b=$(round B "$BIN_B" "$r")
    sum_a=$(awk -v s="$sum_a" -v v="$a" 'BEGIN{printf "%.3f", s+v}')
    sum_b=$(awk -v s="$sum_b" -v v="$b" 'BEGIN{printf "%.3f", s+v}')
done

mean_a=$(compare_div "$sum_a" "$ROUNDS" 3)
mean_b=$(compare_div "$sum_b" "$ROUNDS" 3)
compare_headline storm-ab mean_untar_s_A "$mean_a" s
compare_headline storm-ab mean_untar_s_B "$mean_b" s
compare_headline storm-ab speedup_A_over_B "$(compare_div "$mean_b" "$mean_a" 3)" x
log "A=$SHA_A mean ${mean_a}s   B=$SHA_B mean ${mean_b}s"
