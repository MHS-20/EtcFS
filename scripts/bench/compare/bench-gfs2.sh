#!/bin/bash
# bench-gfs2.sh — GFS2 (Red Hat's shared-disk cluster filesystem) on its own
# 3-node cluster, all three mounting the cluster's raw Multi-Attach volume
# directly — the actual same-shared-device model etcfs uses, unlike
# Gluster/JuiceFS which federate independent per-node/object storage. This is
# the closest real competitor to etcfs's own deployment shape in this suite.
#
# Runs on Amazon Linux 2, not AL2023 (see ETCFS_AMI_NAME_FILTER in
# scripts/infra/state.sh): AL2023 dropped the gfs2-utils/dlm/corosync
# packages GFS2 needs, AL2 still carries them.
#
# GFS2 requires cluster-wide locking (DLM) backed by a quorum service
# (corosync) before the shared device can be mounted from more than one node
# at once — mounting without it is the one thing this script cannot skip or
# simplify away, since an unfenced concurrent mount is exactly the corruption
# case Multi-Attach's block-level guarantee does NOT extend to a filesystem.
#
# ETCFS_BENCH_DIRECT=0 runs the warm-page-cache variant (see bench-etcfs.sh's
# header for what the two measure). psync in that mode, not libaio: libaio
# needs O_DIRECT and degrades to synchronous submission without it.
#
# Usage:
#   ./bench-gfs2.sh
#   ETCFS_BENCH_DIRECT=0 ./bench-gfs2.sh
set -euo pipefail

DIRECT="${ETCFS_BENCH_DIRECT:-1}"
if [[ "$DIRECT" == "1" ]]; then BACKEND=gfs2; ENGINE=libaio; else BACKEND=gfs2-pagecache; ENGINE=psync; fi
export COMPARE_BACKEND="$BACKEND"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"

compare_begin
compare_mount

run_fio "$BACKEND" "filename=$MOUNT_PATH/fio.dat" 1G "$ENGINE" 4 32 "${ETCFS_BENCH_RUNTIME:-30}" "$DIRECT"
compare_finish "$BACKEND"
