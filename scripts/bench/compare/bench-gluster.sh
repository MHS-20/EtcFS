#!/bin/bash
# bench-gluster.sh — GlusterFS replica-3 on its own 3-node cluster, each node
# backed by its own independent 1000-IOPS io2 volume as a true local brick —
# Gluster's real deployment model (it replicates across independent per-node
# storage; it does not take one shared block device). The cluster's own
# Multi-Attach volume from compare_provision is left unused here for the same
# reason: mounting it would mean either only one node ever formatting it, or
# relaying every brick over NFS, neither of which is how Gluster runs in
# production and both of which would benchmark the relay, not Gluster.
#
# Runs on Amazon Linux 2, not AL2023 (see ETCFS_AMI_NAME_FILTER in
# scripts/infra/state.sh): AL2023 has no glusterfs-server package at all.
# AL2's own amzn2-core repo ships GlusterFS *client* packages (cli/fuse/
# client-xlators) but not glusterfs-server — that needs the CentOS Storage
# SIG's el7 build (vault.centos.org, no gpgcheck: ephemeral infra, torn down
# at the end of this same script) plus EPEL's userspace-rcu — the one runtime
# lib el7's gluster9 build needs that AL2 base doesn't carry at a new enough
# ABI (AL2 ships its own older userspace-rcu under the same package name, and
# yum's priorities plugin hides EPEL's behind it; --disableplugin=priorities
# on just that one install is what gets the newer one).
#
# ETCFS_BENCH_DIRECT=0 runs the warm-page-cache variant (see bench-etcfs.sh's
# header for what the two measure). psync in that mode, not libaio: libaio
# needs O_DIRECT and degrades to synchronous submission without it.
#
# Usage:
#   ./bench-gluster.sh
#   ETCFS_BENCH_DIRECT=0 ./bench-gluster.sh
set -euo pipefail

DIRECT="${ETCFS_BENCH_DIRECT:-1}"
if [[ "$DIRECT" == "1" ]]; then BACKEND=gluster; ENGINE=libaio; else BACKEND=gluster-pagecache; ENGINE=psync; fi
export COMPARE_BACKEND="$BACKEND"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"

compare_begin
compare_mount 0   # one client is all the IOPS comparison drives

run_fio "$BACKEND" "filename=$MOUNT_PATH/fio.dat" 1G "$ENGINE" 4 32 "${ETCFS_BENCH_RUNTIME:-30}" "$DIRECT"
compare_finish "$BACKEND"
