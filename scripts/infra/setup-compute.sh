#!/bin/bash
# setup-compute.sh — bring up EtcFS software on the compute nodes created by
# create-infra.sh, for a persistent cluster meant to be poked at manually
# (IO tests, locking tests, run-full-test.sh) between and across multiple
# invocations of this script.
#
# The actual node bootstrap (packages, etcd, both EtcFS binaries, a fresh
# etcd cluster, the daemons) lives in bootstrap-cluster.sh, which this and
# scripts/test/chaos-lib.sh's ephemeral per-run clusters both call — one
# proven implementation instead of two that drift apart. See
# bootstrap-cluster.sh's header for why it looks the way it does (no TLS,
# no systemd) and for the bugs that approach replaced.
#
# Idempotent the same way bootstrap-cluster.sh is idempotent: every node is
# always restarted as part of one fresh cluster. There is no partial state
# to reconcile against a re-run, because nothing is left running between
# invocations that this doesn't itself tear down and restart.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/state.sh"

mapfile -t COMPUTE_IPS < <(state_get compute_public_ips | jq -r '.[]')
COMPUTE_COUNT=${#COMPUTE_IPS[@]}
if [[ "$COMPUTE_COUNT" -eq 0 || "${COMPUTE_IPS[0]}" == "null" ]]; then
    die "No compute IPs in state file. Run create-infra.sh first."
fi

log "=== EtcFS compute node setup ($COMPUTE_COUNT nodes, etcd colocated) ==="
log "Cluster:     $(state_get cluster_name | tr -d '\"')"
log "Mount point: $FUSE_MOUNTPOINT"
log ""

bash "$SCRIPT_DIR/bootstrap-cluster.sh" "$ETCFS_STATE"

log ""
log "============================================"
log "EtcFS compute setup complete"
log "Mount point:    $FUSE_MOUNTPOINT"
log "Next: ./scripts/infra/run-full-test.sh"
log "============================================"
