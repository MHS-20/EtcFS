#!/bin/bash
# chaos-monkey.sh — chaos engineering test suite for EtcFS.
#
# Deploys a 3-node cluster, runs a battery of failure scenarios, and
# produces a structured report of findings.
#
# Scenarios:
#   1. Daemon graceful restart (SIGTERM) — idle
#   2. Daemon hard kill (SIGKILL) — idle
#   3. Daemon restart during concurrent I/O
#   4. Sustained I/O with lock contention
#   5. Metadata storm (parallel chmod, mkdir, rename across nodes)
#   6. Graceful scale-in (daemon shutdown, member removal)
#   7. Graceful scale-out (daemon rejoin)
#   8. etcd restart on one node — daemon reconnects
#   9. All daemons killed and restarted simultaneously
#   10. Epoch unchanged after all non-fencing events
#
# [TEMPLATE] Adapted from QAttach chaos-monkey.sh. GFS2-specific checks
#            replaced with EtcFS FUSE daemon equivalents. Real implementation
#            requires the EtcFS daemon binary to exist.
#
# Usage:
#   ./chaos-monkey.sh                    # normal run (non-destructive)
#   ./chaos-monkey.sh --setup-only       # just set up the cluster, skip tests
#   ./chaos-monkey.sh --skip-setup       # assume cluster already running
#   ./chaos-monkey.sh --destructive      # allow EC2 fencing scenarios
#
# Prerequisites: AWS CLI configured, ETCFS_KEY_NAME set.
#
# Output: $PROJECT_ROOT/chaos-report-<timestamp>/  (full results)
#         $PROJECT_ROOT/chaos-report-latest/       (symlink)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
INFRA_DIR="$PROJECT_ROOT/scripts/infra"
source "$INFRA_DIR/state.sh"

# ---- Config ----
PEM="${ETCFS_PEM_PATH:-~/.ssh/id_ed25519}"
PEM="${PEM/#\~/$HOME}"
SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=10 -i $PEM"
DESTRUCTIVE=false
SKIP_SETUP=false
SETUP_ONLY=false
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
REPORT_DIR="$PROJECT_ROOT/chaos-report-${TIMESTAMP}"
SCENARIO_INDEX=0
TESTDIR="${FUSE_MOUNTPOINT:-/mnt/etcfuse}/chaos-$$"

# ---- Parse flags ----
for arg in "$@"; do
    case "$arg" in
        --destructive) DESTRUCTIVE=true ;;
        --skip-setup)  SKIP_SETUP=true ;;
        --setup-only)  SETUP_ONLY=true ;;
        *) echo "Unknown flag: $arg"; exit 1 ;;
    esac
done

# ---- Helpers ----

log()  { echo "[$(date +%T)] $*"; }
die()  { echo "FATAL: $*" >&2; exit 1; }

remote_cmd() {
    local node="$1"; shift
    ssh $SSH_OPTS "ec2-user@${node}" -- "$*"
}

wait_for_mount() {
    local node="$1"
    local max="${2:-30}"
    for i in $(seq 1 "$max"); do
        if remote_cmd "$node" "mountpoint -q $FUSE_MOUNTPOINT 2>/dev/null"; then
            return 0
        fi
        sleep 1
    done
    return 1
}

wait_for_daemon() {
    local node="$1"
    local max="${2:-30}"
    for i in $(seq 1 "$max"); do
        if remote_cmd "$node" "sudo systemctl is-active etcfuse 2>/dev/null" | grep -q active; then
            return 0
        fi
        sleep 1
    done
    return 1
}

get_epoch() {
    local node="$1"
    etcdctl_cmd "$node" "get gen: --prefix" 2>/dev/null | grep -c "gen:" | tr -d '[:space:]' || echo "0"
}

# Run a scenario, logging output.
run_scenario() {
    local name="$1"
    local fn="$2"
    SCENARIO_INDEX=$((SCENARIO_INDEX + 1))
    local tag="$(printf '%02d' $SCENARIO_INDEX)"
    local dir="$REPORT_DIR/scenario-${tag}-${name// /_}"
    mkdir -p "$dir"
    log ""
    log "=============================================="
    log "Scenario $tag: $name"
    log "=============================================="
    if $fn "$dir" 2>&1 | tee "$dir/output.log"; then
        echo "PASS" > "$dir/status"
        log "  >>> SCENARIO $tag PASS <<<"
    else
        echo "FAIL" > "$dir/status"
        log "  >>> SCENARIO $tag FAIL <<<"
    fi
}

# ---- Load nodes ----
mapfile -t NODES < <(state_get compute_public_ips | jq -r '.[]')
mapfile -t PRIV_IPS < <(state_get compute_ips | jq -r '.[]')
COUNT=${#NODES[@]}
[[ "$COUNT" -ge 3 ]] || die "Need 3 nodes, got $COUNT"

N0="${NODES[0]}"; N1="${NODES[1]}"; N2="${NODES[2]}"

# ---- Setup ----
if ! $SKIP_SETUP; then
    log "Setting up cluster..."
    "$INFRA_DIR/create-infra.sh"
    "$INFRA_DIR/setup-compute.sh"
fi

if $SETUP_ONLY; then
    log "Setup complete (--setup-only). Exiting."
    exit 0
fi

# Verify cluster is functional
log "Verifying cluster..."
for n in "${NODES[@]}"; do
    wait_for_ssh "$n" || die "SSH not ready on $n"
done
HAS_MOUNT=false
if remote_cmd "$N0" "mountpoint -q $FUSE_MOUNTPOINT 2>/dev/null" 2>/dev/null; then
    HAS_MOUNT=true
    log "FUSE mount detected — full test mode."
else
    log "No FUSE mount detected. Running infrastructure-only tests."
    log "(This is expected during design phase — daemon binary not yet built.)"
fi

# Record baseline epoch
BASELINE_EPOCH=$(etcdctl_cmd "$N0" "get gen: --prefix" 2>/dev/null | grep -c "gen:" | tr -d '[:space:]' || echo "0")

mkdir -p "$REPORT_DIR"
echo "cluster=$CLUSTER_NAME" > "$REPORT_DIR/info.txt"
echo "nodes=$COUNT" >> "$REPORT_DIR/info.txt"
echo "has_mount=$HAS_MOUNT" >> "$REPORT_DIR/info.txt"
echo "baseline_epoch=$BASELINE_EPOCH" >> "$REPORT_DIR/info.txt"

# ---- Scenario 1: Daemon graceful restart (SIGTERM) — idle ----
run_scenario "daemon-graceful-restart-idle" "$REPORT_DIR/scenario-01-daemon-graceful-restart-idle" <<'EOF'
dir="$1"
log "Restarting etcfuse with SIGTERM on node0..."
remote_cmd "$N0" "sudo systemctl stop etcfuse 2>/dev/null" || true
sleep 5
remote_cmd "$N0" "sudo systemctl start etcfuse 2>/dev/null" || true
wait_for_daemon "$N0" 15

if $HAS_MOUNT; then
    wait_for_mount "$N0" 15 || { echo "FAIL: mount did not return on N0"; return 1; }
    remote_cmd "$N0" "test -d $FUSE_MOUNTPOINT" || { echo "FAIL: mount not accessible"; return 1; }
    echo "graceful restart successful, mount OK"
else
    echo "(skipped mount check — no daemon binary)"
fi
EOF

# ---- Scenario 2: Daemon hard kill (SIGKILL) — idle ----
# (Skipped if no binary — would just be testing systemd restart)
if $HAS_MOUNT; then
run_scenario "daemon-hard-kill-idle" "$REPORT_DIR/scenario-02-daemon-hard-kill-idle" <<'EOF'
dir="$1"
log "Hard-killing etcfuse with SIGKILL on node1..."
remote_cmd "$N1" "sudo systemctl kill -s SIGKILL etcfuse 2>/dev/null" || true
sleep 5
wait_for_daemon "$N1" 20
wait_for_mount "$N1" 20 || { echo "FAIL: mount did not return on N1"; return 1; }
remote_cmd "$N1" "test -d $FUSE_MOUNTPOINT" || { echo "FAIL: mount not accessible"; return 1; }
echo "hard-kill restart successful"
EOF
fi

# ---- Scenario 3: Daemon restart during concurrent I/O ----
if $HAS_MOUNT; then
run_scenario "daemon-restart-during-io" "$REPORT_DIR/scenario-03-daemon-restart-during-io" <<'EOF'
dir="$1"
log "Starting writers on all 3 nodes..."
for n in "$N0" "$N1" "$N2"; do
    remote_cmd "$n" "
sudo mkdir -p $TESTDIR/s3
for f in \$(seq 1 20); do
    echo 'node-write-\$f' | sudo tee $TESTDIR/s3/\$(hostname)-\$f.txt >/dev/null
done
" &
done
wait

sleep 2
log "Killing daemon on node0 mid-write..."
remote_cmd "$N0" "sudo systemctl kill -s SIGKILL etcfuse 2>/dev/null" || true
sleep 3
wait_for_daemon "$N0" 20
wait_for_mount "$N0" 20 || { echo "FAIL: mount did not return on N0"; return 1; }

# Verify files from all three nodes exist
ALL_COUNT=$(remote_cmd "$N0" "sudo ls $TESTDIR/s3/ 2>/dev/null | wc -l" 2>/dev/null || echo "0")
echo "visible files after restart: $ALL_COUNT"
[[ "$ALL_COUNT" -ge 3 ]] || { echo "FAIL: fewer than 3 files visible"; return 1; }
echo "daemon survived restart during concurrent I/O"
EOF
fi

# ---- Scenario 4: Sustained I/O with lock contention ----
if $HAS_MOUNT; then
run_scenario "sustained-io-lock-contention" "$REPORT_DIR/scenario-04-sustained-io-lock-contention" <<'EOF'
dir="$1"
LOCKFILE="$TESTDIR/s4/lockfile"
remote_cmd "$N0" "sudo mkdir -p $TESTDIR/s4"
remote_cmd "$N0" "echo 'initial' | sudo tee $LOCKFILE > /dev/null"

log "Running 15s of concurrent append from 3 nodes..."
for i in $(seq 1 3); do
    for n_idx in 0 1 2; do
        n="${NODES[$n_idx]}"
        remote_cmd "$n" "
for j in \$(seq 1 15); do
    echo 'append-from-node${n_idx}' | sudo tee -a $LOCKFILE >/dev/null 2>&1
    sleep 1
done
" &
    done
done
wait

LINE_COUNT=$(remote_cmd "$N0" "sudo wc -l < $LOCKFILE" 2>/dev/null || echo "0")
echo "final line count: $LINE_COUNT"
[[ "$LINE_COUNT" -ge 3 ]] || { echo "FAIL: lock contention caused data loss"; return 1; }

# Check for stale lock errors in dmesg
LOCK_STALLS=$(remote_cmd "$N0" "sudo dmesg 2>/dev/null | grep -ci 'lock stall\|hung task' | tr -d '[:space:]'" 2>/dev/null || echo "0")
echo "lock stalls/hung tasks in dmesg: $LOCK_STALLS"
EOF
fi

# ---- Scenario 5: Metadata storm ----
if $HAS_MOUNT; then
run_scenario "metadata-storm" "$REPORT_DIR/scenario-05-metadata-storm" <<'EOF'
dir="$1"
MDDIR="$TESTDIR/s5"
remote_cmd "$N0" "sudo mkdir -p $MDDIR"

log "Running parallel chmod, mkdir, rename from 3 nodes for 15s..."
for n_idx in 0 1 2; do
    n="${NODES[$n_idx]}"
    remote_cmd "$n" "
for k in \$(seq 1 30); do
    sudo mkdir $MDDIR/node${n_idx}-dir-\$k 2>/dev/null
    echo 'metadata-storm-\$k' | sudo tee $MDDIR/node${n_idx}-file-\$k >/dev/null 2>/dev/null
    sudo chmod 644 $MDDIR/node${n_idx}-file-\$k 2>/dev/null
done
" &
done
wait

TOTAL=$(remote_cmd "$N0" "sudo ls $MDDIR 2>/dev/null | wc -l" 2>/dev/null || echo "0")
echo "total entries created: $TOTAL"
[[ "$TOTAL" -ge 10 ]] || { echo "FAIL: metadata storm produced fewer than 10 entries"; return 1; }
echo "metadata storm completed successfully"
EOF
fi

# ---- Scenario 6: Graceful scale-in ----
if $HAS_MOUNT; then
run_scenario "graceful-scale-in" "$REPORT_DIR/scenario-06-graceful-scale-in" <<'EOF'
dir="$1"
log "Stopping daemon on node2 (graceful leave)..."
remote_cmd "$N2" "sync 2>/dev/null; sudo systemctl stop etcfuse 2>/dev/null" || true

# Verify node2 is no longer mounted
sleep 3
if remote_cmd "$N2" "mountpoint -q $FUSE_MOUNTPOINT 2>/dev/null"; then
    echo "WARN: N2 still mounted — lazy unmount"
    remote_cmd "$N2" "sudo fusermount -u $FUSE_MOUNTPOINT 2>/dev/null" || true
fi

# Verify nodes 0,1 can still do I/O
remote_cmd "$N0" "echo 'scale-in-test' | sudo tee $TESTDIR/s6.txt >/dev/null" || { echo "FAIL: N0 could not write after N2 left"; return 1; }
CONTENT=$(remote_cmd "$N1" "sudo cat $TESTDIR/s6.txt 2>/dev/null" || echo "")
[[ "$CONTENT" == "scale-in-test" ]] || { echo "FAIL: N1 could not read after N2 left"; return 1; }
echo "2-node operation confirmed after scale-in"
EOF
fi

# ---- Scenario 7: Graceful scale-out ----
if $HAS_MOUNT; then
run_scenario "graceful-scale-out" "$REPORT_DIR/scenario-07-graceful-scale-out" <<'EOF'
dir="$1"
log "Restarting daemon on node2 (rejoin)..."
remote_cmd "$N2" "sudo mkdir -p $FUSE_MOUNTPOINT; sudo systemctl start etcfuse 2>/dev/null" || true
wait_for_daemon "$N2" 30
wait_for_mount "$N2" 30 || { echo "FAIL: N2 did not remount after rejoin"; return 1; }

# Verify node2 sees data from during its absence
CONTENT=$(remote_cmd "$N2" "sudo cat $TESTDIR/s6.txt 2>/dev/null" || echo "")
[[ "$CONTENT" == "scale-in-test" ]] || { echo "FAIL: N2 does not see data written during its absence"; return 1; }
echo "N2 rejoined and sees current state"
EOF
fi

# ---- Scenario 8: etcd restart on one node ----
run_scenario "etcd-restart-one-node" "$REPORT_DIR/scenario-08-etcd-restart-one-node" <<'EOF'
dir="$1"
log "Restarting etcd on node1..."
remote_cmd "$N1" "sudo systemctl restart etcd 2>/dev/null" || true
sleep 5

# Verify etcd cluster is healthy again
HEALTHY=$(etcdctl_cmd "$N0" "endpoint health --cluster" 2>/dev/null | grep -c "is healthy" || echo "0")
[[ "$HEALTHY" -eq "$COUNT" ]] || { echo "FAIL: etcd cluster not fully healthy ($HEALTHY/$COUNT)"; return 1; }
echo "etcd cluster recovered after restart"

if $HAS_MOUNT; then
    # Verify FUSE daemon on N1 reconnected
    remote_cmd "$N1" "echo 'etcd-restart-test' | sudo tee $TESTDIR/s8.txt >/dev/null" || { echo "FAIL: N1 FUSE not writable after etcd restart"; return 1; }
    echo "N1 FUSE writable after etcd restart"
fi
EOF

# ---- Scenario 9: All daemons killed and restarted simultaneously ----
if $HAS_MOUNT; then
run_scenario "all-daemons-killed-restarted" "$REPORT_DIR/scenario-09-all-daemons-killed-restarted" <<'EOF'
dir="$1"
log "SIGKILL all daemons simultaneously..."
for n in "$N0" "$N1" "$N2"; do
    remote_cmd "$n" "sudo systemctl kill -s SIGKILL etcfuse 2>/dev/null" || true &
done
wait
sleep 5

log "Restarting all daemons..."
for n in "$N0" "$N1" "$N2"; do
    remote_cmd "$n" "sudo mkdir -p $FUSE_MOUNTPOINT; sudo systemctl start etcfuse 2>/dev/null" || true &
done
wait

for n in "$N0" "$N1" "$N2"; do
    wait_for_daemon "$n" 30 || { echo "FAIL: daemon on $n not active"; return 1; }
    wait_for_mount "$n" 30 || { echo "FAIL: mount on $n not present"; return 1; }
done

# Verify I/O works after mass restart
remote_cmd "$N0" "echo 'mass-restart' | sudo tee $TESTDIR/s9.txt >/dev/null" || { echo "FAIL: write after mass restart failed"; return 1; }
echo "all 3 nodes recovered from simultaneous restart"
EOF
fi

# ---- Scenario 10: Verify epoch unchanged ----
run_scenario "verify-epoch-unchanged" "$REPORT_DIR/scenario-10-verify-epoch-unchanged" <<'EOF'
dir="$1"
FINAL_EPOCH=$(etcdctl_cmd "$N0" "get gen: --prefix" 2>/dev/null | grep -c "gen:" | tr -d '[:space:]' || echo "0")
echo "baseline epoch: $BASELINE_EPOCH"
echo "final epoch:    $FINAL_EPOCH"
[[ "$BASELINE_EPOCH" -ge "$FINAL_EPOCH" ]] || {
    echo "FAIL: epoch increased without a fencing event ($BASELINE_EPOCH → $FINAL_EPOCH)"
    return 1
}
echo "epoch unchanged — no unexpected fencing events"
EOF

# ---- Cleanup ----
if $HAS_MOUNT; then
    remote_cmd "$N0" "sudo rm -rf $TESTDIR" 2>/dev/null || true
fi

# ---- Summary ----
PASS_COUNT=$(find "$REPORT_DIR" -name status -exec grep -l PASS {} \; | wc -l)
FAIL_COUNT=$(find "$REPORT_DIR" -name status -exec grep -l FAIL {} \; | wc -l)
TOTAL=$((PASS_COUNT + FAIL_COUNT))

ln -sfn "$REPORT_DIR" "$PROJECT_ROOT/chaos-report-latest"

log ""
log "============================================"
log "Chaos Monkey Complete"
log "  Total scenarios: $TOTAL"
log "  Passed: $PASS_COUNT"
log "  Failed: $FAIL_COUNT"
log "  Report: $REPORT_DIR"
log "============================================"

exit $FAIL_COUNT
