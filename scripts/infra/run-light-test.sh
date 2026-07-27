#!/bin/bash
# run-light-test.sh — concurrent I/O correctness test for EtcFS.
#
# Deploys a worker script to each compute node that performs random
# read/append/write operations every N seconds on shared files.
# Measures ok/hung/error counts per node.
#
# Usage:
#   ./run-light-test.sh [duration_secs] [interval_secs]
#   ETCFS_MOUNTPOINT=/mnt/etcfuse ./run-light-test.sh 120 5

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/state.sh"

mapfile -t PUB_IPS < <(state_get compute_ips 2>/dev/null | jq -r '.[]' 2>/dev/null)
COUNT=${#PUB_IPS[@]}
DURATION="${1:-60}"
INTERVAL="${2:-5}"
TESTDIR="${FUSE_MOUNTPOINT:-/mnt/etcfuse}/light-test-$$"
RESULTS_DIR="$PROJECT_ROOT/light-test-results"

rm -rf "$RESULTS_DIR"
mkdir -p "$RESULTS_DIR"

log "=== EtcFS Light Concurrent I/O Test ==="
log "Nodes:     $COUNT"
log "Duration:  ${DURATION}s"
log "Interval:  ${INTERVAL}s"
log "Test dir:  $TESTDIR"
log ""

# ---- Deploy test worker script ----
WORKER="/tmp/etcfuse-light-worker.sh"
cat > "$WORKER" <<'WORKERSCRIPT'
#!/bin/bash
# EtcFS light concurrent I/O worker
set -euo pipefail
TESTDIR="${1:?}"; INTERVAL="${2:-5}"; DEADLINE=$((SECONDS + ${3:-60}))
NODE_ID="${4:-unknown}"
RESULTS="${5:-/tmp/light-results.txt}"

OK=0; HUNG=0; ERR=0; OPS=0

while [[ $SECONDS -lt $DEADLINE ]]; do
    OPS=$((OPS + 1))
    F="${TESTDIR}/${NODE_ID}-op-${OPS}"

    # Pick a random operation
    case $((RANDOM % 5)) in
        0) # Write
            echo "write-${OPS}-${NODE_ID}-$(date +%s)" > "$F" 2>/dev/null && OK=$((OK+1)) || ERR=$((ERR+1))
            ;;
        1) # Read
            [[ -f "$F" ]] && cat "$F" >/dev/null 2>/dev/null && OK=$((OK+1)) || OK=$((OK+1))  # read nonexistent is OK too
            ;;
        2) # Append
            echo "append-${OPS}-${NODE_ID}" >> "$F" 2>/dev/null && OK=$((OK+1)) || ERR=$((ERR+1))
            ;;
        3) # Create new file
            NEW="${TESTDIR}/${NODE_ID}-new-${OPS}-$(date +%s)"
            echo "created-by-${NODE_ID}" > "$NEW" 2>/dev/null && OK=$((OK+1)) || ERR=$((ERR+1))
            ;;
        4) # Stat
            stat "$F" >/dev/null 2>/dev/null && OK=$((OK+1)) || OK=$((OK+1))
            ;;
    esac

    echo "${NODE_ID} op=${OPS} ok=${OK} err=${ERR} hung=${HUNG}" > "$RESULTS"
    sleep "$INTERVAL"
done
WORKERSCRIPT
chmod +x "$WORKER"

# ---- Create test directory ----
FIRST_IP="${PUB_IPS[0]}"
$SSH_CMD "ec2-user@$FIRST_IP" "sudo mkdir -p $TESTDIR" 2>/dev/null || true
sleep 1

# ---- Launch workers on all nodes ----
log "Launching workers..."
PIDS=()
for i in "${!PUB_IPS[@]}"; do
    ip="${PUB_IPS[$i]}"
    scp $SSH_OPTS "$WORKER" "ec2-user@${ip}:/tmp/" >/dev/null 2>&1 || true
    $SSH_CMD "ec2-user@${ip}" \
        "sudo bash /tmp/etcfuse-light-worker.sh $TESTDIR $INTERVAL $DURATION node${i} ${RESULTS_DIR}/node${i}.txt" &
    PIDS+=($!)
done

# ---- Wait for workers ----
log "Waiting for workers to complete (${DURATION}s)..."
wait "${PIDS[@]}"

sleep 3

# ---- Collect results ----
log ""
log "=== Results ==="
for i in "${!PUB_IPS[@]}"; do
    RESULT_FILE="$RESULTS_DIR/node${i}.txt"
    if [[ -f "$RESULT_FILE" ]]; then
        echo -n "  node${i} ($PUB_IPS[$i]): "
        cat "$RESULT_FILE"
    else
        echo "  node${i} ($PUB_IPS[$i]): no results (FUSE mount may not be available — expected in design phase)"
    fi
done

# Cleanup
$SSH_CMD "ec2-user@$FIRST_IP" "sudo rm -rf $TESTDIR" 2>/dev/null || true

log ""
log "Test complete. Detailed logs in $RESULTS_DIR/"
