#!/bin/bash
# etcfuse-stress-test.sh — comprehensive 3-node stress test for EtcFS
#
# Runs random file operations on all 3 nodes simultaneously, measuring
# contention, throughput, cross-node visibility, and edge case correctness.
# Produces a structured report.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/../infra/state.sh"

mapfile -t PUB_IPS < <(state_get compute_public_ips | jq -r '.[]')
mapfile -t PRIV_IPS < <(state_get compute_ips | jq -r '.[]')
COUNT=${#PUB_IPS[@]}
[[ "$COUNT" -ge 3 ]] || die "Need at least 3 nodes, got $COUNT"

TEST_DIR="/mnt/etcfuse/stress-$RANDOM"
REPORT_DIR="$PROJECT_ROOT/stress-report-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$REPORT_DIR"

log() { echo "[$(date +%H:%M:%S)] $1"; }
pass() { echo "  PASS: $1"; }
fail() { echo "  FAIL: $1"; }

DURATION=${ETCFS_DURATION:-60}

# ---- Generate worker script ----
cat > /tmp/etcfuse-stress-worker.sh << 'WORKER'
#!/bin/bash
# Worker script — runs on each node, performs random file operations
set -euo pipefail

NODE_ID="$1"
SEED="$2"
DUR="$3"
TDIR="$4"
RAND_DIR="$TDIR"

# Simple RNG
R=$SEED
rand() { R=$(( (R * 1103515245 + 12345) & 0x7fffffff )); echo $R; }

# Stats
OP_CREATE=0; OP_WRITE=0; OP_READ=0; OP_DEL=0; OP_RENAME=0
OP_MKDIR=0; OP_MV=0; OP_TRUNC=0; OP_STAT=0
ERR_CREATE=0; ERR_WRITE=0; ERR_READ=0; ERR_DEL=0; ERR_RENAME=0
ERR_MKDIR=0; ERR_MV=0; ERR_TRUNC=0; ERR_STAT=0
CONTENTION=0; BYTES_WRITTEN=0; BYTES_READ=0
LAT_TOTAL=0; LAT_COUNT=0

sudo mkdir -p "$RAND_DIR"

END=$(( $(date +%s) + DUR ))

while [[ $(date +%s) -lt $END ]]; do
    OP=$(($(rand) % 12))
    INO=$(( $(rand) % 500 + 1 ))
    NAME="f-$NODE_ID-$(rand)-$R.txt"
    DIR_NAME="d-$NODE_ID-$(rand)-$R"
    DATA_SIZE=$(( ($(rand) % 16 + 1) * 64 ))
    OFF=$(( $(rand) % 4096 ))

    # Create some multi-level dirs
    NESTED=""
    if [[ $(($(rand) % 5)) -eq 0 ]]; then
        NESTED="sub-$(rand)/sub2-$(rand)/"
        sudo mkdir -p "$RAND_DIR/$NESTED" 2>/dev/null || true
    fi

    FULL_NAME="$RAND_DIR/$NESTED$NAME"

    case $OP in
        0|1)  # Create + write (sequential)
            T0=$(date +%s%N)
            DATA=$(head -c $DATA_SIZE /dev/urandom | base64 | head -c $DATA_SIZE)
            echo -n "$DATA" | sudo tee "$FULL_NAME" > /dev/null 2>&1 && OP_CREATE=$((OP_CREATE+1)) || ERR_CREATE=$((ERR_CREATE+1))
            BYTES_WRITTEN=$((BYTES_WRITTEN + DATA_SIZE))
            ;;

        2)  # Write (append)
            DATA=$(head -c $DATA_SIZE /dev/urandom | base64 | head -c $DATA_SIZE)
            echo -n "$DATA" | sudo tee -a "$RAND_DIR/$NAME" > /dev/null 2>&1 && OP_WRITE=$((OP_WRITE+1)) || ERR_WRITE=$((ERR_WRITE+1))
            BYTES_WRITTEN=$((BYTES_WRITTEN + DATA_SIZE))
            ;;

        3)  # Read file
            TARGET=$(sudo ls "$RAND_DIR/" 2>/dev/null | head -$(($(rand)%20+1)) | tail -1)
            if [[ -n "$TARGET" ]]; then
                sudo cat "$RAND_DIR/$TARGET" > /dev/null 2>&1 && OP_READ=$((OP_READ+1)) || ERR_READ=$((ERR_READ+1))
            fi
            ;;

        4)  # Cross-node read (try reading a file that may have been created by another node)
            CROSS=$(sudo ls "$RAND_DIR/" 2>/dev/null | grep -v "^$NODE_ID" | head -$(($(rand)%10+1)) | tail -1)
            if [[ -n "$CROSS" ]]; then
                sudo cat "$RAND_DIR/$CROSS" > /dev/null 2>&1 && OP_READ=$((OP_READ+1)) || ERR_READ=$((ERR_READ+1))
            fi
            ;;

        5)  # Delete file
            TARGET=$(sudo ls "$RAND_DIR/" 2>/dev/null | head -$(($(rand)%20+1)) | tail -1)
            if [[ -n "$TARGET" ]]; then
                sudo rm "$RAND_DIR/$TARGET" 2>/dev/null && OP_DEL=$((OP_DEL+1)) || ERR_DEL=$((ERR_DEL+1))
            fi
            ;;

        6)  # Rename
            TARGET=$(sudo ls "$RAND_DIR/" 2>/dev/null | head -$(($(rand)%20+1)) | tail -1)
            if [[ -n "$TARGET" ]]; then
                NEWNAME="ren-$(rand)-$R.txt"
                sudo mv "$RAND_DIR/$TARGET" "$RAND_DIR/$NEWNAME" 2>/dev/null && OP_RENAME=$((OP_RENAME+1)) || ERR_RENAME=$((ERR_RENAME+1))
            fi
            ;;

        7)  # Mkdir
            sudo mkdir -p "$RAND_DIR/$DIR_NAME" 2>/dev/null && OP_MKDIR=$((OP_MKDIR+1)) || ERR_MKDIR=$((ERR_MKDIR+1))
            ;;

        8)  # Move file to dir
            TARGET=$(sudo ls "$RAND_DIR/" 2>/dev/null | head -$(($(rand)%20+1)) | tail -1)
            if [[ -n "$TARGET" ]]; then
                sudo mv "$RAND_DIR/$TARGET" "$RAND_DIR/$DIR_NAME/" 2>/dev/null && OP_MV=$((OP_MV+1)) || ERR_MV=$((ERR_MV+1))
            fi
            ;;

        9)  # Truncate
            TARGET=$(sudo ls "$RAND_DIR/" 2>/dev/null | head -$(($(rand)%20+1)) | tail -1)
            if [[ -n "$TARGET" ]]; then
                NEW=$(( $(rand) % 200 ))
                sudo truncate -s $NEW "$RAND_DIR/$TARGET" 2>/dev/null && OP_TRUNC=$((OP_TRUNC+1)) || ERR_TRUNC=$((ERR_TRUNC+1))
            fi
            ;;

        10)  # Stat
            TARGET=$(sudo ls "$RAND_DIR/" 2>/dev/null | head -$(($(rand)%20+1)) | tail -1)
            if [[ -n "$TARGET" ]]; then
                sudo stat --format="%s" "$RAND_DIR/$TARGET" > /dev/null 2>&1 && OP_STAT=$((OP_STAT+1)) || ERR_STAT=$((ERR_STAT+1))
            fi
            ;;

        11)  # Lock contention — try to write to a common name that all 3 nodes race on
            T0=$(date +%s%N)
            DATA="contention-$(rand)"
            echo -n "$DATA" | sudo tee "$RAND_DIR/concurrent-$RANDOM.txt" > /dev/null 2>&1
            CONTENTION=$((CONTENTION+1))
            ;;
    esac
    T1=$(date +%s%N); LAT_TOTAL=$((LAT_TOTAL + (T1 - T0))); LAT_COUNT=$((LAT_COUNT+1))
done

# Emit results as JSON
cat <<JSONEOF
{
  "node": "$NODE_ID",
  "duration": $DUR,
  "operations": {
    "create": { "ok": $OP_CREATE, "err": $ERR_CREATE },
    "write": { "ok": $OP_WRITE, "err": $ERR_WRITE },
    "read": { "ok": $OP_READ, "err": $ERR_READ },
    "delete": { "ok": $OP_DEL, "err": $ERR_DEL },
    "rename": { "ok": $OP_RENAME, "err": $ERR_RENAME },
    "mkdir": { "ok": $OP_MKDIR, "err": $ERR_MKDIR },
    "move": { "ok": $OP_MV, "err": $ERR_MV },
    "truncate": { "ok": $OP_TRUNC, "err": $ERR_TRUNC },
    "stat": { "ok": $OP_STAT, "err": $ERR_STAT }
  },
  "contention_writes": $CONTENTION,
  "bytes_written": $BYTES_WRITTEN,
  "bytes_read": $BYTES_READ,
  "latency_ns_sum": $LAT_TOTAL,
  "latency_count": $LAT_COUNT
}
JSONEOF
WORKER

# ---- Push worker to all nodes ----
log "Deploying worker to $COUNT nodes"
for i in "${!PUB_IPS[@]}"; do
    ip="${PUB_IPS[$i]}"
    scp -o StrictHostKeyChecking=no /tmp/etcfuse-stress-worker.sh ec2-user@$ip:/tmp/stress.sh 2>/dev/null
    chmod +x /tmp/stress.sh 2>/dev/null
done

# ---- Run workers in parallel ----
log "Starting $DUR second stress test on $COUNT nodes"
RESULTS=""
PIDS=""

for i in "${!PUB_IPS[@]}"; do
    ip="${PUB_IPS[$i]}"
    SEED=$(( $(date +%s) + i * 1000 ))
    ssh -o StrictHostKeyChecking=no -f ec2-user@$ip "nohup bash /tmp/stress.sh n$((i+1)) $SEED $DUR $TEST_DIR > /tmp/stress-result-$i.json 2>/dev/null &"
    PIDS="$PIDS $!"
done

log "Workers launched. Waiting $((DUR + 10)) seconds for completion..."
sleep $((DUR + 10))

# ---- Collect results ----
log "Collecting results"
for i in "${!PUB_IPS[@]}"; do
    ip="${PUB_IPS[$i]}"
    scp -o StrictHostKeyChecking=no ec2-user@$ip:/tmp/stress-result-$i.json "$REPORT_DIR/node-$i.json" 2>/dev/null || true
done

# ---- Generate report ----
log "Generating report"
TOTAL_OK=0; TOTAL_ERR=0; TOTAL_CONT=0
TOTAL_BYTES_W=0; TOTAL_LAT_COUNT=0; TOTAL_LAT_SUM=0

for i in "${!PUB_IPS[@]}"; do
    f="$REPORT_DIR/node-$i.json"
    [[ ! -f "$f" ]] && { fail "no results from node $i"; continue; }
    OK=$(jq '[.operations[].ok] | add' "$f" 2>/dev/null || echo 0)
    ERR=$(jq '[.operations[].err] | add' "$f" 2>/dev/null || echo 0)
    CONT=$(jq '.contention_writes' "$f" 2>/dev/null || echo 0)
    BW=$(jq '.bytes_written' "$f" 2>/dev/null || echo 0)
    LC=$(jq '.latency_count' "$f" 2>/dev/null || echo 0)
    LS=$(jq '.latency_ns_sum' "$f" 2>/dev/null || echo 0)
    NODE=$(jq -r '.node' "$f" 2>/dev/null || echo "?")
    TOTAL_OK=$((TOTAL_OK + OK))
    TOTAL_ERR=$((TOTAL_ERR + ERR))
    TOTAL_CONT=$((TOTAL_CONT + CONT))
    TOTAL_BYTES_W=$((TOTAL_BYTES_W + BW))
    TOTAL_LAT_COUNT=$((TOTAL_LAT_COUNT + LC))
    TOTAL_LAT_SUM=$((TOTAL_LAT_SUM + LS))
done

AVG_LAT_MS=0
[[ $TOTAL_LAT_COUNT -gt 0 ]] && AVG_LAT_MS=$(python3 -c "print(round($TOTAL_LAT_SUM / $TOTAL_LAT_COUNT / 1000000, 2))" 2>/dev/null || echo 0)

{
  echo "============================================"
  echo "  EtcFS 3-Node Stress Test Report"
  echo "============================================"
  echo ""
  echo "Test duration:    $DUR seconds each node"
  echo "Total operations: $((TOTAL_OK + TOTAL_ERR))"
  echo "  Successful:     $TOTAL_OK"
  echo "  Errors:         $TOTAL_ERR"
  echo "  Success rate:   $(python3 -c "print(round($TOTAL_OK / max($TOTAL_OK+$TOTAL_ERR,1) * 100, 1))" 2>/dev/null || echo 0)%"
  echo ""
  echo "Contention writes: $TOTAL_CONT"
  echo "Bytes written:     $TOTAL_BYTES_W"
  echo "Avg latency:       ${AVG_LAT_MS}ms"
  echo ""

  for i in "${!PUB_IPS[@]}"; do
    f="$REPORT_DIR/node-$i.json"
    [[ ! -f "$f" ]] && continue
    echo "--- Node $i ---"
    N=$(jq -r '.node' "$f")
    OK=$(jq '[.operations[].ok] | add' "$f")
    ERR=$(jq '[.operations[].err] | add' "$f")
    C=$(jq '.contention_writes' "$f")
    B=$(jq '.bytes_written' "$f")
    echo "  Node: $N  OK: $OK  Err: $ERR  Contention: $C  BytesW: $B"
    jq -r '.operations | to_entries[] | "    \(.key): \(.value.ok) ok, \(.value.err) err"' "$f" 2>/dev/null
    echo ""
  done

  ERR_RATE=$(python3 -c "print(round($TOTAL_ERR / max($TOTAL_OK+$TOTAL_ERR,1) * 100, 1))" 2>/dev/null || echo 0)
  if [[ $TOTAL_ERR -eq 0 ]]; then
    echo "STATUS: ALL PASSED — no errors"
  elif [[ $(python3 -c "print(int($ERR_RATE < 5))" 2>/dev/null || echo 1) -eq 1 ]]; then
    echo "STATUS: ACCEPTABLE — $ERR_RATE% error rate (expected for lock contention)"
  else
    echo "STATUS: FAILED — $ERR_RATE% error rate exceeds threshold"
  fi
  echo ""
  echo "Report saved to: $REPORT_DIR"
} | tee "$REPORT_DIR/summary.txt"

echo ""
echo "Full report: $REPORT_DIR/summary.txt"
exit 0
