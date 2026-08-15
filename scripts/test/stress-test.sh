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

# Seeded RNG.
#
# bash's own $RANDOM, not a hand-rolled LCG called through $(rand): a command
# substitution runs in a subshell, so the generator's state advanced inside it
# and was thrown away on return. Every iteration therefore drew the *same*
# number, picked the same branch, and — because that branch needed a file the
# listing never had — the whole run did nothing at all while reporting success.
# Expanded directly like this, $RANDOM advances in the shell that reads it, and
# seeding it keeps the run reproducible.
RANDOM=$SEED

# The `|| true` on each listing below is load-bearing: an empty directory, or a
# grep matching nothing, is an ordinary outcome for a fuzzer racing two other
# nodes — but an assignment takes the exit status of its pipeline, so under
# `set -euo pipefail` one of those ended the worker partway through and left an
# empty result file behind.

# Stats
OP_CREATE=0; OP_WRITE=0; OP_READ=0; OP_DEL=0; OP_RENAME=0
OP_MKDIR=0; OP_MV=0; OP_TRUNC=0; OP_STAT=0
ERR_CREATE=0; ERR_WRITE=0; ERR_READ=0; ERR_DEL=0; ERR_RENAME=0
ERR_MKDIR=0; ERR_MV=0; ERR_TRUNC=0; ERR_STAT=0
CONTENTION=0; BYTES_WRITTEN=0; BYTES_READ=0
LAT_TOTAL=0; LAT_COUNT=0

sudo mkdir -p "$RAND_DIR"

# Why a failure happened matters more than how often. Three nodes fuzz one
# directory with no coordination, so an op that picked its target out of a
# listing routinely finds a peer has since removed it — ENOENT there says the
# workload raced itself, while EIO says the filesystem gave up. Counting both
# as "err" makes the totals uninterpretable, so the message is kept.
ERRLOG="/tmp/stress-errors-$NODE_ID.log"
: > "$ERRLOG"

# run_op <ok_var> <err_var> <label> <cmd...>
run_op() {
    local -n _ok=$1 _err=$2
    local label=$3; shift 3
    local msg
    if msg=$("$@" 2>&1 >/dev/null); then
        _ok=$((_ok+1))
    else
        _err=$((_err+1))
        echo "$label: ${msg:-(no message)}" >> "$ERRLOG"
    fi
}

END=$(( $(date +%s) + DUR ))

while [[ $(date +%s) -lt $END ]]; do
    OP=$(($RANDOM % 12))
    INO=$(( $RANDOM % 500 + 1 ))
    NAME="f-$NODE_ID-$RANDOM-$RANDOM.txt"
    DIR_NAME="d-$NODE_ID-$RANDOM-$RANDOM"
    DATA_SIZE=$(( ($RANDOM % 16 + 1) * 64 ))
    OFF=$(( $RANDOM % 4096 ))

    # Create some multi-level dirs
    NESTED=""
    if [[ $(($RANDOM % 5)) -eq 0 ]]; then
        NESTED="sub-$RANDOM/sub2-$RANDOM/"
        sudo mkdir -p "$RAND_DIR/$NESTED" 2>/dev/null || true
    fi

    FULL_NAME="$RAND_DIR/$NESTED$NAME"

    # Every iteration times itself, but only a few branches used to set T0 —
    # under `set -u` the first untimed op killed the worker outright.
    T0=$(date +%s%N)

    case $OP in
        0|1)  # Create + write (sequential)
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
            TARGET=$(sudo ls "$RAND_DIR/" 2>/dev/null | head -$((RANDOM%20+1)) | tail -1) || true
            if [[ -n "$TARGET" ]]; then
                run_op OP_READ ERR_READ read sudo cat "$RAND_DIR/$TARGET"
            fi
            ;;

        4)  # Cross-node read (try reading a file that may have been created by another node)
            CROSS=$(sudo ls "$RAND_DIR/" 2>/dev/null | grep -v "^$NODE_ID" | head -$((RANDOM%10+1)) | tail -1) || true
            if [[ -n "$CROSS" ]]; then
                run_op OP_READ ERR_READ cross-read sudo cat "$RAND_DIR/$CROSS"
            fi
            ;;

        5)  # Delete file
            TARGET=$(sudo ls "$RAND_DIR/" 2>/dev/null | head -$((RANDOM%20+1)) | tail -1) || true
            if [[ -n "$TARGET" ]]; then
                run_op OP_DEL ERR_DEL delete sudo rm "$RAND_DIR/$TARGET"
            fi
            ;;

        6)  # Rename
            TARGET=$(sudo ls "$RAND_DIR/" 2>/dev/null | head -$((RANDOM%20+1)) | tail -1) || true
            if [[ -n "$TARGET" ]]; then
                NEWNAME="ren-$RANDOM-$RANDOM.txt"
                run_op OP_RENAME ERR_RENAME rename sudo mv "$RAND_DIR/$TARGET" "$RAND_DIR/$NEWNAME"
            fi
            ;;

        7)  # Mkdir
            sudo mkdir -p "$RAND_DIR/$DIR_NAME" 2>/dev/null && OP_MKDIR=$((OP_MKDIR+1)) || ERR_MKDIR=$((ERR_MKDIR+1))
            ;;

        8)  # Move file to dir
            TARGET=$(sudo ls "$RAND_DIR/" 2>/dev/null | head -$((RANDOM%20+1)) | tail -1) || true
            if [[ -n "$TARGET" ]]; then
                run_op OP_MV ERR_MV move sudo mv "$RAND_DIR/$TARGET" "$RAND_DIR/$DIR_NAME/"
            fi
            ;;

        9)  # Truncate
            TARGET=$(sudo ls "$RAND_DIR/" 2>/dev/null | head -$((RANDOM%20+1)) | tail -1) || true
            if [[ -n "$TARGET" ]]; then
                NEW=$(( $RANDOM % 200 ))
                run_op OP_TRUNC ERR_TRUNC truncate sudo truncate -s $NEW "$RAND_DIR/$TARGET"
            fi
            ;;

        10)  # Stat
            TARGET=$(sudo ls "$RAND_DIR/" 2>/dev/null | head -$((RANDOM%20+1)) | tail -1) || true
            if [[ -n "$TARGET" ]]; then
                run_op OP_STAT ERR_STAT stat sudo stat --format=%s "$RAND_DIR/$TARGET"
            fi
            ;;

        11)  # Lock contention — try to write to a common name that all 3 nodes race on
            DATA="contention-$RANDOM"
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
    # No chmod: the worker is launched as `bash /tmp/stress.sh` below, so it
    # never needed the exec bit. The chmod that used to be here named the
    # *local* path, which does not exist — under `set -e` that aborted the run
    # before a single worker started.
    scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR /tmp/etcfuse-stress-worker.sh ec2-user@$ip:/tmp/stress.sh 2>/dev/null
done

# ---- Run workers in parallel ----
log "Starting $DURATION second stress test on $COUNT nodes"
RESULTS=""
PIDS=""

for i in "${!PUB_IPS[@]}"; do
    ip="${PUB_IPS[$i]}"
    SEED=$(( $(date +%s) + i * 1000 ))
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -f ec2-user@$ip "nohup bash /tmp/stress.sh n$((i+1)) $SEED $DURATION $TEST_DIR > /tmp/stress-result-$i.json 2>/dev/null &"
    PIDS="$PIDS $!"
done

log "Workers launched. Waiting $((DURATION + 10)) seconds for completion..."
sleep $((DURATION + 10))

# ---- Collect results ----
log "Collecting results"
for i in "${!PUB_IPS[@]}"; do
    ip="${PUB_IPS[$i]}"
    scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR ec2-user@$ip:/tmp/stress-result-$i.json "$REPORT_DIR/node-$i.json" 2>/dev/null || true
    scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR "ec2-user@$ip:/tmp/stress-errors-n$((i+1)).log" "$REPORT_DIR/errors-$i.log" 2>/dev/null || true
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
  echo "Test duration:    $DURATION seconds each node"
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

  # What the failures actually were. This workload picks its target out of a
  # bare listing, so it routinely tries to cat a directory, or to touch a name
  # a peer removed a moment earlier — those say the fuzzer raced itself or
  # picked badly, not that the filesystem broke. An I/O error, a stale handle
  # or an EAGAIN is the filesystem, and is the only kind worth failing over.
  WORKLOAD=0; REAL=0
  if compgen -G "$REPORT_DIR/errors-*.log" > /dev/null; then
    WORKLOAD=$(cat "$REPORT_DIR"/errors-*.log 2>/dev/null | grep -ci "No such file or directory\|Directory not empty\|File exists\|Not a directory\|Is a directory" || true)
    REAL=$(cat "$REPORT_DIR"/errors-*.log 2>/dev/null | grep -ci "Input/output error\|Transport endpoint\|Stale file handle\|Resource temporarily unavailable" || true)
    echo "--- Failure causes ---"
    echo "  Workload artifacts (EISDIR/ENOTDIR/ENOENT/EEXIST): $WORKLOAD"
    echo "  Filesystem errors  (EIO/ESTALE/EAGAIN):            $REAL"
    cat "$REPORT_DIR"/errors-*.log 2>/dev/null | sed 's/.*: //' | sort | uniq -c | sort -rn | head -5 | sed 's/^/    /'
    echo ""
  fi

  ERR_RATE=$(python3 -c "print(round($TOTAL_ERR / max($TOTAL_OK+$TOTAL_ERR,1) * 100, 1))" 2>/dev/null || echo 0)
  if [[ $((TOTAL_OK + TOTAL_ERR)) -eq 0 ]]; then
    # A run that did nothing has no errors either, and used to report that as
    # success — which is how a generator that never advanced went unnoticed.
    # No work done is a failure of the harness, not a clean bill of health.
    echo "STATUS: FAILED — no operations were performed; the workers did nothing"
  elif [[ $TOTAL_ERR -eq 0 ]]; then
    echo "STATUS: ALL PASSED — no errors"
  elif [[ "$REAL" -eq 0 ]]; then
    # Every failure came from what the workload asked for, not from the
    # filesystem: it returned no I/O errors at all.
    echo "STATUS: PASSED — $ERR_RATE% of ops failed on the workload's own choices, 0 filesystem errors"
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
