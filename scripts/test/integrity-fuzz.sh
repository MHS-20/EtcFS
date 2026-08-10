#!/bin/bash
# integrity-fuzz.sh — randomized file-operation fuzzer with NO fault
# injection, whose only job is proving data never gets corrupted.
#
# Unlike chaos-fuzz.sh (which mixes random ops with daemon kills, network
# partitions and fencing bumps to prove the cluster stays *reachable*),
# this never touches a daemon, a network link or a fencing generation. It
# is the correctness half of the story: every node runs concurrent
# create/write/append/truncate/read/rename/move/delete/mkdir traffic
# against the shared mount, and every write's expected content is tracked
# against its sha256. Every read — during the run and in a final pass done
# from a *different* node than the one that wrote it — has to match. A
# mismatch is corruption, reported immediately with the expected and
# actual content, not folded into an error-rate percentage the way
# stress-test.sh (throughput/latency, no content verification) reports.
#
# Runs against the persistent cluster already up (create-infra.sh +
# setup-compute.sh), not a throwaway one — this is meant to be run before
# and independent of the chaos suite.
#
# Usage:
#   ./integrity-fuzz.sh [duration_seconds] [seed]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/../infra/state.sh"

mapfile -t PUB_IPS < <(state_get compute_public_ips | jq -r '.[]')
COUNT=${#PUB_IPS[@]}
[[ "$COUNT" -ge 2 ]] || die "Need at least 2 nodes for cross-node verification, got $COUNT"

DURATION="${1:-${ETCFS_DURATION:-120}}"
SEED_BASE="${2:-$RANDOM}"

TEST_DIR="/mnt/etcfuse/integrity-fuzz-$$"
REPORT_DIR="$PROJECT_ROOT/integrity-fuzz-report-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$REPORT_DIR"

log()  { echo "[$(date +%H:%M:%S)] $1" | tee -a "$REPORT_DIR/run.log"; }
pass() { echo "  PASS: $1" | tee -a "$REPORT_DIR/run.log"; }
fail() { echo "  FAIL: $1" | tee -a "$REPORT_DIR/run.log"; FAILED=1; }
FAILED=0

log "=== Integrity fuzz: $COUNT nodes, ${DURATION}s, seed=$SEED_BASE ==="
log "Test dir: $TEST_DIR"

# ============================================================
# Worker — runs on each node. Maintains its own ledger of files it
# believes exist (path -> sha256) and files it believes it deleted.
# Every write records the expected hash; every read verifies it
# immediately, right there in the loop, not deferred to the end. A
# mismatch aborts that worker's run with a hard failure line the
# orchestrator greps for below.
# ============================================================
cat > /tmp/integrity-fuzz-worker.sh << 'WORKER'
#!/bin/bash
set -uo pipefail

NODE_ID="$1"
SEED="$2"
DUR="$3"
TDIR="$4"

# bash builtin PRNG, seeded once — a custom LCG called through $(...) forks
# a subshell per call, so the parent shell's counter never actually
# advances and every call returns the same value.  $RANDOM has no such
# problem: it is a shell variable that reseeds itself on every read.
RANDOM=$SEED

declare -A LEDGER      # path -> expected sha256
declare -A DELETED     # path -> 1, for paths this worker believes are gone

OP_OK=0; OP_ERR=0; CORRUPT=0; OPS_DONE=0; DELETED_COUNT=0
CORRUPT_LOG="/tmp/integrity-fuzz-corrupt-$NODE_ID.log"
: > "$CORRUPT_LOG"

sudo mkdir -p "$TDIR"

sha() { sha256sum "$1" 2>/dev/null | awk '{print $1}'; }

# verify_path <path> — re-read and compare against the ledger. Any
# mismatch is corruption: logged with both hashes and exits nothing (the
# worker keeps going, so one corrupt file doesn't hide a second one), but
# is counted and dumped to CORRUPT_LOG for the orchestrator to fail on.
verify_path() {
    local path="$1" expect="${LEDGER[$1]:-}"
    [[ -z "$expect" ]] && return 0
    local tmp got
    tmp=$(mktemp)
    if ! sudo cat "$path" > "$tmp" 2>/dev/null; then
        echo "CORRUPTION: $path unreadable, expected sha256=$expect" >> "$CORRUPT_LOG"
        CORRUPT=$((CORRUPT+1))
        rm -f "$tmp"
        return 1
    fi
    got=$(sha "$tmp")
    if [[ "$got" != "$expect" ]]; then
        echo "CORRUPTION: $path expected sha256=$expect got=$got size=$(wc -c < "$tmp")" >> "$CORRUPT_LOG"
        CORRUPT=$((CORRUPT+1))
        rm -f "$tmp"
        return 1
    fi
    rm -f "$tmp"
    return 0
}

END=$(( $(date +%s) + DUR ))
NAMES=()  # currently-live names, for picking random targets cheaply

while [[ $(date +%s) -lt $END ]]; do
    OPS_DONE=$((OPS_DONE+1))
    OP=$(($RANDOM % 10))
    NAME="f-$NODE_ID-$RANDOM"
    PATH1="$TDIR/$NAME"

    case $OP in
        0|1)  # create with known random content
            SIZE=$(( ($RANDOM % 32 + 1) * 97 ))
            TMP=$(mktemp)
            head -c "$SIZE" /dev/urandom > "$TMP"
            H=$(sha "$TMP")
            if sudo cp "$TMP" "$PATH1" 2>/dev/null; then
                LEDGER["$PATH1"]="$H"; unset 'DELETED[$PATH1]'
                NAMES+=("$PATH1")
                OP_OK=$((OP_OK+1))
            else
                OP_ERR=$((OP_ERR+1))
            fi
            rm -f "$TMP"
            ;;

        2)  # append — new expected hash is old content + new bytes
            [[ ${#NAMES[@]} -eq 0 ]] && continue
            TARGET="${NAMES[$(( $RANDOM % ${#NAMES[@]} ))]}"
            [[ -n "${LEDGER[$TARGET]:-}" ]] || continue
            SIZE=$(( ($RANDOM % 16 + 1) * 41 ))
            TMP=$(mktemp); NEWTMP=$(mktemp)
            if sudo cat "$TARGET" > "$TMP" 2>/dev/null; then
                head -c "$SIZE" /dev/urandom >> "$TMP"
                if sudo cp "$TMP" "$TARGET" 2>/dev/null; then
                    LEDGER["$TARGET"]=$(sha "$TMP")
                    OP_OK=$((OP_OK+1))
                    verify_path "$TARGET"
                else
                    OP_ERR=$((OP_ERR+1))
                fi
            else
                OP_ERR=$((OP_ERR+1))
            fi
            rm -f "$TMP" "$NEWTMP"
            ;;

        3)  # truncate — expected content is deterministically the first N bytes
            [[ ${#NAMES[@]} -eq 0 ]] && continue
            TARGET="${NAMES[$(( $RANDOM % ${#NAMES[@]} ))]}"
            [[ -n "${LEDGER[$TARGET]:-}" ]] || continue
            TMP=$(mktemp)
            if sudo cat "$TARGET" > "$TMP" 2>/dev/null; then
                CURSIZE=$(wc -c < "$TMP")
                NEWSIZE=$(( $RANDOM % (CURSIZE + 1) ))
                if sudo truncate -s "$NEWSIZE" "$TARGET" 2>/dev/null; then
                    head -c "$NEWSIZE" "$TMP" > "$TMP.trunc"
                    LEDGER["$TARGET"]=$(sha "$TMP.trunc")
                    OP_OK=$((OP_OK+1))
                    verify_path "$TARGET"
                    rm -f "$TMP.trunc"
                else
                    OP_ERR=$((OP_ERR+1))
                fi
            else
                OP_ERR=$((OP_ERR+1))
            fi
            rm -f "$TMP"
            ;;

        4|5)  # read + verify — the core correctness check, run often
            [[ ${#NAMES[@]} -eq 0 ]] && continue
            TARGET="${NAMES[$(( $RANDOM % ${#NAMES[@]} ))]}"
            [[ -n "${LEDGER[$TARGET]:-}" ]] || continue
            if verify_path "$TARGET"; then OP_OK=$((OP_OK+1)); else OP_ERR=$((OP_ERR+1)); fi
            ;;

        6)  # rename — content must survive under the new name
            [[ ${#NAMES[@]} -eq 0 ]] && continue
            IDX=$(( $RANDOM % ${#NAMES[@]} ))
            TARGET="${NAMES[$IDX]}"
            [[ -n "${LEDGER[$TARGET]:-}" ]] || continue
            NEWPATH="$TDIR/ren-$NODE_ID-$RANDOM"
            if sudo mv "$TARGET" "$NEWPATH" 2>/dev/null; then
                LEDGER["$NEWPATH"]="${LEDGER[$TARGET]}"
                unset 'LEDGER[$TARGET]'
                NAMES[$IDX]="$NEWPATH"
                OP_OK=$((OP_OK+1))
                verify_path "$NEWPATH"
            else
                OP_ERR=$((OP_ERR+1))
            fi
            ;;

        7)  # mkdir + move a file into it — content must survive the move
            [[ ${#NAMES[@]} -eq 0 ]] && continue
            IDX=$(( $RANDOM % ${#NAMES[@]} ))
            TARGET="${NAMES[$IDX]}"
            [[ -n "${LEDGER[$TARGET]:-}" ]] || continue
            SUBDIR="$TDIR/d-$NODE_ID-$RANDOM"
            sudo mkdir -p "$SUBDIR" 2>/dev/null
            NEWPATH="$SUBDIR/$(basename "$TARGET")"
            if sudo mv "$TARGET" "$NEWPATH" 2>/dev/null; then
                LEDGER["$NEWPATH"]="${LEDGER[$TARGET]}"
                unset 'LEDGER[$TARGET]'
                NAMES[$IDX]="$NEWPATH"
                OP_OK=$((OP_OK+1))
                verify_path "$NEWPATH"
            else
                OP_ERR=$((OP_ERR+1))
            fi
            ;;

        8)  # delete — must actually be gone
            [[ ${#NAMES[@]} -eq 0 ]] && continue
            IDX=$(( $RANDOM % ${#NAMES[@]} ))
            TARGET="${NAMES[$IDX]}"
            [[ -n "${LEDGER[$TARGET]:-}" ]] || continue
            if sudo rm -f "$TARGET" 2>/dev/null; then
                if sudo test -e "$TARGET" 2>/dev/null; then
                    echo "CORRUPTION: $TARGET still exists after delete" >> "$CORRUPT_LOG"
                    CORRUPT=$((CORRUPT+1))
                else
                    unset 'LEDGER[$TARGET]'
                    DELETED["$TARGET"]=1
                    DELETED_COUNT=$((DELETED_COUNT+1))
                    unset 'NAMES[$IDX]'
                    NAMES=("${NAMES[@]}")
                    OP_OK=$((OP_OK+1))
                fi
            else
                OP_ERR=$((OP_ERR+1))
            fi
            ;;

        9)  # cross-node read — read a file this worker did NOT create, if
            # one is visible in the shared test dir. Cheap continuous
            # cross-node visibility check; the authoritative one is the
            # orchestrator's final pass done from a different node entirely.
            OTHER=$(sudo ls "$TDIR" 2>/dev/null | grep -v "^f-$NODE_ID-\|^ren-$NODE_ID-\|^d-$NODE_ID-" | shuf -n1 2>/dev/null || true)
            if [[ -n "$OTHER" ]]; then
                sudo cat "$TDIR/$OTHER" > /dev/null 2>&1 && OP_OK=$((OP_OK+1)) || OP_ERR=$((OP_ERR+1))
            fi
            ;;
    esac

    # Periodic self-check sweep: every 25 ops, re-verify a handful of
    # currently-live files at random, not just the one this iteration
    # touched — catches drift a fresh-write-then-read can mask.
    if (( OPS_DONE % 25 == 0 && ${#NAMES[@]} > 0 )); then
        for _ in 1 2 3 4 5; do
            [[ ${#NAMES[@]} -eq 0 ]] && break
            T="${NAMES[$(( $RANDOM % ${#NAMES[@]} ))]}"
            [[ -n "${LEDGER[$T]:-}" ]] && verify_path "$T"
        done
    fi
done

# Final: confirm every path this worker believes deleted is truly absent,
# and dump the surviving ledger for the orchestrator's cross-node pass.
LEDGER_FILE="/tmp/integrity-fuzz-ledger-$NODE_ID.tsv"
: > "$LEDGER_FILE"
for p in "${!LEDGER[@]}"; do
    printf '%s\t%s\n' "$p" "${LEDGER[$p]}" >> "$LEDGER_FILE"
done
DELETED_FILE="/tmp/integrity-fuzz-deleted-$NODE_ID.txt"
: > "$DELETED_FILE"
for p in "${!DELETED[@]}"; do
    echo "$p" >> "$DELETED_FILE"
done

cat <<JSONEOF
{
  "node": "$NODE_ID",
  "ops_done": $OPS_DONE,
  "ops_ok": $OP_OK,
  "ops_err": $OP_ERR,
  "corruption_events": $CORRUPT,
  "surviving_files": ${#NAMES[@]},
  "deleted_files": $DELETED_COUNT
}
JSONEOF
WORKER

# ============================================================
# Deploy + launch
# ============================================================
log "Deploying worker to $COUNT nodes"
for ip in "${PUB_IPS[@]}"; do
    scp $SSH_OPTS -q /tmp/integrity-fuzz-worker.sh "ec2-user@$ip:/tmp/integrity-fuzz-worker.sh" 2>/dev/null
    ssh $SSH_OPTS "ec2-user@$ip" "chmod +x /tmp/integrity-fuzz-worker.sh; sudo mkdir -p $TEST_DIR" 2>/dev/null
done

log "Running ${DURATION}s of randomized ops on $COUNT nodes (no fault injection)..."
for i in "${!PUB_IPS[@]}"; do
    ip="${PUB_IPS[$i]}"
    seed=$((SEED_BASE + i * 7919))
    ssh $SSH_OPTS -f "ec2-user@$ip" \
        "nohup bash /tmp/integrity-fuzz-worker.sh n$((i+1)) $seed $DURATION $TEST_DIR > /tmp/integrity-fuzz-result-$i.json 2>/tmp/integrity-fuzz-stderr-$i.log &" 2>/dev/null
done

log "Workers launched. Waiting $((DURATION + 15))s for completion..."
sleep $((DURATION + 15))

# ============================================================
# Collect per-node results + ledgers
# ============================================================
log "Collecting results..."
TOTAL_OK=0; TOTAL_ERR=0; TOTAL_CORRUPT=0
for i in "${!PUB_IPS[@]}"; do
    ip="${PUB_IPS[$i]}"
    scp $SSH_OPTS -q "ec2-user@$ip:/tmp/integrity-fuzz-result-$i.json" "$REPORT_DIR/node-$i.json" 2>/dev/null || true
    scp $SSH_OPTS -q "ec2-user@$ip:/tmp/integrity-fuzz-ledger-n$((i+1)).tsv" "$REPORT_DIR/ledger-$i.tsv" 2>/dev/null || true
    scp $SSH_OPTS -q "ec2-user@$ip:/tmp/integrity-fuzz-deleted-n$((i+1)).txt" "$REPORT_DIR/deleted-$i.txt" 2>/dev/null || true
    scp $SSH_OPTS -q "ec2-user@$ip:/tmp/integrity-fuzz-corrupt-n$((i+1)).log" "$REPORT_DIR/corrupt-$i.log" 2>/dev/null || true

    f="$REPORT_DIR/node-$i.json"
    if [[ -f "$f" ]]; then
        ok=$(jq -r '.ops_ok // 0' "$f" 2>/dev/null || echo 0)
        err=$(jq -r '.ops_err // 0' "$f" 2>/dev/null || echo 0)
        corrupt=$(jq -r '.corruption_events // 0' "$f" 2>/dev/null || echo 0)
        TOTAL_OK=$((TOTAL_OK + ok)); TOTAL_ERR=$((TOTAL_ERR + err)); TOTAL_CORRUPT=$((TOTAL_CORRUPT + corrupt))
        log "  node $i ($ip): ok=$ok err=$err corruption=$corrupt"
    else
        fail "no results collected from node $i ($ip)"
    fi

    if [[ -s "$REPORT_DIR/corrupt-$i.log" ]]; then
        fail "node $i logged corruption during the run:"
        while IFS= read -r line; do log "    $line"; done < "$REPORT_DIR/corrupt-$i.log"
    fi
done

if [[ "$TOTAL_CORRUPT" -gt 0 ]]; then
    fail "in-run corruption events: $TOTAL_CORRUPT"
else
    pass "no in-run corruption across $COUNT nodes ($TOTAL_OK ops ok, $TOTAL_ERR errored)"
fi

# ============================================================
# Final cross-node verification pass: re-read every surviving file from a
# DIFFERENT node than the one that wrote it, and confirm every deleted
# file is truly gone everywhere. This is the authoritative check — the
# in-run one above can't rule out a write that looked fine locally but
# never actually replicated.
# ============================================================
log "--- Final cross-node verification ---"
VERIFY_OK=0; VERIFY_FAIL=0
for i in "${!PUB_IPS[@]}"; do
    ledger="$REPORT_DIR/ledger-$i.tsv"
    [[ -f "$ledger" ]] || continue
    # Verify from a different node than the writer, round-robin.
    vi=$(( (i + 1) % COUNT ))
    vip="${PUB_IPS[$vi]}"
    while IFS=$'\t' read -r path expect; do
        [[ -z "$path" ]] && continue
        got=$(ssh -n $SSH_OPTS -q "ec2-user@$vip" "sudo sha256sum '$path' 2>/dev/null | awk '{print \$1}'" 2>/dev/null || echo "")
        if [[ "$got" == "$expect" ]]; then
            VERIFY_OK=$((VERIFY_OK+1))
        else
            VERIFY_FAIL=$((VERIFY_FAIL+1))
            fail "cross-node mismatch: $path written on node$((i+1)), read from node$((vi+1)): expected=$expect got=${got:-<unreadable>}"
        fi
    done < "$ledger"
done
[[ "$VERIFY_FAIL" -eq 0 ]] && pass "cross-node re-verification: $VERIFY_OK/$VERIFY_OK files match (read from a different node than the writer)"

log "--- Final deletion verification ---"
DEL_OK=0; DEL_FAIL=0
for i in "${!PUB_IPS[@]}"; do
    deleted="$REPORT_DIR/deleted-$i.txt"
    [[ -f "$deleted" ]] || continue
    vi=$(( (i + 1) % COUNT ))
    vip="${PUB_IPS[$vi]}"
    while IFS= read -r path; do
        [[ -z "$path" ]] && continue
        if ssh -n $SSH_OPTS -q "ec2-user@$vip" "sudo test -e '$path'" 2>/dev/null; then
            DEL_FAIL=$((DEL_FAIL+1))
            fail "deleted file still visible from a different node: $path (deleted on node$((i+1)), checked from node$((vi+1)))"
        else
            DEL_OK=$((DEL_OK+1))
        fi
    done < "$deleted"
done
[[ "$DEL_FAIL" -eq 0 ]] && pass "deletion cross-node verification: $DEL_OK/$DEL_OK confirmed gone"

# ---- Cleanup ----
for ip in "${PUB_IPS[@]}"; do
    ssh $SSH_OPTS -q "ec2-user@$ip" "sudo rm -rf $TEST_DIR /tmp/integrity-fuzz-*" 2>/dev/null || true
done

log ""
log "============================================"
log "Integrity fuzz complete"
log "  Ops:                 $((TOTAL_OK + TOTAL_ERR)) ($TOTAL_OK ok, $TOTAL_ERR errored)"
log "  In-run corruption:   $TOTAL_CORRUPT"
log "  Cross-node verified: $VERIFY_OK ok, $VERIFY_FAIL mismatched"
log "  Deletion verified:   $DEL_OK ok, $DEL_FAIL still-visible"
if [[ "$FAILED" -eq 0 ]]; then
    log "STATUS: PASS — no corruption detected"
else
    log "STATUS: FAIL — see above"
fi
log "Report: $REPORT_DIR"
log "============================================"

exit "$FAILED"
