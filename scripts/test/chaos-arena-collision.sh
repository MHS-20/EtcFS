#!/bin/bash
# chaos-arena-collision.sh — arena ownership / cross-node block collision test.
#
# Reproduces the concrete channel through which Kleppmann's stale-write hazard
# reaches EtcFS.  See docs/architecture/kleppmann-stale-write-analysis.md.
#
# The hazard is NOT the textbook "two nodes overwrite the same block in place":
# EtcFS never overwrites a live extent, and a fenced node's late write lands as
# unreferenced garbage because the publishing commit is a generation-guarded
# etcd transaction.  The hazard is upstream of that, in the allocator: if a node
# ever puts an arena it does not own into its own free-list, two *unfenced*
# nodes hand out the same disk offset, both write different data there, and both
# extent commits succeed — the generation guard has nothing to reject, because
# neither node has been fenced.
#
# S8  — restart-adoption: a node restarts and rebuilds its free-list from etcd.
#       It must recover only its own arena.  Regression test for the bug where
#       existingArenaIDs scanned the whole "arena:" prefix.
# S9  — write-collision: after the restart, all three nodes write concurrently.
#       No two inodes may end up claiming the same disk offset.
# S10 — fenced-writer: fence a node mid-write and confirm its bytes never become
#       referenced (the publish gate), while the survivors keep writing.
#
# Usage:
#   ./chaos-arena-collision.sh docker [scenario|all]
#   ./chaos-arena-collision.sh aws    [scenario|all]
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
REPORT_DIR="$PROJECT_ROOT/chaos-report-arena-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$REPORT_DIR"
PASS=0; FAIL=0

MODE="${1:-}"
SCENARIO="${2:-all}"
[[ "$MODE" == "docker" || "$MODE" == "aws" ]] || { echo "usage: $0 docker|aws [scenario|all]"; exit 1; }

log() { echo "[$(date +%H:%M:%S)] $1" | tee -a "$REPORT_DIR/chaos.log"; }
logerr() {
    echo "[$(date +%H:%M:%S)] $1" >&2
    echo "[$(date +%H:%M:%S)] $1" >> "$REPORT_DIR/chaos.log"
}
pass() { log "  PASS: $1"; PASS=$((PASS+1)); }
fail() { log "  FAIL: $1"; FAIL=$((FAIL+1)); }

source "$SCRIPT_DIR/chaos-lib.sh"

# ---- helpers ----

# arena_record <node_index> — the arena ID this node has recorded in etcd,
# decoded from the 8-byte big-endian value at arena:<node_id>.
arena_record() {
    # etcdctl --hex prints the value as "\x00\x00\x00\x00\x00\x00\x00\x03";
    # strip the quotes and the \x separators, leaving bare hex digits.
    etcdctl_on get "arena:n$1" --print-value-only --hex 2>/dev/null |
        tr -d '"\\x' | tr -d '\n' |
        awk '{ if (length($0)) print strtonum("0x" $0); else print "none" }'
}

# extent_offsets — every "<disk_off> <extent_key>" pair currently published.
extent_offsets() {
    etcdctl_on get "extent:" --prefix 2>/dev/null | awk '
        /^extent:/ { key = $0; next }
        key != "" { split($0, f, ","); print f[2], key; key = "" }
    '
}

# collisions — disk offsets claimed by more than one inode.  This is the same
# invariant scrub.CheckExtentCollisions enforces, checked here directly so the
# test fails loudly rather than waiting for a scrub pass.
collisions() {
    extent_offsets | awk '
        {
            ino = $2; sub(/^extent:/, "", ino); sub(/\/.*$/, "", ino)
            if ($1 in owner && owner[$1] != ino)
                print "disk_off=" $1 " claimed by ino " owner[$1] " and ino " ino
            owner[$1] = ino
        }
    '
}

# ---- S8: a restarted node must not adopt another node's arena ----

scenario_s8() {
    log "======== S8: restart must not adopt a foreign arena ========"

    # Force every node to own an arena by writing through each mount.
    for i in 1 2 3; do
        eval "n=\$N$i"
        # shellcheck disable=SC2154
        writef "$n" "s8-seed-n$i-$(date +%s)" "s8-seed-n$i.txt" || true
    done
    sleep 2

    local before_1 before_2 before_3
    before_1=$(arena_record 1); before_2=$(arena_record 2); before_3=$(arena_record 3)
    log "  arena records before restart: n1=$before_1 n2=$before_2 n3=$before_3"

    if [[ "$before_1" == "$before_2" || "$before_1" == "$before_3" || "$before_2" == "$before_3" ]]; then
        fail "two nodes share an arena ID before any restart ($before_1/$before_2/$before_3)"
        return
    fi

    # Restart n1: this is the path that runs Allocator.Reconstruct.
    log "  restarting n1 daemons (triggers arena Reconstruct)..."
    if [[ "$MODE" == "docker" ]]; then
        restart_pair "$M1" "$N1" > /dev/null
    else
        restart_daemons "$N1" "n1" > /dev/null
    fi
    sleep 3

    local after_1
    after_1=$(arena_record 1)
    log "  arena record after restart: n1=$after_1"

    if [[ "$after_1" == "$before_2" || "$after_1" == "$before_3" ]]; then
        fail "n1 adopted a foreign arena after restart (now $after_1, n2=$before_2 n3=$before_3)"
        return
    fi

    # A write from the restarted node must land in its own arena, so its new
    # extents must not collide with anything the other two published.
    writef "$N1" "s8-after-restart-$(date +%s)" "s8-after-restart.txt" || true
    sleep 2

    local c
    c=$(collisions)
    if [[ -n "$c" ]]; then
        fail "disk offset collision after n1 restart:"
        while IFS= read -r line; do logerr "    $line"; done <<< "$c"
        return
    fi
    pass "restarted node kept its own arena, no offset collision"
}

# ---- S9: concurrent writes from all nodes must not collide ----

scenario_s9() {
    log "======== S9: concurrent cross-node writes, no shared offsets ========"

    local stamp; stamp=$(date +%s)
    for round in 1 2 3 4 5; do
        for i in 1 2 3; do
            eval "n=\$N$i"
            # shellcheck disable=SC2154
            writef "$n" "s9-n$i-r$round-$stamp" "s9-n$i-r$round.txt" &
        done
        wait
    done
    sleep 3

    local c
    c=$(collisions)
    if [[ -n "$c" ]]; then
        fail "concurrent writes produced overlapping disk offsets:"
        while IFS= read -r line; do logerr "    $line"; done <<< "$c"
        return
    fi

    # Every file must read back exactly what its writer wrote — a collision that
    # somehow escaped the offset check would show up here as wrong content.
    local bad=0
    for round in 1 2 3 4 5; do
        for i in 1 2 3; do
            eval "n=\$N$i"
            local got want="s9-n$i-r$round-$stamp"
            # shellcheck disable=SC2154
            got=$(readf "$n" "s9-n$i-r$round.txt")
            [[ "$got" == "$want" ]] || { logerr "    content mismatch: got '$got' want '$want'"; bad=1; }
        done
    done
    [[ "$bad" -eq 0 ]] || { fail "file contents corrupted across concurrent writers"; return; }

    pass "15 concurrent cross-node writes, distinct offsets, contents intact"
}

# ---- S10: a fenced node's bytes must never become referenced ----

scenario_s10() {
    log "======== S10: fenced writer's data stays unreferenced ========"

    local before_count
    before_count=$(extent_offsets | wc -l)

    local gen
    gen=$(etcdctl_on get "gen:n1" --print-value-only 2>/dev/null)
    gen=${gen:-0}
    log "  bumping gen:n1 $gen -> $((gen+1)) while n1 writes"

    # Start a write and fence the writer underneath it.
    writef "$N1" "s10-fenced-payload-$(date +%s)" "s10-fenced.txt" &
    local writer=$!
    etcdctl_on put "gen:n1" "$((gen+1))" > /dev/null 2>&1
    wait "$writer" 2>/dev/null
    sleep 2

    # The fenced node's extent commit must have been rejected: either the file
    # never appears, or it appears with no extent referencing the stale bytes.
    if readf "$N1" "s10-fenced.txt" | grep -q "s10-fenced-payload"; then
        # Content readable means the write was published *before* the bump won
        # the race, which is legal.  What is never legal is a published extent
        # stamped with the superseded generation.
        log "  write published before the fence took effect (legal)"
    fi

    local stale
    stale=$(etcdctl_on get "extent:" --prefix 2>/dev/null | awk -v g="$gen" '
        /^extent:/ { key = $0; next }
        key != "" { split($0, f, ","); if (f[4] + 0 > g) print key " gen=" f[4]; key = "" }
    ')
    if [[ -n "$stale" ]]; then
        fail "extent published carrying a generation above the pre-fence value:"
        while IFS= read -r line; do logerr "    $line"; done <<< "$stale"
        return
    fi

    # Survivors must still be writable after the fence.
    writef "$N2" "s10-survivor-$(date +%s)" "s10-survivor.txt" || { fail "survivor n2 not writable after fence"; return; }

    local after_count
    after_count=$(extent_offsets | wc -l)
    log "  extents before=$before_count after=$after_count"

    local c
    c=$(collisions)
    [[ -z "$c" ]] || { fail "collision introduced around the fence"; return; }

    pass "fenced writer left no referenced extent, survivors unaffected"
}

# ---- driver ----

log "Arena collision test: mode=$MODE scenario=$SCENARIO"
provision_cluster || { log "FATAL: provision failed"; teardown_cluster; exit 1; }

case "$SCENARIO" in
    s8)  scenario_s8 ;;
    s9)  scenario_s9 ;;
    s10) scenario_s10 ;;
    all) scenario_s8; scenario_s9; scenario_s10 ;;
    *)   log "unknown scenario: $SCENARIO"; teardown_cluster; exit 1 ;;
esac

teardown_cluster

{
    echo "=== Arena Ownership / Collision Report ($MODE) ==="
    echo "Pass: $PASS  Fail: $FAIL  Total: $((PASS+FAIL))"
    if [[ "$FAIL" -eq 0 ]]; then
        echo "STATUS: ALL PASS"
    else
        echo "STATUS: FAILURES PRESENT"
    fi
} | tee "$REPORT_DIR/summary.txt"

log "Report: $REPORT_DIR/summary.txt"
[[ "$FAIL" -eq 0 ]]
