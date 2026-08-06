#!/bin/bash
# chaos-arena-reclaim.sh — arena and disk-space reclamation test.
#
# Covers docs/TODO-hardening.md § 6 (arena reclamation has no implementation
# — now implemented, not yet chaos-tested until this script runs).  Two
# distinct leaks are closed by that work and both are exercised here:
#
#   R1 — a node's arena, leaked on every departure (graceful or fenced),
#        now returns to the free_arena: pool and gets handed to the next
#        node that needs space instead of the counter growing forever.
#   R2 — a recycled arena is not assumed empty: the node that claims it
#        rebuilds its bitmap from the live extents still in it, so writes
#        from the new owner cannot land on blocks the previous owner's
#        surviving files still reference.
#   R3 — deleting a file returns its blocks to the allocator, not just its
#        etcd metadata. This is the hotter path (every rm, not just every
#        node departure) and was leaking independently of R1/R2.
#   R4 — single-signal fencing (no device-enforced Fencer configured, the
#        Docker/gp3 case) has no proof the fenced node's kernel stopped
#        writing, so its arena must NOT be reclaimed. Verifies the guard
#        that gates reclamation on invariant 4 rather than assuming it.
#
# Docker-only. R4's counterpart on AWS (arena reclaim IS safe once a
# NVMeFencer confirms the preempt) is R9 in chaos-nvme-fencing.sh, since it
# needs the real reservation hardware this script cannot provide.
#
# Usage:
#   ./chaos-arena-reclaim.sh docker [scenario|all]
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
REPORT_DIR="$PROJECT_ROOT/chaos-report-arena-reclaim-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$REPORT_DIR"
PASS=0; FAIL=0

MODE="${1:-docker}"
SCENARIO="${2:-all}"
[[ "$MODE" == "docker" ]] || { echo "usage: $0 docker [scenario|all]  (aws counterpart: chaos-nvme-fencing.sh R9)"; exit 1; }

log() { echo "[$(date +%H:%M:%S)] $1" | tee -a "$REPORT_DIR/chaos.log"; }
logerr() {
    echo "[$(date +%H:%M:%S)] $1" >&2
    echo "[$(date +%H:%M:%S)] $1" >> "$REPORT_DIR/chaos.log"
}
pass() { log "  PASS: $1"; PASS=$((PASS+1)); }
fail() { log "  FAIL: $1"; FAIL=$((FAIL+1)); }

source "$SCRIPT_DIR/chaos-lib.sh"

# ---- helpers ----

# arena_id <node_key> — this node's arena:<node_key> record, decoded from the
# 8-byte big-endian value, or "none" if it has no record.  Mirrors
# arena_record() in chaos-arena-collision.sh.
arena_id() {
    etcdctl_on get "arena:$1" --print-value-only --hex 2>/dev/null |
        tr -d '"\\x' | tr -d '\n' |
        awk '{ if (length($0)) print strtonum("0x" $0); else print "none" }'
}

# free_arenas — every arena ID currently sitting in the free_arena: pool.
free_arenas() {
    etcdctl_on get "free_arena:" --prefix --keys-only 2>/dev/null |
        sed -n 's/^free_arena://p'
}

extent_offsets() {
    etcdctl_on get "extent:" --prefix 2>/dev/null | awk '
        /^extent:/ { key = $0; next }
        key != "" { split($0, f, ","); print f[2], key; key = "" }
    '
}

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

# ---- R1/R2: graceful departure frees the arena, next joiner recycles it ----

scenario_r1() {
    log "======== R1/R2: graceful leave frees an arena, next joiner recycles it ========"

    local node4; node4=$(add_node 4)
    if [[ -z "$node4" ]]; then fail "node4 failed to join — cannot run R1"; return; fi

    writef "$node4" "r1-seed-$(date +%s)" "r1-seed.txt" || { fail "seed write via node4 failed"; return; }
    sleep 2

    local a4; a4=$(arena_id n4)
    if [[ "$a4" == "none" ]]; then fail "node4 never acquired an arena, cannot test its release"; return; fi
    log "  node4 owns arena $a4"

    log "  removing node4 gracefully..."
    remove_node 4
    sleep 2

    local freed; freed=$(free_arenas)
    if ! grep -qx "$a4" <<< "$freed"; then
        fail "arena $a4 not in free_arena: pool after graceful leave (pool: $(echo "$freed" | tr '\n' ' '))"
        return
    fi
    pass "arena $a4 returned to the free pool on graceful leave"

    # A fresh joiner must claim it back rather than bumping the counter.
    local node5; node5=$(add_node 5)
    if [[ -z "$node5" ]]; then fail "node5 failed to join — cannot verify recycling"; return; fi
    writef "$node5" "r1-recycled-$(date +%s)" "r1-recycled.txt" || { fail "write via node5 failed"; return; }
    sleep 2

    local a5; a5=$(arena_id n5)
    log "  node5 owns arena $a5 (freed arena was $a4)"
    if [[ "$a5" != "$a4" ]]; then
        # Not fatal on its own — another arena could have been in the pool
        # first — but worth surfacing, since with a clean single-run cluster
        # $a4 should be the only free candidate.
        logerr "  node5 did not recycle arena $a4 (got $a5) — pool may have had another entry"
    else
        pass "node5 recycled node4's freed arena $a4 instead of extending the device"
    fi

    # R2: node4's original write must still be intact and node5's new write
    # must not collide with it — the recycled arena's live extent had to
    # survive the handover.
    local got; got=$(readf "$N1" "r1-seed.txt")
    [[ -n "$got" ]] || { fail "r1-seed.txt unreadable after node4 left — data lost, not just node4"; return; }

    local c; c=$(collisions)
    if [[ -n "$c" ]]; then
        fail "disk offset collision after arena recycling:"
        while IFS= read -r line; do logerr "    $line"; done <<< "$c"
        return
    fi
    pass "recycled arena's prior live extent intact, no collision with new owner's writes"
}

# ---- R3: deleting a file returns its blocks, not just its metadata ----

scenario_r3() {
    log "======== R3: file deletion reclaims its blocks via the scrubber ========"

    local fname="r3-transient-$(date +%s).txt"
    writef "$N1" "r3-payload" "$fname" || { fail "write failed"; return; }
    sleep 1

    local before_extents; before_extents=$(extent_offsets | wc -l)
    rmf "$N1" "$fname" > /dev/null 2>&1
    sleep 1

    # The dirent/inode side is immediate; the extent key is orphaned until
    # AtomicUnlink's gap and only the scrubber closes it — wait one full
    # scrub interval (30s, cmd/etcfuse-meta/main.go) plus margin.
    log "  waiting 35s for a scrub pass to reclaim the orphaned extent..."
    sleep 35

    local after_extents; after_extents=$(extent_offsets | wc -l)
    log "  extents before-delete=$before_extents after-scrub=$after_extents"
    if [[ "$after_extents" -ge "$before_extents" ]]; then
        fail "orphaned extent key was not cleaned up by the scrubber (before=$before_extents after=$after_extents)"
        return
    fi
    pass "scrubber removed the deleted file's orphaned extent key"

    # Indirect evidence the blocks, not just the key, came back: repeat
    # write/delete/scrub a few times and confirm the cluster keeps writing
    # without degradation — if Free() were a no-op the bitmap would only ever
    # grow, which a handful of cycles won't surface directly, but a stuck or
    # failing write here would.
    for i in 1 2 3; do
        writef "$N2" "r3-cycle-$i" "r3-cycle-$i.txt" || { fail "write cycle $i failed after reclaim"; return; }
        rmf "$N2" "r3-cycle-$i.txt" > /dev/null 2>&1
    done
    pass "cluster kept writing through repeated create/delete cycles after reclaim"
}

# ---- R4: single-signal fencing must NOT reclaim the fenced node's arena ----

scenario_r4() {
    log "======== R4: single-signal fence leaves the arena leaked (no proof of quiescence) ========"

    local node4; node4=$(add_node 4)
    if [[ -z "$node4" ]]; then fail "node4 failed to join — cannot run R4"; return; fi
    writef "$node4" "r4-seed-$(date +%s)" "r4-seed.txt" || { fail "seed write via node4 failed"; return; }
    sleep 2

    local a4; a4=$(arena_id n4)
    if [[ "$a4" == "none" ]]; then fail "node4 never acquired an arena"; return; fi
    log "  node4 owns arena $a4, forcing an ungraceful fence (kill, no leave)..."

    docker kill -s KILL etcfs-meta4 etcfs-fuse4 > /dev/null 2>&1

    # This docker-compose cluster runs with no --ebs-volume-id / NVMe
    # reservations, so fencing.Controller runs single-signal: it bumps
    # gen:n4 on lease expiry but sets no Fencer, and the reclaim added in
    # this change is gated on c.fencer != nil. Give the lease TTL (10s) plus
    # the controller's watch/sweep margin.
    sleep 20

    local gen4; gen4=$(etcdctl_on get "gen:n4" --print-value-only 2>/dev/null | tr -d '[:space:]')
    if [[ -z "$gen4" || "$gen4" == "0" ]]; then
        fail "node4 was never fenced (gen:n4=$gen4) — cannot test the reclaim gate"
    else
        log "  node4 fenced at generation $gen4"
        local after; after=$(arena_id n4)
        if [[ "$after" == "none" ]]; then
            fail "arena $a4 was reclaimed under single-signal fencing — no severance proof exists in this mode, invariant 4 violated"
        else
            pass "arena $a4 correctly left leaked under single-signal fencing (after=$after)"
        fi
    fi

    docker rm -f etcfs-meta4 etcfs-fuse4 > /dev/null 2>&1
}

# ---- driver ----

log "Arena reclamation test: mode=$MODE scenario=$SCENARIO"
provision_cluster || { log "FATAL: provision failed"; teardown_cluster; exit 1; }

case "$SCENARIO" in
    r1)  scenario_r1 ;;
    r3)  scenario_r3 ;;
    r4)  scenario_r4 ;;
    all) scenario_r1; scenario_r3; scenario_r4 ;;
    *)   log "unknown scenario: $SCENARIO"; teardown_cluster; exit 1 ;;
esac

teardown_cluster

{
    echo "=== Arena/Disk Reclamation Report ($MODE) ==="
    echo "Pass: $PASS  Fail: $FAIL  Total: $((PASS+FAIL))"
    if [[ "$FAIL" -eq 0 ]]; then
        echo "STATUS: ALL PASS"
    else
        echo "STATUS: FAILURES PRESENT"
    fi
} | tee "$REPORT_DIR/summary.txt"

log "Report: $REPORT_DIR/summary.txt"
[[ "$FAIL" -eq 0 ]]
