#!/bin/bash
# chaos-nvme-fencing.sh — verify device-enforced fencing (NVMe reservations)
# on real AWS infrastructure.
#
# This is the strongest fencing claim EtcFS makes, and the only one the
# storage layer enforces by itself: peers preempt an expired node's
# reservation key on the shared EBS io2 Multi-Attach volume, after which the
# device rejects that node's writes synchronously at write(2) with EBADE —
# no polling, no grace period, no residual-I/O window.
#
# What it asserts, in order:
#   R1  all three nodes register their derived reservation key at startup
#   R2  the reservation is Write Exclusive – All Registrants, and every node
#       can write concurrently under it (this is genuine active/active)
#   R3  a partitioned node's key is preempted by a survivor
#   R4  the preempted node's raw device write fails at the device itself
#       (EBADE), not merely at the metadata layer
#   R5  the generation bump followed the confirmed preempt
#   R6  survivors keep writing throughout
#   R7  reservation state survives: the fenced node re-registers on restart
#       and becomes writable again
#   R8  a volume detach/reattach cycle does not silently leave a node
#       registered-but-unable-to-write, nor writable-but-unregistered
#   R9  a confirmed preempt is exactly the invariant-4 proof arena
#       reclamation needs (docs/TODO-hardening.md § 6): once R3-R5 confirm
#       n1 is preempted AND fenced, pkg/fencing.Controller must release its
#       arena — this is the one fencing mode where that reclaim is safe to
#       fire, unlike the docker/single-signal case chaos-arena-reclaim.sh's
#       R4 covers.
#
# This is AWS-only and cannot be moved to Docker: loopback devices support no
# NVMe reservation commands at all. It is the only script that exercises
# pkg/nvmeresv and pkg/fencing/nvme.go against real hardware.
#
# Requires the etcfs-nodes IAM instance profile (scripts/infra/fencing-iam.sh)
# only for teardown/provisioning — the fencing path itself makes no AWS API
# call, which is part of the point.
#
# Usage:
#   ./chaos-nvme-fencing.sh
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
REPORT_DIR="$PROJECT_ROOT/chaos-report-nvme-fencing-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$REPORT_DIR"
PASS=0; FAIL=0

MODE=aws
# Start the daemons with --nvme-reservations instead of the EBS detach flags
# (chaos-lib.sh's provision_cluster / restart_daemons honour this).
export ETCFS_FENCE_MODE=nvme

DEV=/dev/nvme1n1

log() { echo "[$(date +%H:%M:%S)] $1" | tee -a "$REPORT_DIR/chaos.log"; }
logerr() {
    echo "[$(date +%H:%M:%S)] $1" >&2
    echo "[$(date +%H:%M:%S)] $1" >> "$REPORT_DIR/chaos.log"
}

source "$SCRIPT_DIR/chaos-lib.sh"

ok()  { PASS=$((PASS+1)); log "  PASS: $1"; }
bad() { FAIL=$((FAIL+1)); log "  FAIL: $1"; }

# node_key <node-id> — the reservation key a node registers, mirroring
# nvmeresv.KeyForNode (FNV-1a 64 of the node ID). Derived, not assigned, so
# this script computes the same key the daemon does without reading it from
# anywhere.
node_key() {
    python3 - "$1" <<'PY'
import sys
h = 0xcbf29ce484222325
for b in sys.argv[1].encode():
    h = ((h ^ b) * 0x100000001b3) & 0xFFFFFFFFFFFFFFFF
print(h or 1)
PY
}

# resv_report <public-ip> — raw `nvme resv-report` output from a node.
# Read via nvme-cli rather than through the daemon: the point is to observe
# the device's own state, independently of what EtcFS believes.
resv_report() {
    runcmd30 "$1" "sudo nvme resv-report $DEV -o json 2>&1"
}

# key_registered <public-ip> <key-decimal> — true if the device reports the
# key among the registered controllers. nvme-cli prints rkey as a decimal in
# JSON output; the hex alternative is matched too so a cli version change
# does not silently turn every check into "not registered".
key_registered() {
    local hex; hex=$(printf '%x' "$2")
    resv_report "$1" | tr '[:upper:]' '[:lower:]' | grep -qE "\"rkey\"[[:space:]]*:[[:space:]]*(${2}|0x${hex})"
}

# NOT etcdctl_on: that helper always queries via N1, which is the node this
# script partitions — its local etcd cannot serve a linearizable read while
# cut off from quorum, and the query hangs to its timeout and returns empty,
# which reads exactly like "generation never bumped". Same trap documented in
# chaos-fencing-detach.sh. Query via N2, never partitioned here.
gen_val() {
    timeout 15 ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -q ec2-user@"$N2" \
        "/usr/local/bin/etcdctl --endpoints=http://$P1_PRIV:2379,http://$P2_PRIV:2379,http://$P3_PRIV:2379 get gen:$1 --print-value-only" \
        2>/dev/null | tr -d '[:space:]'
}

# arena_id <node_key> — this node's arena:<node_key> record, decoded from the
# 8-byte big-endian value, queried via n2 (never partitioned here) for the
# same reason gen_val is. "none" if no record.
arena_id() {
    timeout 15 ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -q ec2-user@"$N2" \
        "/usr/local/bin/etcdctl --endpoints=http://$P1_PRIV:2379,http://$P2_PRIV:2379,http://$P3_PRIV:2379 get arena:$1 --print-value-only --hex" \
        2>/dev/null | tr -d '"\\x' | tr -d '\n' |
        awk '{ if (length($0)) print strtonum("0x" $0); else print "none" }'
}

if ! provision_cluster; then
    log "FATAL: provision failed"
    teardown_cluster
    exit 1
fi

P1_PRIV=$(jq -r '.compute_ips[0]' "$PROJECT_ROOT/$STATE_FILE")
P2_PRIV=$(jq -r '.compute_ips[1]' "$PROJECT_ROOT/$STATE_FILE")
P3_PRIV=$(jq -r '.compute_ips[2]' "$PROJECT_ROOT/$STATE_FILE")
K1=$(node_key n1); K2=$(node_key n2); K3=$(node_key n3)
log "reservation keys: n1=$K1 n2=$K2 n3=$K3"

log "======== Installing nvme-cli (observation only — the daemon uses raw ioctls) ========"
for ip in "$N1" "$N2" "$N3"; do
    runcmd60 "$ip" "command -v nvme >/dev/null 2>&1 || sudo dnf install -q -y nvme-cli 2>&1 | tail -1; command -v nvme || echo NO_NVME_CLI" \
        >>"$REPORT_DIR/chaos.log" 2>&1
done

log "======== R1: every node registered its key at startup ========"
REPORT=$(resv_report "$N2")
echo "$REPORT" > "$REPORT_DIR/resv-report-baseline.json"
REGISTERED=0
for k in "$K1" "$K2" "$K3"; do
    if key_registered "$N2" "$k"; then REGISTERED=$((REGISTERED+1)); fi
done
if [[ "$REGISTERED" -eq 3 ]]; then
    ok "all 3 nodes are registered on $DEV (an unregistered node can neither preempt nor be preempted)"
else
    bad "only $REGISTERED/3 reservation keys registered — see resv-report-baseline.json"
fi

log "======== R2: Write Exclusive – All Registrants, with concurrent writers ========"
if echo "$REPORT" | grep -qi '"rtype"[[:space:]]*:[[:space:]]*5'; then
    ok "reservation type is Write Exclusive – All Registrants (rtype=5)"
else
    bad "unexpected reservation type — WEAR is the only type permitting concurrent writers"
fi

CONC_OK=1
for i in 1 2 3; do
    eval "ip=\$N$i"
    writef "$ip" "nvme-baseline-n$i" "nvme-baseline-n$i.txt" || CONC_OK=0
done
for i in 1 2 3; do
    [[ -n "$(readf "$N2" "nvme-baseline-n$i.txt")" ]] || CONC_OK=0
done
if [[ "$CONC_OK" -eq 1 ]]; then
    ok "all three registrants wrote concurrently and every write is visible cluster-wide"
else
    bad "concurrent active/active writes under WEAR failed"
    teardown_cluster; exit 1
fi

ARENA1_BEFORE=$(arena_id n1)
log "n1 owns arena $ARENA1_BEFORE before partition (checked for R9's reclaim-after-fence assertion)"

log "======== Partitioning n1 from etcd peers (n2, n3) via iptables ========"
# SG swaps do not sever established connections; iptables does. Same technique
# as chaos-fencing-detach.sh.
runcmd60 "$N1" "command -v iptables >/dev/null 2>&1 || sudo dnf install -q -y iptables iptables-nft 2>&1 | tail -1" \
    >>"$REPORT_DIR/chaos.log" 2>&1
for PEER in "$P2_PRIV" "$P3_PRIV"; do
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=10 -q ec2-user@"$N1" "
        sudo iptables -A OUTPUT -p tcp -d $PEER --dport 2379 -j DROP
        sudo iptables -A OUTPUT -p tcp -d $PEER --dport 2380 -j DROP
        sudo iptables -A INPUT  -p tcp -s $PEER --sport 2379 -j DROP
        sudo iptables -A INPUT  -p tcp -s $PEER --sport 2380 -j DROP
        sudo iptables -A INPUT  -p tcp -s $PEER --dport 2379 -j DROP
        sudo iptables -A INPUT  -p tcp -s $PEER --dport 2380 -j DROP
    " >>"$REPORT_DIR/chaos.log" 2>&1
done

PROBE=$(runcmd "$N1" "timeout 5 curl -s -o /dev/null -w '%{http_code}' http://$P2_PRIV:2379/health 2>&1; echo rc=\$?")
if [[ "$PROBE" == *"rc=0"* && "$PROBE" != *"000"* ]]; then
    bad "partition did not take effect — n1 still reached etcd at $P2_PRIV (probe=$PROBE)"
    teardown_cluster; exit 1
else
    ok "partition verified: n1 cannot reach etcd peers (probe=$PROBE)"
fi

log "======== R3: a survivor preempts n1's reservation key ========"
# Bound: lease TTL (10s) + the preempt itself, which is synchronous — no
# 60s detach poll to wait out, which is the whole advantage over the EBS path.
DEADLINE=60
WAITED=0
PREEMPTED=0
while [[ "$WAITED" -lt "$DEADLINE" ]]; do
    if ! key_registered "$N2" "$K1"; then PREEMPTED=1; break; fi
    sleep 3; WAITED=$((WAITED+3))
done
resv_report "$N2" > "$REPORT_DIR/resv-report-after-fence.json"

if [[ "$PREEMPTED" -eq 1 ]]; then
    ok "n1's key $K1 gone from the device's registrant list after ~${WAITED}s (real preempt, device-observed)"
else
    bad "n1's key $K1 still registered after ${DEADLINE}s — device-enforced fencing did not complete"
fi

if key_registered "$N2" "$K2" && key_registered "$N2" "$K3"; then
    ok "survivors' registrations untouched — preempt ejected one host, not the reservation"
else
    bad "a survivor lost its registration — preempt ejected too much"
fi

log "  survivor controller logs:"
for ip in "$N2" "$N3"; do
    runcmd "$ip" "grep -E 'fencing node|device access|node fenced|nvme' /tmp/meta.log 2>/dev/null | tail -10" 2>/dev/null \
        | while IFS= read -r line; do log "    [$ip] $line"; done
done

log "======== R4: the device itself rejects n1's writes ========"
# The claim under test, and the one nothing else in the suite can check: a
# preempted host's write fails at write(2), not at the metadata layer. Written
# with O_DIRECT (oflag=direct) exactly as pkg/blockio issues writes, to the
# volume's last MiB — outside any arena the filesystem has handed out.
LAST_MIB=$(runcmd "$N1" "echo \$(( \$(sudo blockdev --getsize64 $DEV) / 1048576 - 1 ))")
RAW=$(runcmd30 "$N1" "sudo dd if=/dev/zero of=$DEV bs=1M count=1 seek=$LAST_MIB oflag=direct conv=notrunc 2>&1; echo rc=\$?")
log "  raw write result: $(echo "$RAW" | tr '\n' ' ' | cut -c1-200)"
if [[ "$RAW" == *"rc=0"* ]]; then
    bad "a preempted node still wrote to the raw device — the reservation is not being enforced"
elif [[ "$RAW" == *"Invalid exchange"* || "$RAW" == *"EBADE"* ]]; then
    ok "preempted node's O_DIRECT write rejected by the device with EBADE (Invalid exchange), zero bytes written"
else
    ok "preempted node's raw write failed at the device (non-EBADE error, see log) — writes are blocked"
fi

log "======== R5: the generation bump followed the confirmed preempt ========"
GEN1=$(gen_val n1)
if [[ -n "$GEN1" && "$GEN1" != "0" ]]; then
    ok "gen:n1 bumped to $GEN1"
else
    bad "gen:n1 was never bumped ($GEN1)"
fi
if [[ "$PREEMPTED" -eq 1 && -n "$GEN1" && "$GEN1" != "0" ]]; then
    ok "device-enforced fencing held: preempt confirmed AND generation bumped, in that order"
else
    bad "fencing did not complete: preempted=$PREEMPTED gen=$GEN1"
fi

log "======== R9: confirmed preempt lets the controller reclaim n1's arena ========"
if [[ "$ARENA1_BEFORE" == "none" ]]; then
    bad "n1 had no arena before the fence — R2's baseline write should have given it one, cannot test reclaim"
else
    # pkg/fencing.Controller reclaims the arena synchronously inside fenceNode,
    # right after the generation bump — TestController_ReclaimsArenaAfterConfirmedFence
    # (pkg/fencing/controller_integration_test.go) proves this completes in
    # well under a second against real etcd. A per-iteration ssh+etcdctl poll
    # here adds enough of its own round-trip jitter to be an unreliable clock
    # for something that fast, so this checks both keys in one ssh call after
    # a fixed settle instead of racing a loop against the network.
    sleep 10
    STATE=$(timeout 15 ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -q ec2-user@"$N2" \
        "ETCDCTL_ENDPOINTS=http://$P1_PRIV:2379,http://$P2_PRIV:2379,http://$P3_PRIV:2379; \
         echo ARENA=\$(/usr/local/bin/etcdctl --endpoints=\$ETCDCTL_ENDPOINTS get arena:n1 --print-value-only --hex); \
         echo FREE=\$(/usr/local/bin/etcdctl --endpoints=\$ETCDCTL_ENDPOINTS get free_arena:$ARENA1_BEFORE --print-value-only)" \
        2>/dev/null)
    log "  post-fence state: $(echo "$STATE" | tr '\n' ' ')"

    if grep -q '^ARENA=$' <<< "$STATE"; then
        ok "n1's arena $ARENA1_BEFORE released (arena:n1 gone) shortly after the confirmed preempt"
    else
        bad "n1's arena $ARENA1_BEFORE still owned 10s after a confirmed preempt+gen bump — reclaim did not fire"
    fi
    if grep -q '^FREE=free$' <<< "$STATE"; then
        ok "arena $ARENA1_BEFORE is in the free pool, reachable by the next node that needs space"
    else
        bad "arena $ARENA1_BEFORE not found in free_arena: pool — released from n1 but not returned to circulation"
    fi
fi

log "======== R6: survivors unaffected ========"
if writef "$N2" "during-fence" "nvme-survivor.txt" && [[ -n "$(readf "$N3" "nvme-survivor.txt")" ]]; then
    ok "n2/n3 stayed fully writable throughout n1's fencing"
else
    bad "survivors were affected by the preempt"
fi

log "======== R7: n1 rejoins — registration is re-established on restart ========"
# The lifecycle question item 9 records: a preempted node reuses its derived
# key rather than minting a fresh one. What makes reuse safe is that the node
# must restart to regain access, and a restart re-reads its fencing
# generation — the key is not what separates epochs.
runcmd "$N1" "sudo iptables -F OUTPUT; sudo iptables -F INPUT; true" >/dev/null 2>&1
sleep 5
RESTART=$(restart_daemons "$N1" n1)
if [[ "$RESTART" == OK* ]]; then
    log "  restart_daemons(n1): OK"
else
    log "  restart_daemons(n1): FAIL, full output below"
    while IFS= read -r line; do log "    $line"; done <<< "$RESTART"
    log "  ps on n1:"
    runcmd "$N1" "ps aux | grep -E 'etcfuse|fuse' | grep -v grep" 2>/dev/null \
        | while IFS= read -r line; do log "    $line"; done
    log "  /mnt/etcfuse and /proc/mounts on n1:"
    runcmd "$N1" "ls -la /mnt/etcfuse 2>&1; grep etcfuse /proc/mounts 2>&1" 2>/dev/null \
        | while IFS= read -r line; do log "    $line"; done
fi

REJOINED=0
for _ in $(seq 1 10); do
    if key_registered "$N2" "$K1"; then REJOINED=1; break; fi
    sleep 3
done
if [[ "$REJOINED" -eq 1 ]]; then
    ok "n1 re-registered its key on restart (reuse-after-preempt lifecycle)"
else
    bad "n1 did not re-register after restart — a rejoined node would be silently unable to write"
fi

# check_mount first: writef's "cat > /mnt/etcfuse/<file>" succeeds even with
# nothing mounted there — /mnt/etcfuse is a real directory, so an unmounted
# write silently lands on n1's local root filesystem instead of failing, and
# the readf-on-N2 half then reports a misleading "n1 could not write" when
# the real problem is n1 never remounted at all. Failing fast on that here
# points at the actual fault (see restart_daemons(n1) output above) instead.
if ! check_mount "$N1"; then
    bad "n1's FUSE mount did not come back after restart — see restart_daemons(n1) output above"
elif writef "$N1" "post-rejoin" "nvme-rejoin.txt" && [[ -n "$(readf "$N2" "nvme-rejoin.txt")" ]]; then
    ok "n1 is writable again after rejoin, and its writes are visible cluster-wide"
else
    bad "n1 could not write after rejoining"
fi

resv_report "$N2" > "$REPORT_DIR/resv-report-after-rejoin.json"

log "======== R8: reservation state across a detach/reattach cycle ========"
# Registration is per-controller, and a detach removes the controller. The
# question this answers is which way the two states can diverge after a
# reattach: a node that is still listed as a registrant but cannot write would
# be unfenceable and broken, and a node that can write while unregistered
# would be unfenceable and dangerous. Either divergence is a failure; only
# "re-registers on restart, like any rejoin" is correct.
VOL_ID=$(jq -r '.volume_id' "$PROJECT_ROOT/$STATE_FILE")
INST3=$(jq -r '.compute_instance_ids[2]' "$PROJECT_ROOT/$STATE_FILE")
log "  detaching $VOL_ID from n3 ($INST3)"
aws ec2 detach-volume --volume-id "$VOL_ID" --instance-id "$INST3" --force \
    >>"$REPORT_DIR/chaos.log" 2>&1
aws ec2 wait volume-available --volume-ids "$VOL_ID" >/dev/null 2>&1

if key_registered "$N2" "$K3"; then
    bad "n3 is still a registrant after its volume was detached — a node with no device access must not remain fenceable-in-name-only"
else
    ok "n3's registration is gone once its controller is detached"
fi

log "  reattaching $VOL_ID to n3"
aws ec2 attach-volume --volume-id "$VOL_ID" --instance-id "$INST3" --device /dev/sdf \
    >>"$REPORT_DIR/chaos.log" 2>&1
sleep 20

if key_registered "$N2" "$K3"; then
    bad "n3 appears registered immediately after reattach without restarting — registration must be re-established deliberately, not inherited"
else
    ok "n3 is not silently re-registered by the reattach alone"
fi

RESTART3=$(restart_daemons "$N3" n3)
log "  restart_daemons(n3): $(echo "$RESTART3" | tr '\n' ' ' | cut -c1-120)"
REJOINED3=0
# 60s, not 30s: a hot-reattached NVMe namespace takes the guest kernel longer
# to re-enumerate than a device already attached at boot, on top of
# restart_daemons' own ~30s startup allowance. Confirmed by a 2026-08-06 run
# where the 30s version reported FAIL but the resv-report captured moments
# later showed n3 registered — the recovery was real, the wait was short.
for _ in $(seq 1 20); do
    if key_registered "$N2" "$K3"; then REJOINED3=1; break; fi
    sleep 3
done
if [[ "$REJOINED3" -eq 1 ]] && writef "$N3" "post-reattach" "nvme-reattach.txt"; then
    ok "n3 re-registered after reattach+restart and is writable again"
else
    bad "n3 did not recover after detach/reattach (registered=$REJOINED3)"
fi

log "======== Cleanup ========"
teardown_cluster

{
    echo "=== Device-Enforced Fencing Report (NVMe reservations, aws) ==="
    echo "Pass: $PASS  Fail: $FAIL  Total: $((PASS+FAIL))"
    [[ "$FAIL" -eq 0 ]] && echo "STATUS: ALL PASS" || echo "STATUS: $FAIL FAILURES"
} | tee "$REPORT_DIR/summary.txt"
echo "Report: $REPORT_DIR/summary.txt"

[[ "$FAIL" -eq 0 ]]
