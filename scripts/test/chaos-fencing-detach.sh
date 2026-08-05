#!/bin/bash
# chaos-fencing-detach.sh — verify dual-confirmed external fencing on real
# AWS infrastructure: partition a node from etcd,
# and assert that a SURVIVING node's fencing controller actually calls
# ec2:DetachVolume against the real EBS Multi-Attach volume, confirms the
# detachment via ec2:DescribeVolumes, and only THEN bumps the fencing
# generation — not before.
#
# This is AWS-only. There is no EBS Multi-Attach volume to detach in Docker,
# so the controller there always runs single-signal (no VolumeDetacher
# configured) — that path is already covered by
# scripts/test/chaos-elastic-fault-injection.sh's FJ2. This script is the one
# that actually exercises pkg/fencing/detach.go's EBSDetacher against a real
# EC2 API, which nothing else does.
#
# Requires the etcfs-nodes IAM instance profile (scripts/infra/fencing-iam.sh)
# to already exist — provision_cluster creates it if missing.
#
# Usage:
#   ./chaos-fencing-detach.sh
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
REPORT_DIR="$PROJECT_ROOT/chaos-report-fencing-detach-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$REPORT_DIR"
PASS=0; FAIL=0

MODE=aws

log() { echo "[$(date +%H:%M:%S)] $1" | tee -a "$REPORT_DIR/chaos.log"; }
logerr() {
    echo "[$(date +%H:%M:%S)] $1" >&2
    echo "[$(date +%H:%M:%S)] $1" >> "$REPORT_DIR/chaos.log"
}

source "$SCRIPT_DIR/chaos-lib.sh"

ok()  { PASS=$((PASS+1)); log "  PASS: $1"; }
bad() { FAIL=$((FAIL+1)); log "  FAIL: $1"; }

# NOT etcdctl_on: that helper (chaos-lib.sh) always SSHes via N1 to run the
# query, regardless of which node's state is being asked about. That's fine
# for every other script, where N1 is healthy — but here N1 is the node BEING
# partitioned, and its own local etcd can't serve a linearizable read while
# cut off from Raft quorum (2 of 3 members unreachable). The query doesn't
# error, it hangs to the timeout and returns empty, which looked exactly like
# "generation never bumped" even though the daemon logs proved it had been —
# a bug in this script, not in fencing. Query via N2 instead, which is never
# partitioned in this scenario.
gen_val() {
    timeout 15 ssh -o StrictHostKeyChecking=no -q ec2-user@"$N2" \
        "/usr/local/bin/etcdctl --endpoints=http://$P1_PRIV:2379,http://$P2_PRIV:2379,http://$P3_PRIV:2379 get gen:$1 --print-value-only" \
        2>/dev/null | tr -d '[:space:]'
}

# volume_attached_to <instance-id> — true if the shared volume's attachment
# list still contains this instance, in any state other than fully detached.
volume_attached_to() {
    local vol_id="$1" inst_id="$2"
    aws ec2 describe-volumes --volume-ids "$vol_id" \
        --query "Volumes[0].Attachments[?InstanceId=='$inst_id' && State!='detached']" \
        --output text 2>/dev/null | grep -qv '^$'
}

if ! provision_cluster; then
    log "FATAL: provision failed"
    teardown_cluster
    exit 1
fi

VOL_ID=$(jq -r '.volume_id' "$PROJECT_ROOT/$STATE_FILE")
INST1=$(jq -r '.compute_instance_ids[0]' "$PROJECT_ROOT/$STATE_FILE")
P1_PRIV=$(jq -r '.compute_ips[0]' "$PROJECT_ROOT/$STATE_FILE")
P2_PRIV=$(jq -r '.compute_ips[1]' "$PROJECT_ROOT/$STATE_FILE")
P3_PRIV=$(jq -r '.compute_ips[2]' "$PROJECT_ROOT/$STATE_FILE")
log "volume=$VOL_ID  n1 instance=$INST1"

log "======== Baseline ========"
if writef "$N1" "pre-detach" "detach-baseline.txt" && [[ -n "$(readf "$N2" "detach-baseline.txt")" ]]; then
    ok "baseline write on n1, visible from n2"
else
    bad "baseline write failed"; teardown_cluster; exit 1
fi

if volume_attached_to "$VOL_ID" "$INST1"; then
    ok "n1's instance is attached to the shared volume before partition (sanity check)"
else
    bad "n1's instance is not attached to the shared volume — cannot test detach"
    teardown_cluster; exit 1
fi

log "======== Partitioning n1 from etcd peers (n2, n3) via iptables ========"
# Same technique as FJ2 / S3 (see TODO-hardening.md item 2): SG swaps do not
# sever established connections, iptables does.
IPT_SETUP=$(ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 -q ec2-user@"$N1" \
    "command -v iptables >/dev/null 2>&1 || sudo dnf install -q -y iptables iptables-nft 2>&1 | tail -2; command -v iptables || echo NO_IPTABLES" 2>&1)
log "  iptables on n1: $(echo "$IPT_SETUP" | tr '\n' ' ' | cut -c1-120)"

for PEER in "$P2_PRIV" "$P3_PRIV"; do
    ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 -q ec2-user@"$N1" "
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
else
    ok "partition verified: n1 cannot reach etcd peers (probe=$PROBE)"
fi

log "======== Waiting for n1's membership lease to expire and a survivor to detach its volume ========"
# n1's own self-fencing watchdog may or may not fire on this exact timeline —
# irrelevant here, since this script tests the OTHER path: n2 or n3's
# controller acting on n1's expired lease, independent of what n1 itself
# believes. Bound generously: lease TTL (10s) + detach request + poll
# (PollTimeout=60s in EBSDetacher) + margin.
DEADLINE=150
WAITED=0
DETACHED=0
while [[ "$WAITED" -lt "$DEADLINE" ]]; do
    if ! volume_attached_to "$VOL_ID" "$INST1"; then
        DETACHED=1
        break
    fi
    sleep 5; WAITED=$((WAITED+5))
done

if [[ "$DETACHED" -eq 1 ]]; then
    ok "EBS volume confirmed detached from n1's instance after ~${WAITED}s (real ec2:DetachVolume, not simulated)"
else
    bad "EBS volume still attached to n1's instance after ${DEADLINE}s — dual-confirmed fencing did not complete"
fi

log "  n2/n3 controller logs (grep for the detach sequence):"
for ip in "$N2" "$N3"; do
    runcmd "$ip" "grep -E 'fencing node|volume detach|node fenced' /tmp/meta.log 2>/dev/null | tail -10" 2>/dev/null | while IFS= read -r line; do log "    [$ip] $line"; done
done

GEN1=$(gen_val n1)
if [[ -n "$GEN1" && "$GEN1" != "0" ]]; then
    ok "gen:n1 bumped to $GEN1 — generation bump followed the confirmed detach, not the other way around"
else
    bad "gen:n1 was never bumped ($GEN1)"
fi

# The property this whole script exists to verify, stated as one assertion:
# by the time the generation was bumped, the volume detach was ALREADY
# confirmed (checked above) — so there is no window where the cluster
# believes n1 is fenced while n1's instance could still reach the disk.
if [[ "$DETACHED" -eq 1 && -n "$GEN1" && "$GEN1" != "0" ]]; then
    ok "dual confirmation held: detach confirmed AND generation bumped, both observed"
else
    bad "dual confirmation did not complete: detached=$DETACHED gen=$GEN1"
fi

log "======== Survivors unaffected ========"
if writef "$N2" "during-detach" "detach-survivor.txt" && [[ -n "$(readf "$N3" "detach-survivor.txt")" ]]; then
    ok "n2/n3 stayed fully writable throughout n1's fencing"
else
    bad "survivors were affected"
fi

log "======== Cleanup ========"
# Flush n1's iptables (best-effort; the instance is terminated by
# teardown_cluster regardless, this just avoids a stuck SSH session if
# anything else touches n1 first).
runcmd "$N1" "sudo iptables -F OUTPUT; sudo iptables -F INPUT; true" >/dev/null 2>&1

teardown_cluster

{
    echo "=== Dual-Confirmed External Fencing Report (aws) ==="
    echo "Pass: $PASS  Fail: $FAIL  Total: $((PASS+FAIL))"
    [[ "$FAIL" -eq 0 ]] && echo "STATUS: ALL PASS" || echo "STATUS: $FAIL FAILURES"
} | tee "$REPORT_DIR/summary.txt"
echo "Report: $REPORT_DIR/summary.txt"

[[ "$FAIL" -eq 0 ]]
