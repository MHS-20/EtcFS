#!/bin/bash
# chaos-fencing-retry.sh — verify the fence-intent reconciliation
# mechanism: a durable fence intent (fence_pending:<node>) that survives a
# failed/crashed fence attempt, a periodic sweep that retries it, and a
# cluster-wide claim (fence_claim:<node>) that keeps two controllers from
# fencing the same node twice.
#
# Four scenarios, run against one 3-node base cluster:
#
#   Crash-recovery simulant: a fence_pending intent is left behind for a
#       node with no live membership key (as if a controller recorded the
#       intent and died before finishing). Assert the sweep completes it
#       unprompted: generation bumped, intent cleared.
#   Rejoin-drops-intent: a fence_pending intent left behind for a node
#       whose membership key IS present (the node recovered before the fence
#       finished). Assert the sweep drops the intent WITHOUT bumping the
#       generation — fencing a live node would be a regression, not a fix.
#   Real dead node: kill a node outright, let the watch path fence it
#       through the normal edge-triggered path (not the sweep). Assert
#       exactly one generation bump and that fence_pending/fence_claim are
#       both clean afterward.
#   AWS only: force a REAL fence failure — deny ec2:DetachVolume via a
#       temporary IAM policy while a node is partitioned, confirm the
#       generation stays at 0 and the intent survives, then restore the
#       permission and assert the sweep completes the fence within one
#       sweep interval, with no source code involved in the recovery.
#
# Docker mode runs the first three scenarios (no EBS/NVMe device to
# genuinely fail against, so the fourth needs AWS). AWS mode runs all four,
# reusing the same cluster.
#
# Usage:
#   ./chaos-fencing-retry.sh docker [R1|R2|R3|R4|all]
#   ./chaos-fencing-retry.sh aws    [R1|R2|R3|R4|all]
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
REPORT_DIR="$PROJECT_ROOT/chaos-report-fencing-retry-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$REPORT_DIR"
PASS=0; FAIL=0

MODE="${1:-}"
SCENARIO="${2:-all}"
[[ "$MODE" == "docker" || "$MODE" == "aws" ]] || { echo "usage: $0 docker|aws [R1|R2|R3|R4|all]"; exit 1; }
if [[ "$MODE" == "docker" && "$SCENARIO" == "R4" ]]; then
    echo "R4 needs a real EBS volume and IAM to deny/restore against — AWS only."
    exit 1
fi

log() { echo "[$(date +%H:%M:%S)] $1" | tee -a "$REPORT_DIR/chaos.log"; }
logerr() {
    echo "[$(date +%H:%M:%S)] $1" >&2
    echo "[$(date +%H:%M:%S)] $1" >> "$REPORT_DIR/chaos.log"
}

source "$SCRIPT_DIR/chaos-lib.sh"

ok()  { PASS=$((PASS+1)); log "  PASS: $1"; }
bad() { FAIL=$((FAIL+1)); log "  FAIL: $1"; }

gen_val()          { etcdctl_on get "gen:$1" --print-value-only 2>/dev/null | tr -d '[:space:]'; }
fence_pending_val() { etcdctl_on get "fence_pending:$1" --print-value-only 2>/dev/null | tr -d '[:space:]'; }
fence_claim_present() { etcdctl_on get "fence_claim:$1" --print-value-only 2>/dev/null | grep -qv '^$'; }

# sweepInterval in pkg/fencing/controller.go is 30s; wait a full cycle plus
# margin for the sweep to have actually ticked and completed.
wait_sweep() { sleep 40; }

should_run() { [[ "$SCENARIO" == "all" || "$SCENARIO" == "$1" ]]; }

# ============================================================
# sweep completes a fence whose intent outlived its recorder.
# ============================================================
run_r1() {
    log "======== R1: sweep retries an orphaned fence_pending intent ========"
    local node="fake-crashed-node"
    etcdctl_on del "gen:$node" >/dev/null 2>&1
    etcdctl_on del "fence_pending:$node" >/dev/null 2>&1
    etcdctl_on del "membership:$node" >/dev/null 2>&1

    # No membership:$node key exists at all — indistinguishable, from the
    # sweep's point of view, from a node whose lease already expired and
    # whose key was already reaped. That is deliberate: the intent, not the
    # membership key, is what the sweep acts on.
    etcdctl_on put "fence_pending:$node" "i-simulated" >/dev/null
    log "  planted fence_pending:$node with no membership key (simulated controller death after recording intent)"

    # Two sweep cycles: one is enough to prove the retry fires, two proves it
    # keeps firing rather than giving up after a single attempt.
    wait_sweep
    wait_sweep

    local gen pending
    gen=$(gen_val "$node")
    pending=$(fence_pending_val "$node")

    if [[ "$MODE" == "aws" ]]; then
        # On AWS the controller drives a real EBSDetacher, and this node is a
        # fiction — no such EC2 instance exists, so ec2:DetachVolume can never
        # confirm and the fence correctly refuses to complete. That makes this
        # the sharper of the two assertions rather than a weaker one: it proves
        # the retry loop keeps running against a fence that genuinely cannot
        # succeed, and that an unconfirmed fence is never papered over with a
        # generation bump. Completion is asserted against real nodes in the
        # real-dead-node and AWS-only scenarios below.
        local retries
        retries=$(runcmd "$N2" "grep -c 'retrying incomplete fence.*$node' /tmp/meta.log 2>/dev/null" 2>/dev/null | tr -d '[:space:]')
        retries=${retries:-0}
        if [[ "$retries" -ge 2 ]]; then
            ok "sweep retried the unfencable node $retries times on n2 — the retry loop persists"
        else
            bad "sweep retried only $retries times — the edge-triggered gap is not closed"
        fi
        if [[ -z "$gen" || "$gen" == "0" ]]; then
            ok "generation NOT bumped for a fence that was never confirmed (got '$gen')"
        else
            bad "generation bumped to $gen without a confirmed detach — dual confirmation bypassed"
        fi
        if [[ -n "$pending" ]]; then
            ok "fence_pending:$node survives the failed attempts — still owed, still retryable"
        else
            bad "fence_pending:$node was dropped despite the fence never completing"
        fi
        etcdctl_on del "fence_pending:$node" >/dev/null 2>&1
        return
    fi

    if [[ -n "$gen" && "$gen" != "0" ]]; then
        ok "sweep bumped gen:$node to $gen unprompted"
    else
        bad "sweep did not bump gen:$node (got '$gen')"
    fi
    if [[ -z "$pending" ]]; then
        ok "fence_pending:$node cleared once the fence completed"
    else
        bad "fence_pending:$node still present after completion ('$pending')"
    fi
    # Exactly 1, not merely non-zero. Every controller sweeps on its own
    # ticker, so all three see this intent within milliseconds of each
    # other. Before the post-claim intent re-check existed (added
    # 2026-08-06, found by this very scenario) this landed on 2: the
    # straggler won the claim the winner had just released and replayed the
    # whole fence off its stale ListFenceIntents snapshot. A value above 1
    # here means that re-check regressed — a redundant real preempt/detach
    # per straggler, not a correctness failure, but exactly the waste the
    # claim exists to prevent.
    if [[ "$gen" == "1" ]]; then
        ok "fenced exactly once across all three controllers' sweeps (gen=1)"
    else
        bad "gen:$node=$gen — expected exactly 1; the sweep's post-claim intent re-check has regressed"
    fi
}

# ============================================================
# sweep drops an intent for a node that re-registered, without fencing
# it.
# ============================================================
run_r2() {
    log "======== R2: sweep drops intent when the node has re-registered ========"
    local node="fake-rejoined-node"
    etcdctl_on del "gen:$node" >/dev/null 2>&1
    etcdctl_on del "fence_pending:$node" >/dev/null 2>&1
    etcdctl_on del "membership:$node" >/dev/null 2>&1

    etcdctl_on put "fence_pending:$node" "i-simulated" >/dev/null
    etcdctl_on put "membership:$node" '{"node_id":"'"$node"'","instance_id":"i-simulated"}' >/dev/null
    log "  planted fence_pending:$node AND a live membership:$node key (simulated recovery mid-fence)"

    wait_sweep

    local gen pending
    gen=$(gen_val "$node")
    pending=$(fence_pending_val "$node")
    if [[ -z "$gen" || "$gen" == "0" ]]; then
        ok "generation left untouched for a node that re-registered (got '$gen')"
    else
        bad "generation was bumped for a live node (got '$gen') — this would sever a healthy node"
    fi
    if [[ -z "$pending" ]]; then
        ok "fence_pending:$node dropped once the sweep saw the node was alive"
    else
        bad "fence_pending:$node still present — sweep never reconciled it ('$pending')"
    fi

    # Deleting the membership key is itself a DELETE event, so the watch path
    # records a fresh intent for this fictional node. Left behind, it makes
    # every later sweep retry a fence that can never complete, polluting the
    # logs later scenarios read. Clear both, then let one sweep pass consume
    # the race.
    etcdctl_on del "membership:$node" >/dev/null 2>&1
    sleep 5
    etcdctl_on del "fence_pending:$node" >/dev/null 2>&1
    etcdctl_on del "gen:$node" >/dev/null 2>&1
}

# ============================================================
# a real dead node is fenced exactly once via the watch path, and both
# control keys end up clean.
# ============================================================
run_r3_docker() {
    log "======== R3: real node death, watch-path fence, exactly-once + clean keys ========"
    # Deliberately NOT deleting gen:n1 to get a clean baseline. n1 is still
    # live here, and WithGenerationGuard compares that key's VALUE — a value
    # comparison against a missing key always evaluates false, so removing it
    # rejects every guarded write from n1 as ErrFenced, which allocInode
    # surfaces to FUSE as -ENOSPC. That is what "no space left on device" on
    # the baseline write meant when this script first deleted the key; it was
    # the script fencing the node it was about to test, not a disk problem.
    # Read the baseline instead and assert the delta.
    local gen_before; gen_before=$(gen_val n1); gen_before=${gen_before:-0}
    log "  gen:n1 before the kill: $gen_before"

    if writef "$N1" "pre-kill" "retry-r3.txt" && [[ -n "$(readf "$N2" retry-r3.txt)" ]]; then
        ok "baseline write on n1, visible from n2"
    else
        bad "baseline write failed"; return
    fi

    log "  killing n1 (meta+fuse) hard..."
    docker kill -s KILL "$M1" "$N1" >/dev/null 2>&1

    # lease-ttl=10s (chaos-lib.sh docker provision), single-signal fencing in
    # docker (no EBS/NVMe device to confirm against) — bound generously.
    local deadline=90 waited=0 gen=""
    while [[ "$waited" -lt "$deadline" ]]; do
        gen=$(gen_val n1); gen=${gen:-0}
        [[ "$gen" -gt "$gen_before" ]] && break
        sleep 5; waited=$((waited+5))
    done

    if [[ "$gen" -gt "$gen_before" ]]; then
        ok "gen:n1 bumped $gen_before -> $gen after ~${waited}s (watch-path fence)"
    else
        bad "gen:n1 never bumped past $gen_before after ${deadline}s (still $gen)"
    fi
    if [[ "$gen" -eq $((gen_before + 1)) ]]; then
        ok "fenced exactly once (one bump from $gen_before, no duplicate from a second controller)"
    else
        bad "gen:n1=$gen — expected exactly $((gen_before + 1)), duplicate fencing"
    fi

    local pending; pending=$(fence_pending_val n1)
    if [[ -z "$pending" ]]; then
        ok "fence_pending:n1 cleared after the fence completed"
    else
        bad "fence_pending:n1 still present ('$pending')"
    fi
    if fence_claim_present n1; then
        bad "fence_claim:n1 still held — claim was not released"
    else
        ok "fence_claim:n1 released (lease revoked on completion)"
    fi

    log "  n2/n3 controller logs:"
    for c in "$M2" "$M3"; do
        docker logs --tail 40 "$c" 2>&1 | grep -E "fencing node|node fenced|fence already claimed|retrying incomplete fence" | while IFS= read -r line; do log "    [$c] $line"; done
    done

    log "  restarting n1 so teardown has a clean 3-node cluster to remove..."
    docker start etcfs-etcd1 "$M1" "$N1" >/dev/null 2>&1
}

run_r3_aws() {
    log "======== R3: real node death, watch-path fence, exactly-once + clean keys ========"
    # Deliberately NOT deleting gen:n1 to get a clean baseline. n1 is still
    # live here, and WithGenerationGuard compares that key's VALUE — a value
    # comparison against a missing key always evaluates false, so removing it
    # rejects every guarded write from n1 as ErrFenced, which allocInode
    # surfaces to FUSE as -ENOSPC. That is what "no space left on device" on
    # the baseline write meant when this script first deleted the key; it was
    # the script fencing the node it was about to test, not a disk problem.
    # Read the baseline instead and assert the delta.
    local gen_before; gen_before=$(gen_val n1); gen_before=${gen_before:-0}
    log "  gen:n1 before the kill: $gen_before"

    if writef "$N1" "pre-kill" "retry-r3.txt" && [[ -n "$(readf "$N2" retry-r3.txt)" ]]; then
        ok "baseline write on n1, visible from n2"
    else
        bad "baseline write failed"; return
    fi

    log "  killing n1's daemons hard..."
    runcmd "$N1" "sudo pkill -KILL etcfuse-meta; sudo pkill -KILL etcfuse; true" >/dev/null 2>&1

    local deadline=150 waited=0 gen=""
    while [[ "$waited" -lt "$deadline" ]]; do
        gen=$(gen_val n1); gen=${gen:-0}
        [[ "$gen" -gt "$gen_before" ]] && break
        sleep 5; waited=$((waited+5))
    done

    if [[ "$gen" -gt "$gen_before" ]]; then
        ok "gen:n1 bumped $gen_before -> $gen after ~${waited}s (watch-path fence)"
    else
        bad "gen:n1 never bumped past $gen_before after ${deadline}s (still $gen)"
    fi
    if [[ "$gen" -eq $((gen_before + 1)) ]]; then
        ok "fenced exactly once (one bump from $gen_before)"
    else
        bad "gen:n1=$gen — expected exactly $((gen_before + 1))"
    fi

    local pending; pending=$(fence_pending_val n1)
    if [[ -z "$pending" ]]; then ok "fence_pending:n1 cleared"; else bad "fence_pending:n1 still present ('$pending')"; fi
    if fence_claim_present n1; then
        bad "fence_claim:n1 still held"
    else
        ok "fence_claim:n1 released"
    fi

    log "  n2/n3 controller logs:"
    for ip in "$N2" "$N3"; do
        runcmd "$ip" "grep -E 'fencing node|node fenced|fence already claimed|retrying incomplete fence' /tmp/meta.log 2>/dev/null | tail -10" 2>/dev/null | while IFS= read -r line; do log "    [$ip] $line"; done
    done
}

# ============================================================
# AWS only — a genuinely failed fence, forced by denying
# ec2:DetachVolume, then a genuine recovery once the permission returns.
# ============================================================
run_r4_aws() {
    # Targets n3, not n1. The real-dead-node scenario kills n1's daemons and
    # leaves its mount dead ("Transport endpoint is not connected"), so this
    # scenario can't use n1 for its own baseline write when both run in one
    # invocation. n3 is untouched by every preceding scenario.
    log "======== R4: genuine detach failure via IAM deny, then sweep-driven recovery ========"
    local vol_id
    vol_id=$(jq -r '.volume_id' "$PROJECT_ROOT/$STATE_FILE")

    # See run_r3_*: gen:n3 must NOT be deleted while n3 is live, or n3's own
    # guarded writes fail as ErrFenced (surfacing as -ENOSPC) before the test
    # even starts. Baseline the value and assert the delta instead.
    #
    # Only n2's controller can fence n3 here: the real-dead-node scenario
    # killed n1's daemons (its etcd
    # keeps running, so quorum is unaffected, but its fencing controller is
    # gone). One live fencer is enough to exercise fail-then-retry.
    etcdctl_on del "fence_pending:n3" >/dev/null 2>&1
    local gen_before; gen_before=$(gen_val n3); gen_before=${gen_before:-0}
    log "  gen:n3 before the deny: $gen_before"

    if writef "$N3" "pre-deny" "retry-r4.txt" && [[ -n "$(readf "$N2" retry-r4.txt)" ]]; then
        ok "baseline write on n3, visible from n2"
    else
        bad "baseline write failed"; return
    fi

    log "  denying ec2:DetachVolume on the etcfs-nodes role (forces a real, reversible fence failure)..."
    local deny_policy
    deny_policy=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    { "Sid": "DenyDetachForRetryTest", "Effect": "Deny", "Action": "ec2:DetachVolume", "Resource": "*" }
  ]
}
EOF
)
    aws iam put-role-policy --role-name etcfs-nodes --policy-name etcfs-deny-detach-test --policy-document "$deny_policy" || {
        bad "could not attach deny policy — skipping R4"; return
    }
    log "  waiting for IAM propagation..."
    sleep 15

    log "  partitioning n3 from etcd peers (n1, n2)..."
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=10 -q ec2-user@"$N3" \
        "command -v iptables >/dev/null 2>&1 || sudo dnf install -q -y iptables iptables-nft 2>&1 | tail -2" >>"$REPORT_DIR/chaos.log" 2>&1
    local p1 p2
    p1=$(jq -r '.compute_ips[0]' "$PROJECT_ROOT/$STATE_FILE")
    p2=$(jq -r '.compute_ips[1]' "$PROJECT_ROOT/$STATE_FILE")
    # Both ports, both directions — the same rule set chaos-fencing-detach.sh
    # uses. Blocking only the client port (2379) is not a partition: n3's own
    # etcd stays in the Raft quorum over the peer port (2380) and n3's daemon
    # still reaches its *local* etcd, so its lease keeps renewing, the
    # membership key never expires, and no fence is ever triggered. That is
    # exactly how this scenario first failed.
    for peer in "$p1" "$p2"; do
        ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=10 -q ec2-user@"$N3" "
            sudo iptables -A OUTPUT -p tcp -d $peer --dport 2379 -j DROP
            sudo iptables -A OUTPUT -p tcp -d $peer --dport 2380 -j DROP
            sudo iptables -A INPUT  -p tcp -s $peer --sport 2379 -j DROP
            sudo iptables -A INPUT  -p tcp -s $peer --sport 2380 -j DROP
            sudo iptables -A INPUT  -p tcp -s $peer --dport 2379 -j DROP
            sudo iptables -A INPUT  -p tcp -s $peer --dport 2380 -j DROP
        " >>"$REPORT_DIR/chaos.log" 2>&1
    done

    log "  waiting for n3's lease to expire and the (doomed) fence attempt to run..."
    local deadline=120 waited=0 gen="" pending=""
    while [[ "$waited" -lt "$deadline" ]]; do
        pending=$(fence_pending_val n3)
        [[ -n "$pending" ]] && break
        sleep 5; waited=$((waited+5))
    done

    if [[ -n "$pending" ]]; then
        ok "fence_pending:n3 recorded (~${waited}s) — the watch path saw the expiry"
    else
        bad "fence_pending:n3 never appeared after ${deadline}s — cannot proceed with R4"
        log "  diagnostics: membership + generation state, and n1/n2 controller logs"
        etcdctl_on get --prefix "membership:" 2>&1 | while IFS= read -r line; do log "    [membership] $line"; done
        etcdctl_on get --prefix "gen:" 2>&1 | while IFS= read -r line; do log "    [gen] $line"; done
        for ip in "$N1" "$N2"; do
            runcmd "$ip" "grep -E 'fencing|retrying|node fenced|membership' /tmp/meta.log 2>/dev/null | tail -25" 2>/dev/null | while IFS= read -r line; do log "    [$ip] $line"; done
        done
        aws iam delete-role-policy --role-name etcfs-nodes --policy-name etcfs-deny-detach-test >/dev/null 2>&1
        runcmd "$N3" "sudo iptables -F OUTPUT; sudo iptables -F INPUT; true" >/dev/null 2>&1
        return
    fi

    # Give the first (doomed) attempt time to actually fail against the API
    # before checking — PollTimeout in EBSDetacher plus the DetachVolume call
    # itself denied outright, so this should be fast, but leave margin.
    sleep 20
    gen=$(gen_val n3); gen=${gen:-0}
    if [[ "$gen" -eq "$gen_before" ]]; then
        ok "generation NOT bumped while detach is denied (still $gen) — failure did not get papered over"
    else
        bad "generation bumped $gen_before -> $gen despite denied detach — dual confirmation was bypassed"
    fi

    log "  restoring ec2:DetachVolume permission..."
    aws iam delete-role-policy --role-name etcfs-nodes --policy-name etcfs-deny-detach-test || \
        logerr "  WARNING: failed to remove deny policy — remove manually: aws iam delete-role-policy --role-name etcfs-nodes --policy-name etcfs-deny-detach-test"

    # Deleting the policy is not the same as the deny being gone. IAM is
    # eventually consistent, and the delete lands in IAM long before the
    # already-active assumed-role sessions on the survivors stop being denied:
    # measured 3 minutes of continued "explicit deny in an identity-based
    # policy" 403s after a delete that `get-role-policy` already reported as
    # NoSuchEntity. Sleeping a guessed interval races that. Probe instead,
    # with a dry-run detach issued from a survivor under the very instance-role
    # credentials the daemon uses, and only start the recovery clock once the
    # authorisation actually succeeds.
    local inst3 region probe
    inst3=$(jq -r '.compute_instance_ids[2]' "$PROJECT_ROOT/$STATE_FILE")
    region=$(aws configure get region 2>/dev/null); region=${region:-eu-west-1}
    log "  polling until the deny has actually cleared on the instance role..."
    local cleared=0
    waited=0
    while [[ "$waited" -lt 420 ]]; do
        probe=$(runcmd "$N1" "aws ec2 detach-volume --dry-run --region $region --volume-id $vol_id --instance-id $inst3 2>&1 | tail -2")
        if [[ "$probe" == *"DryRunOperation"* ]]; then
            cleared=1
            break
        fi
        if [[ "$probe" != *"UnauthorizedOperation"* && "$probe" != *"AccessDenied"* ]]; then
            # Not a deny and not a dry-run success — most likely no aws CLI on
            # the node. Stop probing and fall back to waiting it out.
            log "  dry-run probe inconclusive ($(echo "$probe" | tr '\n' ' ' | cut -c1-100)), falling back to a fixed wait"
            sleep 120
            break
        fi
        sleep 15; waited=$((waited+15))
    done
    if [[ "$cleared" -eq 1 ]]; then
        log "  deny cleared on the instance role after ~${waited}s"
    else
        log "  deny not confirmed cleared after ${waited}s; continuing anyway"
    fi

    log "  waiting for the reconciliation sweep to retry and complete the fence..."
    # One sweepInterval (30s) plus a detach/poll cycle is the expected case;
    # the margin covers a slow EC2 control-plane round trip.
    deadline=240; waited=0
    while [[ "$waited" -lt "$deadline" ]]; do
        gen=$(gen_val n3); gen=${gen:-0}
        [[ "$gen" -gt "$gen_before" ]] && break
        sleep 10; waited=$((waited+10))
    done

    if [[ "$gen" -gt "$gen_before" ]]; then
        ok "sweep completed the fence after permission was restored ($gen_before -> $gen, ~${waited}s after restore)"
    else
        bad "sweep never completed the fence after ${deadline}s post-restore (still $gen)"
    fi
    pending=$(fence_pending_val n3)
    if [[ -z "$pending" ]]; then ok "fence_pending:n3 cleared"; else bad "fence_pending:n3 still present ('$pending')"; fi

    log "  n1/n2 controller logs (should show failed attempts, then a retry that succeeds):"
    for ip in "$N1" "$N2"; do
        runcmd "$ip" "grep -E 'fencing node|not confirmed severed|retrying incomplete fence|node fenced' /tmp/meta.log 2>/dev/null | tail -15" 2>/dev/null | while IFS= read -r line; do log "    [$ip] $line"; done
    done

    runcmd "$N3" "sudo iptables -F OUTPUT; sudo iptables -F INPUT; true" >/dev/null 2>&1
}

# ============================================================
# Run
# ============================================================
if [[ "$MODE" == "docker" ]]; then
    if ! provision_cluster; then
        log "FATAL: provision failed"; teardown_cluster; exit 1
    fi
    should_run R1 && run_r1
    should_run R2 && run_r2
    should_run R3 && run_r3_docker
    teardown_cluster
else
    if ! provision_cluster; then
        log "FATAL: provision failed"; teardown_cluster; exit 1
    fi
    should_run R1 && run_r1
    should_run R2 && run_r2
    should_run R3 && run_r3_aws
    should_run R4 && run_r4_aws
    teardown_cluster
fi

{
    echo "=== Fencing Reconciliation Retry Report ($MODE, scenario=$SCENARIO) ==="
    echo "Pass: $PASS  Fail: $FAIL  Total: $((PASS+FAIL))"
    [[ "$FAIL" -eq 0 ]] && echo "STATUS: ALL PASS" || echo "STATUS: $FAIL FAILURES"
} | tee "$REPORT_DIR/summary.txt"
echo "Report: $REPORT_DIR/summary.txt"

[[ "$FAIL" -eq 0 ]]
