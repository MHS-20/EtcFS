#!/bin/bash
# chaos-elastic-fault-injection.sh — fault injection DURING the join/leave
# window itself (TODO-hardening.md item 3), not on a stable cluster like
# every other chaos-*.sh script. The membership set, quorum size, and arena
# ownership are all in flux during a join/leave — the most likely place for
# a fencing or allocator bug to hide, and untested until now.
#
# Four scenarios, run in sequence against one 3-node base cluster (each
# fully cleans up its own extra node before the next starts, so node id 4
# is reused throughout):
#
#   FJ1  Kill the joining node after `etcd member add` but before the FUSE
#        mount comes up. Assert the cluster stays writable and the
#        half-joined node never held an arena.
#   FJ2  Partition the joining node from etcd right after it mounts. Assert
#        it stops serving (self-fences) rather than continuing to operate
#        in a split view.
#   FJ3  Bump the leaving node's generation WHILE it is gracefully leaving.
#        Assert clean teardown afterward: no lock left referencing it, and
#        the arena record is unchanged (arena reclamation on leave has no
#        production implementation yet — see the comment in run_fj3 — so
#        "unchanged" is the correct expectation, not "released").
#   FJ4  Kill a SURVIVING node's daemons while a different node is mid-join,
#        then let the survivor recover. Assert the join still completes and
#        the survivor rejoins cleanly (quorum stress).
#
# This script does NOT reuse add_node/remove_node's internals for FJ1 — the
# whole point is to stop mid-sequence, which a black-box function call
# cannot do. FJ1's docker/aws partial_join_* functions below are a
# deliberate, commented duplication of add_node's first half (etcd member +
# meta daemon), stopping short of starting the FUSE container/process. FJ2
# and FJ4 reuse add_node/remove_node from chaos-lib.sh unmodified, since
# their fault lands before/after the join rather than inside it.
#
# Usage:
#   ./chaos-elastic-fault-injection.sh docker [FJ1|FJ2|FJ3|FJ4|all]
#   ./chaos-elastic-fault-injection.sh aws    [FJ1|FJ2|FJ3|FJ4|all]
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
REPORT_DIR="$PROJECT_ROOT/chaos-report-elastic-faultinject-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$REPORT_DIR"
PASS=0; FAIL=0

MODE="${1:-}"
SCENARIO="${2:-all}"
[[ "$MODE" == "docker" || "$MODE" == "aws" ]] || { echo "usage: $0 docker|aws [FJ1|FJ2|FJ3|FJ4|all]"; exit 1; }

log() { echo "[$(date +%H:%M:%S)] $1" | tee -a "$REPORT_DIR/chaos.log"; }
logerr() {
    echo "[$(date +%H:%M:%S)] $1" >&2
    echo "[$(date +%H:%M:%S)] $1" >> "$REPORT_DIR/chaos.log"
}

source "$SCRIPT_DIR/chaos-lib.sh"

ok()  { PASS=$((PASS+1)); log "  PASS: $1"; }
bad() { FAIL=$((FAIL+1)); log "  FAIL: $1"; }

# arena_val <node_key> — raw etcd value at arena:<node>, empty if absent.
arena_val() { etcdctl_on get "arena:$1" --print-value-only 2>/dev/null; }
gen_val()   { etcdctl_on get "gen:$1" --print-value-only 2>/dev/null | tr -d '[:space:]'; }
member_id_for() { etcdctl_on member list 2>/dev/null | grep ", $1," | cut -d, -f1; }

# ============================================================
# FJ1 helpers — partial join, stopping before the FUSE mount.
# Deliberate duplication of add_node's first half (see header comment).
# ============================================================
if [[ "$MODE" == "docker" ]]; then
    partial_join() {
        local id="$1"
        local ename="e$((id-1))"
        logerr "  FJ1: partial join $id: etcd member add $ename..."
        local addout initial_cluster attempt
        for attempt in $(seq 1 10); do
            addout=$(docker exec etcfs-etcd1 etcdctl member add "$ename" --peer-urls="http://etcfs-etcd$id:2380" 2>&1)
            echo "$addout" >> "$REPORT_DIR/chaos.log"
            initial_cluster=$(echo "$addout" | grep '^ETCD_INITIAL_CLUSTER=' | cut -d= -f2- | tr -d '"')
            [[ -n "$initial_cluster" ]] && break
            sleep 2
        done
        [[ -n "$initial_cluster" ]] || { logerr "  FJ1: could not parse ETCD_INITIAL_CLUSTER"; return 1; }

        docker run -d --name "etcfs-etcd$id" --network docker_etcfuse-net "$ETCD_IMG" \
            etcd --name "$ename" --data-dir /etcd-data \
            --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls "http://etcfs-etcd$id:2379" \
            --listen-peer-urls http://0.0.0.0:2380 --initial-advertise-peer-urls "http://etcfs-etcd$id:2380" \
            --initial-cluster "$initial_cluster" --initial-cluster-state existing >/dev/null

        local ok=0
        for i in $(seq 1 20); do
            docker exec "etcfs-etcd$id" etcdctl endpoint health >/dev/null 2>&1 && { ok=1; break; }
            sleep 1
        done
        [[ "$ok" -eq 1 ]] || { logerr "  FJ1: new etcd member never became healthy"; return 1; }

        local endpoints; endpoints=$(etcd_client_urls)
        docker run -d --name "etcfs-meta$id" --network docker_etcfuse-net \
            -v "docker_block_data:/block-device" -v "etcfuse-meta${id}-sock:/var/run" "$META_IMG" \
            --listen=/var/run/etcfuse.sock --etcd-endpoints="$endpoints" --node-id="n$id" \
            --cluster-name=docker-chaos --lease-ttl=10s --block-device=/block-device/etcfuse.img --log-level=1 >/dev/null

        # STOP HERE — no fuse container. This is the "before FUSE mount" window.
        sleep 3
        return 0
    }
    kill_partial_join() {
        local id="$1"
        local ename="e$((id-1))"
        docker rm -f "etcfs-meta$id" >/dev/null 2>&1
        local memberid; memberid=$(member_id_for "$ename")
        [[ -n "$memberid" ]] && etcdctl_on member remove "$memberid" >/dev/null 2>&1
        docker rm -f "etcfs-etcd$id" >/dev/null 2>&1
    }
else
    partial_join() {
        local id="$1"
        local ename="e$((id-1))"
        local name="${TAG}-compute-${id}"
        local ami_id subnet_id key_name sg vol_id
        ami_id=$(jq -r '.ami_id' "$PROJECT_ROOT/$STATE_FILE")
        subnet_id=$(jq -r '.subnet_id' "$PROJECT_ROOT/$STATE_FILE")
        key_name=$(jq -r '.key_name' "$PROJECT_ROOT/$STATE_FILE")
        sg=$(jq -r '.sg_id' "$PROJECT_ROOT/$STATE_FILE")
        vol_id=$(jq -r '.volume_id' "$PROJECT_ROOT/$STATE_FILE")
        logerr "  FJ1: partial join $id: launching EC2 instance..."
        local inst
        inst=$(aws ec2 run-instances \
            --image-id "$ami_id" --instance-type t3.medium --key-name "$key_name" \
            --security-group-ids "$sg" --subnet-id "$subnet_id" --associate-public-ip-address \
            --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":20,"VolumeType":"gp3"}}]' \
            --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$name},{Key=ClusterName,Value=$TAG}]" \
            --query 'Instances[0].InstanceId' --output text 2>/dev/null)
        [[ -n "$inst" && "$inst" != "None" ]] || { logerr "  FJ1: run-instances failed"; return 1; }
        aws ec2 wait instance-running --instance-ids "$inst" 2>/dev/null
        local pub priv
        pub=$(aws ec2 describe-instances --instance-ids "$inst" --query 'Reservations[0].Instances[0].PublicIpAddress' --output text 2>/dev/null)
        priv=$(aws ec2 describe-instances --instance-ids "$inst" --query 'Reservations[0].Instances[0].PrivateIpAddress' --output text 2>/dev/null)
        save_node_info "$id" "$pub" "$priv" "$inst"

        aws ec2 attach-volume --volume-id "$vol_id" --instance-id "$inst" --device /dev/sdf >/dev/null 2>&1
        sleep 8
        {
            wait_ssh "$pub" 20
            ssh_retry "$pub" "sudo dnf install -y fuse3-libs fuse3-devel gcc 2>&1 | tail -2" || true
            ssh_retry "$pub" "
                curl -fsSL https://github.com/etcd-io/etcd/releases/download/v3.5.18/etcd-v3.5.18-linux-amd64.tar.gz -o /tmp/etcd.tar.gz && \
                sudo tar -xzf /tmp/etcd.tar.gz -C /usr/local/bin --strip-components=1 etcd-v3.5.18-linux-amd64/etcd etcd-v3.5.18-linux-amd64/etcdctl && \
                sudo chmod +x /usr/local/bin/etcd /usr/local/bin/etcdctl
            " || true
            scp -o StrictHostKeyChecking=no -o ConnectTimeout=10 -q "$PROJECT_ROOT/bin/etcfuse-meta" ec2-user@"$pub":/tmp/ 2>/dev/null || true
            ssh_retry "$pub" "sudo cp /tmp/etcfuse-meta /usr/local/bin/etcfuse-meta && sudo chmod +x /usr/local/bin/etcfuse-meta" || true
            ssh_retry "$pub" "sudo mkdir -p /mnt/etcfuse" || true
        } >> "$REPORT_DIR/chaos.log" 2>&1

        logerr "  FJ1: etcd member add $ename..."
        local addout initial_cluster attempt
        for attempt in $(seq 1 10); do
            addout=$(runcmd "$N1" "sudo ETCDCTL_API=3 /usr/local/bin/etcdctl --endpoints=http://127.0.0.1:2379 member add $ename --peer-urls=http://$priv:2380" 2>&1)
            echo "$addout" >> "$REPORT_DIR/chaos.log"
            initial_cluster=$(echo "$addout" | grep '^ETCD_INITIAL_CLUSTER=' | cut -d= -f2- | tr -d '"')
            [[ -n "$initial_cluster" ]] && break
            sleep 2
        done
        [[ -n "$initial_cluster" ]] || { logerr "  FJ1: could not parse ETCD_INITIAL_CLUSTER"; return 1; }

        ssh -o StrictHostKeyChecking=no ec2-user@"$pub" "
            sudo mkdir -p /var/lib/etcd
            sudo nohup /usr/local/bin/etcd --name $ename --data-dir /var/lib/etcd \
                --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://$priv:2379 \
                --listen-peer-urls http://0.0.0.0:2380 --initial-advertise-peer-urls http://$priv:2380 \
                --initial-cluster '$initial_cluster' --initial-cluster-state existing > /tmp/etcd.log 2>&1 &
        " 2>/dev/null
        sleep 8

        local endpoints="http://$P1:2379,http://$P2:2379,http://$P3:2379"
        ssh -o StrictHostKeyChecking=no ec2-user@"$pub" "
            sudo nohup /usr/local/bin/etcfuse-meta --listen=/tmp/etcfuse.sock \
                --etcd-endpoints=$endpoints --node-id=n$id --cluster-name=$TAG \
                --lease-ttl=10s --block-device=/dev/nvme1n1 --log-level=1 > /tmp/meta.log 2>&1 &
        " 2>/dev/null

        # STOP HERE — no fuse process/mount. This is the "before FUSE mount" window.
        sleep 3
        return 0
    }
    kill_partial_join() {
        local id="$1"
        local ename="e$((id-1))"
        local pub inst
        pub=$(node_pub "$id"); inst=$(node_inst "$id")
        runcmd "$pub" "sudo pkill -9 etcfuse-meta etcd 2>/dev/null; true"
        local memberid; memberid=$(member_id_for "$ename")
        [[ -n "$memberid" ]] && etcdctl_on member remove "$memberid" >/dev/null 2>&1
        local vol_id; vol_id=$(jq -r '.volume_id' "$PROJECT_ROOT/$STATE_FILE")
        aws ec2 detach-volume --volume-id "$vol_id" --instance-id "$inst" >/dev/null 2>&1
        sleep 5
        aws ec2 terminate-instances --instance-ids "$inst" >/dev/null 2>&1
        rm -f "$(node_info_file "$id")"
    }
fi

cleanup_extras() {
    [[ "$MODE" == "aws" ]] || return
    local f inst ids=()
    for f in "$REPORT_DIR"/node*.info; do
        [[ -f "$f" ]] || continue
        inst=$(cut -d' ' -f3 "$f")
        [[ -n "$inst" ]] && ids+=("$inst")
    done
    [[ "${#ids[@]}" -gt 0 ]] || return
    log "  cleanup: terminating leftover extra AWS instances: ${ids[*]}"
    aws ec2 terminate-instances --instance-ids "${ids[@]}" >/dev/null 2>&1 || true
}
trap cleanup_extras EXIT

# ============================================================
# FJ1: kill the joining node between etcd member add and FUSE mount.
# ============================================================
run_fj1() {
    log "======== FJ1: kill mid-join, before FUSE mount ========"
    check_mount "$N1" || { bad "baseline mount not ready"; return; }

    if ! writef "$N1" "pre-fj1" "fj1-baseline.txt"; then bad "pre-scenario write failed"; return; fi

    if ! partial_join 4; then
        bad "partial_join setup itself failed (infra issue, not the thing under test)"
        kill_partial_join 4
        return
    fi

    # The half-joined node never reached a write — arena acquisition is lazy
    # (pkg/arena.Allocator.AcquireArena is only called from handleWriteBlock,
    # never at daemon startup), so this proves that laziness holds under a
    # real kill, not just in the harness.
    local a4; a4=$(arena_val n4)
    if [[ -z "$a4" ]]; then
        ok "half-joined node n4 holds no arena"
    else
        bad "half-joined node n4 unexpectedly has an arena record: $a4"
    fi

    if writef "$N1" "during-fj1" "fj1-during.txt" && [[ -n "$(readf "$N2" "fj1-during.txt")" ]]; then
        ok "cluster stayed writable with a half-joined member present"
    else
        bad "cluster was not writable while n4 was half-joined"
    fi

    kill_partial_join 4
    sleep 3

    if [[ -n "$(readf "$N1" "fj1-baseline.txt")" ]]; then
        ok "baseline data intact after killing the half-joined node"
    else
        bad "baseline data lost after FJ1"
    fi
}

# ============================================================
# FJ2: partition the joining node from etcd right after it mounts.
#
# KNOWN CAVEAT (confirmed by manual investigation, not fixed here — see
# docs/TODO-hardening.md item 3 follow-up and the chat report this ran
# under): on AWS, the SG-swap partition technique below (same technique
# chaos-test.sh's pre-existing S3 scenario uses) does NOT sever a node's
# already-established connections — AWS security groups are stateful and
# only evaluate NEW connection attempts against the new rules. A fresh
# `etcdctl` process from node4 correctly sees "unhealthy cluster" after the
# swap, but node4's own daemon, which opened its etcd connection before the
# swap, may keep using that pre-existing connection right through the
# "partition." This means the AWS run's write-probe result can look like
# "fencing failed" when it is actually "the node was never fully cut off in
# the first place." Docker's `docker network disconnect` does not have this
# problem — it is a hard, total interface removal (verified: empty network
# attachment map immediately after disconnect) — so the Docker run's
# results for this scenario are the reliable ones; treat the AWS run's
# results for this specific scenario as informative but not conclusive.
# ============================================================
run_fj2() {
    log "======== FJ2: partition joining node from etcd post-join ========"
    check_mount "$N1" || { bad "baseline mount not ready"; return; }

    local node4; node4=$(add_node 4)
    if [[ -z "$node4" ]]; then bad "node4 failed to join at all — cannot run FJ2"; return; fi
    ok "node4 joined normally ($node4)"

    log "  partitioning node4 from etcd (lease_ttl=10s, self-fence margin ~20s)..."
    if [[ "$MODE" == "docker" ]]; then
        docker network disconnect docker_etcfuse-net etcfs-meta4 2>/dev/null
    else
        local eni sg
        eni=$(aws ec2 describe-instances --instance-ids "$(node_inst 4)" \
            --query 'Reservations[0].Instances[0].NetworkInterfaces[0].NetworkInterfaceId' --output text 2>/dev/null)
        sg=$(jq -r '.sg_id' "$PROJECT_ROOT/$STATE_FILE")
        local vpc; vpc=$(jq -r '.vpc_id' "$PROJECT_ROOT/$STATE_FILE")
        local my_ip; my_ip=$(curl -s http://checkip.amazonaws.com 2>/dev/null || echo "0.0.0.0")
        local temp_sg; temp_sg=$(aws ec2 create-security-group --group-name "fj2-temp-$$" \
            --description "FJ2 partition SG" --vpc-id "$vpc" --query 'GroupId' --output text 2>/dev/null)
        aws ec2 authorize-security-group-ingress --group-id "$temp_sg" --protocol tcp --port 22 --cidr "${my_ip}/32" >/dev/null 2>&1
        aws ec2 modify-network-interface-attribute --network-interface-id "$eni" --groups "$temp_sg" 2>/dev/null
        echo "$temp_sg $eni $sg" > "$REPORT_DIR/fj2-partition.info"
    fi

    log "  waiting ~25s for self-fence margin (2x lease_ttl)..."
    sleep 25

    # Check 1: did the CLIENT-SIDE self-fencing watchdog fire (still mounted
    # implies it did not)? Verified by direct investigation (see chat report)
    # that under a full network partition this does NOT fire within 2x TTL,
    # or within many minutes — pkg/metadata.Membership.Run()'s alive flag is
    # only set false when the etcd client's lease KeepAlive channel closes,
    # and that channel does not appear to close under total unreachability
    # (DNS/connection failures just retry forever without ever surfacing as
    # a channel close). This is a real, reproducible finding, not fixed here
    # per instruction — see docs/TODO-hardening.md item 3 follow-up.
    local still_mounted=1
    if [[ "$MODE" == "docker" ]]; then
        docker exec etcfs-fuse4 sh -c "grep -q ' /mnt/etcfuse ' /proc/mounts" 2>/dev/null && still_mounted=0
    else
        check_mount "$(node_pub 4)" && still_mounted=0
    fi
    if [[ "$still_mounted" -eq 0 ]]; then
        bad "node4 is still mounted/serving after being partitioned from etcd — self-fencing watchdog did not fire"
    else
        ok "node4 stopped serving after partition (self-fenced or crashed out)"
    fi

    # Check 2: did the SERVER-SIDE mechanism (external fencing, independent
    # of node4's own client-side belief) still work? membership:n4's lease
    # expires at etcd regardless of what node4's client thinks, the fencing
    # controller on a healthy node watches for that deletion and bumps
    # gen:n4 — this does not depend on node4's watchdog at all, and is the
    # real backstop this investigation confirmed still functions correctly
    # even while the self-fencing watchdog is inert.
    local gen4; gen4=$(gen_val n4)
    if [[ -n "$gen4" && "$gen4" != "0" ]]; then
        ok "gen:n4 was bumped to $gen4 by external fencing despite the self-fencing gap above"
    else
        bad "gen:n4 was never bumped ($gen4) — external fencing did not fire either"
    fi

    # Check 3: does a write attempt from the now-fenced node fail FAST, or
    # hang? Direct investigation found it hangs indefinitely (killed after
    # 7+ minutes with no response) rather than returning EIO in bounded
    # time — a second, independent finding from the self-fence gap above.
    # Bounded here so a re-run of this script can never hang on it again.
    log "  probing a write from the (now fenced, still-mounted) node4, bounded to 20s..."
    local probe_rc=0
    if [[ "$MODE" == "docker" ]]; then
        timeout 20 docker exec etcfs-fuse4 sh -c "echo probe > /mnt/etcfuse/fj2-probe.txt" >/dev/null 2>&1 || probe_rc=$?
    else
        timeout 20 ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 -q ec2-user@"$(node_pub 4)" \
            "echo probe | sudo tee /mnt/etcfuse/fj2-probe.txt >/dev/null" >/dev/null 2>&1 || probe_rc=$?
    fi
    if [[ "$probe_rc" -eq 124 ]]; then
        bad "write from fenced node4 hung past 20s instead of failing fast (matches the 7+ minute hang found under manual investigation)"
    elif [[ "$probe_rc" -ne 0 ]]; then
        ok "write from fenced node4 failed fast (rc=$probe_rc) — generation guard rejected it promptly"
    else
        # Landed without error — confirm it did NOT actually reach the
        # survivors, since that would mean the guard failed to protect data,
        # not merely that it was slow to reject.
        if [[ -z "$(readf "$N1" "fj2-probe.txt")" ]]; then
            ok "write call returned success but never reached survivors — cosmetic success, not a corruption"
        else
            bad "write from fenced node4 SUCCEEDED and is visible on survivors — fencing failed to protect data"
        fi
    fi

    if writef "$N1" "during-fj2" "fj2-survivor.txt" && [[ -n "$(readf "$N2" "fj2-survivor.txt")" ]]; then
        ok "survivors (n1/n2/n3) stayed writable while node4 was partitioned"
    else
        bad "survivors were affected by node4's partition"
    fi

    log "  cleanup: removing node4 (bounded — node4 itself may be wedged)..."
    if [[ "$MODE" == "docker" ]]; then
        docker network connect docker_etcfuse-net etcfs-meta4 2>/dev/null
    else
        if [[ -f "$REPORT_DIR/fj2-partition.info" ]]; then
            read -r temp_sg eni orig_sg < "$REPORT_DIR/fj2-partition.info"
            aws ec2 modify-network-interface-attribute --network-interface-id "$eni" --groups "$orig_sg" 2>/dev/null
            sleep 5
            aws ec2 delete-security-group --group-id "$temp_sg" 2>/dev/null || true
        fi
    fi
    remove_node 4 &
    local rm_pid=$! rm_waited=0 rm_ok=0
    while [[ "$rm_waited" -lt 90 ]]; do
        kill -0 "$rm_pid" 2>/dev/null || { rm_ok=1; break; }
        sleep 3; rm_waited=$((rm_waited+3))
    done
    if [[ "$rm_ok" -eq 0 ]]; then
        logerr "  remove_node 4 did not complete within 90s — force-killing instead"
        kill -9 "$rm_pid" 2>/dev/null
        if [[ "$MODE" == "docker" ]]; then
            docker rm -f etcfs-fuse4 etcfs-meta4 etcfs-etcd4 >/dev/null 2>&1
            local memberid; memberid=$(member_id_for e3)
            [[ -n "$memberid" ]] && etcdctl_on member remove "$memberid" >/dev/null 2>&1
        else
            local inst; inst=$(node_inst 4)
            [[ -n "$inst" ]] && aws ec2 terminate-instances --instance-ids "$inst" >/dev/null 2>&1
            local memberid; memberid=$(member_id_for e3)
            [[ -n "$memberid" ]] && etcdctl_on member remove "$memberid" >/dev/null 2>&1
            rm -f "$(node_info_file 4)"
        fi
    fi
    sleep 3
}

# ============================================================
# FJ3: bump the leaving node's generation WHILE it leaves gracefully.
# ============================================================
run_fj3() {
    log "======== FJ3: generation bump during graceful leave ========"
    check_mount "$N1" || { bad "baseline mount not ready"; return; }

    local node4; node4=$(add_node 4)
    if [[ -z "$node4" ]]; then bad "node4 failed to join — cannot run FJ3"; return; fi

    if ! writef "$node4" "fj3-data" "fj3-file.txt"; then bad "write from node4 failed before leave"; return; fi

    local arena_before; arena_before=$(arena_val n4)
    if [[ -z "$arena_before" ]]; then bad "node4 has no arena before it can leave (unexpected)"; return; fi

    log "  starting graceful remove_node 4, bumping gen:n4 concurrently..."
    remove_node 4 &
    local remove_pid=$!
    sleep 1
    local gbefore; gbefore=$(gen_val n4)
    [[ -z "$gbefore" ]] && gbefore=0
    etcdctl_on put "gen:n4" "$((gbefore + 1))" >/dev/null 2>&1
    log "  bumped gen:n4 $gbefore -> $((gbefore + 1)) mid-leave"
    wait "$remove_pid"

    # NOTE: production's real shutdown path (pkg/metadata.Membership.Run,
    # what cmd/etcfuse-meta/main.go actually wires up on SIGTERM) only
    # revokes the node's own lease-backed membership key. It never touches
    # arena:<node_id> — that key isn't lease-bound at all (plain Put, see
    # pkg/arena/allocator.go), and there is no code path anywhere that
    # deletes it on departure, graceful or not. This is a pre-existing,
    # already-documented gap (docs/architecture/storage/
    # kleppmann-stale-write-analysis.md § Remaining Exposure: "arena space
    # leaks on graceful leave"), not something this scenario's generation
    # bump causes — so the correct assertion here is that the arena record
    # is UNCHANGED by the leave, not that it's released. What the bump
    # actually needs to prove is that remove_node's own steps (unmount,
    # kill daemons, `member remove`) complete cleanly despite firing
    # concurrently with a fencing event, not that arena reclamation happens
    # (it doesn't exist yet).
    sleep 2
    local arena_after; arena_after=$(arena_val n4)
    if [[ "$arena_after" == "$arena_before" ]]; then
        ok "arena:n4 unchanged by the leave (expected — no reclamation path exists yet, see comment above)"
    else
        bad "arena:n4 changed unexpectedly during leave: was '$arena_before', now '$arena_after'"
    fi

    local stray_locks; stray_locks=$(etcdctl_on get "lock:" --prefix --keys-only 2>/dev/null | xargs -I{} etcdctl_on get {} --print-value-only 2>/dev/null | grep -c '"n4"' || true)
    if [[ "${stray_locks:-0}" -eq 0 ]]; then
        ok "no lock left referencing n4"
    else
        bad "found $stray_locks lock record(s) still referencing n4"
    fi

    if writef "$N1" "after-fj3" "fj3-after.txt" && [[ -n "$(readf "$N2" "fj3-after.txt")" ]]; then
        ok "cluster healthy after FJ3"
    else
        bad "cluster unhealthy after FJ3"
    fi
}

# ============================================================
# FJ4: kill a surviving node's daemons while a different node is mid-join.
# ============================================================
run_fj4() {
    log "======== FJ4: kill a survivor while a different node joins ========"
    check_mount "$N1" || { bad "baseline mount not ready"; return; }

    if ! writef "$N1" "fj4-baseline" "fj4-baseline.txt"; then bad "baseline write failed"; return; fi

    log "  starting add_node 4 in background, will kill n2 mid-join..."
    add_node 4 > "$REPORT_DIR/fj4-join.out" 2>>"$REPORT_DIR/chaos.log" &
    local join_pid=$!

    sleep 5
    log "  killing n2's daemons now (join should be roughly mid-flight)..."
    if [[ "$MODE" == "docker" ]]; then
        docker kill -s KILL "$M2" "$N2" >/dev/null 2>&1
    else
        runcmd "$N2" "sudo pkill -9 etcfuse-meta etcfuse 2>/dev/null; true"
    fi

    wait "$join_pid"
    local node4; node4=$(cat "$REPORT_DIR/fj4-join.out" 2>/dev/null)
    if [[ -n "$node4" ]]; then
        ok "node4 completed its join despite n2 crashing concurrently ($node4)"
    else
        bad "node4 failed to join while n2 was crashing"
    fi

    log "  restarting n2..."
    local r
    if [[ "$MODE" == "docker" ]]; then
        r=$(restart_pair "$M2" "$N2")
    else
        r=$(restart_daemons "$N2" "n2")
    fi
    [[ "$r" == "OK" ]] || log "  WARN: n2 did not remount cleanly ($r)"

    if [[ -n "$(readf "$N2" "fj4-baseline.txt")" ]]; then
        ok "n2 recovered and sees pre-crash data"
    else
        bad "n2 did not recover cleanly"
    fi

    if [[ -n "$node4" ]] && [[ -n "$(readf "$node4" "fj4-baseline.txt")" ]]; then
        ok "node4 (joined during the crash) sees baseline data"
    else
        bad "node4 cannot see baseline data"
    fi

    [[ -n "$node4" ]] && remove_node 4
    sleep 3
}

# ============================================================
# MAIN
# ============================================================
if ! provision_cluster; then
    log "FATAL: provision failed"
    teardown_cluster
    exit 1
fi

[[ "$MODE" == "aws" ]] && TAG=$(jq -r '.cluster_name' "$PROJECT_ROOT/$STATE_FILE")

case "$SCENARIO" in
    FJ1) run_fj1 ;;
    FJ2) run_fj2 ;;
    FJ3) run_fj3 ;;
    FJ4) run_fj4 ;;
    all) run_fj1; run_fj2; run_fj3; run_fj4 ;;
    *) log "unknown scenario: $SCENARIO" ;;
esac

teardown_cluster

{
    echo "=== Fault-Injection-During-Join/Leave Report ($MODE) ==="
    echo "Pass: $PASS  Fail: $FAIL  Total: $((PASS+FAIL))"
    [[ "$FAIL" -eq 0 ]] && echo "STATUS: ALL PASS" || echo "STATUS: $FAIL FAILURES"
} | tee "$REPORT_DIR/summary.txt"
echo "Report: $REPORT_DIR/summary.txt"

[[ "$FAIL" -eq 0 ]]
