#!/bin/bash
# etcfuse-chaos-test.sh — per-scenario-fresh-infra fault injection suite
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
REPORT_DIR="$PROJECT_ROOT/chaos-report-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$REPORT_DIR"
PASS=0; FAIL=0
log() { echo "[$(date +%H:%M:%S)] $1" | tee -a "$REPORT_DIR/chaos.log"; }

# logerr — like log, but never writes to stdout.  Helpers whose stdout is
# captured by command substitution (readf) MUST use this: a diagnostic echoed
# to stdout would be captured as if it were file content and turn a failed
# read into a spurious non-empty result.
logerr() {
    echo "[$(date +%H:%M:%S)] $1" >&2
    echo "[$(date +%H:%M:%S)] $1" >> "$REPORT_DIR/chaos.log"
}

wait_ssh() {
    local ip=$1; local max=$2
    for i in $(seq 1 $max); do
        timeout 5 ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=5 ec2-user@$ip "echo ok" 2>/dev/null && return 0
        sleep 5
    done
    return 1
}

ssh_retry() {
    local ip=$1; shift
    wait_ssh "$ip" 12
    timeout 120 ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=10 ec2-user@$ip "$@" 2>&1
}

log "Building Go binary..."
go build -o "$PROJECT_ROOT/bin/etcfuse-meta" "$PROJECT_ROOT/cmd/etcfuse-meta/" 2>&1 | tail -1
log "Packaging C source..."
tar czf /tmp/chaos.tar.gz -C "$PROJECT_ROOT" cmd/etcfuse pkg/fuse pkg/block

provision() {
    local TAG="chaos-$1-$$"
    STATE_FILE="infra-state-$1-$$.json"
    log "  Provisioning cluster=$TAG (state=$STATE_FILE)..."
    cd "$PROJECT_ROOT" || exit
    rm -f "$STATE_FILE"
    ETCFS_STATE="$STATE_FILE" ETCFS_CLUSTER="$TAG" ETCFS_COMPUTE_NODES=3 ETCFS_INSTANCE_TYPE=t3.medium \
      ETCFS_VOLUME_SIZE=30 bash scripts/infra/create-infra.sh 2>&1 | tail -3

    N1=$(jq -r '.compute_public_ips[0]' "$STATE_FILE")
    N2=$(jq -r '.compute_public_ips[1]' "$STATE_FILE")
    N3=$(jq -r '.compute_public_ips[2]' "$STATE_FILE")
    P1=$(jq -r '.compute_ips[0]' "$STATE_FILE")
    P2=$(jq -r '.compute_ips[1]' "$STATE_FILE")
    P3=$(jq -r '.compute_ips[2]' "$STATE_FILE")
    SG=$(jq -r '.sg_id' "$STATE_FILE")

    log "  Nodes: $N1 $N2 $N3  SG: $SG"

    for ip in $N1 $N2 $N3; do
        log "  Setting up $ip ..."
        wait_ssh "$ip" 12
        ssh_retry "$ip" "sudo dnf install -y fuse3-libs fuse3-devel gcc 2>&1 | tail -2" || true
        # Install etcd
        wait_ssh "$ip" 6
        ssh_retry "$ip" "
            curl -fsSL https://github.com/etcd-io/etcd/releases/download/v3.5.18/etcd-v3.5.18-linux-amd64.tar.gz -o /tmp/etcd.tar.gz && \
            sudo tar -xzf /tmp/etcd.tar.gz -C /usr/local/bin --strip-components=1 etcd-v3.5.18-linux-amd64/etcd etcd-v3.5.18-linux-amd64/etcdctl && \
            sudo chmod +x /usr/local/bin/etcd /usr/local/bin/etcdctl
        " || true
        # Copy Go binary
        wait_ssh "$ip" 3
        scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=10 -q "$PROJECT_ROOT/bin/etcfuse-meta" ec2-user@$ip:/tmp/ 2>/dev/null || true
        ssh_retry "$ip" "sudo cp /tmp/etcfuse-meta /usr/local/bin/etcfuse-meta && sudo chmod +x /usr/local/bin/etcfuse-meta" || true
        # Copy and compile C source
        scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=10 -q /tmp/chaos.tar.gz ec2-user@$ip:/tmp/ 2>/dev/null || true
        ssh_retry "$ip" "
            rm -rf /tmp/s && mkdir /tmp/s && cd /tmp/s && tar xzf /tmp/chaos.tar.gz && \
            gcc -I. -Wall -Wextra -Werror -std=c11 -D_GNU_SOURCE -O2 -g \
                cmd/etcfuse/main.c pkg/fuse/fuse.c pkg/fuse/ops.c pkg/block/block.c \
                -o /tmp/etcfuse -lfuse3 -lpthread 2>&1 && \
            sudo cp /tmp/etcfuse /usr/local/bin/etcfuse && sudo chmod +x /usr/local/bin/etcfuse
        " || true
        # Create mount point
        ssh_retry "$ip" "sudo mkdir -p /mnt/etcfuse" || true
    done
    log "  Nodes ready"

    # Start etcd (simultaneous bootstrap)
    log "  Starting etcd cluster..."
    INIT="e0=http://$P1:2380,e1=http://$P2:2380,e2=http://$P3:2380"
    for i in 1 2 3; do
        eval "ip=\$N$i"; eval "priv=\$P$i"; ename="e$((i-1))"
        # shellcheck disable=SC2154
        ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR ec2-user@$ip "
            sudo rm -rf /var/lib/etcd && sudo mkdir -p /var/lib/etcd
            sudo nohup /usr/local/bin/etcd --name $ename --data-dir /var/lib/etcd \
                --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://$priv:2379 \
                --listen-peer-urls http://0.0.0.0:2380 --initial-advertise-peer-urls http://$priv:2380 \
                --initial-cluster $INIT --initial-cluster-state new > /tmp/etcd.log 2>&1 &
        " 2>/dev/null
    done
    sleep 10
    # NOTE: do NOT seed inode_alloc_counter here.  The daemon stores that key
    # as 8-byte big-endian; seeding it with etcdctl writes ASCII, which decodes
    # to 0 and makes the allocator's CAS unsatisfiable — every create fails
    # ENOSPC.  The absent-key path bootstraps correctly on its own.
    log "  etcd cluster running"

    # Start Go meta daemon + C FUSE daemon on all nodes
    log "  Starting daemons..."
    local ETCD="http://$P1:2379,http://$P2:2379,http://$P3:2379"
    for i in 1 2 3; do
        eval "ip=\$N$i"
        ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR ec2-user@$ip "
            sudo killall -9 etcfuse-meta etcfuse 2>/dev/null; sudo umount -l /mnt/etcfuse 2>/dev/null; sleep 1
            sudo rm -f /run/etcfuse/etcfuse.sock /run/etcfuse/etcfuse-notify.sock
            sudo nohup /usr/local/bin/etcfuse-meta --listen=/run/etcfuse/etcfuse.sock \
                --etcd-endpoints=$ETCD --node-id=n$i --cluster-name=$TAG \
                --lease-ttl=10s --block-device=/dev/nvme1n1 --log-level=1 > /tmp/meta.log 2>&1 &
            sleep 4
            sudo nohup /usr/local/bin/etcfuse --socket=/run/etcfuse/etcfuse.sock \
                --node-id=n$i --log-level=1 /mnt/etcfuse > /tmp/fuse.log 2>&1 &
            sleep 7
            sudo mountpoint -q /mnt/etcfuse && echo OK || echo FAIL
        " 2>/dev/null
    done
    # Verify FUSE mount on all nodes
    local MOUNT_OK=0
    for i in 1 2 3; do
        eval "ip=\$N$i"
        local M=$(runcmd "$ip" "sudo mountpoint -q /mnt/etcfuse && echo OK || echo FAIL")
        if [[ "$M" != "OK" ]]; then
            log "  ERROR: FUSE mount on $ip FAILED!"
            runcmd "$ip" "sudo tail -10 /tmp/meta.log 2>/dev/null; echo '==='; sudo tail -10 /tmp/fuse.log 2>/dev/null" || true
        else
            MOUNT_OK=$((MOUNT_OK+1))
        fi
    done
    log "  Daemons running ($MOUNT_OK/3 mounts OK)"
}

check_mount() {
    local ip=$1
    local M=$(runcmd "$ip" "sudo mountpoint -q /mnt/etcfuse && echo OK || echo FAIL")
    [[ "$M" == "OK" ]]
}

runcmd() { timeout 10 ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -q ec2-user@$1 "$2" 2>/dev/null || echo "ERR:$?"; }
runcmd30() { timeout 30 ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -q ec2-user@$1 "$2" 2>/dev/null || echo "ERR:$?"; }
# Daemon restarts poll for the socket (up to 10s) and the mountpoint (up to
# 20s), so they need more headroom than runcmd30 allows.
runcmd60() { timeout 60 ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -q ec2-user@$1 "$2" 2>/dev/null || echo "ERR:$?"; }

# readf <ip> <file> — prints file content on success.
# On SSH/read failure prints nothing (so callers' -n tests still work) and
# logs why, so a transport failure is distinguishable from real data loss.
readf() {
    local out rc
    out=$(timeout 10 ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -q ec2-user@"$1" "sudo cat /mnt/etcfuse/$2" 2>&1)
    rc=$?
    if [[ $rc -ne 0 ]]; then
        logerr "    readf($1,$2) failed rc=$rc: $(echo "$out" | tr '\n' ' ' | cut -c1-140)"
        return 1
    fi
    printf '%s' "$out"
}

# writef <ip> <content> <file> — returns non-zero if the write did not land.
writef() {
    local out rc
    out=$(printf '%s' "$2" | timeout 10 ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -q ec2-user@"$1" \
        "sudo tee /mnt/etcfuse/$3 > /dev/null" 2>&1)
    rc=$?
    if [[ $rc -ne 0 ]]; then
        logerr "    writef($1,$3) FAILED rc=$rc: $(echo "$out" | tr '\n' ' ' | cut -c1-140)"
        return 1
    fi
    return 0
}

# restart_daemons <ip> <node_id> <etcd_endpoints> <cluster_tag>
# Restarts the Go meta daemon, then the C FUSE daemon, polling for the IPC
# socket and then the mountpoint. Echoes OK once remounted.
#
# The FUSE client must not start before the socket exists: etcfuse has no
# retry of its own on a failed connect (production relies on systemd's
# Restart=on-failure for that; this raw nohup harness has no supervisor), so
# starting it early is a guaranteed, permanent failure rather than a race
# that sometimes wins — a restart called right after etcd churn (leader
# election, reconnect) can legitimately take longer than 10s to open the
# socket. See scripts/test/chaos-lib.sh's restart_daemons for the same fix
# with the root cause recorded in detail.
restart_daemons() {
    timeout 100 ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -q ec2-user@"$1" "
      sudo killall -9 etcfuse-meta etcfuse 2>/dev/null
      sleep 1
      for k in \$(seq 1 5); do sudo umount /mnt/etcfuse 2>/dev/null && break; sleep 1; done
      sudo rm -f /run/etcfuse/etcfuse.sock /run/etcfuse/etcfuse-notify.sock
      sudo nohup /usr/local/bin/etcfuse-meta --listen=/run/etcfuse/etcfuse.sock --etcd-endpoints=$3 --node-id=$2 --cluster-name=$4 --lease-ttl=10s --block-device=/dev/nvme1n1 --log-level=1 > /tmp/meta.log 2>&1 &
      SOCK_OK=0
      for k in \$(seq 1 40); do [ -S /run/etcfuse/etcfuse.sock ] && SOCK_OK=1 && break; sleep 1; done
      if [ \"\$SOCK_OK\" -ne 1 ]; then
        echo 'FAIL (socket never appeared)'; echo '--- meta.log tail ---'; sudo tail -20 /tmp/meta.log 2>/dev/null
        exit 1
      fi
      sudo nohup /usr/local/bin/etcfuse --socket=/run/etcfuse/etcfuse.sock --node-id=$2 --log-level=1 /mnt/etcfuse > /tmp/fuse.log 2>&1 &
      for k in \$(seq 1 30); do
        sudo mountpoint -q /mnt/etcfuse 2>/dev/null && echo OK && exit 0
        sleep 1
      done
      echo 'FAIL'; echo '--- fuse.log tail ---'; sudo tail -15 /tmp/fuse.log 2>/dev/null
    " 2>/dev/null || echo "ERR:$?"
}

# dump_logs <ip> — pull the daemon logs into the chaos log.  Called whenever a
# setup step fails, so the report explains *why* rather than just that it did.
dump_logs() {
    local out
    out=$(timeout 20 ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -q ec2-user@"$1" \
        "echo '--- meta.log ---'; sudo tail -40 /tmp/meta.log 2>/dev/null; echo '--- fuse.log ---'; sudo tail -20 /tmp/fuse.log 2>/dev/null" 2>&1)
    log "  ---- daemon logs from $1 ----"
    while IFS= read -r line; do log "    $line"; done <<< "$out"
    log "  ---- end daemon logs ----"
}

# etcd_endpoints — client URLs for all three nodes, read from the state file.
etcd_endpoints() {
    echo "http://$(jq -r '.compute_ips[0]' "$PROJECT_ROOT/$STATE_FILE"):2379,http://$(jq -r '.compute_ips[1]' "$PROJECT_ROOT/$STATE_FILE"):2379,http://$(jq -r '.compute_ips[2]' "$PROJECT_ROOT/$STATE_FILE"):2379"
}

teardown() {
    log "  Teardown ($STATE_FILE)..."
    # nohup + disown: a bare `&` dies with the parent shell, orphaning the
    # volume and security group (still billing) if this script exits first.
    ETCFS_STATE="$STATE_FILE" nohup bash "$PROJECT_ROOT/scripts/infra/destroy-infra.sh" --force \
        > "$PROJECT_ROOT/teardown-$STATE_FILE.log" 2>&1 &
    disown
    log "  Teardown launched (async, log: teardown-$STATE_FILE.log)"
}

# === S1: C daemon SIGKILL ===
run_s1() {
    log "======== S1: C daemon SIGKILL ========"
    provision s1 || { FAIL=$((FAIL+1)); log "  FAIL: provision failed"; teardown; return; }
    if ! check_mount "$N1"; then FAIL=$((FAIL+1)); log "  FAIL: FUSE mount not ready"; teardown; return; fi
    if ! writef "$N1" "s1-data" "hello.txt"; then
        FAIL=$((FAIL+1)); log "  FAIL: pre-crash write did not land"; dump_logs "$N1"; teardown; return
    fi
    readf "$N1" "hello.txt" > /dev/null
    # -x is required: pkill matches the process name as an unanchored regex, so
    # a bare "etcfuse" also kills etcfuse-meta.  This scenario must kill only the
    # C daemon and leave the Go daemon (and its socket) up.
    runcmd "$N1" "sudo pkill -9 -x etcfuse 2>/dev/null; sleep 1; sudo fusermount -uz /mnt/etcfuse 2>/dev/null; sleep 1; true"
    # Do NOT remove /run/etcfuse/etcfuse.sock here either: the Go daemon owns it, and
    # unlinking it makes the new C daemon's connect() fail with ENOENT, so it
    # exits before fuse_session_mount is ever reached.
    local M=$(runcmd30 "$N1" "
      sudo nohup /usr/local/bin/etcfuse --socket=/run/etcfuse/etcfuse.sock --node-id=n1 --log-level=1 /mnt/etcfuse > /tmp/fuse.log 2>&1 &
      for i in \$(seq 1 20); do
        sudo mountpoint -q /mnt/etcfuse 2>/dev/null && echo OK && exit 0
        sleep 1
      done
      echo FAIL
    ")
    [[ "$M" == "OK" ]] || log "  WARN: n1 did not remount cleanly ($M)"
    local V=$(readf "$N1" "hello.txt")
    # shellcheck disable=SC2015
    [[ -n "$V" ]] && { PASS=$((PASS+1)); log "  PASS: $V"; } || { FAIL=$((FAIL+1)); log "  FAIL"; }
    teardown
}

# === S2: Go daemon SIGKILL ===
run_s2() {
    log "======== S2: Go daemon SIGKILL ========"
    provision s2 || { FAIL=$((FAIL+1)); log "  FAIL: provision failed"; teardown; return; }
    if ! check_mount "$N1"; then FAIL=$((FAIL+1)); log "  FAIL: FUSE mount not ready"; teardown; return; fi
    if ! writef "$N1" "go-data" "hello.txt"; then
        FAIL=$((FAIL+1)); log "  FAIL: pre-crash write did not land"; dump_logs "$N1"; teardown; return
    fi
    runcmd "$N1" "sudo pkill -9 etcfuse-meta 2>/dev/null; sleep 1; sudo rm -f /run/etcfuse/etcfuse.sock; true"
    runcmd "$N1" "sudo fusermount -uz /mnt/etcfuse 2>/dev/null; sleep 1; true"
    local ETCD="http://$(jq -r '.compute_ips[0]' $PROJECT_ROOT/$STATE_FILE):2379,http://$(jq -r '.compute_ips[1]' $PROJECT_ROOT/$STATE_FILE):2379,http://$(jq -r '.compute_ips[2]' $PROJECT_ROOT/$STATE_FILE):2379"
    local TAG=$(jq -r '.cluster_name' $PROJECT_ROOT/$STATE_FILE)
    local M=$(runcmd60 "$N1" "
      sudo nohup /usr/local/bin/etcfuse-meta --listen=/run/etcfuse/etcfuse.sock --etcd-endpoints=$ETCD --node-id=n1 --cluster-name=$TAG --lease-ttl=10s --block-device=/dev/nvme1n1 --log-level=1 > /tmp/meta.log 2>&1 &
      for i in \$(seq 1 10); do [ -S /run/etcfuse/etcfuse.sock ] && break; sleep 1; done
      sudo nohup /usr/local/bin/etcfuse --socket=/run/etcfuse/etcfuse.sock --node-id=n1 --log-level=1 /mnt/etcfuse > /tmp/fuse.log 2>&1 &
      for i in \$(seq 1 20); do
        sudo mountpoint -q /mnt/etcfuse 2>/dev/null && echo OK && exit 0
        sleep 1
      done
      echo FAIL
    ")
    [[ "$M" == "OK" ]] || log "  WARN: n1 did not remount cleanly ($M)"
    local V=$(readf "$N1" "hello.txt")
    # shellcheck disable=SC2015
    [[ -n "$V" ]] && { PASS=$((PASS+1)); log "  PASS: $V"; } || { FAIL=$((FAIL+1)); log "  FAIL"; }
    teardown
}

# === S3: Network partition via iptables ===
#
# Partitions with iptables on the instance, not by swapping its security
# group. SG swaps do not sever already-established connections — AWS security
# groups are stateful and only evaluate NEW connection attempts against the
# changed rules — so a daemon whose etcd connection predates the swap keeps
# using it and is never actually partitioned. Verified directly: a fresh
# etcdctl on the "partitioned" node correctly reported "unhealthy cluster"
# while the daemon's existing connection kept working normally.
#
# This scenario did not surface a wrong result from that, because it only
# asserts survivors keep working and that N1 recovers afterwards — never that
# N1's own operations were blocked while cut off. It was still testing
# something weaker than it claimed. iptables filters every packet regardless
# of connection state, so the partition is now real, and it is verified to
# have taken effect rather than assumed.
run_s3() {
    log "======== S3: Network partition ========"
    provision s3 || { FAIL=$((FAIL+1)); log "  FAIL: provision failed"; teardown; return; }
    if ! check_mount "$N1"; then FAIL=$((FAIL+1)); log "  FAIL: FUSE mount not ready"; teardown; return; fi
    if ! writef "$N1" "pre-part" "p1.txt"; then
        FAIL=$((FAIL+1)); log "  FAIL: pre-partition write did not land"; dump_logs "$N1"; teardown; return
    fi
    writef "$N2" "survivor-write" "p2.txt" || log "  WARN: N2 pre-partition write failed"
    readf "$N3" "p1.txt" > /dev/null

    # Stock Amazon Linux 2023 ships neither iptables nor nft, and the compute
    # setup only installs fuse3/gcc — install it here if absent. iptables-nft
    # is the shim over the nftables backend AL2023 actually uses.
    local IPT_SETUP
    IPT_SETUP=$(ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=10 -q ec2-user@"$N1" \
        "command -v iptables >/dev/null 2>&1 || sudo dnf install -q -y iptables iptables-nft 2>&1 | tail -2; command -v iptables || echo NO_IPTABLES" 2>&1)
    log "  iptables on N1: $(echo "$IPT_SETUP" | tr '\n' ' ' | cut -c1-120)"

    log "  Partitioning N1 from etcd peers via iptables..."
    # stderr is captured deliberately: the shared runcmd() discards it, which
    # previously hid the reason these rules failed behind a bare "ERR:1".
    local PEER IPT_OUT IPT_RC
    for PEER in "$P2" "$P3"; do
        IPT_OUT=$(ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=10 -q ec2-user@"$N1" "
            set -e
            sudo iptables -A OUTPUT -p tcp -d $PEER --dport 2379 -j DROP
            sudo iptables -A OUTPUT -p tcp -d $PEER --dport 2380 -j DROP
            sudo iptables -A INPUT  -p tcp -s $PEER --sport 2379 -j DROP
            sudo iptables -A INPUT  -p tcp -s $PEER --sport 2380 -j DROP
            sudo iptables -A INPUT  -p tcp -s $PEER --dport 2379 -j DROP
            sudo iptables -A INPUT  -p tcp -s $PEER --dport 2380 -j DROP
        " 2>&1); IPT_RC=$?
        [[ "$IPT_RC" -ne 0 ]] && log "  WARN: iptables for peer $PEER rc=$IPT_RC: $(echo "$IPT_OUT" | tr '\n' ' ' | cut -c1-160)"
    done

    # Verify the cut actually happened rather than assuming it did — this is
    # the check whose absence let the SG-swap version look like it worked.
    local PROBE
    PROBE=$(runcmd "$N1" "timeout 5 curl -s -o /dev/null -w '%{http_code}' http://$P2:2379/health 2>&1; echo rc=\$?")
    if [[ "$PROBE" == *"rc=0"* && "$PROBE" != *"000"* ]]; then
        FAIL=$((FAIL+1)); log "  FAIL: partition did not take effect — N1 still reached etcd at $P2 (probe=$PROBE)"
    else
        log "  Partition verified: N1 cannot reach etcd (probe=$PROBE)"
    fi
    sleep 15

    # Survivors should still work
    writef "$N2" "during-part" "p3.txt"
    local V2=$(readf "$N3" "p3.txt")
    # shellcheck disable=SC2015
    [[ -n "$V2" ]] && { PASS=$((PASS+1)); log "  PASS: Survivors work: $V2"; } || { FAIL=$((FAIL+1)); log "  FAIL: survivors"; }

    log "  Restoring N1 connectivity (flushing iptables)..."
    runcmd "$N1" "sudo iptables -F OUTPUT; sudo iptables -F INPUT; true" >/dev/null 2>&1
    sleep 20

    # N1's watchdog self-fences during the partition (os.Exit(77)) — that is
    # correct behaviour, not a failure.  Nothing supervises the raw nohup'd
    # daemons here, so restart them explicitly before asserting N1 recovers.
    #
    # Worth noting this comment predates the fix that made it true: with the
    # SG-swap partition the connection was never actually severed, and even
    # under a genuine cut the watchdog did not fire at all until
    # Membership.IsAlive() gained a keepalive-deadline check.  The restart
    # below was therefore restarting daemons that had never exited.
    log "  Restarting N1 daemons after self-fence..."
    local ETCD=$(etcd_endpoints)
    local TAG=$(jq -r '.cluster_name' "$PROJECT_ROOT/$STATE_FILE")
    local R=$(restart_daemons "$N1" "n1" "$ETCD" "$TAG")
    [[ "$R" == "OK" ]] || log "  WARN: n1 did not remount cleanly ($R)"

    local V3=$(readf "$N1" "p3.txt")
    # shellcheck disable=SC2015
    [[ -n "$V3" ]] && { PASS=$((PASS+1)); log "  PASS: N1 reads survivor: $V3"; } || { FAIL=$((FAIL+1)); log "  FAIL: N1 restore"; }
    teardown
}

# === S5: Generation bump ===
run_s5() {
    log "======== S5: Generation bump ========"
    provision s5 || { FAIL=$((FAIL+1)); log "  FAIL: provision failed"; teardown; return; }
    if ! check_mount "$N1"; then FAIL=$((FAIL+1)); log "  FAIL: FUSE mount not ready"; teardown; return; fi
    local GEN=$(runcmd "$N1" 'sudo ETCDCTL_API=3 /usr/local/bin/etcdctl --endpoints=http://127.0.0.1:2379 get gen:n1 --print-value-only' 2>/dev/null || echo "1")
    local NEWGEN=$((GEN + 1))
    log "  bumping gen:n1 $GEN -> $NEWGEN"
    runcmd "$N1" "sudo ETCDCTL_API=3 /usr/local/bin/etcdctl --endpoints=http://127.0.0.1:2379 put gen:n1 $NEWGEN" 2>/dev/null
    writef "$N1" "post-fence" "fence.txt"
    local V=$(readf "$N1" "fence.txt")
    # shellcheck disable=SC2015
    [[ -z "$V" ]] && { PASS=$((PASS+1)); log "  PASS: write blocked"; } || { FAIL=$((FAIL+1)); log "  FAIL: write succeeded"; }
    teardown
}

# === S6: 3-node simultaneous crash ===
run_s6() {
    log "======== S6: All 3 crash ========"
    provision s6 || { FAIL=$((FAIL+1)); log "  FAIL: provision failed"; teardown; return; }
    if ! check_mount "$N1"; then FAIL=$((FAIL+1)); log "  FAIL: FUSE mount not ready"; teardown; return; fi
    for i in 1 2 3; do
        eval "ip=\$N$i"
        if ! writef "$ip" "data-n$i" "ac$i.txt"; then
            FAIL=$((FAIL+1)); log "  FAIL: pre-crash write on n$i did not land"; dump_logs "$ip"; teardown; return
        fi
    done
    for i in 1 2 3; do
        eval "ip=\$N$i"
        runcmd "$ip" "sudo pkill -9 etcfuse-meta etcfuse 2>/dev/null; sudo umount -l /mnt/etcfuse 2>/dev/null; true"
    done
    sleep 3

    local ETCD="http://$(jq -r '.compute_ips[0]' $PROJECT_ROOT/$STATE_FILE):2379,http://$(jq -r '.compute_ips[1]' $PROJECT_ROOT/$STATE_FILE):2379,http://$(jq -r '.compute_ips[2]' $PROJECT_ROOT/$STATE_FILE):2379"
    local TAG=$(jq -r '.cluster_name' $PROJECT_ROOT/$STATE_FILE)
    for i in 1 2 3; do
        eval "ip=\$N$i"
        local R=$(restart_daemons "$ip" "n$i" "$ETCD" "$TAG")
        [[ "$R" == "OK" ]] || log "  WARN: n$i did not remount cleanly ($R)"
    done

    local V ALL=0
    for i in 1 2 3; do
        eval "ip=\$N$i"
        V=$(readf "$ip" "ac$i.txt")
        [[ -n "$V" ]] && ALL=$((ALL+1))
    done
    # shellcheck disable=SC2015
    [[ "$ALL" -ge 3 ]] && { PASS=$((PASS+1)); log "  PASS: $ALL/3 survived"; } || { FAIL=$((FAIL+1)); log "  FAIL: $ALL/3"; }
    teardown
}

# === S7: Mid-write crash ===
run_s7() {
    log "======== S7: Mid-write crash ========"
    provision s7 || { FAIL=$((FAIL+1)); log "  FAIL: provision failed"; teardown; return; }
    if ! check_mount "$N1"; then FAIL=$((FAIL+1)); log "  FAIL: FUSE mount not ready"; teardown; return; fi
    for f in a b c; do
        if ! writef "$N1" "wal-$f" "w$f.txt"; then
            FAIL=$((FAIL+1)); log "  FAIL: pre-crash write w$f.txt did not land"; dump_logs "$N1"; teardown; return
        fi
    done

    runcmd "$N1" "sudo pkill -9 etcfuse-meta etcfuse 2>/dev/null; sudo umount -l /mnt/etcfuse 2>/dev/null; sleep 2; true"
    local ETCD=$(etcd_endpoints)
    local TAG=$(jq -r '.cluster_name' "$PROJECT_ROOT/$STATE_FILE")
    local R=$(restart_daemons "$N1" "n1" "$ETCD" "$TAG")
    [[ "$R" == "OK" ]] || log "  WARN: n1 did not remount cleanly ($R)"

    local S=0
    for f in wa wb wc; do local V=$(readf "$N1" "$f.txt"); [[ -n "$V" ]] && S=$((S+1)); done
    # shellcheck disable=SC2015
    [[ "$S" -ge 1 ]] && { PASS=$((PASS+1)); log "  PASS: $S/3 survived"; } || { FAIL=$((FAIL+1)); log "  FAIL: 0/3"; }
    teardown
}

# === MAIN ===
SCENARIO="${1:-all}"
case $SCENARIO in
    1) run_s1 ;; 2) run_s2 ;; 3|4) run_s3 ;; 5) run_s5 ;; 6) run_s6 ;; 7) run_s7 ;;
    all)
        run_s1; run_s2; run_s3; run_s5; run_s6; run_s7
        ;;
esac

{
    echo "=== Chaos Test Report ==="
    echo "Pass: $PASS  Fail: $FAIL  Total: $((PASS+FAIL))"
    [[ "$FAIL" -eq 0 ]] && echo "STATUS: ALL PASS" || echo "STATUS: $FAIL FAILURES"
} | tee "$REPORT_DIR/summary.txt"
echo "Report: $REPORT_DIR/summary.txt"
