#!/bin/bash
# chaos-elastic.sh — verify graceful scale-out/scale-in: add 2 nodes to a
# running 3-node cluster, confirm they join and serve correctly, then
# remove them gracefully, confirming the cluster keeps working throughout.
# Models aggressive AWS autoscaling (rapid add/remove of compute nodes).
#
# Usage:
#   ./chaos-elastic.sh docker
#   ./chaos-elastic.sh aws
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
REPORT_DIR="$PROJECT_ROOT/chaos-report-elastic-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$REPORT_DIR"
PASS=0; FAIL=0

MODE="${1:-}"
[[ "$MODE" == "docker" || "$MODE" == "aws" ]] || { echo "usage: $0 docker|aws"; exit 1; }

log() { echo "[$(date +%H:%M:%S)] $1" | tee -a "$REPORT_DIR/chaos.log"; }
logerr() {
    echo "[$(date +%H:%M:%S)] $1" >&2
    echo "[$(date +%H:%M:%S)] $1" >> "$REPORT_DIR/chaos.log"
}

source "$SCRIPT_DIR/chaos-lib.sh"

# Extra nodes launched outside the base 3-node cluster this run owns; used
# by the exit trap so a failed remove_node doesn't leak AWS instances.
# add_node/remove_node persist per-node info to $REPORT_DIR/node<id>.info
# (see the aws branch below for why — command substitution subshells lose
# in-memory state), and remove_node deletes that file once terminated, so
# any leftover file at exit means its instance is still running.
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
# add_node <id> — join a new node (id 4 or 5) to the running cluster.
# ============================================================
if [[ "$MODE" == "docker" ]]; then
    ETCD_IMG="quay.io/coreos/etcd:v3.5.18"
    META_IMG="docker-etcfuse-meta1:latest"
    FUSE_IMG="docker-etcfuse1:latest"

    etcd_client_urls() {
        local out=""
        for c in etcfs-etcd1 etcfs-etcd2 etcfs-etcd3 etcfs-etcd4 etcfs-etcd5; do
            docker inspect "$c" >/dev/null 2>&1 && out="${out:+$out,}http://$c:2379"
        done
        echo "$out"
    }

    add_node() {
        local id="$1"
        local ename="e$((id-1))"
        # etcd rejects `member add` with "unhealthy cluster" during the
        # brief internal stabilization window right after a previous
        # member join, even when endpoint health already reports OK —
        # so retry the add itself rather than trust a health pre-check.
        logerr "  add_node $id: etcd member add $ename..."
        local addout initial_cluster attempt
        for attempt in $(seq 1 10); do
            addout=$(docker exec etcfs-etcd1 etcdctl member add "$ename" --peer-urls="http://etcfs-etcd$id:2380" 2>&1)
            echo "$addout" >> "$REPORT_DIR/chaos.log"
            initial_cluster=$(echo "$addout" | grep '^ETCD_INITIAL_CLUSTER=' | cut -d= -f2- | tr -d '"')
            [[ -n "$initial_cluster" ]] && break
            sleep 2
        done
        [[ -n "$initial_cluster" ]] || { logerr "  add_node $id: could not parse ETCD_INITIAL_CLUSTER from member add output after $attempt attempts"; return 1; }

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
        [[ "$ok" -eq 1 ]] || { logerr "  add_node $id: new etcd member never became healthy"; return 1; }

        local endpoints
        endpoints=$(etcd_client_urls)
        docker run -d --name "etcfs-meta$id" --network docker_etcfuse-net \
            -v "docker_block_data:/block-device" -v "etcfuse-meta${id}-sock:/var/run" "$META_IMG" \
            --listen=/var/run/etcfuse.sock --etcd-endpoints="$endpoints" --node-id="n$id" \
            --cluster-name=docker-chaos --lease-ttl=10s --block-device=/block-device/etcfuse.img --log-level=1 >/dev/null

        sleep 2
        docker run -d --name "etcfs-fuse$id" --network docker_etcfuse-net --privileged --device /dev/fuse \
            -v "etcfuse-meta${id}-sock:/var/run" --entrypoint /bin/sh "$FUSE_IMG" \
            -c "mkdir -p /mnt/etcfuse && exec /usr/local/bin/etcfuse --socket=/var/run/etcfuse.sock --node-id=n$id --log-level=1 /mnt/etcfuse" >/dev/null

        for i in $(seq 1 20); do
            check_mount "etcfs-fuse$id" && { echo "etcfs-fuse$id"; return 0; }
            sleep 1
        done
        logerr "  add_node $id: FUSE never mounted"
        dump_logs "etcfs-fuse$id" 1>&2
        return 1
    }

    remove_node() {
        local id="$1"
        local ename="e$((id-1))"
        log "  remove_node $id: unmounting FUSE gracefully..."
        docker exec "etcfs-fuse$id" fusermount -u /mnt/etcfuse >/dev/null 2>&1
        sleep 1
        docker stop "etcfs-fuse$id" >/dev/null 2>&1
        docker stop "etcfs-meta$id" >/dev/null 2>&1

        local memberid
        memberid=$(docker exec etcfs-etcd1 etcdctl member list 2>/dev/null | grep ", $ename," | cut -d, -f1)
        if [[ -n "$memberid" ]]; then
            log "  remove_node $id: etcd member remove $memberid ($ename)..."
            docker exec etcfs-etcd1 etcdctl member remove "$memberid" >/dev/null 2>&1 || logerr "  member remove failed for $ename"
        else
            logerr "  remove_node $id: could not find etcd member id for $ename"
        fi
        docker stop "etcfs-etcd$id" >/dev/null 2>&1
        docker rm -f "etcfs-fuse$id" "etcfs-meta$id" "etcfs-etcd$id" >/dev/null 2>&1
    }
else
    # aws mode: launch/terminate real EC2 instances joining the same etcd
    # cluster + shared Multi-Attach EBS volume as the base 3-node cluster.
    #
    # add_node/remove_node are invoked as `X=$(add_node N)` so the caller
    # can capture the winning node's IP — command substitution runs the
    # function in a subshell, so any in-memory var/array it sets (pub, priv,
    # instance id) is gone the instant it returns. Persist that per-node
    # state to files instead, so it survives across the subshell boundary.
    node_info_file() { echo "$REPORT_DIR/node$1.info"; }
    save_node_info() { echo "$2 $3 $4" > "$(node_info_file "$1")"; }
    node_pub()  { cut -d' ' -f1 "$(node_info_file "$1")" 2>/dev/null; }
    node_priv() { cut -d' ' -f2 "$(node_info_file "$1")" 2>/dev/null; }
    node_inst() { cut -d' ' -f3 "$(node_info_file "$1")" 2>/dev/null; }

    add_node() {
        local id="$1"
        local ename="e$((id-1))"
        local name="${TAG}-compute-${id}"
        # SG/AMI_ID/SUBNET_ID/KEY_NAME/VOL_ID are locals scoped inside
        # chaos-lib.sh's aws provision_cluster(); read fresh from the state
        # file it wrote instead of depending on that scope.
        local ami_id subnet_id key_name sg vol_id
        ami_id=$(jq -r '.ami_id' "$PROJECT_ROOT/$STATE_FILE")
        subnet_id=$(jq -r '.subnet_id' "$PROJECT_ROOT/$STATE_FILE")
        key_name=$(jq -r '.key_name' "$PROJECT_ROOT/$STATE_FILE")
        sg=$(jq -r '.sg_id' "$PROJECT_ROOT/$STATE_FILE")
        vol_id=$(jq -r '.volume_id' "$PROJECT_ROOT/$STATE_FILE")
        logerr "  add_node $id: launching EC2 instance..."
        local inst
        inst=$(aws ec2 run-instances \
            --image-id "$ami_id" --instance-type t3.medium --key-name "$key_name" \
            --security-group-ids "$sg" --subnet-id "$subnet_id" --associate-public-ip-address \
            --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":20,"VolumeType":"gp3"}}]' \
            --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$name},{Key=ClusterName,Value=$TAG}]" \
            --query 'Instances[0].InstanceId' --output text 2>/dev/null)
        [[ -n "$inst" && "$inst" != "None" ]] || { logerr "  add_node $id: run-instances failed"; return 1; }
        aws ec2 wait instance-running --instance-ids "$inst" 2>/dev/null
        local pub priv
        pub=$(aws ec2 describe-instances --instance-ids "$inst" --query 'Reservations[0].Instances[0].PublicIpAddress' --output text 2>/dev/null)
        priv=$(aws ec2 describe-instances --instance-ids "$inst" --query 'Reservations[0].Instances[0].PrivateIpAddress' --output text 2>/dev/null)
        save_node_info "$id" "$pub" "$priv" "$inst"
        logerr "  add_node $id: $inst -> $pub / $priv"

        aws ec2 attach-volume --volume-id "$vol_id" --instance-id "$inst" --device /dev/sdf >/dev/null 2>&1
        sleep 8

        # add_node's own stdout is captured via $(add_node N) by the caller
        # to get the winning node's public IP, so none of this setup noise
        # (raw ssh/scp output) may reach stdout — redirect it all to the log.
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
            scp -o StrictHostKeyChecking=no -o ConnectTimeout=10 -q /tmp/chaos.tar.gz ec2-user@"$pub":/tmp/ 2>/dev/null || true
            ssh_retry "$pub" "
                rm -rf /tmp/s && mkdir /tmp/s && cd /tmp/s && tar xzf /tmp/chaos.tar.gz && \
                gcc -I. -Wall -Wextra -Werror -std=c11 -D_GNU_SOURCE -O2 -g \
                    cmd/etcfuse/main.c pkg/fuse/fuse.c pkg/fuse/ops.c pkg/block/block.c pkg/wal/wal.c \
                    -o /tmp/etcfuse -lfuse3 -lpthread 2>&1 && \
                sudo cp /tmp/etcfuse /usr/local/bin/etcfuse && sudo chmod +x /usr/local/bin/etcfuse
            " || true
            ssh_retry "$pub" "sudo mkdir -p /mnt/etcfuse" || true
        } >> "$REPORT_DIR/chaos.log" 2>&1

        # See the docker branch's add_node for why this retries the add
        # itself instead of trusting a health pre-check.
        logerr "  add_node $id: etcd member add $ename..."
        local addout initial_cluster attempt
        for attempt in $(seq 1 10); do
            addout=$(runcmd "$N1" "sudo ETCDCTL_API=3 /usr/local/bin/etcdctl --endpoints=http://127.0.0.1:2379 member add $ename --peer-urls=http://$priv:2380" 2>&1)
            echo "$addout" >> "$REPORT_DIR/chaos.log"
            initial_cluster=$(echo "$addout" | grep '^ETCD_INITIAL_CLUSTER=' | cut -d= -f2- | tr -d '"')
            [[ -n "$initial_cluster" ]] && break
            sleep 2
        done
        [[ -n "$initial_cluster" ]] || { logerr "  add_node $id: could not parse ETCD_INITIAL_CLUSTER after $attempt attempts"; return 1; }

        ssh -o StrictHostKeyChecking=no ec2-user@"$pub" "
            sudo mkdir -p /var/lib/etcd
            sudo nohup /usr/local/bin/etcd --name $ename --data-dir /var/lib/etcd \
                --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://$priv:2379 \
                --listen-peer-urls http://0.0.0.0:2380 --initial-advertise-peer-urls http://$priv:2380 \
                --initial-cluster '$initial_cluster' --initial-cluster-state existing > /tmp/etcd.log 2>&1 &
        " 2>/dev/null
        sleep 8

        local endpoints="http://$P1:2379,http://$P2:2379,http://$P3:2379"
        local j jpriv
        for j in 4 5; do
            [[ "$j" -eq "$id" ]] && continue
            jpriv=$(node_priv "$j")
            [[ -n "$jpriv" ]] && endpoints="$endpoints,http://$jpriv:2379"
        done

        ssh -o StrictHostKeyChecking=no ec2-user@"$pub" "
            sudo nohup /usr/local/bin/etcfuse-meta --listen=/tmp/etcfuse.sock \
                --etcd-endpoints=$endpoints --node-id=n$id --cluster-name=$TAG \
                --lease-ttl=10s --block-device=/dev/nvme1n1 --log-level=1 > /tmp/meta.log 2>&1 &
            sleep 4
            sudo nohup /usr/local/bin/etcfuse --socket=/tmp/etcfuse.sock \
                --node-id=n$id --log-level=1 /mnt/etcfuse > /tmp/fuse.log 2>&1 &
            sleep 7
        " 2>/dev/null

        for i in $(seq 1 20); do
            check_mount "$pub" && { echo "$pub"; return 0; }
            sleep 1
        done
        logerr "  add_node $id: FUSE never mounted on $pub"
        dump_logs "$pub" 1>&2
        return 1
    }

    remove_node() {
        local id="$1"
        local ename="e$((id-1))"
        local pub inst
        pub=$(node_pub "$id")
        inst=$(node_inst "$id")
        log "  remove_node $id ($pub): unmounting FUSE gracefully..."
        runcmd "$pub" "sudo fusermount -u /mnt/etcfuse 2>/dev/null; sleep 1; sudo pkill -TERM etcfuse-meta 2>/dev/null; sleep 2; true"

        local memberid
        memberid=$(runcmd "$N1" "sudo ETCDCTL_API=3 /usr/local/bin/etcdctl --endpoints=http://127.0.0.1:2379 member list" 2>/dev/null | grep ", $ename," | cut -d, -f1)
        if [[ -n "$memberid" ]]; then
            log "  remove_node $id: etcd member remove $memberid ($ename)..."
            runcmd "$N1" "sudo ETCDCTL_API=3 /usr/local/bin/etcdctl --endpoints=http://127.0.0.1:2379 member remove $memberid" >/dev/null 2>&1 || logerr "  member remove failed for $ename"
        else
            logerr "  remove_node $id: could not find etcd member id for $ename"
        fi

        local vol_id
        vol_id=$(jq -r '.volume_id' "$PROJECT_ROOT/$STATE_FILE")
        aws ec2 detach-volume --volume-id "$vol_id" --instance-id "$inst" >/dev/null 2>&1
        sleep 5
        aws ec2 terminate-instances --instance-ids "$inst" >/dev/null 2>&1
        rm -f "$(node_info_file "$id")"
    }
fi

# ============================================================
# canary <label> — write from N1, verify readable from N2 and N3 (the
# original, never-removed nodes). Cheap correctness check reused at every
# phase of the scale-out/scale-in sequence.
# ============================================================
canary() {
    local label="$1"
    local fname="elastic-$label.txt"
    if ! writef "$N1" "data-$label" "$fname"; then
        FAIL=$((FAIL+1)); log "  FAIL ($label): write on N1 did not land"; return 1
    fi
    local v2 v3
    v2=$(readf "$N2" "$fname"); v3=$(readf "$N3" "$fname")
    if [[ -n "$v2" && -n "$v3" ]]; then
        PASS=$((PASS+1)); log "  PASS ($label): N2/N3 both read back '$v2'"
    else
        FAIL=$((FAIL+1)); log "  FAIL ($label): N2='$v2' N3='$v3'"
    fi
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

log "======== Baseline (3 nodes) ========"
canary baseline

log "======== Scale-out: adding node 4 ========"
NODE4=$(add_node 4)
if [[ -n "$NODE4" ]]; then
    PASS=$((PASS+1)); log "  PASS: node4 joined and mounted ($NODE4)"
    # node4 must see data written before it existed.
    V=$(readf "$NODE4" "elastic-baseline.txt")
    # shellcheck disable=SC2015
    [[ -n "$V" ]] && { PASS=$((PASS+1)); log "  PASS: node4 sees pre-join data: $V"; } || { FAIL=$((FAIL+1)); log "  FAIL: node4 cannot see pre-join data"; }
    # a write from node4 must be visible to the original nodes.
    writef "$NODE4" "from-node4" "elastic-node4-write.txt"
    V=$(readf "$N1" "elastic-node4-write.txt")
    # shellcheck disable=SC2015
    [[ -n "$V" ]] && { PASS=$((PASS+1)); log "  PASS: N1 sees node4's write: $V"; } || { FAIL=$((FAIL+1)); log "  FAIL: N1 cannot see node4's write"; }
else
    FAIL=$((FAIL+1)); log "  FAIL: node4 failed to join"
fi
canary after-add-node4

log "======== Scale-out: adding node 5 ========"
NODE5=$(add_node 5)
if [[ -n "$NODE5" ]]; then
    PASS=$((PASS+1)); log "  PASS: node5 joined and mounted ($NODE5)"
    V=$(readf "$NODE5" "elastic-node4-write.txt")
    # shellcheck disable=SC2015
    [[ -n "$V" ]] && { PASS=$((PASS+1)); log "  PASS: node5 sees node4's write: $V"; } || { FAIL=$((FAIL+1)); log "  FAIL: node5 cannot see node4's write"; }
else
    FAIL=$((FAIL+1)); log "  FAIL: node5 failed to join"
fi
canary five-node

log "======== Scale-in: removing node 5 gracefully ========"
remove_node 5
sleep 3
canary after-remove-node5
# shellcheck disable=SC2015
[[ -n "$NODE4" ]] && { V=$(readf "$NODE4" "elastic-baseline.txt"); [[ -n "$V" ]] && { PASS=$((PASS+1)); log "  PASS: node4 still functional after node5 removed"; } || { FAIL=$((FAIL+1)); log "  FAIL: node4 broken after node5 removed"; }; }

log "======== Scale-in: removing node 4 gracefully ========"
remove_node 4
sleep 3
canary after-remove-node4

log "======== Final check: back to 3-node baseline ========"
V=$(readf "$N1" "elastic-baseline.txt")
# shellcheck disable=SC2015
[[ -n "$V" ]] && { PASS=$((PASS+1)); log "  PASS: original baseline data intact: $V"; } || { FAIL=$((FAIL+1)); log "  FAIL: baseline data lost"; }

teardown_cluster

{
    echo "=== Elastic Scale-Out/Scale-In Chaos Report ($MODE) ==="
    echo "Pass: $PASS  Fail: $FAIL  Total: $((PASS+FAIL))"
    [[ "$FAIL" -eq 0 ]] && echo "STATUS: ALL PASS" || echo "STATUS: $FAIL FAILURES"
} | tee "$REPORT_DIR/summary.txt"
echo "Report: $REPORT_DIR/summary.txt"
