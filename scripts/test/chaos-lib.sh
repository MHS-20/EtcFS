#!/bin/bash
# chaos-lib.sh — shared docker/aws transport for the single-cluster chaos
# scripts. Source this after setting MODE=docker|aws, PROJECT_ROOT, REPORT_DIR,
# and defining log()/logerr(). Provides: runcmd/runcmd30/runcmd60, check_mount,
# readf, writef, dump_logs, restart_fuse, restart_pair, partition_node,
# heal_node, etcdctl_on, provision_cluster, teardown_cluster.
COMPOSE="docker compose -f $PROJECT_ROOT/deploy/docker/docker-compose.yml"

if [[ "$MODE" == "docker" ]]; then
    N1=etcfs-fuse1; N2=etcfs-fuse2; N3=etcfs-fuse3
    M1=etcfs-meta1; M2=etcfs-meta2; M3=etcfs-meta3

    runcmd()   { docker exec "$1" sh -c "$2" 2>&1 || echo "ERR:$?"; }
    runcmd30() { timeout 30 docker exec "$1" sh -c "$2" 2>&1 || echo "ERR:$?"; }
    runcmd60() { timeout 60 docker exec "$1" sh -c "$2" 2>&1 || echo "ERR:$?"; }

    check_mount() { docker exec "$1" sh -c "grep -q ' /mnt/etcfuse ' /proc/mounts" 2>/dev/null; }

    readf() {
        local out rc
        out=$(docker exec "$1" cat "/mnt/etcfuse/$2" 2>&1); rc=$?
        if [[ $rc -ne 0 ]]; then
            logerr "    readf($1,$2) failed rc=$rc: $(echo "$out" | tr '\n' ' ' | cut -c1-140)"
            return 1
        fi
        printf '%s' "$out"
    }
    writef() {
        local out rc
        out=$(printf '%s' "$2" | docker exec -i "$1" sh -c "cat > /mnt/etcfuse/$3" 2>&1); rc=$?
        if [[ $rc -ne 0 ]]; then
            logerr "    writef($1,$3) FAILED rc=$rc: $(echo "$out" | tr '\n' ' ' | cut -c1-140)"
            return 1
        fi
        return 0
    }
    rmf()  { docker exec "$1" rm -f "/mnt/etcfuse/$2" 2>&1; }
    mvf()  { docker exec "$1" mv "/mnt/etcfuse/$2" "/mnt/etcfuse/$3" 2>&1; }
    mkdirf() { docker exec "$1" mkdir -p "/mnt/etcfuse/$2" 2>&1; }
    lsf()  { docker exec "$1" sh -c "ls /mnt/etcfuse/ 2>/dev/null"; }

    dump_logs() {
        log "  ---- daemon logs from $1 ----"
        docker logs --tail 40 "$1" 2>&1 | while IFS= read -r line; do log "    $line"; done
        log "  ---- end daemon logs ----"
    }
    # restart_fuse <fuse_container> — SIGKILL + restart just the FUSE
    # container, paired meta container keeps running (mirrors S1).
    restart_fuse() {
        docker kill -s KILL "$1" >/dev/null 2>&1
        docker start "$1" >/dev/null 2>&1
        for i in $(seq 1 20); do
            check_mount "$1" && { echo OK; return; }
            sleep 1
        done
        echo FAIL
    }
    # restart_pair <meta_container> <fuse_container> — kill+restart both
    # (mirrors S2/S7).
    restart_pair() {
        docker kill -s KILL "$1" "$2" >/dev/null 2>&1
        docker start "$1" >/dev/null 2>&1
        sleep 3
        docker start "$2" >/dev/null 2>&1
        for i in $(seq 1 20); do
            check_mount "$2" && { echo OK; return; }
            sleep 1
        done
        echo FAIL
    }
    partition_node() { docker network disconnect docker_etcfuse-net "$1" 2>/dev/null; docker network disconnect docker_etcfuse-net "$2" 2>/dev/null; }
    heal_node()      { docker network connect docker_etcfuse-net "$1" 2>/dev/null; docker network connect docker_etcfuse-net "$2" 2>/dev/null; }
    etcdctl_on()     { docker exec etcfs-etcd1 etcdctl --endpoints=http://127.0.0.1:2379 "$@"; }

    provision_cluster() {
        log "  Building + starting docker-compose cluster..."
        $COMPOSE down -v >/dev/null 2>&1 || true
        $COMPOSE up -d --build >/dev/null 2>&1
        log "  Waiting for mounts..."
        for c in "$N1" "$N2" "$N3"; do
            local ok=0
            for i in $(seq 1 30); do check_mount "$c" && { ok=1; break; }; sleep 2; done
            [[ "$ok" -eq 1 ]] || { log "  ERROR: $c never mounted"; dump_logs "$c"; return 1; }
        done
        log "  Cluster up: $N1 $N2 $N3"
    }
    teardown_cluster() {
        log "  Tearing down docker-compose cluster..."
        $COMPOSE down -v >/dev/null 2>&1 &
    }
else
    # aws mode: single persistent EC2 cluster, ssh transport.
    STATE_FILE="infra-state-single-$$.json"
    wait_ssh() {
        local ip=$1; local max=$2
        for i in $(seq 1 $max); do
            timeout 5 ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 ec2-user@$ip "echo ok" 2>/dev/null && return 0
            sleep 5
        done
        return 1
    }
    ssh_retry() {
        local ip=$1; shift
        wait_ssh "$ip" 12
        timeout 120 ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 ec2-user@$ip "$@" 2>&1
    }
    runcmd()   { timeout 10 ssh -o StrictHostKeyChecking=no -q ec2-user@$1 "$2" 2>/dev/null || echo "ERR:$?"; }
    runcmd30() { timeout 30 ssh -o StrictHostKeyChecking=no -q ec2-user@$1 "$2" 2>/dev/null || echo "ERR:$?"; }
    runcmd60() { timeout 60 ssh -o StrictHostKeyChecking=no -q ec2-user@$1 "$2" 2>/dev/null || echo "ERR:$?"; }
    check_mount() { local M=$(runcmd "$1" "sudo mountpoint -q /mnt/etcfuse && echo OK || echo FAIL"); [[ "$M" == "OK" ]]; }
    readf() {
        local out rc
        out=$(timeout 10 ssh -o StrictHostKeyChecking=no -q ec2-user@"$1" "sudo cat /mnt/etcfuse/$2" 2>&1); rc=$?
        if [[ $rc -ne 0 ]]; then
            logerr "    readf($1,$2) failed rc=$rc: $(echo "$out" | tr '\n' ' ' | cut -c1-140)"
            return 1
        fi
        printf '%s' "$out"
    }
    writef() {
        local out rc
        out=$(printf '%s' "$2" | timeout 10 ssh -o StrictHostKeyChecking=no -q ec2-user@"$1" "sudo tee /mnt/etcfuse/$3 > /dev/null" 2>&1); rc=$?
        if [[ $rc -ne 0 ]]; then
            logerr "    writef($1,$3) FAILED rc=$rc: $(echo "$out" | tr '\n' ' ' | cut -c1-140)"
            return 1
        fi
        return 0
    }
    rmf()  { timeout 10 ssh -o StrictHostKeyChecking=no -q ec2-user@"$1" "sudo rm -f /mnt/etcfuse/$2" 2>&1; }
    mvf()  { timeout 10 ssh -o StrictHostKeyChecking=no -q ec2-user@"$1" "sudo mv /mnt/etcfuse/$2 /mnt/etcfuse/$3" 2>&1; }
    mkdirf() { timeout 10 ssh -o StrictHostKeyChecking=no -q ec2-user@"$1" "sudo mkdir -p /mnt/etcfuse/$2" 2>&1; }
    lsf()  { timeout 10 ssh -o StrictHostKeyChecking=no -q ec2-user@"$1" "sudo ls /mnt/etcfuse/ 2>/dev/null"; }

    dump_logs() {
        local out
        out=$(timeout 20 ssh -o StrictHostKeyChecking=no -q ec2-user@"$1" \
            "echo '--- meta.log ---'; sudo tail -40 /tmp/meta.log 2>/dev/null; echo '--- fuse.log ---'; sudo tail -20 /tmp/fuse.log 2>/dev/null" 2>&1)
        log "  ---- daemon logs from $1 ----"
        while IFS= read -r line; do log "    $line"; done <<< "$out"
        log "  ---- end daemon logs ----"
    }
    etcd_endpoints() {
        echo "http://$(jq -r '.compute_ips[0]' "$PROJECT_ROOT/$STATE_FILE"):2379,http://$(jq -r '.compute_ips[1]' "$PROJECT_ROOT/$STATE_FILE"):2379,http://$(jq -r '.compute_ips[2]' "$PROJECT_ROOT/$STATE_FILE"):2379"
    }
    restart_daemons() {
        local etcd=$(etcd_endpoints); local tag=$(jq -r '.cluster_name' "$PROJECT_ROOT/$STATE_FILE")
        runcmd60 "$1" "
          sudo rm -f /tmp/etcfuse.sock /tmp/etcfuse-notify.sock
          sudo nohup /usr/local/bin/etcfuse-meta --listen=/tmp/etcfuse.sock --etcd-endpoints=$etcd --node-id=$2 --cluster-name=$tag --lease-ttl=10s --block-device=/dev/nvme1n1 --log-level=1 > /tmp/meta.log 2>&1 &
          for k in \$(seq 1 10); do [ -S /tmp/etcfuse.sock ] && break; sleep 1; done
          sudo nohup /usr/local/bin/etcfuse --socket=/tmp/etcfuse.sock --node-id=$2 --log-level=1 /mnt/etcfuse > /tmp/fuse.log 2>&1 &
          for k in \$(seq 1 20); do sudo mountpoint -q /mnt/etcfuse 2>/dev/null && echo OK && exit 0; sleep 1; done
          echo FAIL
        "
    }

    provision_cluster() {
        local TAG="chaos-single-$$"
        log "  Provisioning cluster=$TAG (state=$STATE_FILE)..."
        cd "$PROJECT_ROOT" || return 1
        rm -f "$STATE_FILE"
        go build -o "$PROJECT_ROOT/bin/etcfuse-meta" "$PROJECT_ROOT/cmd/etcfuse-meta/" 2>&1 | tail -1
        tar czf /tmp/chaos.tar.gz -C "$PROJECT_ROOT" cmd/etcfuse pkg/fuse pkg/block pkg/wal
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
            wait_ssh "$ip" 12
            ssh_retry "$ip" "sudo dnf install -y fuse3-libs fuse3-devel gcc 2>&1 | tail -2" || true
            wait_ssh "$ip" 6
            ssh_retry "$ip" "
                curl -fsSL https://github.com/etcd-io/etcd/releases/download/v3.5.18/etcd-v3.5.18-linux-amd64.tar.gz -o /tmp/etcd.tar.gz && \
                sudo tar -xzf /tmp/etcd.tar.gz -C /usr/local/bin --strip-components=1 etcd-v3.5.18-linux-amd64/etcd etcd-v3.5.18-linux-amd64/etcdctl && \
                sudo chmod +x /usr/local/bin/etcd /usr/local/bin/etcdctl
            " || true
            wait_ssh "$ip" 3
            scp -o StrictHostKeyChecking=no -o ConnectTimeout=10 -q "$PROJECT_ROOT/bin/etcfuse-meta" ec2-user@$ip:/tmp/ 2>/dev/null || true
            ssh_retry "$ip" "sudo cp /tmp/etcfuse-meta /usr/local/bin/etcfuse-meta && sudo chmod +x /usr/local/bin/etcfuse-meta" || true
            scp -o StrictHostKeyChecking=no -o ConnectTimeout=10 -q /tmp/chaos.tar.gz ec2-user@$ip:/tmp/ 2>/dev/null || true
            ssh_retry "$ip" "
                rm -rf /tmp/s && mkdir /tmp/s && cd /tmp/s && tar xzf /tmp/chaos.tar.gz && \
                gcc -I. -Wall -Wextra -Werror -std=c11 -D_GNU_SOURCE -O2 -g \
                    cmd/etcfuse/main.c pkg/fuse/fuse.c pkg/fuse/ops.c pkg/block/block.c pkg/wal/wal.c \
                    -o /tmp/etcfuse -lfuse3 -lpthread 2>&1 && \
                sudo cp /tmp/etcfuse /usr/local/bin/etcfuse && sudo chmod +x /usr/local/bin/etcfuse
            " || true
            ssh_retry "$ip" "sudo mkdir -p /mnt/etcfuse" || true
        done

        log "  Starting etcd cluster..."
        local INIT="e0=http://$P1:2380,e1=http://$P2:2380,e2=http://$P3:2380"
        for i in 1 2 3; do
            eval "ip=\$N$i"; eval "priv=\$P$i"; local ename="e$((i-1))"
            # shellcheck disable=SC2154
            ssh -o StrictHostKeyChecking=no ec2-user@$ip "
                sudo rm -rf /var/lib/etcd && sudo mkdir -p /var/lib/etcd
                sudo nohup /usr/local/bin/etcd --name $ename --data-dir /var/lib/etcd \
                    --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://$priv:2379 \
                    --listen-peer-urls http://0.0.0.0:2380 --initial-advertise-peer-urls http://$priv:2380 \
                    --initial-cluster $INIT --initial-cluster-state new > /tmp/etcd.log 2>&1 &
            " 2>/dev/null
        done
        sleep 10
        log "  etcd cluster running"

        log "  Starting daemons..."
        local ETCD="http://$P1:2379,http://$P2:2379,http://$P3:2379"
        for i in 1 2 3; do
            eval "ip=\$N$i"
            ssh -o StrictHostKeyChecking=no ec2-user@$ip "
                sudo killall -9 etcfuse-meta etcfuse 2>/dev/null; sudo umount -l /mnt/etcfuse 2>/dev/null; sleep 1
                sudo rm -f /tmp/etcfuse.sock /tmp/etcfuse-notify.sock
                sudo nohup /usr/local/bin/etcfuse-meta --listen=/tmp/etcfuse.sock \
                    --etcd-endpoints=$ETCD --node-id=n$i --cluster-name=$TAG \
                    --lease-ttl=10s --block-device=/dev/nvme1n1 --log-level=1 > /tmp/meta.log 2>&1 &
                sleep 4
                sudo nohup /usr/local/bin/etcfuse --socket=/tmp/etcfuse.sock \
                    --node-id=n$i --log-level=1 /mnt/etcfuse > /tmp/fuse.log 2>&1 &
                sleep 7
                sudo mountpoint -q /mnt/etcfuse && echo OK || echo FAIL
            " 2>/dev/null
        done
        local MOUNT_OK=0
        for i in 1 2 3; do
            eval "ip=\$N$i"
            # shellcheck disable=SC2015
            check_mount "$ip" && MOUNT_OK=$((MOUNT_OK+1)) || { log "  ERROR: FUSE mount on $ip FAILED!"; dump_logs "$ip"; }
        done
        # aws has no separate meta "container" — alias M1..M3 to the same
        # host as N1..N3 so callers written for docker's meta/fuse split
        # (chaos-fuzz.sh's inject_fault) don't hit unbound variables.
        M1="$N1"; M2="$N2"; M3="$N3"
        log "  Daemons running ($MOUNT_OK/3 mounts OK)"
        [[ "$MOUNT_OK" -eq 3 ]]
    }
    teardown_cluster() {
        log "  Teardown ($STATE_FILE)..."
        # nohup + disown: this runs async so the caller doesn't block on
        # instance termination, but a bare `&` dies with the parent shell —
        # if the script exits (or is killed) before termination finishes,
        # the volume and security group are orphaned and keep billing.
        ETCFS_STATE="$STATE_FILE" nohup bash "$PROJECT_ROOT/scripts/infra/destroy-infra.sh" --force \
            > "$PROJECT_ROOT/teardown-$STATE_FILE.log" 2>&1 &
        disown
        log "  Teardown launched (async, log: teardown-$STATE_FILE.log)"
    }
fi
