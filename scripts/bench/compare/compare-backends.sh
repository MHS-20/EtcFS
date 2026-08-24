#!/bin/bash
# compare-backends.sh — "bring backend $COMPARE_BACKEND up on the cluster
# compare_provision just created, and hand back a path".
#
# The IOPS suite has one script per backend, each of which sets its backend up
# and immediately benchmarks it. The scenario suite inverts that: one script
# per *scenario*, run against whichever backend COMPARE_BACKEND names, so the
# setup half has to be callable on its own. It lives here rather than in
# compare-lib.sh only for size — compare-lib.sh sources this at the end, so
# every caller already has it.
#
# compare_mount sets, for the caller:
#   MOUNT_PATH   path of the filesystem under test, identical on every node
#                that has it
#   BENCH_NODES  public IPs that have it mounted, in cluster order
#   N0           BENCH_NODES[0] (bench-lib.sh's run_fio drives this one)
#
# Every backend's own quirks — which AMI, which ports, why gluster gets local
# volumes and juicefs does not — are documented in the matching bench-*.sh
# header, which is where the reasoning was worked out.

# compare_mount [mount_all]
# mount_all=1 (default) mounts on every node; the IOPS runs only ever needed
# one client and some backends charge real setup time per client.
compare_mount() {
    local mount_all="${1:-1}"
    case "$COMPARE_BACKEND_BASE" in
        etcfs)   compare_mount_etcfs ;;
        nfs)     compare_mount_nfs ;;
        gfs2)    compare_mount_gfs2 ;;
        gluster) compare_mount_gluster "$mount_all" ;;
        juicefs) compare_mount_juicefs "$mount_all" ;;
        *) die "compare_mount: unknown backend $COMPARE_BACKEND" ;;
    esac
    N0="${BENCH_NODES[0]}"
    compare_install_fio "${BENCH_NODES[@]}"
    log "[$COMPARE_BACKEND] mounted at $MOUNT_PATH on ${#BENCH_NODES[@]} node(s)"
}

compare_mount_etcfs() {
    bash "$INFRA_DIR/bootstrap-cluster.sh" "$ETCFS_STATE" || die "bootstrap-cluster.sh failed"
    local ip
    for ip in "${COMPARE_PUB_IPS[@]}"; do
        wait_for_fuse_mount "$ip" 60 2 || die "etcfs never mounted on $ip"
    done
    MOUNT_PATH="$FUSE_MOUNTPOINT"
    BENCH_NODES=("${COMPARE_PUB_IPS[@]}")

    # Whether the kernel is allowed to cache this mount's data pages, reported
    # rather than assumed. The daemon only answers an open as cacheable while a
    # cache-invalidation client is connected to take those pages back again, and
    # the C side makes exactly one connect attempt at startup with no retry and
    # no message of its own -- so a lost race leaves --page-cache silently inert
    # and every read measurement device-bound, which is indistinguishable from
    # the coordination layer simply being slow.
    for ip in "${COMPARE_PUB_IPS[@]}"; do
        if $SSH_CMD "ec2-user@$ip" "sudo grep -q 'notify: client connected' /tmp/meta.log" 2>/dev/null; then
            log "  $ip: cache-invalidation client connected (page cache available)"
        else
            log "  $ip: WARNING cache-invalidation client NOT connected — the kernel will not"
            log "      cache this mount's pages, so every read is device-bound"
        fi
    done
}

compare_mount_nfs() {
    compare_export_backing "${COMPARE_PUB_IPS[0]}" "${COMPARE_PRIV_IPS[0]}" "${COMPARE_PUB_IPS[@]:1}"
    MOUNT_PATH="$BACKING_PATH"
    BENCH_NODES=("${COMPARE_PUB_IPS[@]:1}" "${COMPARE_PUB_IPS[0]}")
}

compare_mount_gfs2() {
    local ip i dev
    compare_open_port_udp 5404 5405  # corosync totem
    compare_open_port 21064          # dlm_controld

    for ip in "${COMPARE_PUB_IPS[@]}"; do
        $SSH_CMD "ec2-user@$ip" "set -e; sudo yum install -y corosync dlm gfs2-utils"
    done

    local conf="
totem {
    version: 2
    cluster_name: comparegfs2
    transport: udpu
    crypto_hash: none
    crypto_cipher: none
}
nodelist {
$(for i in "${!COMPARE_PRIV_IPS[@]}"; do printf '    node {\n        ring0_addr: %s\n        nodeid: %d\n    }\n' "${COMPARE_PRIV_IPS[$i]}" "$((i+1))"; done)
}
quorum {
    provider: corosync_votequorum
}
logging {
    to_syslog: yes
}
"
    for ip in "${COMPARE_PUB_IPS[@]}"; do
        $SSH_CMD "ec2-user@$ip" "sudo mkdir -p /etc/corosync"
        echo "$conf" | $SSH_CMD "ec2-user@$ip" "sudo tee /etc/corosync/corosync.conf >/dev/null"
        $SSH_CMD "ec2-user@$ip" "sudo systemctl enable --now corosync"
    done

    log "Waiting for corosync quorum..."
    for _ in $(seq 1 30); do
        $SSH_CMD "ec2-user@${COMPARE_PUB_IPS[0]}" \
            "sudo corosync-quorumtool -s 2>/dev/null | grep -q 'Quorate:.*Yes'" && break
        sleep 2
    done
    for ip in "${COMPARE_PUB_IPS[@]}"; do
        $SSH_CMD "ec2-user@$ip" "sudo systemctl enable --now dlm"
    done
    sleep 5

    dev=$(compare_shared_device "${COMPARE_PUB_IPS[0]}")
    # One journal per node, allocated at mkfs and never afterwards — the
    # structural limit the scaling scenarios exist to show.
    $SSH_CMD "ec2-user@${COMPARE_PUB_IPS[0]}" \
        "sudo mkfs.gfs2 -O -p lock_dlm -t comparegfs2:vol1 -j ${#COMPARE_PUB_IPS[@]} $dev"
    for ip in "${COMPARE_PUB_IPS[@]}"; do
        $SSH_CMD "ec2-user@$ip" "sudo mkdir -p /mnt/compare-gfs2 && sudo mount -t gfs2 -o noatime $dev /mnt/compare-gfs2"
    done
    MOUNT_PATH=/mnt/compare-gfs2
    BENCH_NODES=("${COMPARE_PUB_IPS[@]}")
}

compare_mount_gluster() {
    local mount_all="$1" i ip entries=() bricks=""
    compare_open_port 24007
    compare_open_port 49152 49251

    for i in "${!COMPARE_PUB_IPS[@]}"; do
        entries+=("${COMPARE_PUB_IPS[$i]}:${COMPARE_PRIV_IPS[$i]}:$(state_get compute_instance_ids | jq -r ".[$i]")")
    done
    local devs=()
    mapfile -t devs < <(compare_create_local_volumes "$ETCFS_VOLUME_IOPS" "${entries[@]}")

    for i in "${!COMPARE_PUB_IPS[@]}"; do
        $SSH_CMD "ec2-user@${COMPARE_PUB_IPS[$i]}" "set -e
            sudo amazon-linux-extras install -y epel
            printf '%s\n' '[gluster9]' 'name=Gluster 9' \
                'baseurl=https://vault.centos.org/centos/7/storage/\$basearch/gluster-9/' \
                'gpgcheck=0' 'enabled=1' | sudo tee /etc/yum.repos.d/gluster9.repo >/dev/null
            sudo yum install -y --disableplugin=priorities userspace-rcu
            sudo yum install -y xfsprogs ncurses-compat-libs
            sudo yum install -y --disablerepo=amzn2-core --disableplugin=priorities glusterfs-server
            sudo mkfs.xfs -f ${devs[$i]}
            sudo mkdir -p /mnt/gluster-brick
            sudo mount ${devs[$i]} /mnt/gluster-brick
            sudo mkdir -p /mnt/gluster-brick/brick
            sudo systemctl enable --now glusterd
        "
        bricks+=" ${COMPARE_PRIV_IPS[$i]}:/mnt/gluster-brick/brick"
    done

    for i in "${!COMPARE_PRIV_IPS[@]}"; do
        [[ "$i" -eq 0 ]] && continue
        $SSH_CMD "ec2-user@${COMPARE_PUB_IPS[0]}" "sudo gluster peer probe ${COMPARE_PRIV_IPS[$i]}"
    done
    sleep 5
    $SSH_CMD "ec2-user@${COMPARE_PUB_IPS[0]}" "set -e
        sudo gluster volume create compare-vol replica ${#COMPARE_PRIV_IPS[@]} $bricks force
        sudo gluster volume start compare-vol
    "
    sleep 5

    BENCH_NODES=()
    for ip in "${COMPARE_PUB_IPS[@]:1}"; do
        $SSH_CMD "ec2-user@$ip" "set -e
            sudo yum install -y glusterfs-fuse >/dev/null 2>&1
            sudo mkdir -p /mnt/compare-gluster
            sudo mount -t glusterfs ${COMPARE_PRIV_IPS[0]}:/compare-vol /mnt/compare-gluster
        "
        BENCH_NODES+=("$ip")
        [[ "$mount_all" == "1" ]] || break
    done
    MOUNT_PATH=/mnt/compare-gluster
}

compare_mount_juicefs() {
    local mount_all="$1" ip dev
    compare_open_port 6379  # redis (metadata)
    compare_open_port 9000  # minio (object storage)
    dev=$(compare_shared_device "${COMPARE_PUB_IPS[0]}")

    $SSH_CMD "ec2-user@${COMPARE_PUB_IPS[0]}" "set -e
        sudo mkfs.ext4 -q -F $dev
        sudo mkdir -p /mnt/juicefs-minio-data
        sudo mount $dev /mnt/juicefs-minio-data

        sudo dnf install -y redis6 fio || sudo dnf install -y redis fio || sudo yum install -y redis fio
        REDIS_BIN=\$(command -v redis-server || command -v redis6-server)
        sudo nohup \"\$REDIS_BIN\" --bind 0.0.0.0 --protected-mode no > /tmp/redis.log 2>&1 &
        sleep 2

        curl -fsSL https://dl.min.io/server/minio/release/linux-amd64/minio -o /tmp/minio
        sudo install -m 755 /tmp/minio /usr/local/bin/minio
        sudo useradd -r -m -d /home/minio-user minio-user 2>/dev/null || true
        sudo chown -R minio-user:minio-user /mnt/juicefs-minio-data
        sudo -u minio-user MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \
            nohup /usr/local/bin/minio server /mnt/juicefs-minio-data --address :9000 \
            > /tmp/minio.log 2>&1 &
        sleep 3

        curl -fsSL https://d.juicefs.com/install | sudo sh -
        sudo juicefs format --storage minio \
            --bucket http://${COMPARE_PRIV_IPS[0]}:9000/juicefs-bucket \
            --access-key minioadmin --secret-key minioadmin \
            redis://127.0.0.1:6379/1 compare-vol
    "

    BENCH_NODES=()
    for ip in "${COMPARE_PUB_IPS[@]:1}"; do
        $SSH_CMD "ec2-user@$ip" "set -e
            curl -fsSL https://d.juicefs.com/install | sudo sh -
            sudo mkdir -p /mnt/compare-juicefs
            sudo juicefs mount -d redis://${COMPARE_PRIV_IPS[0]}:6379/1 /mnt/compare-juicefs
        "
        BENCH_NODES+=("$ip")
        [[ "$mount_all" == "1" ]] || break
    done
    sleep 3
    MOUNT_PATH=/mnt/compare-juicefs
}

# ---- node failure ----
#
# compare_kill_node <pub_ip> — take that node out the way a real failure would.
#
# The three shared-device backends are killed identically, and that is the whole
# point: the machine is powered off through sysrq, so nothing runs an exit path,
# nothing unmounts, nothing releases a lock, and no daemon gets to notice it is
# dying. Killing hand-picked daemons instead measured a different failure per
# backend — `killall -9 corosync dlm_controld` in particular is an orderly
# membership change, which is most of what "fence plus journal replay" was
# supposed to cost, so gfs2 was being asked to recover from something far milder
# than etcfs was. A comparison is only worth publishing if the fault is the same.
#
# The two server-mediated backends have no comparable partial failure — the
# server IS the filesystem — so they get an outage instead: the ports are
# blocked before the processes are killed, because stopping a service lets its
# clients drain into a socket that is still accepting, and a graceful stop can
# complete long after the probe's next write has already been acknowledged. The
# block is what makes the outage begin at a known instant.
#
# Powering the victim off is deliberately not undone: the survivor is what is
# being measured, and a victim that reboots and rejoins mid-run changes what the
# survivor is recovering from. Teardown destroys the cluster regardless.
compare_kill_node() {
    local ip="$1"
    case "$COMPARE_BACKEND_BASE" in
        etcfs | gfs2 | gluster)
            # The connection dies with the machine, so a failed ssh here is the
            # expected outcome rather than an error.
            $SSH_CMD -o ConnectTimeout=5 "ec2-user@$ip" \
                "sudo sh -c 'echo 1 > /proc/sys/kernel/sysrq; echo o > /proc/sysrq-trigger' &" \
                >/dev/null 2>&1 || true
            ;;
        nfs)
            $SSH_CMD "ec2-user@$ip" "
                sudo iptables -I INPUT -p tcp --dport 2049 -j DROP
                sudo iptables -I OUTPUT -p tcp --sport 2049 -j DROP
                sudo systemctl kill -s SIGKILL nfs-server 2>/dev/null
                true"
            ;;
        juicefs)
            $SSH_CMD "ec2-user@$ip" "
                sudo iptables -I INPUT -p tcp --dport 6379 -j DROP
                sudo iptables -I INPUT -p tcp --dport 9000 -j DROP
                sudo killall -9 redis-server redis6-server minio 2>/dev/null
                true"
            ;;
        *) die "compare_kill_node: unknown backend $COMPARE_BACKEND" ;;
    esac
}

# compare_failure_target — the node whose loss is worth timing. For the
# shared-device backends that is any node holding locks under load; for the
# server-mediated ones it is the server, and killing anything else measures
# nothing.
compare_failure_target() {
    case "$COMPARE_BACKEND_BASE" in
        nfs|juicefs) echo "${COMPARE_PUB_IPS[0]}" ;;
        *) echo "${BENCH_NODES[-1]}" ;;
    esac
}

# ---- etcfs node lifecycle (join / leave / rejoin) ----
#
# Restarting a node means re-running the two daemons with the arguments
# bootstrap-cluster.sh gave them — endpoints, node ID, device, fencing flags,
# all derived from state this library does not have. Rather than duplicate that
# derivation and drift from it, snapshot each daemon's own command line off the
# running process before killing it, and replay that.
compare_etcfs_snapshot_cmdline() {
    $SSH_CMD "ec2-user@$1" "sudo sh -c '
        ps -o args= -C etcfuse-meta | head -1 > /tmp/meta.cmd
        ps -o args= -C etcfuse | head -1 > /tmp/fuse.cmd
        test -s /tmp/meta.cmd && test -s /tmp/fuse.cmd'" \
        || die "compare_etcfs_snapshot_cmdline: no running daemons on $1"
}

# compare_reattach_volume_if_missing <pub_ip> — re-attaches COMPARE_VOL_ID to
# that node's instance if the shared device isn't there, the same recovery
# scripts/infra/bootstrap-cluster.sh already does for a full cluster rebuild.
# External fencing detaches on purpose and never puts a volume back by itself
# (see bootstrap-cluster.sh's own comment) — this is a benchmark script
# standing in for the operator action that fencing expects to follow it.
compare_reattach_volume_if_missing() {
    local ip="$1" i inst
    $SSH_CMD "ec2-user@$ip" "[[ -e /dev/nvme1n1 ]]" 2>/dev/null && return 0
    for i in "${!COMPARE_PUB_IPS[@]}"; do
        [[ "${COMPARE_PUB_IPS[$i]}" == "$ip" ]] && break
    done
    inst=$(state_get compute_instance_ids | jq -r ".[$i]")
    [[ -n "$inst" && "$inst" != "null" ]] || die "compare_reattach_volume_if_missing: no instance id for $ip"
    log "  $ip ($inst) has lost its device (fenced), reattaching $COMPARE_VOL_ID..."
    aws ec2 attach-volume --volume-id "$COMPARE_VOL_ID" --instance-id "$inst" --device /dev/sdf >/dev/null 2>&1 || true
    aws ec2 wait volume-in-use --volume-ids "$COMPARE_VOL_ID" 2>/dev/null || true
    local n
    for n in 1 2 3 4 5 6 7 8 9 10; do
        $SSH_CMD "ec2-user@$ip" "[[ -e /dev/nvme1n1 ]]" 2>/dev/null && return 0
        sleep 2
    done
    die "compare_reattach_volume_if_missing: device still missing on $ip after reattach"
}

# compare_etcfs_start <pub_ip> — prints seconds from process start to a mount
# that answers a write, which is the join-latency number.
#
# Reattaches the shared volume first if it is missing, which a clean leave no
# longer causes: a node that shuts down gracefully gives back its locks and
# arenas and then announces the departure in the same transaction that removes
# it from membership, so its peers can tell an intentional leave from a crash
# and do not detach it. The reattach stays because a node that was killed, or
# one whose arena release failed, is still fenced for real — and fencing never
# puts a volume back by itself (see bootstrap-cluster.sh's own comment).
compare_etcfs_start() {
    local ip="$1" start_s
    start_s=$(compare_epoch)
    compare_reattach_volume_if_missing "$ip"
    # 120s, not 60: under concurrent write load on the survivors (the caller's
    # own fio job, running the whole time this restarts), the joiner's first
    # etcd writes (membership register, arena claim) contend with that load
    # for commit throughput, and 60s was tight enough to fail a run that was
    # still making progress, not stuck.
    $SSH_CMD "ec2-user@$ip" "
        sudo umount -l $FUSE_MOUNTPOINT 2>/dev/null
        sudo rm -f /run/etcfuse/etcfuse.sock /run/etcfuse/etcfuse-notify.sock
        sudo sh -c 'nohup \$(cat /tmp/meta.cmd) >> /tmp/meta.log 2>&1 &'
        # etcfuse (the FUSE client) does not retry a missing socket — it exits
        # immediately if /run/etcfuse/etcfuse.sock isn't there yet, which a bare
        # back-to-back launch races against etcfuse-meta creating it.
        # bootstrap-cluster.sh's own first-boot sequence sidesteps this with a
        # flat sleep between the two; polling for the socket is the same fix
        # without waiting longer than the daemon actually needs.
        sock_deadline=\$((\$(date +%s) + 30))
        until sudo test -S /run/etcfuse/etcfuse.sock; do
            [[ \$(date +%s) -lt \$sock_deadline ]] || { echo 'compare_etcfs_start: etcfuse-meta never created its socket' >&2; exit 1; }
            sleep 0.1
        done
        sudo sh -c 'nohup \$(cat /tmp/fuse.cmd) >> /tmp/fuse.log 2>&1 &'
        deadline=\$((\$(date +%s) + 120))
        until sudo mountpoint -q $FUSE_MOUNTPOINT && \
              sudo dd if=/dev/zero of=$FUSE_MOUNTPOINT/join-probe.\$\$ bs=4k count=1 >/dev/null 2>&1; do
            [[ \$(date +%s) -lt \$deadline ]] || { echo 'compare_etcfs_start: mount never came up within 120s' >&2; exit 1; }
            sleep 0.05
        done
        sudo rm -f $FUSE_MOUNTPOINT/join-probe.* 2>/dev/null
    " || die "compare_etcfs_start: daemon on $ip never produced a working mount"
    local end_s
    end_s=$(compare_epoch)
    awk -v s="$start_s" -v e="$end_s" 'BEGIN{printf "%.3f", e-s}'
}
