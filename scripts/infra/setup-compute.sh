#!/bin/bash
# setup-compute.sh — install etcd + EtcFS FUSE daemon on compute nodes.
#
# Etcd is colocated on each compute node (no dedicated etcd instances).
# For each compute node:
#   1. Install build tools + kernel headers (for io_uring/O_DIRECT)
#   2. Install etcd binary + TLS certs + systemd unit
#   3. Build and deploy EtcFS FUSE daemon
#   4. First node: start etcd (bootstrap cluster), init etcd schema, start daemon, mount FUSE
#   5. Other nodes: join existing etcd cluster, start daemon, mount FUSE
#
# Daemons start sequentially — first node initialises the etcd schema
# before other nodes join. This prevents races during etcd bootstrap.
#
# Idempotent: safe to re-run. Skips already-completed steps.
#
# NOTE: This is a template. EtcFS daemon binary does not exist yet (design phase).
#       Placeholders marked with [TEMPLATE] will be replaced during implementation.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/state.sh"

mapfile -t COMPUTE_IPS < <(state_get compute_public_ips | jq -r '.[]')
if [[ -z "${COMPUTE_IPS[*]}" || "${COMPUTE_IPS[0]}" == "null" ]]; then
    mapfile -t COMPUTE_IPS < <(state_get compute_ips | jq -r '.[]')
fi
mapfile -t COMPUTE_PRIV_IPS < <(state_get compute_ips | jq -r '.[]')
COMPUTE_COUNT=${#COMPUTE_IPS[@]}

if [[ "$COMPUTE_COUNT" -eq 0 ]]; then
    die "No compute IPs in state file. Run create-infra.sh first."
fi

ETCD_ENDPOINTS=$(state_get etcd_endpoints)
if [[ -z "$ETCD_ENDPOINTS" || "$ETCD_ENDPOINTS" == "null" ]]; then
    die "etcd endpoints not found in state."
fi

VOL_ID=$(state_get volume_id)
CLUSTER=$(state_get cluster_name)
CERT_DIR="$PROJECT_ROOT/certs"
ETCD_URL="https://github.com/etcd-io/etcd/releases/download/${ETCD_VER}/etcd-${ETCD_VER}-linux-amd64.tar.gz"

log "=== EtcFS compute node setup ($COMPUTE_COUNT nodes, etcd colocated) ==="
log "etcd:        $ETCD_ENDPOINTS"
log "Volume:      $VOL_ID"
log "Cluster:     $CLUSTER"
log "Mount point: $FUSE_MOUNTPOINT"
log "Lease TTL:   ${LEASH_TTL}s (heartbeat: ${HEARTBEAT_INTERVAL}s)"
log ""

# ---- Helper: check if etcd is installed ----

is_etcd_installed() {
    local ip="$1"
    $SSH_CMD "ec2-user@$ip" "test -f /usr/local/bin/etcd" 2>/dev/null
}

# ---- Helper: check if FUSE is mounted ----

is_fuse_mounted() {
    local ip="$1"
    $SSH_CMD "ec2-user@$ip" "mountpoint -q $FUSE_MOUNTPOINT 2>/dev/null" 2>/dev/null
}

# ---- Generate TLS certs ----

# Check if existing certs contain the current IPs
CERTS_VALID=false
if [[ -f "$CERT_DIR/ca.crt" ]]; then
    CERTS_VALID=true
    for ip in "${COMPUTE_PRIV_IPS[@]}"; do
        if ! openssl x509 -in "$CERT_DIR/peer-etcd-0.crt" -noout -text 2>/dev/null | grep -q "IP Address: ${ip}"; then
            CERTS_VALID=false
            break
        fi
    done
fi

if [[ "$CERTS_VALID" != "true" ]]; then
    log "Generating TLS certificates..."
    rm -rf "$CERT_DIR" && mkdir -p "$CERT_DIR"

    openssl genrsa -out "$CERT_DIR/ca.key" 2048 2>/dev/null
    openssl req -new -x509 -days 3650 -key "$CERT_DIR/ca.key" \
        -out "$CERT_DIR/ca.crt" -subj "/CN=etcd-ca" 2>/dev/null

    SAN_IPS=""
    for ip in "${COMPUTE_PRIV_IPS[@]}"; do
        [[ -n "$SAN_IPS" ]] && SAN_IPS+=","
        SAN_IPS+="IP:${ip}"
    done
    SAN_IPS+=",IP:127.0.0.1"

    for i in "${!COMPUTE_PRIV_IPS[@]}"; do
        name="etcd-${i}"
        ip="${COMPUTE_PRIV_IPS[$i]}"

        cat > "$CERT_DIR/ext-${name}.cnf" <<EOF
subjectAltName=${SAN_IPS},DNS:${name},DNS:localhost
EOF

        openssl genrsa -out "$CERT_DIR/server-${name}.key" 2048 2>/dev/null
        openssl req -new -key "$CERT_DIR/server-${name}.key" \
            -out "$CERT_DIR/server-${name}.csr" -subj "/CN=${name}" 2>/dev/null
        openssl x509 -req -days 3650 -in "$CERT_DIR/server-${name}.csr" \
            -CA "$CERT_DIR/ca.crt" -CAkey "$CERT_DIR/ca.key" -CAcreateserial \
            -out "$CERT_DIR/server-${name}.crt" \
            -extfile "$CERT_DIR/ext-${name}.cnf" 2>/dev/null

        openssl genrsa -out "$CERT_DIR/peer-${name}.key" 2048 2>/dev/null
        openssl req -new -key "$CERT_DIR/peer-${name}.key" \
            -out "$CERT_DIR/peer-${name}.csr" -subj "/CN=${name}-peer" 2>/dev/null
        openssl x509 -req -days 3650 -in "$CERT_DIR/peer-${name}.csr" \
            -CA "$CERT_DIR/ca.crt" -CAkey "$CERT_DIR/ca.key" -CAcreateserial \
            -out "$CERT_DIR/peer-${name}.crt" \
            -extfile "$CERT_DIR/ext-${name}.cnf" 2>/dev/null
    done

    openssl genrsa -out "$CERT_DIR/client.key" 2048 2>/dev/null
    openssl req -new -key "$CERT_DIR/client.key" \
        -out "$CERT_DIR/client.csr" -subj "/CN=etcfuse" 2>/dev/null
    openssl x509 -req -days 3650 -in "$CERT_DIR/client.csr" \
        -CA "$CERT_DIR/ca.crt" -CAkey "$CERT_DIR/ca.key" -CAcreateserial \
        -out "$CERT_DIR/client.crt" 2>/dev/null

    chmod 600 "$CERT_DIR"/*.key
    log "TLS certs generated"
else
    log "TLS certs already exist, skipping generation"
fi

# ---- Build initial-cluster string ----

INITIAL_CLUSTER=""
for i in "${!COMPUTE_PRIV_IPS[@]}"; do
    name="etcd-${i}"
    [[ -n "$INITIAL_CLUSTER" ]] && INITIAL_CLUSTER+=","
    INITIAL_CLUSTER+="${name}=https://${COMPUTE_PRIV_IPS[$i]}:2380"
done
log "Initial cluster: $INITIAL_CLUSTER"

# [TEMPLATE] Build EtcFS FUSE daemon locally.
# When implementation exists, replace this section with the actual build command.
# For now, we check for a pre-built binary or skip.
log "=== EtcFS daemon (template — binary not built yet) ==="
DAEMON_BIN="$PROJECT_ROOT/target/release/etcfuse"
if [[ -f "$DAEMON_BIN" ]]; then
    log "Using pre-built binary: $DAEMON_BIN"
elif [[ -f "$PROJECT_ROOT/bin/etcfuse" ]]; then
    DAEMON_BIN="$PROJECT_ROOT/bin/etcfuse"
    log "Using pre-built binary: $DAEMON_BIN"
else
    log "NOTE: EtcFS daemon binary not found at $DAEMON_BIN."
    log "      This is expected during design phase."
    log "      Build command will go here (e.g. 'cargo build --release' or 'go build')."
    log "      When binary exists, the deploy section below will push it to nodes."
    DAEMON_BIN=""
fi

# ---- Setup each node ----

FIRST_NODE=true

for i in "${!COMPUTE_IPS[@]}"; do
    ip="${COMPUTE_IPS[$i]}"
    priv_ip="${COMPUTE_PRIV_IPS[$i]}"
    wait_for_ssh "$ip" || { log "WARNING: SSH not ready for $ip — skipping"; continue; }
    etcd_name="etcd-${i}"
    node_name="${CLUSTER}-node-$((i+1))"

    log ""
    log "========================================="
    log "Setting up $node_name ($ip, etcd=$etcd_name)"
    log "========================================="

    # --- Install packages (idempotent) ---
    if ! $SSH_CMD "ec2-user@$ip" "rpm -q gcc make git &>/dev/null"; then
        log "Installing packages..."
        $SSH_CMD "ec2-user@$ip" <<'PACKAGES'
set -e
sudo dnf install -y gcc make git rsync 2>&1 | tail -5
# kernel-headers needed for io_uring at build time
sudo dnf install -y kernel-headers 2>&1 | tail -3 || true
# FUSE3 userspace libraries (needed if linking against libfuse)
sudo dnf install -y fuse3-libs fuse3-devel 2>&1 | tail -3 || true
PACKAGES
    else
        log "Packages already installed"
    fi

    # --- Install etcd binary (idempotent) ---
    if ! is_etcd_installed "$ip"; then
        log "Installing etcd $ETCD_VER..."
        $SSH_CMD "ec2-user@$ip" <<ETCDINST
set -e
sudo mkdir -p /etc/etcd/tls /var/lib/etcd
sudo chown ec2-user:ec2-user /var/lib/etcd
curl -sLo /tmp/etcd.tar.gz '${ETCD_URL}'
tar xzf /tmp/etcd.tar.gz -C /tmp
sudo mv /tmp/etcd-${ETCD_VER}-linux-amd64/etcd /tmp/etcd-${ETCD_VER}-linux-amd64/etcdctl /usr/local/bin/
sudo chmod 755 /usr/local/bin/etcd /usr/local/bin/etcdctl
rm -rf /tmp/etcd*
ETCDINST
    else
        log "etcd already installed"
    fi

    # --- Push etcd certs ---
    log "Pushing etcd TLS certs..."
    scp $SSH_OPTS \
        "$CERT_DIR/ca.crt" \
        "$CERT_DIR/server-${etcd_name}.crt" "$CERT_DIR/server-${etcd_name}.key" \
        "$CERT_DIR/peer-${etcd_name}.crt" "$CERT_DIR/peer-${etcd_name}.key" \
        "ec2-user@$ip:/tmp/"

    $SSH_CMD "ec2-user@$ip" <<ETCDCERTS
sudo mkdir -p /etc/etcd/tls
sudo mv /tmp/ca.crt /tmp/server-${etcd_name}.crt /tmp/server-${etcd_name}.key \
    /tmp/peer-${etcd_name}.crt /tmp/peer-${etcd_name}.key /etc/etcd/tls/
sudo chown -R root:root /etc/etcd/tls
sudo chmod 600 /etc/etcd/tls/*.key
ETCDCERTS

    # --- etcd systemd unit ---
    log "Creating etcd systemd unit..."
    $SSH_CMD "ec2-user@$ip" "sudo tee /etc/systemd/system/etcd.service" <<'ETCDUNIT'
[Unit]
Description=etcd (EtcFS colocated)
After=network-online.target
Wants=network-online.target
Documentation=https://etcd.io

[Service]
Type=notify
ExecStart=/bin/true
Restart=always
RestartSec=5
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
ETCDUNIT
    $SSH_CMD "ec2-user@$ip" "sudo mkdir -p /etc/systemd/system/etcd.service.d"

    # --- Push daemon certs (shared directory: /etc/etcfuse) ---
    log "Pushing EtcFS daemon TLS certs..."
    scp $SSH_OPTS "$CERT_DIR/ca.crt" "ec2-user@$ip:/tmp/"
    scp $SSH_OPTS "$CERT_DIR/client.crt" "ec2-user@$ip:/tmp/"
    scp $SSH_OPTS "$CERT_DIR/client.key" "ec2-user@$ip:/tmp/"
    $SSH_CMD "ec2-user@$ip" <<DAEMONCERTS
sudo mkdir -p /etc/etcfuse
sudo mv /tmp/ca.crt /tmp/client.crt /tmp/client.key /etc/etcfuse/
sudo chown -R root:root /etc/etcfuse
sudo chmod 600 /etc/etcfuse/*.key
DAEMONCERTS

    # --- Clean stale etcd data from a previous failed run ---
    $SSH_CMD "ec2-user@$ip" <<'CLEAN_STALE'
if [ -d /var/lib/etcd/member ] && ! sudo systemctl is-active etcd.service &>/dev/null; then
    echo "Stale etcd data with no running etcd — cleaning"
    sudo rm -rf /var/lib/etcd/member /etc/etcd/etcd.args
fi
CLEAN_STALE

    # --- Deploy EtcFS FUSE daemon ---
    # [TEMPLATE] This section will push and install the actual EtcFS binary.
    # For now, it creates the systemd unit with the correct configuration
    # so that when the binary is built, only the ExecStart line needs updating.
    log "Installing EtcFS FUSE daemon (template)..."
    if [[ -n "$DAEMON_BIN" && -f "$DAEMON_BIN" ]]; then
        gzip -c "$DAEMON_BIN" > /tmp/etcfuse.gz
        scp $SSH_OPTS /tmp/etcfuse.gz "ec2-user@$ip:/tmp/"
        rm -f /tmp/etcfuse.gz
        $SSH_CMD "ec2-user@$ip" "
set -e
gzip -d -f /tmp/etcfuse.gz
sudo mv /tmp/etcfuse /usr/local/bin/etcfuse
sudo chmod 755 /usr/local/bin/etcfuse
"
        log "  Binary deployed to /usr/local/bin/etcfuse"
    else
        log "  (no binary — systemd unit will be created but daemon won't start)"
    fi

    PEER_URL="https://${priv_ip}:2380"

    $SSH_CMD "ec2-user@$ip" "sudo tee /etc/systemd/system/etcfuse.service" <<DAEMONUNIT
# EtcFS FUSE Daemon systemd unit
# [TEMPLATE] CLI flags below are the planned interface.
# Adjust as needed when the daemon binary is implemented.
[Unit]
Description=EtcFS FUSE Daemon
After=network-online.target etcd.service
Wants=network-online.target etcd.service

[Service]
Type=simple
ExecStart=/usr/local/bin/etcfuse \\
  --node-id=${node_name} \\
  --etcd-endpoints=${ETCD_ENDPOINTS} \\
  --etcd-cert=/etc/etcfuse/client.crt \\
  --etcd-key=/etc/etcfuse/client.key \\
  --etcd-ca=/etc/etcfuse/ca.crt \\
  --etcd-name=${etcd_name} \\
  --peer-url=${PEER_URL} \\
  --initial-cluster=${INITIAL_CLUSTER} \\
  --volume-id=${VOL_ID} \\
  --block-device=/dev/nvme1n1 \\
  --mount-point=${FUSE_MOUNTPOINT} \\
  --cluster-name=${CLUSTER} \\
  --az=${AZ} \\
  --lease-ttl=${LEASH_TTL} \\
  --heartbeat-interval=${HEARTBEAT_INTERVAL}
Restart=on-failure
RestartSec=5
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
DAEMONUNIT

    $SSH_CMD "ec2-user@$ip" "sudo systemctl daemon-reload"
    $SSH_CMD "ec2-user@$ip" "sudo systemctl enable etcd"
    $SSH_CMD "ec2-user@$ip" "sudo systemctl enable etcfuse"
    log "  systemd units created and enabled"

    # --- First node: bootstrap etcd cluster + init schema + mount FUSE ---
    if $FIRST_NODE; then
        log ""
        log "=== First node: bootstrapping etcd cluster ==="

        # Write etcd args for first node (initial-cluster-state new)
        $SSH_CMD "ec2-user@$ip" <<FIRSTARTGS
sudo mkdir -p /etc/etcd /etc/systemd/system/etcd.service.d
sudo tee /etc/etcd/etcd.args > /dev/null <<'ARGS'
--name ${etcd_name}
--data-dir /var/lib/etcd
--listen-client-urls https://0.0.0.0:2379
--advertise-client-urls https://${priv_ip}:2379
--listen-peer-urls https://0.0.0.0:2380
--initial-advertise-peer-urls https://${priv_ip}:2380
--initial-cluster ${etcd_name}=https://${priv_ip}:2380
--initial-cluster-state new
--initial-cluster-token ${CLUSTER}
--client-cert-auth
--trusted-ca-file /etc/etcd/tls/ca.crt
--cert-file /etc/etcd/tls/server-${etcd_name}.crt
--key-file /etc/etcd/tls/server-${etcd_name}.key
--peer-client-cert-auth
--peer-trusted-ca-file /etc/etcd/tls/ca.crt
--peer-cert-file /etc/etcd/tls/peer-${etcd_name}.crt
--peer-key-file /etc/etcd/tls/peer-${etcd_name}.key
ARGS

sudo tee /etc/systemd/system/etcd.service.d/etcfuse.conf > /dev/null <<'DROPIN'
[Service]
ExecStart=
ExecStart=/bin/sh -c 'exec /usr/local/bin/etcd \$(cat /etc/etcd/etcd.args)'
Restart=always
DROPIN
FIRSTARTGS

        $SSH_CMD "ec2-user@$ip" "sudo systemctl daemon-reload && sudo systemctl start etcd"
        log "Waiting for etcd to become healthy..."
        if ! wait_for_etcd "$ip" 60; then
            log "ERROR: etcd did not become healthy on first node within 2 min"
            $SSH_CMD "ec2-user@$ip" "sudo journalctl -u etcd --no-pager -n 30" || true
            exit 1
        fi
        log "etcd healthy on $node_name"

        # [TEMPLATE] Initialize etcd schema.
        # The EtcFS daemon will do this on first start, or we can use a
        # dedicated init tool. For now: write the initial fencing generation
        # and reserve the first inode range block.
        log "Initialising EtcFS etcd schema..."
        # Placeholder: when implementation exists, this writes:
        #   gen:<node-0> = 1
        #   inode_alloc = {next_block: 0, ranges: []}
        #   arena_alloc_log = []
        etcdctl_cmd "$ip" "put gen:${node_name}" "1"
        etcdctl_cmd "$ip" "put inode_alloc_counter" "0"
        log "  Schema initialised (template — extend as schema evolves)"

        # Start EtcFS daemon
        if [[ -n "$DAEMON_BIN" && -f "$DAEMON_BIN" ]]; then
            log "Starting EtcFS FUSE daemon on first node..."
            $SSH_CMD "ec2-user@$ip" "
sudo mkdir -p $FUSE_MOUNTPOINT
sudo systemctl start etcfuse
"
            log "Waiting for daemon to be ready..."
            if ! wait_for_daemon_ready "$ip" 60 2; then
                log "ERROR: EtcFS daemon did not become ready on $node_name within 2 min"
                $SSH_CMD "ec2-user@$ip" "sudo journalctl -u etcfuse --no-pager -n 30" || true
                exit 1
            fi
            log "EtcFS FUSE mounted at $FUSE_MOUNTPOINT on $node_name"
            $SSH_CMD "ec2-user@$ip" "df -h $FUSE_MOUNTPOINT"
        else
            log "  (no daemon binary — skipping FUSE mount)"
        fi

        FIRST_NODE=false
    else
        # --- Other nodes: join existing etcd cluster, mount FUSE ---
        log ""
        log "=== $node_name: joining existing etcd cluster ==="

        # Write etcd args for follower (initial-cluster-state existing)
        $SSH_CMD "ec2-user@$ip" <<JOINARGS
sudo mkdir -p /etc/etcd /etc/systemd/system/etcd.service.d
sudo tee /etc/etcd/etcd.args > /dev/null <<'ARGS'
--name ${etcd_name}
--data-dir /var/lib/etcd
--listen-client-urls https://0.0.0.0:2379
--advertise-client-urls https://${priv_ip}:2379
--listen-peer-urls https://0.0.0.0:2380
--initial-advertise-peer-urls https://${priv_ip}:2380
--initial-cluster ${INITIAL_CLUSTER}
--initial-cluster-state existing
--initial-cluster-token ${CLUSTER}
--client-cert-auth
--trusted-ca-file /etc/etcd/tls/ca.crt
--cert-file /etc/etcd/tls/server-${etcd_name}.crt
--key-file /etc/etcd/tls/server-${etcd_name}.key
--peer-client-cert-auth
--peer-trusted-ca-file /etc/etcd/tls/ca.crt
--peer-cert-file /etc/etcd/tls/peer-${etcd_name}.crt
--peer-key-file /etc/etcd/tls/peer-${etcd_name}.key
ARGS

sudo tee /etc/systemd/system/etcd.service.d/etcfuse.conf > /dev/null <<'DROPIN'
[Service]
ExecStart=
ExecStart=/bin/sh -c 'exec /usr/local/bin/etcd \$(cat /etc/etcd/etcd.args)'
Restart=always
DROPIN
JOINARGS

        $SSH_CMD "ec2-user@$ip" "sudo systemctl daemon-reload && sudo systemctl start etcd"
        log "Waiting for etcd to become healthy..."
        if ! wait_for_etcd "$ip" 60; then
            log "ERROR: etcd did not become healthy on $node_name within 2 min"
            $SSH_CMD "ec2-user@$ip" "sudo journalctl -u etcd --no-pager -n 30" || true
            exit 1
        fi
        log "etcd healthy on $node_name"

        # Give etcd a moment to stabilise after joining
        sleep 3

        # Start EtcFS daemon
        if [[ -n "$DAEMON_BIN" && -f "$DAEMON_BIN" ]]; then
            log "Starting EtcFS FUSE daemon on $node_name..."
            $SSH_CMD "ec2-user@$ip" "
sudo mkdir -p $FUSE_MOUNTPOINT
sudo systemctl start etcfuse
"
            log "Waiting for daemon to be ready..."
            if ! wait_for_daemon_ready "$ip" 60 2; then
                log "ERROR: EtcFS daemon did not become ready on $node_name within 2 min"
                $SSH_CMD "ec2-user@$ip" "sudo journalctl -u etcfuse --no-pager -n 30" || true
                exit 1
            fi
            log "EtcFS FUSE mounted at $FUSE_MOUNTPOINT on $node_name"
            $SSH_CMD "ec2-user@$ip" "df -h $FUSE_MOUNTPOINT"
        else
            log "  (no daemon binary — skipping FUSE mount)"
        fi
    fi

    log "$node_name setup complete."
done

log ""
log "=== All compute nodes ready (etcd colocated) ==="
log "etcd endpoints: $ETCD_ENDPOINTS"
log "Mount point:    $FUSE_MOUNTPOINT"
log ""
log "Next steps:"
log "  1. Run validation:  ./scripts/infra/run-full-test.sh"
log "  2. Run stress:       ./scripts/infra/run-light-test.sh"
log "  3. Run chaos:        ./scripts/test/chaos-monkey.sh"
