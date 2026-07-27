#!/bin/bash
# isolate-node.sh — simulate network partition for EtcFS testing.
#
# Uses iptables on the target node to DROP traffic to/from the etcd cluster,
# simulating a partition scenario where the node still has block device access
# but loses etcd connectivity. This is the exact scenario the self-fencing
# watchdog is designed to handle.
#
# Usage:
#   ./isolate-node.sh <target_ip> drop        # partition the node from etcd
#   ./isolate-node.sh <target_ip> restore     # restore connectivity
#
# [TEMPLATE] Adapted from QAttach isolate-node.sh. EtcFS replaces
#            cluster-agent checks with FUSE daemon checks.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../infra/state.sh"

TARGET="${1:?Usage: $0 <target_ip> drop|restore}"
ACTION="${2:?Usage: $0 <target_ip> drop|restore}"

mapfile -t PRIV_IPS < <(state_get compute_ips 2>/dev/null | jq -r '.[]' 2>/dev/null)

log() { echo "[$(date +%T)] $*"; }

case "$ACTION" in
    drop)
        log "=== Isolating $TARGET from etcd cluster ==="

        # Build list of etcd peer IPs to block
        ETCD_PEERS=""
        for ip in "${PRIV_IPS[@]}"; do
            [[ "$ip" == "$TARGET" ]] && continue
            [[ -n "$ETCD_PEERS" ]] && ETCD_PEERS+=","
            ETCD_PEERS+="$ip"
        done

        log "  Blocking traffic to/from etcd peers: $ETCD_PEERS"
        log "  TTL on etcd lease: ${LEASH_TTL:-10}s"
        log "  Self-fencing should trigger within 2x TTL = $((2 * ${LEASH_TTL:-10}))s"

        # Wait for SSH first
        wait_for_ssh "$TARGET" 10 || die "SSH not available on $TARGET"

        # Apply iptables DROP rules for etcd ports to all peers
        for peer_ip in "${PRIV_IPS[@]}"; do
            [[ "$peer_ip" == "$TARGET" ]] && continue
            $SSH_CMD "ec2-user@$TARGET" "
sudo iptables -A OUTPUT -p tcp -d $peer_ip --dport 2379 -j DROP
sudo iptables -A OUTPUT -p tcp -d $peer_ip --dport 2380 -j DROP
sudo iptables -A INPUT  -p tcp -s $peer_ip --dport 2379 -j DROP
sudo iptables -A INPUT  -p tcp -s $peer_ip --dport 2380 -j DROP
" 2>/dev/null
        done

        log "  iptables rules applied"
        log "  Node $TARGET is now isolated from etcd."
        log "  Monitor with: watch -n1 'ssh $TARGET \"sudo systemctl is-active etcfuse\"'"
        log "  Expect: self-fence within $((2 * ${LEASH_TTL:-10}))s"
        log "    - FUSE mount becomes read-only or EIO"
        log "    - etcfuse daemon logs 'self-fenced'"
        ;;

    restore)
        log "=== Restoring connectivity for $TARGET ==="

        wait_for_ssh "$TARGET" 10 || die "SSH not available on $TARGET"

        # Flush iptables rules for etcd ports
        $SSH_CMD "ec2-user@$TARGET" "
sudo iptables -D OUTPUT -p tcp --dport 2379 -j DROP 2>/dev/null || true
sudo iptables -D OUTPUT -p tcp --dport 2380 -j DROP 2>/dev/null || true
sudo iptables -D INPUT  -p tcp --dport 2379 -j DROP 2>/dev/null || true
sudo iptables -D INPUT  -p tcp --dport 2380 -j DROP 2>/dev/null || true
" 2>/dev/null

        log "  iptables rules removed"
        log "  Node $TARGET should reconnect to etcd."

        # Check if etcd is healthy again after restore
        sleep 5
        if $SSH_CMD "ec2-user@$TARGET" "sudo ETCDCTL_API=3 etcdctl \
            --endpoints=https://localhost:2379 \
            --cacert=/etc/etcfuse/ca.crt \
            --cert=/etc/etcfuse/client.crt \
            --key=/etc/etcfuse/client.key \
            endpoint health" 2>/dev/null | grep -q "is healthy"; then
            log "  etcd reconnected successfully"
        else
            log "  etcd may need restart: systemctl restart etcd"
        fi
        ;;

    *)
        die "Unknown action: $ACTION (use 'drop' or 'restore')"
        ;;
esac

log "Done."
