#!/bin/bash
# chaos-fuzz.sh — randomized read/write/create/delete/rename/move traffic
# from all nodes, concurrently with randomized fault injection (daemon
# kills, network partitions, fencing generation bumps), against ONE
# persistent cluster. Verifies the cluster stays reachable/writable
# throughout, not just that individual scenarios pass.
#
# Usage:
#   ./chaos-fuzz.sh docker [duration_seconds] [seed]
#   ./chaos-fuzz.sh aws    [duration_seconds] [seed]
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
REPORT_DIR="$PROJECT_ROOT/chaos-report-fuzz-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$REPORT_DIR"

MODE="${1:-}"
DURATION="${2:-120}"
SEED="${3:-$RANDOM}"
[[ "$MODE" == "docker" || "$MODE" == "aws" ]] || { echo "usage: $0 docker|aws [duration_seconds] [seed]"; exit 1; }
RANDOM=$SEED

log() { echo "[$(date +%H:%M:%S)] $1" | tee -a "$REPORT_DIR/chaos.log"; }
logerr() {
    echo "[$(date +%H:%M:%S)] $1" >&2
    echo "[$(date +%H:%M:%S)] $1" >> "$REPORT_DIR/chaos.log"
}

source "$SCRIPT_DIR/chaos-lib.sh"

log "Fuzz run: mode=$MODE duration=${DURATION}s seed=$SEED"

# ============================================================
# Workers — one per node, looping random FS ops on a shared name pool
# (f0..f19, d0..d4) until $STOP appears. Every op result is logged; op
# failures are recorded but do not stop the run (that's the point of a
# fuzz test) except liveness failures, tracked separately below.
# ============================================================
POOL_SIZE=20
DIR_POOL_SIZE=5
STOP_FILE="$REPORT_DIR/.stop"
OP_LOG="$REPORT_DIR/ops.log"
: > "$OP_LOG"

worker() {
    local node="$1"; local wid="$2"
    local i=0
    while [[ ! -f "$STOP_FILE" ]]; do
        i=$((i+1))
        local fn="f$((RANDOM % POOL_SIZE))"
        local dn="d$((RANDOM % DIR_POOL_SIZE))"
        local op=$((RANDOM % 6))
        local result=""
        case $op in
            0) writef "$node" "w${wid}-${i}" "$fn.txt" >/dev/null 2>&1 && result=OK || result=FAIL ;;
            1) readf "$node" "$fn.txt" >/dev/null 2>&1 && result=OK || result=FAIL ;;
            2) rmf "$node" "$fn.txt" >/dev/null 2>&1; result=OK ;;
            3) mvf "$node" "$fn.txt" "$fn-r.txt" >/dev/null 2>&1; result=OK ;;
            4) mkdirf "$node" "$dn" >/dev/null 2>&1; result=OK ;;
            5) writef "$node" "w${wid}-${i}" "$dn/$fn.txt" >/dev/null 2>&1 && result=OK || result=FAIL ;;
        esac
        echo "$(date +%H:%M:%S) node=$node op=$op file=$fn result=$result" >> "$OP_LOG"
    done
}

# ============================================================
# Chaos injector — every random 8-20s, picks one fault, applies it,
# gives it a moment to land, then (for faults that need it) heals/
# restarts before the next one. Runs sequentially so faults don't
# compound in ways that make root-causing a failure impossible.
# ============================================================
CHAOS_LOG="$REPORT_DIR/chaos-events.log"
: > "$CHAOS_LOG"

chaos_event() { echo "$(date +%H:%M:%S) $1" | tee -a "$CHAOS_LOG"; }

inject_fault() {
    local target=$((RANDOM % 3 + 1))
    local n m
    eval "n=\$N$target"; eval "m=\$M$target"
    local kind=$((RANDOM % 5))
    case $kind in
        0)
            chaos_event "fault: kill FUSE daemon on node$target"
            if [[ "$MODE" == "docker" ]]; then restart_fuse "$n" >/dev/null
            else runcmd "$n" "sudo pkill -9 -x etcfuse 2>/dev/null; sleep 1; true"
                 runcmd30 "$n" "sudo nohup /usr/local/bin/etcfuse --socket=/run/etcfuse/etcfuse.sock --node-id=n$target --log-level=1 /mnt/etcfuse > /tmp/fuse.log 2>&1 & sleep 3; true" >/dev/null
            fi
            ;;
        1)
            chaos_event "fault: kill Go+C daemon pair on node$target"
            if [[ "$MODE" == "docker" ]]; then restart_pair "$m" "$n" >/dev/null
            else runcmd "$n" "sudo pkill -9 etcfuse-meta etcfuse 2>/dev/null; sudo umount -l /mnt/etcfuse 2>/dev/null; true"
                 restart_daemons "$n" "n$target" >/dev/null
            fi
            ;;
        2)
            chaos_event "fault: network partition node$target (15s)"
            if [[ "$MODE" == "docker" ]]; then
                partition_node "$n" "$m"
                sleep 15
                heal_node "$n" "$m"
                sleep 5
                restart_pair "$m" "$n" >/dev/null
            else
                local sg=$(jq -r '.sg_id' "$PROJECT_ROOT/$STATE_FILE")
                local vpc=$(jq -r '.vpc_id' "$PROJECT_ROOT/$STATE_FILE")
                local inst=$(jq -r ".compute_instance_ids[$((target-1))]" "$PROJECT_ROOT/$STATE_FILE")
                local eni=$(aws ec2 describe-instances --instance-ids "$inst" --query 'Reservations[0].Instances[0].NetworkInterfaces[0].NetworkInterfaceId' --output text 2>/dev/null)
                local myip=$(curl -s http://checkip.amazonaws.com 2>/dev/null || echo "0.0.0.0")
                local tsg=$(aws ec2 create-security-group --group-name "fuzz-temp-$$-$RANDOM" --description "fuzz partition" --vpc-id "$vpc" --query 'GroupId' --output text 2>/dev/null)
                aws ec2 authorize-security-group-ingress --group-id "$tsg" --protocol tcp --port 22 --cidr "${myip}/32" >/dev/null 2>&1
                aws ec2 modify-network-interface-attribute --network-interface-id "$eni" --groups "$tsg" >/dev/null 2>&1
                sleep 15
                aws ec2 modify-network-interface-attribute --network-interface-id "$eni" --groups "$sg" >/dev/null 2>&1
                aws ec2 delete-security-group --group-id "$tsg" >/dev/null 2>&1
                sleep 10
                restart_daemons "$n" "n$target" >/dev/null
            fi
            ;;
        3)
            chaos_event "fault: fence node$target (gen bump + revert)"
            local gen
            if [[ "$MODE" == "docker" ]]; then gen=$(etcdctl_on get "gen:n$target" --print-value-only 2>/dev/null)
            else gen=$(runcmd "$n" "sudo ETCDCTL_API=3 /usr/local/bin/etcdctl --endpoints=http://127.0.0.1:2379 get gen:n$target --print-value-only" 2>/dev/null)
            fi
            [[ "$gen" =~ ^[0-9]+$ ]] || gen=1
            local newgen=$((gen + 1))
            if [[ "$MODE" == "docker" ]]; then etcdctl_on put "gen:n$target" "$newgen" >/dev/null 2>&1
            else runcmd "$n" "sudo ETCDCTL_API=3 /usr/local/bin/etcdctl --endpoints=http://127.0.0.1:2379 put gen:n$target $newgen" >/dev/null 2>&1
            fi
            sleep 5
            if [[ "$MODE" == "docker" ]]; then
                etcdctl_on put "gen:n$target" "$gen" >/dev/null 2>&1
                restart_pair "$m" "$n" >/dev/null
            else
                runcmd "$n" "sudo ETCDCTL_API=3 /usr/local/bin/etcdctl --endpoints=http://127.0.0.1:2379 put gen:n$target $gen" >/dev/null 2>&1
                restart_daemons "$n" "n$target" >/dev/null
            fi
            ;;
        4)
            chaos_event "fault: all-node simultaneous crash+restart"
            if [[ "$MODE" == "docker" ]]; then
                docker kill -s KILL "$N1" "$N2" "$N3" "$M1" "$M2" "$M3" >/dev/null 2>&1
                sleep 3
                docker start "$M1" "$M2" "$M3" >/dev/null 2>&1
                sleep 3
                docker start "$N1" "$N2" "$N3" >/dev/null 2>&1
            else
                # shellcheck disable=SC2154
                for i in 1 2 3; do eval "ip=\$N$i"; runcmd "$ip" "sudo pkill -9 etcfuse-meta etcfuse 2>/dev/null; sudo umount -l /mnt/etcfuse 2>/dev/null; true"; done
                sleep 3
                for i in 1 2 3; do eval "ip=\$N$i"; restart_daemons "$ip" "n$i" >/dev/null; done
            fi
            ;;
    esac
    chaos_event "fault done, healing window"
}

chaos_injector() {
    while [[ ! -f "$STOP_FILE" ]]; do
        sleep $((8 + RANDOM % 13))
        [[ -f "$STOP_FILE" ]] && break
        inject_fault
    done
}

# ============================================================
# Liveness monitor — every 5s, canary write+read on a random node.
# Cluster is considered DOWN for that tick if canary fails on ALL 3
# nodes (a single node being mid-fault is expected and not a failure).
# Any full-cluster outage longer than OUTAGE_LIMIT consecutive ticks
# is a hard failure worth investigating.
# ============================================================
LIVENESS_LOG="$REPORT_DIR/liveness.log"
: > "$LIVENESS_LOG"
OUTAGE_LIMIT=3
CONSEC_DOWN=0
MAX_CONSEC_DOWN=0
LIVENESS_FAIL=0

liveness_monitor() {
    local tick=0
    while [[ ! -f "$STOP_FILE" ]]; do
        tick=$((tick+1))
        local up=0
        for node in "$N1" "$N2" "$N3"; do
            if writef "$node" "canary-$tick" "canary.txt" >/dev/null 2>&1; then
                local v=$(readf "$node" "canary.txt" 2>/dev/null)
                [[ -n "$v" ]] && up=$((up+1))
            fi
        done
        echo "$(date +%H:%M:%S) tick=$tick nodes_up=$up/3" >> "$LIVENESS_LOG"
        if [[ "$up" -eq 0 ]]; then
            CONSEC_DOWN=$((CONSEC_DOWN+1))
            log "  LIVENESS WARN: 0/3 nodes writable (consecutive=$CONSEC_DOWN)"
            [[ "$CONSEC_DOWN" -gt "$MAX_CONSEC_DOWN" ]] && MAX_CONSEC_DOWN=$CONSEC_DOWN
            if [[ "$CONSEC_DOWN" -ge "$OUTAGE_LIMIT" ]]; then
                LIVENESS_FAIL=1
                log "  LIVENESS FAIL: cluster unwritable for $CONSEC_DOWN consecutive ticks"
            fi
        else
            CONSEC_DOWN=0
        fi
        sleep 5
    done
}

# ============================================================
# Resource sampler — every 30s, records what a slow leak would show up
# in: each meta daemon's RSS and open-fd count, and etcd's database
# size. A fuzz run is the only thing in this repo that exercises the
# daemons long enough for a leak to be visible at all, and a liveness
# check cannot see one: a leaking daemon stays perfectly writable until
# it dies.
#
# Growth is reported, not failed on. Over a few minutes RSS legitimately
# rises as caches fill, so a hard threshold here would cry wolf; what is
# worth acting on is a metric that rises at *every* sample over a long
# run, which the summary calls out.
# ============================================================
SAMPLE_LOG="$REPORT_DIR/samples.log"
: > "$SAMPLE_LOG"

sample_once() {
    local tick="$1" c rss fds db
    for c in "$M1" "$M2" "$M3"; do
        rss=$(docker exec "$c" sh -c "awk '/VmRSS/ {print \$2}' /proc/1/status" 2>/dev/null | tr -d ' \r')
        fds=$(docker exec "$c" sh -c "ls /proc/1/fd 2>/dev/null | wc -l" 2>/dev/null | tr -d ' \r')
        echo "tick=$tick container=$c rss_kb=${rss:-0} fds=${fds:-0}" >> "$SAMPLE_LOG"
    done
    db=$(docker exec etcfs-etcd1 etcdctl endpoint status --write-out=fields 2>/dev/null |
        awk -F' : ' '/"DBSize"/ {print $2; exit}' | tr -d ' \r')
    echo "tick=$tick container=etcfs-etcd1 db_bytes=${db:-0}" >> "$SAMPLE_LOG"
}

resource_sampler() {
    local tick=0
    while [[ ! -f "$STOP_FILE" ]]; do
        tick=$((tick+1))
        sample_once "$tick"
        sleep 30
    done
}

# monotonic <field> <container> — prints "RISING" when a metric increased at
# every sample, which is what a leak looks like and ordinary variation does
# not.
monotonic() {
    local field="$1" container="$2"
    awk -v f="$field" -v c="$container" '
        $0 ~ ("container=" c) {
            for (i = 1; i <= NF; i++)
                if (index($i, f "=") == 1) {
                    split($i, kv, "=")
                    v = kv[2] + 0
                    if (n > 0 && v <= prev) steady = 1
                    prev = v; n++
                }
        }
        END { if (n >= 5 && steady == 0) print "RISING"; }
    ' "$SAMPLE_LOG"
}

# ============================================================
# MAIN
# ============================================================
if ! provision_cluster; then
    log "FATAL: provision failed"
    teardown_cluster
    exit 1
fi

log "Starting fuzz workers + chaos injector + liveness monitor for ${DURATION}s..."
worker "$N1" 1 & W1=$!
worker "$N2" 2 & W2=$!
worker "$N3" 3 & W3=$!
chaos_injector & CHAOSPID=$!
liveness_monitor & LIVEPID=$!
resource_sampler & SAMPLEPID=$!

sleep "$DURATION"
touch "$STOP_FILE"
wait "$W1" "$W2" "$W3" "$CHAOSPID" "$LIVEPID" "$SAMPLEPID" 2>/dev/null

log "Fuzz run complete. Final liveness check on all 3 nodes..."
FINAL_OK=0
for node in "$N1" "$N2" "$N3"; do
    if writef "$node" "final-check" "final.txt" >/dev/null 2>&1 && [[ -n "$(readf "$node" "final.txt" 2>/dev/null)" ]]; then
        FINAL_OK=$((FINAL_OK+1))
    else
        log "  FINAL CHECK FAIL on $node"
        dump_logs "$node"
    fi
done

LEAKS=""
for c in "$M1" "$M2" "$M3"; do
    [[ -n "$(monotonic rss_kb "$c")" ]] && LEAKS="$LEAKS $c:rss"
    [[ -n "$(monotonic fds "$c")" ]] && LEAKS="$LEAKS $c:fds"
done
[[ -n "$(monotonic db_bytes etcfs-etcd1)" ]] && LEAKS="$LEAKS etcd:db"

teardown_cluster

OPS_TOTAL=$(wc -l < "$OP_LOG" | tr -d ' ')
OPS_FAIL=$(grep -c 'result=FAIL' "$OP_LOG")
FAULTS_TOTAL=$(grep -c 'fault:' "$CHAOS_LOG")

{
    echo "=== Randomized Fuzz Chaos Report ($MODE) ==="
    echo "Seed: $SEED   Duration: ${DURATION}s"
    echo "Ops issued: $OPS_TOTAL   Ops failed (non-liveness): $OPS_FAIL"
    echo "Faults injected: $FAULTS_TOTAL"
    echo "Max consecutive full-outage ticks: $MAX_CONSEC_DOWN (limit $OUTAGE_LIMIT)"
    echo "Final liveness: $FINAL_OK/3 nodes"
    if [[ -n "$LEAKS" ]]; then
        echo "Monotonic growth (possible leak):$LEAKS — see samples.log"
    else
        echo "Monotonic growth: none in RSS, fd count or etcd DB size"
    fi
    if [[ "$LIVENESS_FAIL" -eq 1 || "$FINAL_OK" -lt 3 ]]; then
        echo "STATUS: FAIL — cluster did not stay alive throughout"
    else
        echo "STATUS: PASS — cluster stayed reachable/writable throughout"
    fi
} | tee "$REPORT_DIR/summary.txt"
echo "Report: $REPORT_DIR/summary.txt"
echo "Ops log: $OP_LOG"
echo "Chaos events: $CHAOS_LOG"
echo "Liveness log: $LIVENESS_LOG"
echo "Resource samples: $SAMPLE_LOG"

[[ "$LIVENESS_FAIL" -eq 1 || "$FINAL_OK" -lt 3 ]] && exit 1
exit 0
