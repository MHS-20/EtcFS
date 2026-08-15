#!/bin/bash
# bench-arena-soak.sh — etcfs only. Churn (create, grow, delete) across every
# node while sampling allocatable space against live data and how many arenas
# each node holds. The question is whether the per-node arena model
# fragments: live bytes flat while allocatable space keeps falling, or arena
# count climbing with nothing to show for it, is the failure this looks for.
#
# ETCFS_SOAK_SECONDS defaults to 600 (ten minutes) so a normal run is cheap.
# A real soak is the same script with a bigger number —
# `ETCFS_SOAK_SECONDS=86400 ./bench-arena-soak.sh` — and costs a day of
# cluster time; ETCFS_SOAK_SAMPLE (default 30) is the sampling interval.
#
# Usage:
#   ./bench-arena-soak.sh
set -euo pipefail
export COMPARE_BACKEND="${COMPARE_BACKEND:-etcfs}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"
[[ "$COMPARE_BACKEND_BASE" == "etcfs" ]] || die "bench-arena-soak.sh is etcfs-only"

SECONDS_TOTAL="${ETCFS_SOAK_SECONDS:-600}"
SAMPLE="${ETCFS_SOAK_SAMPLE:-30}"

compare_begin
compare_mount

# Churn: each node repeatedly creates a file, grows it, and deletes an older
# one, so live data hovers around a constant while the allocator keeps being
# asked for and given back space — the pattern that fragments arenas if
# anything does.
for i in "${!BENCH_NODES[@]}"; do
    $SSH_CMD -n -f "ec2-user@${BENCH_NODES[$i]}" "sudo sh -c '
        rm -f /tmp/soak.stop
        n=0
        while [ ! -f /tmp/soak.stop ]; do
            dd if=/dev/zero of=$MOUNT_PATH/soak-$i-\$n.dat bs=1M count=64 oflag=direct >/dev/null 2>&1
            dd if=/dev/zero of=$MOUNT_PATH/soak-$i-\$n.dat bs=1M count=64 seek=64 oflag=direct >/dev/null 2>&1
            [ \$n -ge 8 ] && rm -f $MOUNT_PATH/soak-$i-\$((n - 8)).dat
            n=\$((n + 1))
        done' >/dev/null 2>&1"
done

SAMPLES="$RESULTS_DIR/arena-soak-samples.tsv"
: > "$SAMPLES"
deadline=$((SECONDS + SECONDS_TOTAL))
while [[ $SECONDS -lt $deadline ]]; do
    avail=$($SSH_CMD "ec2-user@$N0" "df -B1 --output=avail $MOUNT_PATH | tail -1" | tr -d ' ')
    live=$($SSH_CMD "ec2-user@$N0" "sudo du -sb $MOUNT_PATH | awk '{print \$1}'")
    arenas=0
    for priv in "${COMPARE_PRIV_IPS[@]}"; do
        v=$($SSH_CMD "ec2-user@$N0" \
            "curl -s --max-time 5 http://$priv:9090/metrics | awk '/^etcfuse_arenas_owned /{print \$2}'" || echo 0)
        arenas=$(awk -v a="$arenas" -v b="${v:-0}" 'BEGIN{printf "%.0f", a+b}')
    done
    printf '%s\t%s\t%s\t%s\n' "$(date +%s)" "$avail" "$live" "$arenas" >> "$SAMPLES"
    sleep "$SAMPLE"
done

for ip in "${BENCH_NODES[@]}"; do
    $SSH_CMD "ec2-user@$ip" "sudo touch /tmp/soak.stop" || true
done

# The headline is the drift: allocatable space lost per byte of live data
# still there at the end. 1.0 means the allocator gave back exactly what the
# churn stopped using; well above 1.0 is fragmentation.
read -r first_avail first_live < <(head -1 "$SAMPLES" | awk '{print $2, $3}')
read -r last_avail last_live last_arenas < <(tail -1 "$SAMPLES" | awk '{print $2, $3, $4}')
compare_headline arena-soak soak_seconds "$SECONDS_TOTAL" s
compare_headline arena-soak arenas_owned_end "$last_arenas" arenas
compare_headline arena-soak avail_lost_per_live_byte \
    "$(awk -v fa="$first_avail" -v la="$last_avail" -v fl="$first_live" -v ll="$last_live" \
        'BEGIN{ d = ll - fl; if (d <= 0) { print "0" } else { printf "%.3f", (fa - la) / d } }')" ratio
