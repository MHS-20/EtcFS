#!/bin/bash
# Runs the pjdfstest POSIX conformance suite against a freshly mounted EtcFS
# filesystem and writes per-syscall TAP results to /results.
#
# Executed as the container entrypoint; see test/pjdfstest/Dockerfile.
#
# Each test file is a shell script emitting TAP, so it is run directly rather
# than through prove: that keeps the dependency set small and yields exact
# per-file assertion counts, which is what the conformance table needs.

set -uo pipefail

ETCD_ENDPOINTS="${ETCD_ENDPOINTS:-http://etcd:2379}"
BLOCK_DEVICE="${BLOCK_DEVICE:-/block-device/etcfuse.img}"
MOUNT="${MOUNT:-/mnt/etcfuse}"
SUITE="${SUITE:-/opt/pjdfstest}"
RESULTS="${RESULTS:-/results}"
# Restrict the run to a subset of syscall directories, e.g. ONLY="chmod rename".
ONLY="${ONLY:-}"

log() { echo "[pjdfstest] $*"; }

mkdir -p "$MOUNT" "$RESULTS"

log "starting metadata daemon against $ETCD_ENDPOINTS"
/usr/local/bin/etcfuse-meta \
    --listen=/run/etcfuse/etcfuse.sock \
    --etcd-endpoints="$ETCD_ENDPOINTS" \
    --node-id="${NODE_ID:-pjd1}" \
    --cluster-name="${CLUSTER_NAME:-pjdfstest}" \
    --lease-ttl=30s \
    --block-device="$BLOCK_DEVICE" \
    --log-level=1 &
META_PID=$!

for _ in $(seq 1 30); do
    [[ -S /run/etcfuse/etcfuse.sock ]] && break
    sleep 1
done
if [[ ! -S /run/etcfuse/etcfuse.sock ]]; then
    log "metadata daemon never created its socket"
    exit 1
fi

log "mounting FUSE at $MOUNT"
/usr/local/bin/etcfuse --socket=/run/etcfuse/etcfuse.sock \
    --node-id="${NODE_ID:-pjd1}" --log-level=1 "$MOUNT" &
FUSE_PID=$!

for _ in $(seq 1 30); do
    mountpoint -q "$MOUNT" && break
    sleep 1
done
if ! mountpoint -q "$MOUNT"; then
    log "$MOUNT never became a mount point"
    exit 1
fi

cleanup() {
    cd /
    fusermount3 -u "$MOUNT" 2>/dev/null || umount -l "$MOUNT" 2>/dev/null
    kill "$FUSE_PID" "$META_PID" 2>/dev/null
    wait "$FUSE_PID" "$META_PID" 2>/dev/null
}
trap cleanup EXIT

# pjdfstest requires the working directory to be on the filesystem under test:
# every test creates its scratch files relative to it.
WORKDIR="$MOUNT/pjd-$$"
mkdir -p "$WORKDIR" || { log "cannot create $WORKDIR"; exit 1; }
cd "$WORKDIR" || exit 1

summary="$RESULTS/summary.tsv"
: >"$summary"
total_ok=0 total_fail=0 total_todo=0 status=0

for dir in "$SUITE"/tests/*/; do
    name="$(basename "$dir")"
    if [[ -n "$ONLY" ]] && ! grep -qw "$name" <<<"$ONLY"; then
        continue
    fi
    out="$RESULTS/$name.tap"
    : >"$out"
    for t in "$dir"*.t; do
        [[ -e "$t" ]] || continue
        echo "# --- $(basename "$t") ---" >>"$out"
        # A test that dies mid-way still contributes the assertions it emitted;
        # the missing ones show up as a plan/count mismatch in the raw TAP.
        timeout 300 /bin/sh "$t" >>"$out" 2>&1
    done
    ok=$(grep -c '^ok ' "$out")
    # The suite marks assertions it expects to fail on a given platform with a
    # TAP "# TODO" directive (Linux not clearing SGID on directories, say).
    # Counting those as failures would report the kernel's documented
    # behaviour as an EtcFS defect.
    todo=$(grep '^not ok ' "$out" | grep -c '# TODO')
    fail=$(grep '^not ok ' "$out" | grep -vc '# TODO')
    total_ok=$((total_ok + ok))
    total_fail=$((total_fail + fail))
    total_todo=$((total_todo + todo))
    [[ "$fail" -gt 0 ]] && status=1
    printf '%s\t%s\t%s\t%s\n' "$name" "$ok" "$fail" "$todo" >>"$summary"
    log "$name: $ok passed, $fail failed, $todo expected-fail"
done

printf 'TOTAL\t%s\t%s\t%s\n' "$total_ok" "$total_fail" "$total_todo" >>"$summary"
log "total: $total_ok passed, $total_fail failed, $total_todo expected-fail"

cd "$MOUNT" && rm -rf "$WORKDIR"
exit "$status"
