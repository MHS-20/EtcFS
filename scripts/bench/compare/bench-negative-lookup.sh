#!/bin/bash
# bench-negative-lookup.sh — repeated stat() of names that do not exist.
#
# The pattern a compiler walking an include path generates, or a package
# manager probing for an optional config, or a build system checking whether a
# target needs rebuilding: thousands of lookups a second for files that are
# not there. Nothing else in this suite produces it — the rest is block I/O
# plus one directory walk, and a walk only ever asks about names that exist.
#
# It is worth its own scenario because a missing name is the one lookup a
# cluster filesystem can get badly wrong. Answering it costs a full round trip
# to the metadata store unless the kernel is allowed to remember the absence,
# and "remember that something is absent" is exactly the claim that goes stale
# when another node creates it.
#
# The cold pass sweeps the set exactly ONCE, with the caches dropped, so every
# lookup is a real one. The warm pass then sweeps the same names repeatedly,
# immediately afterwards, so they are answered from whatever the client cached.
#
# Sweeping more than once in the cold pass is what an earlier version of this
# did, and it measured almost nothing: only the first round is cold, so a
# 200-round "cold" pass is 199 rounds of warm hidden inside it and the ratio
# collapses towards 1. The two passes have to differ in *sweeps*, not in name.
#
# The set must be small enough that a whole sweep finishes inside the client's
# entry timeout, and that bound is the reason for the default. A cached absence
# on etcfs lives for one second; a cold lookup costs ~1.9 ms, so a sweep of more
# than ~500 names cannot come back round to its first name before that name has
# expired, and every sweep re-misses. Measured directly: 2000 names give 1.54x,
# 200 names give ~229x, on the same cluster and the same code. The small number
# is not the cache failing — it is the working set outrunning the timeout, which
# is a real limit worth knowing and not the thing this scenario is measuring.
#
# ETCFS_NEG_NAMES (default 200) is how many distinct missing names to probe.
#   Raise it past ~500 to observe the timeout bound instead of the cache.
# ETCFS_NEG_ROUNDS (default 200) is how many warm sweeps to average over.
#
# Usage:
#   COMPARE_BACKEND=etcfs ./bench-negative-lookup.sh
set -euo pipefail
export COMPARE_BACKEND="${COMPARE_BACKEND:-etcfs}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"

NAMES="${ETCFS_NEG_NAMES:-200}"
ROUNDS="${ETCFS_NEG_ROUNDS:-200}"

compare_begin
compare_mount

# python3 rather than a shell loop: a `test -e` per name would be measuring
# fork() far more than lookup, and every AL2023/AL2 image in this suite has it.
# Names are probed in a fixed order so both passes ask the same questions.
probe_py='
import os, sys, time
d, names, rounds = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
paths = [os.path.join(d, "missing-%06d" % i) for i in range(names)]
start = time.time()
for _ in range(rounds):
    for p in paths:
        try:
            os.stat(p)
        except FileNotFoundError:
            pass
print("%.4f" % (time.time() - start))
'

DIR="$MOUNT_PATH/negative"
$SSH_CMD "ec2-user@$N0" "sudo mkdir -p $DIR"

# A directory that is not empty, so the lookups miss among real entries rather
# than in a directory the backend might shortcut as empty.
$SSH_CMD "ec2-user@$N0" "sudo sh -c 'for i in \$(seq 1 50); do : > $DIR/present-\$i; done'"

compare_drop_caches "$N0"
cold=$($SSH_CMD "ec2-user@$N0" "sudo python3 -c '$probe_py' $DIR $NAMES 1")
warm=$($SSH_CMD "ec2-user@$N0" "sudo python3 -c '$probe_py' $DIR $NAMES $ROUNDS")

# Per-lookup, so the two passes are comparable despite sweeping different
# numbers of times.
cold_us=$(compare_div "$(compare_div "$cold" "$NAMES" 9)" 0.000001 2)
warm_us=$(compare_div "$(compare_div "$warm" "$((NAMES * ROUNDS))" 9)" 0.000001 2)

compare_headline negative-lookup us_per_lookup_cold "$cold_us" us
compare_headline negative-lookup us_per_lookup_warm "$warm_us" us
compare_headline negative-lookup lookups_per_sec_cold "$(compare_div "$NAMES" "$cold")" lookups/s
compare_headline negative-lookup lookups_per_sec_warm "$(compare_div "$((NAMES * ROUNDS))" "$warm")" lookups/s
compare_headline negative-lookup warm_speedup "$(compare_div "$cold_us" "$warm_us")" x
