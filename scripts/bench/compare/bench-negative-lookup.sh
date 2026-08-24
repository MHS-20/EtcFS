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
# Two passes over the same set of missing names: the first pays whatever a
# lookup costs, the second is answered from whatever the client cached. The
# ratio is the number.
#
# ETCFS_NEG_NAMES (default 2000) is how many distinct missing names to probe.
# ETCFS_NEG_ROUNDS (default 20) is how many times to sweep the set per pass.
#
# Usage:
#   COMPARE_BACKEND=etcfs ./bench-negative-lookup.sh
set -euo pipefail
export COMPARE_BACKEND="${COMPARE_BACKEND:-etcfs}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/compare-lib.sh"

NAMES="${ETCFS_NEG_NAMES:-2000}"
ROUNDS="${ETCFS_NEG_ROUNDS:-20}"

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
cold=$($SSH_CMD "ec2-user@$N0" "sudo python3 -c '$probe_py' $DIR $NAMES $ROUNDS")
warm=$($SSH_CMD "ec2-user@$N0" "sudo python3 -c '$probe_py' $DIR $NAMES $ROUNDS")

TOTAL=$((NAMES * ROUNDS))
compare_headline negative-lookup stat_s_cold "$cold" s
compare_headline negative-lookup stat_s_warm "$warm" s
compare_headline negative-lookup lookups_per_sec_cold "$(compare_div "$TOTAL" "$cold")" lookups/s
compare_headline negative-lookup lookups_per_sec_warm "$(compare_div "$TOTAL" "$warm")" lookups/s
compare_headline negative-lookup warm_speedup "$(compare_div "$cold" "$warm")" x
