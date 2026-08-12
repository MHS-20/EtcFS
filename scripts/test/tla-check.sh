#!/bin/bash
# Runs TLC over every model configuration in specs/ and asserts the outcome
# each one is supposed to have.
#
# Half the configurations are supposed to FAIL. A spec that proves everything
# proves nothing, so the deliberately broken variants are checked to still
# produce a counterexample — the same reasoning as the negative controls in
# test/verify (see docs/verification/porcupine.md).
#
# Usage:
#   scripts/test/tla-check.sh            # the 2-node models, ~10s total
#   DEEP=1 scripts/test/tla-check.sh     # adds the 3-node model, minutes and hot
#   TLA_WORKERS=4 scripts/test/tla-check.sh
#
# TLC is brute-force state enumeration and will use every core it is given.
# The default is deliberately 2 workers and nice'd: the 2-node models settle
# in seconds either way, and the difference on a laptop is a warm fan rather
# than a hot one.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SPECS="$PROJECT_ROOT/specs"
TOOLS="$SPECS/.tools"
JAR="$TOOLS/tla2tools.jar"
WORKERS="${TLA_WORKERS:-2}"

if [[ ! -f "$JAR" ]]; then
    echo "[tla] fetching tla2tools.jar"
    mkdir -p "$TOOLS"
    curl -sSL -o "$JAR" \
        https://github.com/tlaplus/tlaplus/releases/latest/download/tla2tools.jar || {
        echo "[tla] could not download tla2tools.jar" >&2
        exit 1
    }
fi

# config:expected — "pass" means TLC must find no error, "fail" means it must
# produce a counterexample, and the named invariant is the one it must break.
CHECKS=(
    "Fencing:pass:"
    "FencingNoFencer:pass:"
    "FencingUnreliableFencer:pass:"
    "FencingGuardIsBackstop:pass:"
    "FencingNoIncarnationCheck:fail:NoHealthyNodeSevered"
    "FencingNoGuard:fail:StaleWriteRejected"
    "FencingArenaBug:fail:ReleasedArenaHasNoLiveWriter"
)
[[ "${DEEP:-0}" == "1" ]] && CHECKS+=("Fencing3Nodes:pass:")

cd "$SPECS" || exit 1
fails=0

for check in "${CHECKS[@]}"; do
    cfg="${check%%:*}"
    rest="${check#*:}"
    expected="${rest%%:*}"
    wanted="${rest#*:}"

    printf '%-26s ' "$cfg"
    out=$(nice -n 10 java -XX:+UseParallelGC -cp "$JAR" tlc2.TLC \
            -workers "$WORKERS" -config "$cfg.cfg" Fencing.tla 2>&1)

    states=$(echo "$out" | grep -oE '[0-9]+ distinct states found' | tail -1 | grep -oE '^[0-9]+')
    broke=$(echo "$out" | grep -oE 'Invariant [A-Za-z]+ is violated' | head -1 | awk '{print $2}')

    if echo "$out" | grep -q "No error has been found"; then
        if [[ "$expected" == "pass" ]]; then
            echo "OK    no counterexample, ${states:-?} states"
        else
            echo "WRONG expected a counterexample on $wanted, found none"
            fails=$((fails + 1))
        fi
    elif [[ -n "$broke" ]]; then
        if [[ "$expected" == "fail" && "$broke" == "$wanted" ]]; then
            echo "OK    broke $broke as expected"
        elif [[ "$expected" == "fail" ]]; then
            echo "WRONG expected $wanted to break, but $broke broke first"
            fails=$((fails + 1))
        else
            echo "WRONG unexpected violation of $broke"
            fails=$((fails + 1))
        fi
    else
        echo "ERROR TLC did not run:"
        echo "$out" | tail -5
        fails=$((fails + 1))
    fi
done

echo
if [[ "$fails" -eq 0 ]]; then
    echo "[tla] all models behaved as specified"
else
    echo "[tla] $fails model(s) did not behave as specified" >&2
fi
exit "$fails"
