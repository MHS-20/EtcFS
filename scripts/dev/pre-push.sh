#!/bin/bash
# pre-push.sh — run linters and tests before pushing.
#
# Install:
#   ln -sf ../../scripts/dev/pre-push.sh .git/hooks/pre-push
#
# Bypass (emergency only):
#   git push --no-verify

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

failures=0

run_check() {
    local name="$1"; shift
    printf "  %-30s " "$name"
    if "$@" &>/dev/null; then
        echo -e "${GREEN}OK${NC}"
    else
        echo -e "${RED}FAIL${NC}"
        ((failures++))
        "$@" 2>&1 | head -20
        echo ""
    fi
}

echo ""
echo "=== pre-push checks ==="

run_check "go build"  go build ./...
run_check "go test"   go test -race -count=1 ./...
run_check "go vet"    go vet ./...
run_check "golangci-lint" golangci-lint run --timeout=5m ./...
run_check "C build"   make -C cmd/etcfuse
run_check "clang-format" clang-format --dry-run --Werror \
    $(find cmd/etcfuse pkg/fuse pkg/block -name '*.c' -o -name '*.h')
run_check "shellcheck" shellcheck scripts/infra/*.sh scripts/test/*.sh

if [[ "$failures" -gt 0 ]]; then
    echo ""
    echo -e "${RED}${failures} check(s) failed. Push aborted.${NC}"
    echo "Fix with: make fmt && make check"
    exit 1
fi

echo ""
echo -e "${GREEN}All checks passed.${NC}"
