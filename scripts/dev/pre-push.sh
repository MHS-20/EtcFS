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
YELLOW='\033[0;33m'
NC='\033[0m'

failures=0

run_check() {
    local name="$1"; shift
    printf "  %-30s " "$name"
    if "$@" &>/dev/null; then
        echo -e "${GREEN}OK${NC}"
    else
        echo -e "${RED}FAIL${NC}"
        failures=$((failures + 1))
        "$@" 2>&1 | head -20 || true
        echo ""
    fi
}

# Same as run_check, but skips when the tool is not installed instead of
# failing.  The docs checks need mkdocs and lychee, which not every contributor
# has; CI runs them regardless, so a skip here is a missing early warning
# rather than a hole.
run_optional_check() {
    local name="$1" tool="$2"; shift 2
    if ! command -v "$tool" &>/dev/null; then
        printf "  %-30s " "$name"
        echo -e "${YELLOW}SKIP${NC} ($tool not installed)"
        return
    fi
    run_check "$name" "$@"
}

echo ""
echo "=== pre-push checks ==="

run_check "go build"  go build ./...
run_check "go test"   go test -race -count=1 ./...
run_check "go vet"    go vet ./...
# Tagged tests are invisible to the untagged build, so a signature change that
# breaks one compiles clean here and fails in CI.  Vetting with the tag is the
# cheapest way to compile them without needing the etcd a run would want.
run_check "go vet (integration)" go vet -tags=integration ./...
# The linter must be the version CI installs.  A different one is not a
# stricter or looser opinion, it is a different set of findings — and one built
# with an older Go refuses go.mod's target outright, which is a green hook and
# a red CI.
pinned_lint="$(cat .golangci-version)"
installed_lint="$(golangci-lint --version 2>/dev/null | grep -o 'v[0-9]\+\.[0-9]\+\.[0-9]\+' | head -1)"
if [[ "$installed_lint" != "$pinned_lint" ]]; then
    printf "  %-30s " "golangci-lint"
    echo -e "${RED}FAIL${NC}"
    failures=$((failures + 1))
    echo "  installed ${installed_lint:-none}, CI uses ${pinned_lint} (.golangci-version)"
    echo "  install it with:"
    echo "    go install github.com/golangci/golangci-lint/cmd/golangci-lint@${pinned_lint}"
    echo ""
else
    run_check "golangci-lint ${pinned_lint}" golangci-lint run --timeout=5m ./...
fi
run_check "C build"   make -C cmd/etcfuse
run_check "C test"    make test-c
mapfile -d '' c_files < <(find cmd/etcfuse pkg/fuse pkg/block test/c \( -name '*.c' -o -name '*.h' \) -print0)
run_check "clang-format" clang-format --dry-run --Werror "${c_files[@]}"
run_check "shellcheck" shellcheck scripts/infra/*.sh scripts/test/*.sh

# The docs site and the link check are what the Docs workflow runs, and both
# fail on things the Go and C checks above cannot see: a heading renamed out
# from under a table of contents, or a relative link that resolves from the
# repository root but not from inside the docs tree.
run_optional_check "mkdocs --strict" mkdocs \
    mkdocs build --strict --site-dir "$(mktemp -d)"
run_optional_check "link check" lychee \
    lychee --no-progress --include-fragments --exclude-path site \
    README.md AGENTS.md 'docs/**/*.md'

if [[ "$failures" -gt 0 ]]; then
    echo ""
    echo -e "${RED}${failures} check(s) failed. Push aborted.${NC}"
    echo "Fix with: make fmt && make check"
    exit 1
fi

echo ""
echo -e "${GREEN}All checks passed.${NC}"
