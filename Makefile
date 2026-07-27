# EtcFS — top-level build orchestration.
#
# Targets:
#   make all         build both binaries (etcfuse, etcfuse-meta)
#   make proto       generate protobuf code
#   make test        run unit tests (Go + C)
#   make lint        run linters (Go: golangci-lint, C: clang-format check, bash: shellcheck)
#   make fmt         auto-format all code
#   make clean       remove build artifacts
#   make dev         start docker-compose development environment
#   make check       lint + test (CI entry point)

.PHONY: all proto test lint fmt clean dev check

GO_MODULE  := github.com/anomalyco/etcfuse
GO_ENTRY   := ./cmd/etcfuse-meta
GO_OUT     := bin/etcfuse-meta
C_ENTRY    := cmd/etcfuse
C_OUT      := bin/etcfuse
PROTO_DIR  := proto
PROTO_FILE := $(PROTO_DIR)/ipc.proto
PROTO_OUT  := internal/ipc

all: proto $(GO_OUT) $(C_OUT)

# ---- Protobuf codegen ----

proto: $(PROTO_OUT)/ipc.pb.go

$(PROTO_OUT)/ipc.pb.go: $(PROTO_FILE)
	@mkdir -p $(PROTO_OUT)
	protoc \
		--go_out=$(PROTO_OUT) \
		--go_opt=paths=source_relative \
		--go-grpc_out=$(PROTO_OUT) \
		--go-grpc_opt=paths=source_relative \
		-I$(PROTO_DIR) \
		$(PROTO_FILE)

# ---- Go build ----

$(GO_OUT): $(shell find . -name '*.go' -not -path './vendor/*' -not -path './test/*')
	go build -o $(GO_OUT) $(GO_ENTRY)

# ---- C build ----

C_SRCS := $(shell find cmd/etcfuse pkg/fuse pkg/block pkg/wal -name '*.c')
C_HDRS := $(shell find cmd/etcfuse pkg/fuse pkg/block pkg/wal -name '*.h')
C_CFLAGS := -Wall -Wextra -Werror -std=c11 -D_GNU_SOURCE -O2 -g
C_LIBS   := -lfuse3 -lpthread

$(C_OUT): $(C_SRCS) $(C_HDRS)
	$(CC) $(C_CFLAGS) $(C_SRCS) -o $(C_OUT) $(C_LIBS)

# ---- Testing ----

test: test-go test-c

test-go:
	go test -race -count=1 ./...

test-c:
	# C tests: uses Unity test framework (test/c/)
	@echo "C tests will be added in subsequent phases"

test-integration:
	bash test/e2e/run.sh

# ---- Linting & formatting ----

lint: lint-go lint-c lint-sh

lint-go:
	golangci-lint run ./...

lint-c:
	clang-format --dry-run --Werror $(C_SRCS) $(C_HDRS)

lint-sh:
	shellcheck scripts/infra/*.sh scripts/test/*.sh

fmt: fmt-go fmt-c

fmt-go:
	goimports -w .

fmt-c:
	clang-format -i $(C_SRCS) $(C_HDRS)

# ---- Docker dev environment ----

dev:
	cd deploy/docker && docker compose up -d
	@echo "EtcFS dev environment started."
	@echo "  etcd endpoints: http://localhost:2379, http://localhost:2380, http://localhost:2381"
	@echo "  Logs: docker compose -f deploy/docker/docker-compose.yml logs -f"

dev-down:
	cd deploy/docker && docker compose down -v

# ---- Clean ----

clean:
	rm -rf bin/
	go clean -cache

# ---- CI check (lint + test) ----

check: lint test

# ---- Help ----

help:
	@echo "EtcFS build targets:"
	@echo "  make all              build everything"
	@echo "  make proto            generate protobuf code"
	@echo "  make test             run unit tests"
	@echo "  make lint             run all linters"
	@echo "  make fmt              auto-format code"
	@echo "  make dev              start dev environment"
	@echo "  make clean            remove artifacts"
	@echo "  make check            CI pipeline (lint + test)"
