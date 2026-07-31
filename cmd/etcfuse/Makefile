# Makefile for the EtcFS FUSE daemon (C component).
#
# Builds the etcfuse binary from source files in:
#   cmd/etcfuse/   (entry point)
#   pkg/fuse/      (FUSE protocol, op handlers, IPC)
#   pkg/block/     (block device I/O)
#
# Invoked from the top-level Makefile or directly:
#   make -C cmd/etcfuse

ROOT := $(CURDIR)/../..

SRCS := $(ROOT)/cmd/etcfuse/main.c \
        $(ROOT)/pkg/fuse/fuse.c \
        $(ROOT)/pkg/fuse/ops.c \
        $(ROOT)/pkg/fuse/pool.c \
        $(ROOT)/pkg/block/block.c

BINARY := $(ROOT)/bin/etcfuse
CFLAGS := -Wall -Wextra -Werror -std=c11 -D_GNU_SOURCE -O2 -g \
          -I$(ROOT)
LDLIBS := -lfuse3 -lpthread

.PHONY: all clean

all: $(BINARY)

$(BINARY): $(SRCS)
	@mkdir -p $(dir $(BINARY))
	$(CC) $(CFLAGS) -o $@ $(SRCS) $(LDLIBS)
	@echo "  C binary: $@ ($(shell ls -lh $(BINARY) | awk '{print $$5}'))"

clean:
	rm -rf $(ROOT)/bin
