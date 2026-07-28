#ifndef ETCFS_FUSE_H
#define ETCFS_FUSE_H

#define FUSE_USE_VERSION FUSE_MAKE_VERSION(3, 10)

/*
 * fuse.h — EtcFS FUSE daemon types and helpers.
 *
 * Maps Linux kernel FUSE protocol structures (from include/uapi/linux/fuse.h)
 * to the EtcFS internal representation.  The daemon uses libfuse's low-level
 * API (fuse_lowlevel.h), which speaks raw FUSE messages, not the high-level
 * path-based API.
 *
 * This file does NOT replicate kernel structs — libfuse provides those.
 * It defines EtcFS's own wrapper types for the metadata IPC layer.
 */

#include <stdint.h>
#include <stdio.h>
#include <sys/types.h>

/* Maximum number of FUSE worker threads */
#define ETCFS_MAX_THREADS 16

/* Default cache timeouts (seconds) */
#define ETCFS_ENTRY_TIMEOUT    1.0
#define ETCFS_ATTR_TIMEOUT     1.0
#define ETCFS_NEGATIVE_TIMEOUT 0.0

/* EtcFS internal attribute structure.
 * Matches kernel struct fuse_attr in layout but uses host byte order. */
struct etcfs_attr {
    uint64_t ino;
    uint64_t size;
    uint64_t blocks;
    uint64_t atime;
    uint64_t mtime;
    uint64_t ctime;
    uint32_t atime_nsec;
    uint32_t mtime_nsec;
    uint32_t ctime_nsec;
    uint32_t mode;
    uint32_t nlink;
    uint32_t uid;
    uint32_t gid;
    uint32_t rdev;
    uint32_t blksize;
};

/* Setattr validity bitmask — which fields of struct etcfs_attr to apply. */
#define ETCFS_FATTR_MODE      (1 << 0)
#define ETCFS_FATTR_UID       (1 << 1)
#define ETCFS_FATTR_GID       (1 << 2)
#define ETCFS_FATTR_SIZE      (1 << 3)
#define ETCFS_FATTR_ATIME     (1 << 4)
#define ETCFS_FATTR_MTIME     (1 << 5)
#define ETCFS_FATTR_ATIME_NOW (1 << 7)
#define ETCFS_FATTR_MTIME_NOW (1 << 8)

/* A disk extent: maps a file logical range to a block device range. */
struct etcfs_extent {
    uint64_t logical_off;
    uint64_t disk_off;
    uint64_t length;
    uint64_t generation;
};

/* A directory entry. */
struct etcfs_dirent {
    uint64_t ino;
    char name[256];
    uint32_t type; /* DT_REG, DT_DIR, DT_LNK, etc. */
    uint64_t off;  /* offset cookie */
};

/* IPC worker (async, dedicated thread) */
struct ipc_worker;
struct etcfs_context {
    struct ipc_worker *ipc;
    int block_fd;    /* raw block device FD, opened with O_DIRECT */
    int ipc_fd;      /* Unix socket fd to Go backend */
    int ipc_sync;    /* 1 = synchronous IPC mode (no worker thread) */
    int self_fenced; /* set to 1 when lease expiry detected */
    char *mountpoint;
    char *volume_id;
    char *node_id;
};

/* Log levels */
#define ETCFS_LOG_ERROR 0
#define ETCFS_LOG_WARN  1
#define ETCFS_LOG_INFO  2
#define ETCFS_LOG_DEBUG 3

void etcfs_log(int level, const char *fmt, ...) __attribute__((format(printf, 2, 3)));

/* daemon lifecycle */
void etcfs_set_log_level(int level);
int etcfs_run(struct etcfs_context *ctx);

#endif /* ETCFS_FUSE_H */
