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
    int block_fd;     /* raw block device FD, opened with O_DIRECT */
    int ipc_fd;       /* Unix socket fd to Go backend */
    int ipc_sync;     /* 1 = synchronous IPC mode (no worker thread) */
    int self_fenced;  /* set to 1 when lease expiry detected */
    uint64_t next_fh; /* next file handle to assign */
    void *notify_se;  /* fuse_session for notification thread */
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

/* Returns the calling thread's IPC socket to the metadata backend, connecting
 * on first use and reusing it for every later request from that thread.  A
 * connection is never shared between threads: the protocol is a bare sequence
 * of frames with no request identifiers, so two threads on one socket would
 * interleave their frames and read each other's replies.  Returns -1 if the
 * connection cannot be established, which the handlers surface as EIO. */
int etcfs_ipc_fd(void);

/* Closes and forgets the calling thread's IPC socket, so the next etcfs_ipc_fd
 * establishes a fresh one.  Called whenever an exchange fails: a broken stream
 * and a desynchronised one are indistinguishable from the client side, and both
 * make every later frame on that socket meaningless.  Without this a daemon
 * restart left the mount returning EIO forever on every thread that had already
 * connected. */
void etcfs_ipc_drop(void);

/* Whether the kernel negotiated FUSE_AUTO_INVAL_DATA, and so whether a cached
 * directory listing has anything bounding how long a lost invalidation can
 * leave it stale.  Valid only once the FUSE session has been initialised. */
int etcfs_dir_cache_allowed(void);

#endif /* ETCFS_FUSE_H */
