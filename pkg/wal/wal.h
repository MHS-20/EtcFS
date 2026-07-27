#ifndef ETCFS_WAL_H
#define ETCFS_WAL_H

/*
 * wal.h — local write-ahead log for EtcFS.
 *
 * Records extent writes that have been issued to the block device but not
 * yet committed to etcd.  This covers the window between the data write
 * and the metadata commit (data-then-metadata ordering invariant).
 *
 * On restart, the daemon replays the WAL to reconcile in-flight operations
 * against etcd state: committed extents are kept, uncommitted extents are
 * discarded and their blocks returned to the arena free-list.
 *
 * The WAL is intentionally small and short-lived — entries live only as
 * long as the data-write-to-metadata-commit gap (typically <100ms).
 */

#include <stdint.h>
#include <sys/types.h>

/* A single WAL entry: an extent that was written to disk. */
struct wal_entry {
    uint64_t ino;          /* owning inode */
    uint64_t logical_off;   /* file offset */
    uint64_t disk_off;      /* block device offset */
    uint64_t length;        /* extent length */
    uint64_t generation;    /* fencing generation at write time */
    uint64_t timestamp_ns;  /* wall-clock time of write */
    uint64_t committed;     /* 0 = uncommitted, 1 = etcd confirmed */
};

/* WAL handle (opaque). */
struct etcfs_wal;

/* Open/create the WAL at the given path. */
struct etcfs_wal *etcfs_wal_open(const char *path);

/* Close and free the WAL. */
void etcfs_wal_close(struct etcfs_wal *wal);

/* Append an entry (data written to disk, metadata not yet committed). */
int etcfs_wal_append(struct etcfs_wal *wal, const struct wal_entry *entry);

/* Mark an entry as committed (etcd confirmed the extent list update). */
int etcfs_wal_mark_committed(struct etcfs_wal *wal, uint64_t ino,
                             uint64_t logical_off);

/*
 * Replay the WAL on restart.
 * For each entry:
 *   - If committed=1: verify the inode's extent list in etcd includes it.
 *   - If committed=0: the extent was written to disk but never committed.
 *     Return these entries so the caller can discard them (free the blocks).
 *
 * Caller provides a callback invoked for each uncommitted entry.
 */
typedef void (*wal_replay_cb)(const struct wal_entry *entry, void *userdata);
int etcfs_wal_replay(struct etcfs_wal *wal, wal_replay_cb cb, void *userdata);

/* Truncate the WAL (remove entries older than a given timestamp). */
int etcfs_wal_truncate_before(struct etcfs_wal *wal, uint64_t timestamp_ns);

#endif /* ETCFS_WAL_H */
