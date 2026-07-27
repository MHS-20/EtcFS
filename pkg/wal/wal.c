/*
 * wal.c — local write-ahead log implementation.
 *
 * WAL format (simple binary log):
 *   [magic: 4 bytes "ETWL"]
 *   [entry_count: 4 bytes]
 *   [entries...]
 *
 * Each entry:
 *   [flags: 1 byte]  (bit 0 = committed)
 *   [ino: 8 bytes]
 *   [logical_off: 8 bytes]
 *   [disk_off: 8 bytes]
 *   [length: 8 bytes]
 *   [generation: 8 bytes]
 *   [timestamp_ns: 8 bytes]
 *
 * The WAL is a single file, append-only during normal operation.
 * On restart it is read fully, reconciled, and truncated.
 */

#include "wal.h"
#include "../fuse/fuse.h"

#include <errno.h>
#include <fcntl.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define WAL_MAGIC          0x4c575445 /* "ETWL" little-endian */
#define WAL_ENTRY_SIZE     49         /* 1 + 6*8 */
#define WAL_FLAG_COMMITTED 0x01

struct etcfs_wal {
    int fd;
    char path[256];
    uint32_t entry_count;
};

static uint64_t wall_time_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t) ts.tv_sec * 1000000000ULL + (uint64_t) ts.tv_nsec;
}

struct etcfs_wal *etcfs_wal_open(const char *path)
{
    struct etcfs_wal *wal;
    int fd;

    if (!path || !path[0])
        return NULL;

    wal = calloc(1, sizeof(*wal));
    if (!wal)
        return NULL;

    snprintf(wal->path, sizeof(wal->path), "%s", path);

    fd = open(path, O_RDWR | O_CREAT | O_APPEND, 0644);
    if (fd < 0) {
        etcfs_log(ETCFS_LOG_WARN, "wal_open: %s: %s", path, strerror(errno));
        free(wal);
        return NULL;
    }
    wal->fd = fd;

    /* Read header to get entry count */
    lseek(fd, 0, SEEK_SET);
    uint32_t magic = 0, count = 0;
    if (read(fd, &magic, 4) == 4 && magic == WAL_MAGIC) {
        if (read(fd, &count, 4) == 4)
            wal->entry_count = count;
    } else {
        /* New file — write header */
        lseek(fd, 0, SEEK_SET);
        magic = WAL_MAGIC;
        count = 0;
        do {
            ssize_t _w = write(fd, &magic, 4);
            (void) _w;
        } while (0);
        do {
            ssize_t _w = write(fd, &count, 4);
            (void) _w;
        } while (0);
        (void) fsync(fd);
    }

    /* Seek to end for appends */
    lseek(fd, 0, SEEK_END);

    etcfs_log(ETCFS_LOG_INFO, "wal_open: %s entries=%u", path, wal->entry_count);
    return wal;
}

void etcfs_wal_close(struct etcfs_wal *wal)
{
    if (wal) {
        if (wal->fd >= 0) {
            /* update header with final count */
            uint32_t count = wal->entry_count;
            lseek(wal->fd, 4, SEEK_SET);
            do {
                ssize_t _w = write(wal->fd, &count, 4);
                (void) _w;
            } while (0);
            (void) fsync(wal->fd);
            close(wal->fd);
        }
        free(wal);
    }
}

static int wal_write_entry(struct etcfs_wal *wal, uint8_t flags, uint64_t ino, uint64_t log_off,
                           uint64_t disk_off, uint64_t len, uint64_t gen, uint64_t ts)
{
    uint8_t buf[WAL_ENTRY_SIZE];
    size_t pos = 0;

    buf[pos++] = flags;
    memcpy(buf + pos, &ino, 8);
    pos += 8;
    memcpy(buf + pos, &log_off, 8);
    pos += 8;
    memcpy(buf + pos, &disk_off, 8);
    pos += 8;
    memcpy(buf + pos, &len, 8);
    pos += 8;
    memcpy(buf + pos, &gen, 8);
    pos += 8;
    memcpy(buf + pos, &ts, 8);
    pos += 8;

    if (write(wal->fd, buf, sizeof(buf)) != sizeof(buf))
        return -1;
    wal->entry_count++;
    return 0;
}

int etcfs_wal_append(struct etcfs_wal *wal, const struct wal_entry *entry)
{
    if (!wal || wal->fd < 0)
        return -1;

    int ret = wal_write_entry(wal, entry->committed ? WAL_FLAG_COMMITTED : 0, entry->ino,
                              entry->logical_off, entry->disk_off, entry->length, entry->generation,
                              entry->timestamp_ns ? entry->timestamp_ns : wall_time_ns());
    if (ret == 0)
        fsync(wal->fd);
    return ret;
}

int etcfs_wal_mark_committed(struct etcfs_wal *wal, uint64_t ino, uint64_t logical_off)
{
    if (!wal || wal->fd < 0)
        return -1;

    /* Append a commit marker entry (flags |= COMMITTED, same ino+logical_off).
     * On replay, the latest entry with matching ino+logical_off wins. */
    return wal_write_entry(wal, WAL_FLAG_COMMITTED, ino, logical_off, 0, 0, 0, wall_time_ns());
}

int etcfs_wal_replay(struct etcfs_wal *wal, wal_replay_cb cb, void *userdata)
{
    if (!wal || wal->fd < 0)
        return -1;

    lseek(wal->fd, 8, SEEK_SET); /* skip header */

    uint8_t buf[WAL_ENTRY_SIZE];
    struct wal_entry entry;
    ssize_t n;

    etcfs_log(ETCFS_LOG_INFO, "wal_replay: scanning %u entries", wal->entry_count);

    for (uint32_t i = 0; i < wal->entry_count; i++) {
        n = read(wal->fd, buf, sizeof(buf));
        if (n != sizeof(buf)) {
            etcfs_log(ETCFS_LOG_WARN, "wal_replay: short read at entry %u", i);
            break;
        }

        uint8_t flags = buf[0];
        memset(&entry, 0, sizeof(entry));
        memcpy(&entry.ino, buf + 1, 8);
        memcpy(&entry.logical_off, buf + 9, 8);
        memcpy(&entry.disk_off, buf + 17, 8);
        memcpy(&entry.length, buf + 25, 8);
        memcpy(&entry.generation, buf + 33, 8);
        memcpy(&entry.timestamp_ns, buf + 41, 8);
        entry.committed = (flags & WAL_FLAG_COMMITTED) ? 1 : 0;

        if (!entry.committed && cb)
            cb(&entry, userdata);
    }

    return 0;
}

int etcfs_wal_truncate_before(struct etcfs_wal *wal, uint64_t timestamp_ns)
{
    (void) wal;
    (void) timestamp_ns;
    /* Phase 0: no truncation needed — WAL is empty.
     * Phase 6+: read all entries, keep only those newer than timestamp_ns,
     * rewrite the file. */
    return 0;
}
