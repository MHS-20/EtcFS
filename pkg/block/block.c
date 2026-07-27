/*
 * block.c — O_DIRECT raw block device I/O.
 *
 * Opens the EBS Multi-Attach volume as a raw block device (no filesystem).
 * All I/O bypasses the kernel page cache via O_DIRECT.  Alignment is
 * enforced at the API level — mismatched offsets/lengths/buffers return
 * -EINVAL immediately.
 *
 * Future (Phase 6): replace synchronous pread/pwrite with io_uring batched
 * I/O for lower latency and higher concurrency.
 */

#include "block.h"
#include "../fuse/fuse.h"

#include <errno.h>
#include <fcntl.h>
#include <linux/fs.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <unistd.h>

struct etcfs_block_dev {
    int      fd;
    int      sector_size;
    size_t   total_sectors;
    char     path[256];
};

struct etcfs_block_dev *
etcfs_block_open(const char *volume_id)
{
    struct etcfs_block_dev *dev;
    unsigned long long bytes;
    int sector;

    if (!volume_id || !volume_id[0])
        return NULL;

    dev = calloc(1, sizeof(*dev));
    if (!dev) return NULL;

    dev->fd = -1;

    /* Try the volume_id as a direct device path first */
    if (volume_id[0] == '/') {
        snprintf(dev->path, sizeof(dev->path), "%s", volume_id);
        dev->fd = open(dev->path, O_RDWR | O_DIRECT);
        if (dev->fd < 0) {
            /* try read-only */
            dev->fd = open(dev->path, O_RDONLY | O_DIRECT);
        }
    } else {
        /* Volume ID (e.g. vol-0abc...) — probe common NVMe paths */
        const char *paths[] = {"/dev/nvme1n1", "/dev/sdf", "/dev/xvdf", NULL};
        for (int i = 0; paths[i]; i++) {
            dev->fd = open(paths[i], O_RDWR | O_DIRECT);
            if (dev->fd >= 0) {
                snprintf(dev->path, sizeof(dev->path), "%s", paths[i]);
                break;
            }
            dev->fd = open(paths[i], O_RDONLY | O_DIRECT);
            if (dev->fd >= 0) {
                snprintf(dev->path, sizeof(dev->path), "%s", paths[i]);
                break;
            }
        }
    }

    if (dev->fd < 0) {
        etcfs_log(ETCFS_LOG_WARN, "block_open: cannot open %s: %s",
                  volume_id, strerror(errno));
        free(dev);
        return NULL;
    }

    /* query geometry */
    if (ioctl(dev->fd, BLKSSZGET, &sector) < 0) {
        etcfs_log(ETCFS_LOG_WARN, "block_open: BLKSSZGET failed: %s",
                  strerror(errno));
        dev->sector_size = 512;  /* safe default */
    } else {
        dev->sector_size = sector;
    }

    if (ioctl(dev->fd, BLKGETSIZE64, &bytes) < 0) {
        etcfs_log(ETCFS_LOG_WARN, "block_open: BLKGETSIZE64 failed: %s",
                  strerror(errno));
        dev->total_sectors = 0;
    } else {
        dev->total_sectors = (size_t)(bytes / dev->sector_size);
    }

    etcfs_log(ETCFS_LOG_INFO,
              "block_open: %s sector=%d total_sectors=%zu (%.1f GB)",
              dev->path, dev->sector_size, dev->total_sectors,
              (double)bytes / 1e9);

    return dev;
}

void
etcfs_block_close(struct etcfs_block_dev *dev)
{
    if (dev) {
        if (dev->fd >= 0)
            close(dev->fd);
        free(dev);
    }
}

int
etcfs_block_get_sector_size(const struct etcfs_block_dev *dev)
{
    return dev ? dev->sector_size : 512;
}

size_t
etcfs_block_get_total_sectors(const struct etcfs_block_dev *dev)
{
    return dev ? dev->total_sectors : 0;
}

void *
etcfs_block_alloc_buffer(const struct etcfs_block_dev *dev, size_t size)
{
    int align = dev ? dev->sector_size : 4096;
    void *buf = NULL;
    if (posix_memalign(&buf, align, size) != 0)
        return NULL;
    return buf;
}

/* Validate alignment: offset, length, and buffer must be sector-aligned. */
static int
check_alignment(const struct etcfs_block_dev *dev, const void *buf,
                size_t count, uint64_t offset)
{
    int align = dev->sector_size;
    if (offset % (uint64_t)align != 0) return -EINVAL;
    if (count % (size_t)align != 0)      return -EINVAL;
    if (((uintptr_t)buf) % (uintptr_t)align != 0) return -EINVAL;
    return 0;
}

ssize_t
etcfs_block_read(struct etcfs_block_dev *dev, void *buf,
                 size_t count, uint64_t byte_offset)
{
    if (!dev || dev->fd < 0) return -EBADF;

    int ret = check_alignment(dev, buf, count, byte_offset);
    if (ret < 0) return ret;

    return pread(dev->fd, buf, count, (off_t)byte_offset);
}

ssize_t
etcfs_block_write(struct etcfs_block_dev *dev, const void *buf,
                  size_t count, uint64_t byte_offset)
{
    if (!dev || dev->fd < 0) return -EBADF;

    int ret = check_alignment(dev, buf, count, byte_offset);
    if (ret < 0) return ret;

    return pwrite(dev->fd, buf, count, (off_t)byte_offset);
}

int
etcfs_block_sync(struct etcfs_block_dev *dev, uint64_t byte_offset,
                 size_t count)
{
    if (!dev || dev->fd < 0) return -EBADF;

    off_t off_aligned = (off_t)(byte_offset & ~(off_t)(dev->sector_size - 1));
    size_t len_aligned = count + (size_t)(byte_offset - (uint64_t)off_aligned);

    return sync_file_range(dev->fd, off_aligned, len_aligned,
                           SYNC_FILE_RANGE_WRITE | SYNC_FILE_RANGE_WAIT_AFTER);
}
