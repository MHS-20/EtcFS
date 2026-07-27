#ifndef ETCFS_BLOCK_H
#define ETCFS_BLOCK_H

/*
 * block.h — raw block device I/O for EtcFS.
 *
 * All reads and writes use O_DIRECT to bypass the kernel page cache.
 * The block device carries only raw file extents — no filesystem format.
 *
 * Alignment: all I/O buffers must be page-aligned (posix_memalign),
 * offsets must be sector-aligned (logical block size), and transfer
 * lengths must be sector-multiples.  Violations return -EINVAL.
 */

#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

/* Block device handle */
struct etcfs_block_dev;

/*
 * Open a block device by volume ID.
 * The volume ID is the EBS volume-id (e.g. "vol-0abcdef1234567890") or
 * a device path (e.g. "/dev/nvme1n1").  Returns NULL on failure.
 */
struct etcfs_block_dev *etcfs_block_open(const char *volume_id);

/* Close and free the block device handle. */
void etcfs_block_close(struct etcfs_block_dev *dev);

/* Get device geometry. */
int    etcfs_block_get_sector_size(const struct etcfs_block_dev *dev);
size_t etcfs_block_get_total_sectors(const struct etcfs_block_dev *dev);

/*
 * Allocate an aligned buffer suitable for O_DIRECT I/O on this device.
 * Caller must free() the returned pointer.
 * Returns NULL with *len = 0 on failure.
 */
void *etcfs_block_alloc_buffer(const struct etcfs_block_dev *dev, size_t size);

/*
 * Synchronous I/O (Phase 0–5).
 *
 * Phase 6+: these are replaced with io_uring batched submission.
 */

/* Read from a raw byte offset on the block device. */
ssize_t etcfs_block_read(struct etcfs_block_dev *dev, void *buf,
                         size_t count, uint64_t byte_offset);

/* Write to a raw byte offset on the block device. */
ssize_t etcfs_block_write(struct etcfs_block_dev *dev, const void *buf,
                          size_t count, uint64_t byte_offset);

/* Flush any in-flight I/O to stable storage. */
int etcfs_block_sync(struct etcfs_block_dev *dev, uint64_t byte_offset,
                     size_t count);

#endif /* ETCFS_BLOCK_H */
