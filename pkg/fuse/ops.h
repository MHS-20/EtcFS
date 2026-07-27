#ifndef ETCFS_FUSE_OPS_H
#define ETCFS_FUSE_OPS_H

/*
 * ops.h — FUSE low-level operation handler declarations.
 *
 * Each function is a libfuse callback.  The implementation serialises the
 * operation to a protobuf request, sends it to the Go metadata backend,
 * receives the response, and calls the appropriate fuse_reply_* function.
 */

#include "fuse.h"
#include <fuse3/fuse_lowlevel.h>

/* Initialise the op table. Returns a fully populated struct fuse_lowlevel_ops. */
struct fuse_lowlevel_ops *etcfs_fuse_ops(void);

/* IPC layer — serialises requests, deserialises responses. */
struct etcfs_ipc;
struct etcfs_ipc *etcfs_ipc_init(int fd);
void etcfs_ipc_destroy(struct etcfs_ipc *ipc);

/* Shutdown notification — the Go side tells us to stop. */
int etcfs_ipc_recv_shutdown(struct etcfs_ipc *ipc);

#endif /* ETCFS_FUSE_OPS_H */
