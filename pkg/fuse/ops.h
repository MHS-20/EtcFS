#ifndef ETCFS_FUSE_OPS_H
#define ETCFS_FUSE_OPS_H

#include "fuse.h"
#include "pool.h"
#include <fuse3/fuse_lowlevel.h>

/* Initialise the op table. Returns a fully populated struct fuse_lowlevel_ops. */
struct fuse_lowlevel_ops *etcfs_fuse_ops(void);

#endif /* ETCFS_FUSE_OPS_H */
