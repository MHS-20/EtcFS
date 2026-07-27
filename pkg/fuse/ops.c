/*
 * ops.c — FUSE low-level operation handlers.
 *
 * Each handler:
 *   1. Extracts arguments from the fuse_req_t
 *   2. Serialises them into a protobuf request
 *   3. Sends the request to the Go metadata backend via Unix socket
 *   4. Deserialises the response
 *   5. Replies to the kernel
 *
 * The IPC protocol is length-prefixed protobuf over a stream socket:
 *   [4-byte big-endian length] [serialized protobuf message]
 *
 * During Phase 0, the protocol uses a simplified binary encoding rather
 * than full protobuf.  Full protobuf is wired in when the Go side is built.
 * For now, we provide the correct dispatch structure with stub encoders.
 */

#include "ops.h"
#include "fuse.h"

#include <arpa/inet.h>
#include <errno.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <fuse3/fuse_lowlevel.h>

/* ---- IPC wire format ----
 *
 * Request:  [u16: opcode] [u32: payload_len] [payload_bytes]
 * Response: [u32: payload_len] [payload_bytes]
 *
 * This is a temporary encoding.  Full protobuf replaces it in Phase 1.
 */

#define IPC_OP_LOOKUP     1
#define IPC_OP_GETATTR    2
#define IPC_OP_READDIR    3
#define IPC_OP_READLINK   4
#define IPC_OP_CREATE     5
#define IPC_OP_MKDIR      6
#define IPC_OP_UNLINK     7
#define IPC_OP_RMDIR      8
#define IPC_OP_RENAME     9
#define IPC_OP_SYMLINK    10
#define IPC_OP_LINK       11
#define IPC_OP_SETATTR    12
#define IPC_OP_OPEN       13
#define IPC_OP_RELEASE    14
#define IPC_OP_OPENDIR    15
#define IPC_OP_RELEASEDIR 16
#define IPC_OP_STATFS     17
#define IPC_OP_ALLOC      18
#define IPC_OP_COMMIT     19
#define IPC_OP_GETLK      20
#define IPC_OP_SETLK      21
#define IPC_OP_READ       22
#define IPC_OP_WRITE      23
#define IPC_OP_FSYNC      24
#define IPC_OP_MKNOD      25

#define IPC_RESP_SUCCESS 0

struct etcfs_ipc {
    int fd;
};

struct etcfs_ipc *etcfs_ipc_init(int fd)
{
    struct etcfs_ipc *ipc = calloc(1, sizeof(*ipc));
    if (!ipc)
        return NULL;
    ipc->fd = fd;
    return ipc;
}

void etcfs_ipc_destroy(struct etcfs_ipc *ipc)
{
    if (ipc) {
        close(ipc->fd);
        free(ipc);
    }
}

/* ---- FUSE operation handlers ---- */

static void ec_lookup(fuse_req_t req, fuse_ino_t parent, const char *name)
{
    if (parent == FUSE_ROOT_ID && strcmp(name, ".") == 0) {
        struct fuse_entry_param e;
        memset(&e, 0, sizeof(e));
        e.ino = FUSE_ROOT_ID;
        e.attr_timeout = ETCFS_ATTR_TIMEOUT;
        e.entry_timeout = ETCFS_ENTRY_TIMEOUT;
        e.attr.st_ino = FUSE_ROOT_ID;
        e.attr.st_mode = S_IFDIR | 0755;
        e.attr.st_nlink = 2;
        e.attr.st_size = 4096;
        e.attr.st_blksize = 4096;
        fuse_reply_entry(req, &e);
        return;
    }
    fuse_reply_err(req, ENOENT);
}

static void ec_getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    (void) fi;
    if (ino == FUSE_ROOT_ID || ino == 0) {
        struct stat st;
        memset(&st, 0, sizeof(st));
        st.st_ino = FUSE_ROOT_ID;
        st.st_mode = S_IFDIR | 0755;
        st.st_nlink = 2;
        st.st_uid = getuid();
        st.st_gid = getgid();
        st.st_size = 4096;
        st.st_blksize = 4096;
        fuse_reply_attr(req, &st, ETCFS_ATTR_TIMEOUT);
        return;
    }
    fuse_reply_err(req, ENOENT);
}

static void ec_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                       struct fuse_file_info *fi)
{
    (void) fi;
    if (ino != FUSE_ROOT_ID || off != 0) {
        fuse_reply_buf(req, NULL, 0);
        return;
    }

    char buf[512];
    size_t used = 0;
    struct stat st;
    memset(&st, 0, sizeof(st));
    st.st_ino = FUSE_ROOT_ID;
    st.st_mode = S_IFDIR | 0755;
    used += fuse_add_direntry(req, buf + used, size - used, ".", &st, 1);
    used += fuse_add_direntry(req, buf + used, size - used, "..", &st, 2);
    fuse_reply_buf(req, buf, used);
}

static void ec_readlink(fuse_req_t req, fuse_ino_t ino)
{
    (void) ino;
    fuse_reply_err(req, ENOENT);
}

static void ec_create(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode,
                      struct fuse_file_info *fi)
{
    (void) parent;
    (void) name;
    (void) mode;
    (void) fi;
    fuse_reply_err(req, EROFS);
}

static void ec_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode)
{
    (void) parent;
    (void) name;
    (void) mode;
    fuse_reply_err(req, EROFS);
}

static void ec_unlink(fuse_req_t req, fuse_ino_t parent, const char *name)
{
    (void) parent;
    (void) name;
    fuse_reply_err(req, EROFS);
}

static void ec_rmdir(fuse_req_t req, fuse_ino_t parent, const char *name)
{
    (void) parent;
    (void) name;
    fuse_reply_err(req, EROFS);
}

static void ec_rename(fuse_req_t req, fuse_ino_t parent, const char *name, fuse_ino_t newparent,
                      const char *newname, unsigned int flags)
{
    (void) parent;
    (void) name;
    (void) newparent;
    (void) newname;
    (void) flags;
    fuse_reply_err(req, EROFS);
}

static void ec_symlink(fuse_req_t req, const char *link, fuse_ino_t parent, const char *name)
{
    (void) link;
    (void) parent;
    (void) name;
    fuse_reply_err(req, EROFS);
}

static void ec_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent, const char *newname)
{
    (void) ino;
    (void) newparent;
    (void) newname;
    fuse_reply_err(req, EROFS);
}

static void ec_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr, int to_set,
                       struct fuse_file_info *fi)
{
    (void) ino;
    (void) attr;
    (void) to_set;
    (void) fi;
    fuse_reply_err(req, EROFS);
}

static void ec_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    (void) ino;
    fi->fh = 0;
    fi->direct_io = 1;
    fi->keep_cache = 0;
    fuse_reply_open(req, fi);
}

static void ec_release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    (void) ino;
    (void) fi;
    fuse_reply_err(req, 0);
}

static void ec_opendir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    (void) ino;
    fi->fh = 0;
    fuse_reply_open(req, fi);
}

static void ec_releasedir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    (void) ino;
    (void) fi;
    fuse_reply_err(req, 0);
}

static void ec_statfs(fuse_req_t req, fuse_ino_t ino)
{
    (void) ino;
    struct statvfs sv;
    memset(&sv, 0, sizeof(sv));
    sv.f_namemax = 255;
    sv.f_bsize = 4096;
    sv.f_frsize = 4096;
    fuse_reply_statfs(req, &sv);
}

static void ec_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                    struct fuse_file_info *fi)
{
    (void) ino;
    (void) size;
    (void) off;
    (void) fi;
    fuse_reply_buf(req, NULL, 0);
}

static void ec_write(fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size, off_t off,
                     struct fuse_file_info *fi)
{
    (void) ino;
    (void) buf;
    (void) size;
    (void) off;
    (void) fi;
    fuse_reply_err(req, EROFS);
}

static void ec_write_buf(fuse_req_t req, fuse_ino_t ino, struct fuse_bufvec *bufv, off_t off,
                         struct fuse_file_info *fi)
{
    (void) req;
    (void) ino;
    (void) bufv;
    (void) off;
    (void) fi;
    fuse_reply_err(req, EROFS);
}

static void ec_flush(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    (void) ino;
    (void) fi;
    fuse_reply_err(req, 0);
}

static void ec_fsync(fuse_req_t req, fuse_ino_t ino, int datasync, struct fuse_file_info *fi)
{
    (void) ino;
    (void) datasync;
    (void) fi;
    fuse_reply_err(req, 0);
}

static void ec_fsyncdir(fuse_req_t req, fuse_ino_t ino, int datasync, struct fuse_file_info *fi)
{
    (void) ino;
    (void) datasync;
    (void) fi;
    fuse_reply_err(req, 0);
}

static void ec_getlk(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi, struct flock *lock)
{
    (void) ino;
    (void) fi;
    struct flock lk = *lock;
    lk.l_type = F_UNLCK;
    fuse_reply_lock(req, &lk);
}

static void ec_setlk(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi, struct flock *lock,
                     int sleep)
{
    (void) ino;
    (void) fi;
    (void) lock;
    (void) sleep;
    fuse_reply_err(req, 0);
}

static void ec_mknod(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, dev_t rdev)
{
    (void) parent;
    (void) name;
    (void) mode;
    (void) rdev;
    fuse_reply_err(req, EROFS);
}

static void ec_fallocate(fuse_req_t req, fuse_ino_t ino, int mode, off_t offset, off_t length,
                         struct fuse_file_info *fi)
{
    (void) ino;
    (void) mode;
    (void) offset;
    (void) length;
    (void) fi;
    fuse_reply_err(req, EROFS);
}

/* ---- populate the op table ---- */

struct fuse_lowlevel_ops *etcfs_fuse_ops(void)
{
    static struct fuse_lowlevel_ops ops;
    memset(&ops, 0, sizeof(ops));

    ops.lookup = ec_lookup;
    ops.getattr = ec_getattr;
    ops.readdir = ec_readdir;
    ops.readlink = ec_readlink;
    ops.create = ec_create;
    ops.mkdir = ec_mkdir;
    ops.unlink = ec_unlink;
    ops.rmdir = ec_rmdir;
    ops.rename = ec_rename;
    ops.symlink = ec_symlink;
    ops.link = ec_link;
    ops.setattr = ec_setattr;
    ops.open = ec_open;
    ops.release = ec_release;
    ops.opendir = ec_opendir;
    ops.releasedir = ec_releasedir;
    ops.statfs = ec_statfs;
    ops.read = ec_read;
    ops.write = ec_write;
    ops.write_buf = ec_write_buf;
    ops.flush = ec_flush;
    ops.fsync = ec_fsync;
    ops.fsyncdir = ec_fsyncdir;
    ops.getlk = ec_getlk;
    ops.setlk = ec_setlk;
    ops.mknod = ec_mknod;
    ops.fallocate = ec_fallocate;

    return &ops;
}
