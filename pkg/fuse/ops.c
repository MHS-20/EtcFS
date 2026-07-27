/*
 * ops.c — FUSE low-level operation handlers.
 *
 * Phase 2: read-only ops (LOOKUP, GETATTR, READDIR, READLINK, STATFS)
 * are wired to the Go metadata backend via a Unix socket.  Write ops
 * return EROFS.  Lock ops return success (no locks).
 *
 * IPC wire format (C ↔ Go):
 *   Request:  [u16:be opcode] [u32:be payload_len] [payload]
 *   Response: [u32:be payload_len] [payload]
 */

#include "ops.h"
#include "fuse.h"

#include <arpa/inet.h>
#include <errno.h>
#include <pthread.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <fuse3/fuse_lowlevel.h>

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

struct etcfs_ipc {
    int              fd;
    pthread_mutex_t  mu;
};

struct etcfs_ipc *etcfs_ipc_init(int fd)
{
    struct etcfs_ipc *ipc = calloc(1, sizeof(*ipc));
    if (!ipc) return NULL;
    ipc->fd = fd;
    pthread_mutex_init(&ipc->mu, NULL);
    return ipc;
}

void etcfs_ipc_destroy(struct etcfs_ipc *ipc)
{
    if (ipc) {
        pthread_mutex_destroy(&ipc->mu);
        close(ipc->fd);
        free(ipc);
    }
}

/* ---- raw socket I/O ---- */

static int send_full(int fd, const void *buf, size_t len)
{
    const char *p = buf;
    while (len > 0) {
        ssize_t n = write(fd, p, len);
        if (n < 0) { if (errno == EINTR) continue; return -1; }
        len -= (size_t)n; p += n;
    }
    return 0;
}

static int recv_full(int fd, void *buf, size_t len)
{
    char *p = buf;
    while (len > 0) {
        ssize_t n = read(fd, p, len);
        if (n <= 0) { if (n == 0) return -1; if (errno == EINTR) continue; return -1; }
        len -= (size_t)n; p += n;
    }
    return 0;
}

/* ---- IPC call: lock, send request, recv response, unlock ---- */

static int ipc_call(struct etcfs_ipc *ipc, uint16_t op,
                    const void *req_payload, uint32_t req_len,
                    uint8_t **resp_payload, uint32_t *resp_len)
{
    uint8_t hdr[6];
    hdr[0] = (uint8_t)(op >> 8);
    hdr[1] = (uint8_t) op;
    hdr[2] = (uint8_t)(req_len >> 24);
    hdr[3] = (uint8_t)(req_len >> 16);
    hdr[4] = (uint8_t)(req_len >> 8);
    hdr[5] = (uint8_t) req_len;

    pthread_mutex_lock(&ipc->mu);

    if (send_full(ipc->fd, hdr, 6) < 0) goto fail;
    if (req_len > 0 && send_full(ipc->fd, req_payload, req_len) < 0) goto fail;

    /* recv response header */
    uint8_t rhdr[4];
    if (recv_full(ipc->fd, rhdr, 4) < 0) goto fail;
    uint32_t rlen = ((uint32_t)rhdr[0] << 24) | ((uint32_t)rhdr[1] << 16) |
                    ((uint32_t)rhdr[2] << 8)  |  (uint32_t)rhdr[3];

    uint8_t *rbuf = malloc(rlen > 0 ? rlen : 1);
    if (!rbuf) goto fail;
    if (rlen > 0 && recv_full(ipc->fd, rbuf, rlen) < 0) { free(rbuf); goto fail; }

    pthread_mutex_unlock(&ipc->mu);
    *resp_payload = rbuf;
    *resp_len    = rlen;
    return 0;

fail:
    pthread_mutex_unlock(&ipc->mu);
    return -1;
}

/* ---- binary response readers (on flat buffers) ---- */

static uint64_t rb_u64(const uint8_t *p, uint32_t *off) {
    uint64_t v = ((uint64_t)p[*off] << 56) | ((uint64_t)p[*off+1] << 48) |
                 ((uint64_t)p[*off+2] << 40) | ((uint64_t)p[*off+3] << 32) |
                 ((uint64_t)p[*off+4] << 24) | ((uint64_t)p[*off+5] << 16) |
                 ((uint64_t)p[*off+6] << 8)  |  (uint64_t)p[*off+7];
    *off += 8; return v;
}

static uint32_t rb_u32(const uint8_t *p, uint32_t *off) {
    uint32_t v = ((uint32_t)p[*off] << 24) | ((uint32_t)p[*off+1] << 16) |
                 ((uint32_t)p[*off+2] << 8)  |  (uint32_t)p[*off+3];
    *off += 4; return v;
}

static int32_t rb_i32(const uint8_t *p, uint32_t *off) {
    return (int32_t)rb_u32(p, off);
}

static void rb_attr(const uint8_t *p, uint32_t *off, struct etcfs_attr *a)
{
    a->ino        = rb_u64(p, off);
    a->size       = rb_u64(p, off);
    a->blocks     = rb_u64(p, off);
    a->atime      = rb_u64(p, off);
    a->mtime      = rb_u64(p, off);
    a->ctime      = rb_u64(p, off);
    a->atime_nsec = rb_u32(p, off);
    a->mtime_nsec = rb_u32(p, off);
    a->ctime_nsec = rb_u32(p, off);
    a->mode       = rb_u32(p, off);
    a->nlink      = rb_u32(p, off);
    a->uid        = rb_u32(p, off);
    a->gid        = rb_u32(p, off);
    a->rdev       = rb_u32(p, off);
    a->blksize    = rb_u32(p, off);
}

/* ---- helpers: etcfs_attr → struct stat, request builders ---- */

static void fill_stat(struct stat *st, const struct etcfs_attr *a)
{
    memset(st, 0, sizeof(*st));
    st->st_ino     = a->ino;
    st->st_mode    = a->mode;
    st->st_nlink   = a->nlink;
    st->st_uid     = a->uid;
    st->st_gid     = a->gid;
    st->st_size    = (off_t)a->size;
    st->st_blksize = a->blksize;
    st->st_blocks  = (blkcnt_t)a->blocks;
    st->st_rdev    = a->rdev;
    st->st_atime   = (time_t)a->atime;
    st->st_mtime   = (time_t)a->mtime;
    st->st_ctime   = (time_t)a->ctime;
}

static uint32_t wb_u64(uint8_t *buf, uint64_t v)
{
    buf[0] = (uint8_t)(v >> 56); buf[1] = (uint8_t)(v >> 48);
    buf[2] = (uint8_t)(v >> 40); buf[3] = (uint8_t)(v >> 32);
    buf[4] = (uint8_t)(v >> 24); buf[5] = (uint8_t)(v >> 16);
    buf[6] = (uint8_t)(v >> 8);  buf[7] = (uint8_t)v;
    return 8;
}

static uint32_t wb_u32(uint8_t *buf, uint32_t v)
{
    buf[0] = (uint8_t)(v >> 24); buf[1] = (uint8_t)(v >> 16);
    buf[2] = (uint8_t)(v >> 8);  buf[3] = (uint8_t)v;
    return 4;
}

/* ---- FUSE operation handlers ---- */

#define GET_CTX(req) ((struct etcfs_context *)fuse_req_userdata(req))

static void ec_lookup(fuse_req_t req, fuse_ino_t parent, const char *name)
{
    struct etcfs_context *ctx = GET_CTX(req);

    /* Root directory or self-references: handle locally */
    if (parent == FUSE_ROOT_ID && (strcmp(name, ".") == 0 || strcmp(name, "..") == 0 || strcmp(name, "/") == 0)) {
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

    /* build request: u64 parent, u32 name_len, name bytes */
    uint8_t req_payload[12 + 256];
    uint32_t off = 0;
    off += wb_u64(req_payload + off, parent);
    off += wb_u32(req_payload + off, (uint32_t)nlen);
    memcpy(req_payload + off, name, nlen); off += (uint32_t)nlen;

    uint8_t *resp; uint32_t rlen;
    if (ipc_call(ctx->ipc, IPC_OP_LOOKUP, req_payload, off, &resp, &rlen) < 0) {
        fuse_reply_err(req, EIO); return;
    }
    if (rlen < 4) { free(resp); fuse_reply_err(req, EIO); return; }

    uint32_t pos = 0;
    int32_t err = rb_i32(resp, &pos);
    if (err != 0) { free(resp); fuse_reply_err(req, -err); return; }

    struct fuse_entry_param e;
    memset(&e, 0, sizeof(e));
    e.ino = rb_u64(resp, &pos);
    struct etcfs_attr a;
    rb_attr(resp, &pos, &a);
    e.attr_timeout  = (double)rb_u32(resp, &pos);
    e.entry_timeout = (double)rb_u32(resp, &pos);
    fill_stat(&e.attr, &a);
    free(resp);

    fuse_reply_entry(req, &e);
}

static void ec_getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    (void)fi;

    /* Root inode: return locally */
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

    struct etcfs_context *ctx = GET_CTX(req);

    uint8_t req_payload[8];
    wb_u64(req_payload, ino);

    uint8_t *resp; uint32_t rlen;
    if (ipc_call(ctx->ipc, IPC_OP_GETATTR, req_payload, 8, &resp, &rlen) < 0) {
        fuse_reply_err(req, EIO); return;
    }
    if (rlen < 4) { free(resp); fuse_reply_err(req, EIO); return; }

    uint32_t pos = 0;
    int32_t err = rb_i32(resp, &pos);
    if (err != 0) { free(resp); fuse_reply_err(req, -err); return; }

    struct etcfs_attr a;
    rb_attr(resp, &pos, &a);
    uint32_t to = rb_u32(resp, &pos);
    free(resp);

    struct stat st;
    fill_stat(&st, &a);
    fuse_reply_attr(req, &st, (double)to);
}

static void ec_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                       struct fuse_file_info *fi)
{
    (void)fi;
    struct etcfs_context *ctx = GET_CTX(req);

    /* build request: u64 ino, u64 offset, u32 size */
    uint8_t req_payload[20];
    uint32_t p = 0;
    p += wb_u64(req_payload + p, ino);
    p += wb_u64(req_payload + p, (uint64_t)off);
    p += wb_u32(req_payload + p, (uint32_t)size);

    uint8_t *resp; uint32_t rlen;
    if (ipc_call(ctx->ipc, IPC_OP_READDIR, req_payload, p, &resp, &rlen) < 0) {
        fuse_reply_err(req, EIO); return;
    }
    if (rlen < 8) { free(resp); fuse_reply_err(req, EIO); return; }

    uint32_t pos = 0;
    int32_t err = rb_i32(resp, &pos);
    if (err != 0) { free(resp); fuse_reply_err(req, -err); return; }

    uint32_t count = rb_u32(resp, &pos);

    /* build dirent buffer */
    char *dbuf = malloc(size + 512);
    size_t used = 0;

    for (uint32_t i = 0; i < count; i++) {
        uint64_t d_ino  = rb_u64(resp, &pos);
        uint32_t nlen   = rb_u32(resp, &pos);
        char     *dname = (char *)(resp + pos); pos += nlen;
        uint32_t d_type = rb_u32(resp, &pos);
        uint64_t d_off  = rb_u64(resp, &pos);

        struct stat st;
        memset(&st, 0, sizeof(st));
        st.st_ino  = d_ino;
        st.st_mode = (d_type == 4) ? (S_IFDIR | 0755) : (S_IFREG | 0644);

        size_t sz = fuse_add_direntry(req, dbuf + used, size - used,
                                      dname, &st, (off_t)d_off);
        if (sz > size - used) break;
        used += sz;
    }
    free(resp);
    fuse_reply_buf(req, dbuf, used);
    free(dbuf);
}

static void ec_readlink(fuse_req_t req, fuse_ino_t ino)
{
    struct etcfs_context *ctx = GET_CTX(req);

    uint8_t req_payload[8];
    wb_u64(req_payload, ino);

    uint8_t *resp; uint32_t rlen;
    if (ipc_call(ctx->ipc, IPC_OP_READLINK, req_payload, 8, &resp, &rlen) < 0) {
        fuse_reply_err(req, EIO); return;
    }
    if (rlen < 4) { free(resp); fuse_reply_err(req, EIO); return; }

    uint32_t pos = 0;
    int32_t err = rb_i32(resp, &pos);
    if (err != 0) { free(resp); fuse_reply_err(req, -err); return; }

    uint32_t target_len = rb_u32(resp, &pos);
    char *target = (char *)(resp + pos);
    resp[pos + target_len] = '\0'; /* ensure NUL-terminated */
    fuse_reply_readlink(req, target);
    free(resp);
}

static void ec_statfs(fuse_req_t req, fuse_ino_t ino)
{
    (void)ino;
    struct etcfs_context *ctx = GET_CTX(req);

    uint8_t *resp; uint32_t rlen;
    if (ipc_call(ctx->ipc, IPC_OP_STATFS, NULL, 0, &resp, &rlen) < 0) {
        fuse_reply_err(req, EIO); return;
    }
    if (rlen < 4) { free(resp); fuse_reply_err(req, EIO); return; }

    uint32_t pos = 0;
    int32_t err = rb_i32(resp, &pos);
    if (err != 0) { free(resp); fuse_reply_err(req, -err); return; }

    struct statvfs sv;
    memset(&sv, 0, sizeof(sv));
    sv.f_blocks  = rb_u64(resp, &pos);
    sv.f_bfree   = rb_u64(resp, &pos);
    sv.f_bavail  = rb_u64(resp, &pos);
    sv.f_files   = rb_u64(resp, &pos);
    sv.f_ffree   = rb_u64(resp, &pos);
    sv.f_bsize   = rb_u32(resp, &pos);
    sv.f_namemax = rb_u32(resp, &pos);
    sv.f_frsize  = rb_u32(resp, &pos);
    free(resp);

    fuse_reply_statfs(req, &sv);
}

static void ec_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    (void)ino;
    fi->fh = 0; fi->direct_io = 1; fi->keep_cache = 0;
    fuse_reply_open(req, fi);
}

static void ec_opendir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    (void)ino;
    fi->fh = 0;
    fuse_reply_open(req, fi);
}

/* ---- stub handlers for write ops (return EROFS) ---- */

static void ec_create(fuse_req_t req, fuse_ino_t parent, const char *name,
                      mode_t mode, struct fuse_file_info *fi)
{ (void)parent; (void)name; (void)mode; (void)fi; fuse_reply_err(req, EROFS); }

static void ec_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name,
                     mode_t mode)
{ (void)parent; (void)name; (void)mode; fuse_reply_err(req, EROFS); }

static void ec_unlink(fuse_req_t req, fuse_ino_t parent, const char *name)
{ (void)parent; (void)name; fuse_reply_err(req, EROFS); }

static void ec_rmdir(fuse_req_t req, fuse_ino_t parent, const char *name)
{ (void)parent; (void)name; fuse_reply_err(req, EROFS); }

static void ec_rename(fuse_req_t req, fuse_ino_t parent, const char *name,
                      fuse_ino_t newparent, const char *newname, unsigned int flags)
{ (void)parent; (void)name; (void)newparent; (void)newname; (void)flags; fuse_reply_err(req, EROFS); }

static void ec_symlink(fuse_req_t req, const char *link, fuse_ino_t parent,
                       const char *name)
{ (void)link; (void)parent; (void)name; fuse_reply_err(req, EROFS); }

static void ec_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent,
                    const char *newname)
{ (void)ino; (void)newparent; (void)newname; fuse_reply_err(req, EROFS); }

static void ec_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr,
                       int to_set, struct fuse_file_info *fi)
{ (void)ino; (void)attr; (void)to_set; (void)fi; fuse_reply_err(req, EROFS); }

static void ec_write(fuse_req_t req, fuse_ino_t ino, const char *buf,
                     size_t size, off_t off, struct fuse_file_info *fi)
{ (void)ino; (void)buf; (void)size; (void)off; (void)fi; fuse_reply_err(req, EROFS); }

static void ec_write_buf(fuse_req_t req, fuse_ino_t ino,
                         struct fuse_bufvec *bufv, off_t off,
                         struct fuse_file_info *fi)
{ (void)req; (void)ino; (void)bufv; (void)off; (void)fi; fuse_reply_err(req, EROFS); }

static void ec_mknod(fuse_req_t req, fuse_ino_t parent, const char *name,
                     mode_t mode, dev_t rdev)
{ (void)parent; (void)name; (void)mode; (void)rdev; fuse_reply_err(req, EROFS); }

static void ec_fallocate(fuse_req_t req, fuse_ino_t ino, int mode,
                         off_t offset, off_t length, struct fuse_file_info *fi)
{ (void)ino; (void)mode; (void)offset; (void)length; (void)fi; fuse_reply_err(req, EROFS); }

/* ---- no-op handlers (return success) ---- */

static void ec_release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{ (void)ino; (void)fi; fuse_reply_err(req, 0); }

static void ec_releasedir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{ (void)ino; (void)fi; fuse_reply_err(req, 0); }

static void ec_flush(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{ (void)ino; (void)fi; fuse_reply_err(req, 0); }

static void ec_fsync(fuse_req_t req, fuse_ino_t ino, int datasync,
                     struct fuse_file_info *fi)
{ (void)ino; (void)datasync; (void)fi; fuse_reply_err(req, 0); }

static void ec_fsyncdir(fuse_req_t req, fuse_ino_t ino, int datasync,
                        struct fuse_file_info *fi)
{ (void)ino; (void)datasync; (void)fi; fuse_reply_err(req, 0); }

static void ec_read(fuse_req_t req, fuse_ino_t ino, size_t size,
                    off_t off, struct fuse_file_info *fi)
{ (void)ino; (void)size; (void)off; (void)fi; fuse_reply_buf(req, NULL, 0); }

static void ec_getlk(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi,
                     struct flock *lock)
{
    (void)ino; (void)fi;
    struct flock lk = *lock;
    lk.l_type = F_UNLCK;
    fuse_reply_lock(req, &lk);
}

static void ec_setlk(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi,
                     struct flock *lock, int sleep)
{ (void)ino; (void)fi; (void)lock; (void)sleep; fuse_reply_err(req, 0); }

/* ---- op table ---- */

struct fuse_lowlevel_ops *etcfs_fuse_ops(void)
{
    static struct fuse_lowlevel_ops ops;
    memset(&ops, 0, sizeof(ops));

    ops.lookup     = ec_lookup;
    ops.getattr    = ec_getattr;
    ops.readdir    = ec_readdir;
    ops.readlink   = ec_readlink;
    ops.statfs     = ec_statfs;
    ops.open       = ec_open;
    ops.opendir    = ec_opendir;
    ops.create     = ec_create;
    ops.mkdir      = ec_mkdir;
    ops.unlink     = ec_unlink;
    ops.rmdir      = ec_rmdir;
    ops.rename     = ec_rename;
    ops.symlink    = ec_symlink;
    ops.link       = ec_link;
    ops.setattr    = ec_setattr;
    ops.write      = ec_write;
    ops.write_buf  = ec_write_buf;
    ops.mknod      = ec_mknod;
    ops.fallocate  = ec_fallocate;
    ops.release    = ec_release;
    ops.releasedir = ec_releasedir;
    ops.flush      = ec_flush;
    ops.fsync      = ec_fsync;
    ops.fsyncdir   = ec_fsyncdir;
    ops.read       = ec_read;
    ops.getlk      = ec_getlk;
    ops.setlk      = ec_setlk;

    return &ops;
}
