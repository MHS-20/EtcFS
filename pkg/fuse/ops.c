/*
 * ops.c — FUSE low-level operation handlers (async IPC via worker thread).
 *
 * Each handler builds a binary request payload, submits it to the IPC worker
 * thread, and returns immediately.  The IPC worker does the socket I/O and
 * invokes the callback on the original fuse_req_t.
 *
 * IPC wire format (C ↔ Go):
 *   Request:  [u16:be opcode] [u32:be payload_len] [payload]
 *   Response: [u32:be payload_len] [payload]
 */

#include "ops.h"
#include "fuse.h"
#include "pool.h"

#include <errno.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <fuse3/fuse_lowlevel.h>

/* ---- opcodes (match Go side) ---- */
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
#define IPC_OP_FLUSH      26

/* ---- binary readers (on flat response buffers) ---- */

static uint64_t rb_u64(const uint8_t *p, uint32_t *off)
{
    uint64_t v = ((uint64_t)p[*off] << 56) | ((uint64_t)p[*off+1] << 48) |
                 ((uint64_t)p[*off+2] << 40) | ((uint64_t)p[*off+3] << 32) |
                 ((uint64_t)p[*off+4] << 24) | ((uint64_t)p[*off+5] << 16) |
                 ((uint64_t)p[*off+6] << 8)  |  (uint64_t)p[*off+7];
    *off += 8; return v;
}

static uint32_t rb_u32(const uint8_t *p, uint32_t *off)
{
    uint32_t v = ((uint32_t)p[*off] << 24) | ((uint32_t)p[*off+1] << 16) |
                 ((uint32_t)p[*off+2] << 8)  |  (uint32_t)p[*off+3];
    *off += 4; return v;
}

static int32_t rb_i32(const uint8_t *p, uint32_t *off)
{
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

/* ---- binary writers (for building request payloads) ---- */

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

/* ---- async submit helper: alloc payload, submit to pool ---- */

#define GET_IPC(ctx) (((struct etcfs_context *)fuse_req_userdata(req))->ipc)

static void submit_req(fuse_req_t req, uint16_t op,
                       const uint8_t *payload, uint32_t plen,
                       ipc_resp_cb cb, void *cb_data)
{
    struct etcfs_context *ctx = fuse_req_userdata(req);
    if (!ctx || !ctx->ipc) { fuse_reply_err(req, EIO); return; }
    uint8_t *copy = malloc(plen);
    if (!copy) { fuse_reply_err(req, ENOMEM); return; }
    memcpy(copy, payload, plen);
    if (ipc_worker_submit(ctx->ipc, req, op, copy, plen, cb, cb_data) < 0) {
        free(copy);
        fuse_reply_err(req, EIO);
    }
}

/* ---- response callbacks for each op type ---- */

/* LOOKUP callback: parse resp → fuse_reply_entry */
static void cb_lookup(fuse_req_t req, int32_t err, uint8_t *resp, uint32_t rlen, void *data)
{
    (void)data;
    etcfs_log(ETCFS_LOG_INFO, "cb_lookup err=%d rlen=%u", err, rlen);
    if (err != 0) { fuse_reply_err(req, -err); return; }
    if (rlen < 4)  { fuse_reply_err(req, EIO); return; }

    uint32_t pos = 0;
    int32_t e = rb_i32(resp, &pos);
    if (e != 0) { fuse_reply_err(req, -e); free(resp); return; }

    struct fuse_entry_param ep;
    memset(&ep, 0, sizeof(ep));
    ep.ino = rb_u64(resp, &pos);
    struct etcfs_attr a;
    rb_attr(resp, &pos, &a);
    ep.entry_timeout = (double)rb_u32(resp, &pos);
    ep.attr_timeout  = (double)rb_u32(resp, &pos);
    fill_stat(&ep.attr, &a);
    free(resp);
    fuse_reply_entry(req, &ep);
}

/* GETATTR callback */
static void cb_getattr(fuse_req_t req, int32_t err, uint8_t *resp, uint32_t rlen, void *data)
{
    (void)data;
    if (err != 0) { fuse_reply_err(req, -err); return; }
    if (rlen < 4) { fuse_reply_err(req, EIO); return; }

    uint32_t pos = 0;
    int32_t e = rb_i32(resp, &pos);
    if (e != 0) { fuse_reply_err(req, -e); free(resp); return; }

    struct etcfs_attr a;
    rb_attr(resp, &pos, &a);
    uint32_t to = rb_u32(resp, &pos);
    free(resp);

    struct stat st;
    fill_stat(&st, &a);
    fuse_reply_attr(req, &st, (double)to);
}

/* READDIR callback */
static void cb_readdir(fuse_req_t req, int32_t err, uint8_t *resp, uint32_t rlen, void *data)
{
    (void)data;
    if (err != 0) { fuse_reply_err(req, -err); return; }
    if (rlen < 8) { fuse_reply_err(req, EIO); return; }

    uint32_t pos = 0;
    int32_t e = rb_i32(resp, &pos);
    if (e != 0) { fuse_reply_err(req, -e); free(resp); return; }

    uint32_t count = rb_u32(resp, &pos);
    size_t size = 4096;
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

/* READLINK callback */
static void cb_readlink(fuse_req_t req, int32_t err, uint8_t *resp, uint32_t rlen, void *data)
{
    (void)data;
    if (err != 0) { fuse_reply_err(req, -err); return; }
    if (rlen < 4) { fuse_reply_err(req, EIO); return; }

    uint32_t pos = 0;
    int32_t e = rb_i32(resp, &pos);
    if (e != 0) { fuse_reply_err(req, -e); free(resp); return; }

    uint32_t tlen = rb_u32(resp, &pos);
    char *target = (char *)(resp + pos);
    resp[pos + tlen] = '\0';
    fuse_reply_readlink(req, target);
    free(resp);
}

/* STATFS callback */
static void cb_statfs(fuse_req_t req, int32_t err, uint8_t *resp, uint32_t rlen, void *data)
{
    (void)data;
    if (err != 0) { fuse_reply_err(req, -err); return; }
    if (rlen < 4) { fuse_reply_err(req, EIO); return; }

    uint32_t pos = 0;
    int32_t e = rb_i32(resp, &pos);
    if (e != 0) { fuse_reply_err(req, -e); free(resp); return; }

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

/* Generic error-only callback (for write ops returning just [i32:error]) */
static void cb_error(fuse_req_t req, int32_t err, uint8_t *resp, uint32_t rlen, void *data)
{
    (void)data;
    if (err != 0) { fuse_reply_err(req, -err); return; }
    int32_t e = 0;
    if (rlen >= 4) {
        e = (int32_t)((uint32_t)resp[0] << 24 | (uint32_t)resp[1] << 16 |
                      (uint32_t)resp[2] << 8  | (uint32_t)resp[3]);
    }
    free(resp);
    fuse_reply_err(req, e != 0 ? -e : 0);
}

/* CREATE/MKDIR/SYMLINK/LINK/MKNOD callback (entry response) */
static void cb_create_entry(fuse_req_t req, int32_t err, uint8_t *resp, uint32_t rlen, void *data)
{
    (void)data;
    if (err != 0) { fuse_reply_err(req, -err); return; }
    if (rlen < 4) { fuse_reply_err(req, EIO); return; }

    uint32_t pos = 0;
    int32_t e = rb_i32(resp, &pos);
    if (e != 0) { fuse_reply_err(req, -e); free(resp); return; }

    struct fuse_entry_param ep;
    memset(&ep, 0, sizeof(ep));
    ep.ino = rb_u64(resp, &pos);
    struct etcfs_attr a;
    rb_attr(resp, &pos, &a);
    ep.entry_timeout = (double)rb_u32(resp, &pos);
    ep.attr_timeout  = (double)rb_u32(resp, &pos);
    fill_stat(&ep.attr, &a);
    free(resp);
    fuse_reply_entry(req, &ep);
}

/* SETATTR callback */
static void cb_setattr(fuse_req_t req, int32_t err, uint8_t *resp, uint32_t rlen, void *data)
{
    (void)data;
    if (err != 0) { fuse_reply_err(req, -err); return; }
    if (rlen < 4) { fuse_reply_err(req, EIO); return; }

    uint32_t pos = 0;
    int32_t e = rb_i32(resp, &pos);
    if (e != 0) { fuse_reply_err(req, -e); free(resp); return; }

    struct etcfs_attr a;
    rb_attr(resp, &pos, &a);
    uint32_t to = rb_u32(resp, &pos);
    free(resp);

    struct stat st;
    fill_stat(&st, &a);
    fuse_reply_attr(req, &st, (double)to);
}

/* WRITE callback */
static void cb_write(fuse_req_t req, int32_t err, uint8_t *resp, uint32_t rlen, void *data)
{
    (void)data;
    if (err != 0) { fuse_reply_err(req, -err); return; }
    if (rlen < 8) { fuse_reply_err(req, EIO); return; }

    uint32_t pos = 0;
    int32_t e = rb_i32(resp, &pos);
    uint32_t written = rb_u32(resp, &pos);
    free(resp);

    if (e != 0) { fuse_reply_err(req, -e); return; }
    fuse_reply_write(req, (size_t)written);
}

/* ---- FUSE operation handlers ---- */

static void ec_lookup(fuse_req_t req, fuse_ino_t parent, const char *name)
{
    /* Root: handle locally */
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

    size_t nlen = strlen(name);
    uint8_t payload[12 + 256];
    uint32_t off = 0;
    off += wb_u64(payload + off, parent);
    off += wb_u32(payload + off, (uint32_t)nlen);
    memcpy(payload + off, name, nlen); off += (uint32_t)nlen;
    submit_req(req, IPC_OP_LOOKUP, payload, off, cb_lookup, NULL);
}

static void ec_getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    (void)fi;
    /* Root: handle locally */
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

    uint8_t payload[8];
    wb_u64(payload, ino);
    submit_req(req, IPC_OP_GETATTR, payload, 8, cb_getattr, NULL);
}

static void ec_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                       struct fuse_file_info *fi)
{
    (void)fi;
    uint8_t payload[20];
    uint32_t p = 0;
    p += wb_u64(payload + p, ino);
    p += wb_u64(payload + p, (uint64_t)off);
    p += wb_u32(payload + p, (uint32_t)size);
    submit_req(req, IPC_OP_READDIR, payload, p, cb_readdir, NULL);
}

static void ec_readlink(fuse_req_t req, fuse_ino_t ino)
{
    uint8_t payload[8];
    wb_u64(payload, ino);
    submit_req(req, IPC_OP_READLINK, payload, 8, cb_readlink, NULL);
}

static void ec_statfs(fuse_req_t req, fuse_ino_t ino)
{
    (void)ino;
    submit_req(req, IPC_OP_STATFS, NULL, 0, cb_statfs, NULL);
}

static void ec_statx(fuse_req_t req, fuse_ino_t ino, int flags, int mask,
                     struct fuse_file_info *fi)
{
    (void)flags; (void)mask;
    ec_getattr(req, ino, fi);
}

static void ec_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    (void)ino; fi->fh = 0; fi->direct_io = 1; fi->keep_cache = 0;
    fuse_reply_open(req, fi);
}

static void ec_opendir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    (void)ino; fi->fh = 0;
    fuse_reply_open(req, fi);
}

/* ---- write handlers (async) ---- */

static void ec_create(fuse_req_t req, fuse_ino_t parent, const char *name,
                      mode_t mode, struct fuse_file_info *fi)
{
    size_t nlen = strlen(name);
    uint8_t payload[20 + 256];
    uint32_t off = 0;
    off += wb_u64(payload + off, parent);
    off += wb_u32(payload + off, (uint32_t)nlen);
    memcpy(payload + off, name, nlen); off += (uint32_t)nlen;
    off += wb_u32(payload + off, (uint32_t)mode);
    off += wb_u32(payload + off, (uint32_t)(fi ? fi->flags : 0));
    off += wb_u32(payload + off, 022);
    submit_req(req, IPC_OP_CREATE, payload, off, cb_create_entry, fi);
}

static void ec_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode)
{
    size_t nlen = strlen(name);
    uint8_t payload[20 + 256];
    uint32_t off = 0;
    off += wb_u64(payload + off, parent);
    off += wb_u32(payload + off, (uint32_t)nlen);
    memcpy(payload + off, name, nlen); off += (uint32_t)nlen;
    off += wb_u32(payload + off, (uint32_t)mode);
    off += wb_u32(payload + off, 022);
    submit_req(req, IPC_OP_MKDIR, payload, off, cb_create_entry, NULL);
}

static void ec_unlink(fuse_req_t req, fuse_ino_t parent, const char *name)
{
    size_t nlen = strlen(name);
    uint8_t payload[12 + 256];
    uint32_t off = 0;
    off += wb_u64(payload + off, parent);
    off += wb_u32(payload + off, (uint32_t)nlen);
    memcpy(payload + off, name, nlen); off += (uint32_t)nlen;
    submit_req(req, IPC_OP_UNLINK, payload, off, cb_error, NULL);
}

static void ec_rmdir(fuse_req_t req, fuse_ino_t parent, const char *name)
{
    size_t nlen = strlen(name);
    uint8_t payload[12 + 256];
    uint32_t off = 0;
    off += wb_u64(payload + off, parent);
    off += wb_u32(payload + off, (uint32_t)nlen);
    memcpy(payload + off, name, nlen); off += (uint32_t)nlen;
    submit_req(req, IPC_OP_RMDIR, payload, off, cb_error, NULL);
}

static void ec_rename(fuse_req_t req, fuse_ino_t old_parent, const char *old_name,
                      fuse_ino_t new_parent, const char *new_name, unsigned int flags)
{
    size_t olen = strlen(old_name), nlen = strlen(new_name);
    uint8_t payload[40 + 512];
    uint32_t off = 0;
    off += wb_u64(payload + off, old_parent);
    off += wb_u32(payload + off, (uint32_t)olen);
    memcpy(payload + off, old_name, olen); off += (uint32_t)olen;
    off += wb_u64(payload + off, new_parent);
    off += wb_u32(payload + off, (uint32_t)nlen);
    memcpy(payload + off, new_name, nlen); off += (uint32_t)nlen;
    off += wb_u32(payload + off, flags);
    submit_req(req, IPC_OP_RENAME, payload, off, cb_error, NULL);
}

static void ec_write(fuse_req_t req, fuse_ino_t ino, const char *buf,
                     size_t size, off_t off, struct fuse_file_info *fi)
{
    (void)fi;
    uint8_t *payload = malloc(20 + size);
    uint32_t pos = 0;
    pos += wb_u64(payload + pos, ino);
    pos += wb_u64(payload + pos, (uint64_t)off);
    pos += wb_u32(payload + pos, (uint32_t)size);
    memcpy(payload + pos, buf, size); pos += (uint32_t)size;
    submit_req(req, IPC_OP_WRITE, payload, pos, cb_write, NULL);
    free(payload);
}

static void ec_write_buf(fuse_req_t req, fuse_ino_t ino,
                         struct fuse_bufvec *bufv, off_t off,
                         struct fuse_file_info *fi)
{
    (void)ino; (void)bufv; (void)off; (void)fi;
    fuse_reply_err(req, ENOSYS);
}

static void ec_symlink(fuse_req_t req, const char *target, fuse_ino_t parent, const char *name)
{
    size_t nlen = strlen(name), tlen = strlen(target);
    uint8_t payload[20 + 512];
    uint32_t off = 0;
    off += wb_u64(payload + off, parent);
    off += wb_u32(payload + off, (uint32_t)nlen);
    memcpy(payload + off, name, nlen); off += (uint32_t)nlen;
    off += wb_u32(payload + off, (uint32_t)tlen);
    memcpy(payload + off, target, tlen); off += (uint32_t)tlen;
    submit_req(req, IPC_OP_SYMLINK, payload, off, cb_create_entry, NULL);
}

static void ec_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t new_parent, const char *new_name)
{
    size_t nlen = strlen(new_name);
    uint8_t payload[24 + 256];
    uint32_t off = 0;
    off += wb_u64(payload + off, ino);
    off += wb_u64(payload + off, new_parent);
    off += wb_u32(payload + off, (uint32_t)nlen);
    memcpy(payload + off, new_name, nlen); off += (uint32_t)nlen;
    submit_req(req, IPC_OP_LINK, payload, off, cb_create_entry, NULL);
}

static void ec_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr,
                       int to_set, struct fuse_file_info *fi)
{
    (void)attr; (void)to_set;
    uint8_t payload[20 + 84];
    uint32_t off = 0;
    off += wb_u64(payload + off, ino);
    off += wb_u64(payload + off, (fi ? fi->fh : 0));
    off += wb_u32(payload + off, (uint32_t)to_set);
    submit_req(req, IPC_OP_SETATTR, payload, off, cb_setattr, NULL);
}

static void ec_mknod(fuse_req_t req, fuse_ino_t parent, const char *name,
                     mode_t mode, dev_t rdev)
{
    size_t nlen = strlen(name);
    uint8_t payload[24 + 256];
    uint32_t off = 0;
    off += wb_u64(payload + off, parent);
    off += wb_u32(payload + off, (uint32_t)nlen);
    memcpy(payload + off, name, nlen); off += (uint32_t)nlen;
    off += wb_u32(payload + off, (uint32_t)mode);
    off += wb_u32(payload + off, (uint32_t)rdev);
    submit_req(req, IPC_OP_MKNOD, payload, off, cb_create_entry, NULL);
}

/* ---- no-op / stub handlers ---- */

static void ec_release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{ (void)ino; (void)fi; fuse_reply_err(req, 0); }

static void ec_releasedir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{ (void)ino; (void)fi; fuse_reply_err(req, 0); }

static void ec_flush(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{ (void)ino; (void)fi; fuse_reply_err(req, 0); }

static void ec_fsync(fuse_req_t req, fuse_ino_t ino, int datasync, struct fuse_file_info *fi)
{ (void)ino; (void)datasync; (void)fi; fuse_reply_err(req, 0); }

static void ec_fsyncdir(fuse_req_t req, fuse_ino_t ino, int datasync, struct fuse_file_info *fi)
{ (void)ino; (void)datasync; (void)fi; fuse_reply_err(req, 0); }

static void ec_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi)
{ (void)ino; (void)size; (void)off; (void)fi; fuse_reply_buf(req, NULL, 0); }

static void ec_getlk(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi, struct flock *lock)
{
    (void)ino; (void)fi;
    struct flock lk = *lock;
    lk.l_type = F_UNLCK;
    fuse_reply_lock(req, &lk);
}

static void ec_setlk(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi,
                     struct flock *lock, int sleep)
{ (void)ino; (void)fi; (void)lock; (void)sleep; fuse_reply_err(req, 0); }

static void ec_fallocate(fuse_req_t req, fuse_ino_t ino, int mode,
                         off_t offset, off_t length, struct fuse_file_info *fi)
{ (void)ino; (void)mode; (void)offset; (void)length; (void)fi; fuse_reply_err(req, EROFS); }

/* ---- op table ---- */

struct fuse_lowlevel_ops *etcfs_fuse_ops(void)
{
    static struct fuse_lowlevel_ops ops;
    memset(&ops, 0, sizeof(ops));

    ops.lookup     = ec_lookup;
    ops.getattr    = ec_getattr;
    ops.statx      = ec_statx;
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
