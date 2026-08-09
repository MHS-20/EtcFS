/*
 * ops.c — FUSE low-level operation handlers (synchronous IPC).
 *
 * Each handler builds a binary request payload, performs a synchronous
 * IPC exchange with the Go metadata backend over the Unix socket,
 * parses the response, and calls the appropriate fuse_reply_* function.
 *
 * IPC wire format (C ↔ Go):
 *   Request:  [u16:be opcode] [u32:be payload_len] [payload]
 *   Response: [u32:be payload_len] [payload]
 */

#include "ops.h"
#include "fuse.h"

#include <errno.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <fuse3/fuse_lowlevel.h>

/* ---- opcodes (match Go side) ---- */
#define IPC_OP_LOOKUP      1
#define IPC_OP_GETATTR     2
#define IPC_OP_READDIR     3
#define IPC_OP_READLINK    4
#define IPC_OP_CREATE      5
#define IPC_OP_MKDIR       6
#define IPC_OP_UNLINK      7
#define IPC_OP_RMDIR       8
#define IPC_OP_RENAME      9
#define IPC_OP_SYMLINK     10
#define IPC_OP_LINK        11
#define IPC_OP_SETATTR     12
#define IPC_OP_OPEN        13
#define IPC_OP_RELEASE     14
#define IPC_OP_OPENDIR     15
#define IPC_OP_RELEASEDIR  16
#define IPC_OP_STATFS      17
#define IPC_OP_READ        22
#define IPC_OP_WRITE       23
#define IPC_OP_FSYNC       24
#define IPC_OP_MKNOD       25
#define IPC_OP_FLUSH       26
#define IPC_OP_READDIRPLUS 29

#define MAX_NAME_LEN 255

static int send_full(int fd, const void *buf, size_t len)
{
    const char *p = buf;
    while (len > 0) {
        ssize_t n = write(fd, p, len);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        len -= (size_t) n;
        p += n;
    }
    return 0;
}

static int recv_full(int fd, void *buf, size_t len)
{
    char *p = buf;
    while (len > 0) {
        ssize_t n = read(fd, p, len);
        if (n <= 0) {
            if (n == 0)
                return -1;
            if (errno == EINTR)
                continue;
            return -1;
        }
        len -= (size_t) n;
        p += n;
    }
    return 0;
}

/* ---- synchronous IPC ---- */

static int ipc_sync(int fd, uint16_t op, const uint8_t *payload, uint32_t plen, uint8_t **resp,
                    uint32_t *rlen)
{
    uint8_t hdr[6];
    hdr[0] = (uint8_t) (op >> 8);
    hdr[1] = (uint8_t) op;
    hdr[2] = (uint8_t) (plen >> 24);
    hdr[3] = (uint8_t) (plen >> 16);
    hdr[4] = (uint8_t) (plen >> 8);
    hdr[5] = (uint8_t) plen;
    if (send_full(fd, hdr, 6) < 0)
        return -1;
    if (plen > 0 && send_full(fd, payload, plen) < 0)
        return -1;

    uint8_t rhdr[4];
    if (recv_full(fd, rhdr, 4) < 0)
        return -1;
    uint32_t rl = ((uint32_t) rhdr[0] << 24) | ((uint32_t) rhdr[1] << 16) |
                  ((uint32_t) rhdr[2] << 8) | (uint32_t) rhdr[3];
    uint8_t *rb = malloc(rl > 0 ? rl : 1);
    if (!rb)
        return -1;
    if (rl > 0 && recv_full(fd, rb, rl) < 0) {
        free(rb);
        return -1;
    }
    *resp = rb;
    *rlen = rl;
    return 0;
}

/* ---- binary readers on response buffers ---- */

static uint64_t rb_u64(const uint8_t *p, uint32_t *off)
{
    uint64_t v = ((uint64_t) p[*off] << 56) | ((uint64_t) p[*off + 1] << 48) |
                 ((uint64_t) p[*off + 2] << 40) | ((uint64_t) p[*off + 3] << 32) |
                 ((uint64_t) p[*off + 4] << 24) | ((uint64_t) p[*off + 5] << 16) |
                 ((uint64_t) p[*off + 6] << 8) | (uint64_t) p[*off + 7];
    *off += 8;
    return v;
}
static uint32_t rb_u32(const uint8_t *p, uint32_t *off)
{
    uint32_t v = ((uint32_t) p[*off] << 24) | ((uint32_t) p[*off + 1] << 16) |
                 ((uint32_t) p[*off + 2] << 8) | (uint32_t) p[*off + 3];
    *off += 4;
    return v;
}
static int32_t rb_i32(const uint8_t *p, uint32_t *off)
{
    return (int32_t) rb_u32(p, off);
}
static void rb_attr(const uint8_t *p, uint32_t *off, struct etcfs_attr *a)
{
    a->ino = rb_u64(p, off);
    a->size = rb_u64(p, off);
    a->blocks = rb_u64(p, off);
    a->atime = rb_u64(p, off);
    a->mtime = rb_u64(p, off);
    a->ctime = rb_u64(p, off);
    a->atime_nsec = rb_u32(p, off);
    a->mtime_nsec = rb_u32(p, off);
    a->ctime_nsec = rb_u32(p, off);
    a->mode = rb_u32(p, off);
    a->nlink = rb_u32(p, off);
    a->uid = rb_u32(p, off);
    a->gid = rb_u32(p, off);
    a->rdev = rb_u32(p, off);
    a->blksize = rb_u32(p, off);
}

/* ---- binary writers for building request payloads ---- */

static uint32_t wb_u64(uint8_t *buf, uint64_t v)
{
    buf[0] = (uint8_t) (v >> 56);
    buf[1] = (uint8_t) (v >> 48);
    buf[2] = (uint8_t) (v >> 40);
    buf[3] = (uint8_t) (v >> 32);
    buf[4] = (uint8_t) (v >> 24);
    buf[5] = (uint8_t) (v >> 16);
    buf[6] = (uint8_t) (v >> 8);
    buf[7] = (uint8_t) v;
    return 8;
}
static uint32_t wb_u32(uint8_t *buf, uint32_t v)
{
    buf[0] = (uint8_t) (v >> 24);
    buf[1] = (uint8_t) (v >> 16);
    buf[2] = (uint8_t) (v >> 8);
    buf[3] = (uint8_t) v;
    return 4;
}

static void fill_stat(struct stat *st, const struct etcfs_attr *a)
{
    memset(st, 0, sizeof(*st));
    st->st_ino = a->ino;
    st->st_mode = a->mode;
    st->st_nlink = a->nlink;
    st->st_uid = a->uid;
    st->st_gid = a->gid;
    st->st_size = (off_t) a->size;
    st->st_blksize = a->blksize;
    st->st_blocks = (blkcnt_t) a->blocks;
    st->st_rdev = a->rdev;
    st->st_atime = (time_t) a->atime;
    st->st_mtime = (time_t) a->mtime;
    st->st_ctime = (time_t) a->ctime;
}

/* ---- helper: get fd from context ---- */

#define FD(ctx) (((struct etcfs_context *) fuse_req_userdata(req))->ipc_fd)

/* ---- FUSE operation handlers ---- */

static void ec_lookup(fuse_req_t req, fuse_ino_t parent, const char *name)
{
    if (parent == FUSE_ROOT_ID &&
        (strcmp(name, ".") == 0 || strcmp(name, "..") == 0 || strcmp(name, "/") == 0)) {
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
    if (nlen > MAX_NAME_LEN) {
        fuse_reply_err(req, ENAMETOOLONG);
        return;
    }
    uint8_t payload[12 + 256];
    uint32_t off = 0;
    off += wb_u64(payload + off, parent);
    off += wb_u32(payload + off, (uint32_t) nlen);
    memcpy(payload + off, name, nlen);
    off += (uint32_t) nlen;

    uint8_t *resp;
    uint32_t rlen;
    if (ipc_sync(FD(ctx), IPC_OP_LOOKUP, payload, off, &resp, &rlen) < 0) {
        fuse_reply_err(req, EIO);
        return;
    }

    uint32_t pos = 0;
    int32_t e = rb_i32(resp, &pos);
    if (e != 0) {
        fuse_reply_err(req, -e);
        free(resp);
        return;
    }
    struct fuse_entry_param ep;
    memset(&ep, 0, sizeof(ep));
    ep.ino = rb_u64(resp, &pos);
    struct etcfs_attr a;
    rb_attr(resp, &pos, &a);
    ep.entry_timeout = (double) rb_u32(resp, &pos);
    ep.attr_timeout = (double) rb_u32(resp, &pos);
    fill_stat(&ep.attr, &a);
    free(resp);
    fuse_reply_entry(req, &ep);
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
    uint8_t payload[8];
    wb_u64(payload, ino);
    uint8_t *resp;
    uint32_t rlen;
    if (ipc_sync(FD(ctx), IPC_OP_GETATTR, payload, 8, &resp, &rlen) < 0) {
        fuse_reply_err(req, EIO);
        return;
    }
    uint32_t pos = 0;
    int32_t e = rb_i32(resp, &pos);
    if (e != 0) {
        fuse_reply_err(req, -e);
        free(resp);
        return;
    }
    struct etcfs_attr a;
    rb_attr(resp, &pos, &a);
    uint32_t to = rb_u32(resp, &pos);
    free(resp);
    struct stat st;
    fill_stat(&st, &a);
    fuse_reply_attr(req, &st, (double) to);
}

static void ec_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                       struct fuse_file_info *fi)
{
    (void) fi;
    uint8_t payload[20];
    uint32_t p = 0;
    p += wb_u64(payload + p, ino);
    p += wb_u64(payload + p, (uint64_t) off);
    p += wb_u32(payload + p, (uint32_t) size);

    uint8_t *resp;
    uint32_t rlen;
    if (ipc_sync(FD(ctx), IPC_OP_READDIR, payload, p, &resp, &rlen) < 0) {
        fuse_reply_err(req, EIO);
        return;
    }
    uint32_t pos = 0;
    int32_t e = rb_i32(resp, &pos);
    if (e != 0) {
        fuse_reply_err(req, -e);
        free(resp);
        return;
    }

    uint32_t count = rb_u32(resp, &pos);
    size_t bufsz = size > 0 ? size : 4096;
    char *dbuf = malloc(bufsz + 512);
    size_t used = 0;
    uint64_t off_cookie = (uint64_t) off;
    for (uint32_t i = 0; i < count; i++) {
        uint64_t di = rb_u64(resp, &pos);
        uint32_t nl = rb_u32(resp, &pos);
        char *dn = (char *) (resp + pos);
        pos += nl;
        uint32_t dt = rb_u32(resp, &pos);
        uint64_t d_off = rb_u64(resp, &pos);
        if (d_off <= off_cookie)
            continue; /* skip already-returned entries */
        struct stat st;
        memset(&st, 0, sizeof(st));
        st.st_ino = di;
        st.st_mode = (dt == 4) ? (S_IFDIR | 0755) : (S_IFREG | 0644);
        size_t sz = fuse_add_direntry(req, dbuf + used, bufsz - used, dn, &st, (off_t) d_off);
        if (sz > bufsz - used)
            break;
        used += sz;
    }
    free(resp);
    fuse_reply_buf(req, dbuf, used);
    free(dbuf);
}

static void ec_readdirplus(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                           struct fuse_file_info *fi)
{
    (void) fi;
    uint8_t payload[20];
    uint32_t p = 0;
    p += wb_u64(payload + p, ino);
    p += wb_u64(payload + p, (uint64_t) off);
    p += wb_u32(payload + p, (uint32_t) size);

    uint8_t *resp;
    uint32_t rlen;
    if (ipc_sync(FD(ctx), IPC_OP_READDIRPLUS, payload, p, &resp, &rlen) < 0) {
        fuse_reply_err(req, EIO);
        return;
    }
    uint32_t pos = 0;
    int32_t e = rb_i32(resp, &pos);
    if (e != 0) {
        fuse_reply_err(req, -e);
        free(resp);
        return;
    }

    uint32_t count = rb_u32(resp, &pos);
    size_t bufsz = size > 0 ? size : 4096;
    char *dbuf = malloc(bufsz + 512);
    size_t used = 0;
    uint64_t off_cookie = (uint64_t) off;

    for (uint32_t i = 0; i < count; i++) {
        uint64_t di = rb_u64(resp, &pos);
        uint32_t nl = rb_u32(resp, &pos);
        char *dn = (char *) (resp + pos);
        pos += nl;
        uint32_t dt __attribute__((unused)) = rb_u32(resp, &pos);
        uint64_t d_off = rb_u64(resp, &pos);

        /* Consume the whole entry before deciding whether to skip it.  Each
         * readdirplus entry carries an attr block and two timeouts after the
         * offset cookie; skipping straight to the next iteration would leave
         * those bytes unread and desynchronise the parser, turning every
         * following entry into garbage. */
        struct fuse_entry_param ep;
        memset(&ep, 0, sizeof(ep));
        ep.ino = di;
        struct etcfs_attr a;
        rb_attr(resp, &pos, &a);
        ep.entry_timeout = (double) rb_u32(resp, &pos);
        ep.attr_timeout = (double) rb_u32(resp, &pos);
        fill_stat(&ep.attr, &a);

        if (d_off <= off_cookie)
            continue; /* already returned in an earlier call */

        size_t sz = fuse_add_direntry_plus(req, dbuf + used, bufsz - used, dn, &ep, d_off);
        if (sz > bufsz - used)
            break;
        used += sz;
    }
    free(resp);
    fuse_reply_buf(req, dbuf, used);
    free(dbuf);
}

static void ec_readlink(fuse_req_t req, fuse_ino_t ino)
{
    uint8_t payload[8];
    wb_u64(payload, ino);
    uint8_t *resp;
    uint32_t rlen;
    if (ipc_sync(FD(ctx), IPC_OP_READLINK, payload, 8, &resp, &rlen) < 0) {
        fuse_reply_err(req, EIO);
        return;
    }
    uint32_t pos = 0;
    int32_t e = rb_i32(resp, &pos);
    if (e != 0) {
        fuse_reply_err(req, -e);
        free(resp);
        return;
    }
    uint32_t tl = rb_u32(resp, &pos);
    char *target = (char *) (resp + pos);
    resp[pos + tl] = '\0';
    fuse_reply_readlink(req, target);
    free(resp);
}

static void ec_statfs(fuse_req_t req, fuse_ino_t ino)
{
    (void) ino;
    uint8_t *resp;
    uint32_t rlen;
    if (ipc_sync(FD(ctx), IPC_OP_STATFS, NULL, 0, &resp, &rlen) < 0) {
        fuse_reply_err(req, EIO);
        return;
    }
    uint32_t pos = 0;
    int32_t e = rb_i32(resp, &pos);
    if (e != 0) {
        fuse_reply_err(req, -e);
        free(resp);
        return;
    }
    struct statvfs sv;
    memset(&sv, 0, sizeof(sv));
    sv.f_blocks = rb_u64(resp, &pos);
    sv.f_bfree = rb_u64(resp, &pos);
    sv.f_bavail = rb_u64(resp, &pos);
    sv.f_files = rb_u64(resp, &pos);
    sv.f_ffree = rb_u64(resp, &pos);
    sv.f_bsize = rb_u32(resp, &pos);
    sv.f_namemax = rb_u32(resp, &pos);
    sv.f_frsize = rb_u32(resp, &pos);
    free(resp);
    fuse_reply_statfs(req, &sv);
}

static void ec_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    (void) ino;
    struct etcfs_context *ctx = fuse_req_userdata(req);
    fi->fh = ++ctx->next_fh;
    fi->direct_io = 1;
    fi->keep_cache = 0;
    fuse_reply_open(req, fi);
}

static void ec_opendir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    (void) ino;
    struct etcfs_context *ctx = fuse_req_userdata(req);
    fi->fh = ++ctx->next_fh;
    fuse_reply_open(req, fi);
}

static void ec_create(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode,
                      struct fuse_file_info *fi)
{
    size_t nlen = strlen(name);
    uint8_t payload[20 + 256];
    uint32_t off = 0;
    off += wb_u64(payload + off, parent);
    off += wb_u32(payload + off, (uint32_t) nlen);
    memcpy(payload + off, name, nlen);
    off += (uint32_t) nlen;
    off += wb_u32(payload + off, (uint32_t) mode);
    off += wb_u32(payload + off, (uint32_t) (fi ? fi->flags : 0));
    off += wb_u32(payload + off, 022);

    uint8_t *resp;
    uint32_t rlen;
    if (ipc_sync(FD(ctx), IPC_OP_CREATE, payload, off, &resp, &rlen) < 0) {
        fuse_reply_err(req, EIO);
        return;
    }
    uint32_t pos = 0;
    int32_t e = rb_i32(resp, &pos);
    if (e != 0) {
        fuse_reply_err(req, -e);
        free(resp);
        return;
    }
    struct fuse_entry_param ep;
    memset(&ep, 0, sizeof(ep));
    ep.ino = rb_u64(resp, &pos);
    struct etcfs_attr a;
    rb_attr(resp, &pos, &a);
    ep.entry_timeout = (double) rb_u32(resp, &pos);
    ep.attr_timeout = (double) rb_u32(resp, &pos);
    fill_stat(&ep.attr, &a);
    free(resp);
    struct etcfs_context *ctx = fuse_req_userdata(req);
    fi->fh = ++ctx->next_fh;
    fi->direct_io = 1;
    fi->keep_cache = 0;
    fuse_reply_create(req, &ep, fi);
}

static void ec_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode)
{
    size_t nlen = strlen(name);
    uint8_t payload[20 + 256];
    uint32_t off = 0;
    off += wb_u64(payload + off, parent);
    off += wb_u32(payload + off, (uint32_t) nlen);
    memcpy(payload + off, name, nlen);
    off += (uint32_t) nlen;
    off += wb_u32(payload + off, (uint32_t) mode);
    off += wb_u32(payload + off, 022);

    uint8_t *resp;
    uint32_t rlen;
    if (ipc_sync(FD(ctx), IPC_OP_MKDIR, payload, off, &resp, &rlen) < 0) {
        fuse_reply_err(req, EIO);
        return;
    }
    uint32_t pos = 0;
    int32_t e = rb_i32(resp, &pos);
    if (e != 0) {
        fuse_reply_err(req, -e);
        free(resp);
        return;
    }
    struct fuse_entry_param ep;
    memset(&ep, 0, sizeof(ep));
    ep.ino = rb_u64(resp, &pos);
    struct etcfs_attr a;
    rb_attr(resp, &pos, &a);
    ep.entry_timeout = (double) rb_u32(resp, &pos);
    ep.attr_timeout = (double) rb_u32(resp, &pos);
    fill_stat(&ep.attr, &a);
    free(resp);
    fuse_reply_entry(req, &ep);
}

static void ec_unlink(fuse_req_t req, fuse_ino_t parent, const char *name)
{
    size_t nlen = strlen(name);
    uint8_t payload[12 + 256];
    uint32_t off = 0;
    off += wb_u64(payload + off, parent);
    off += wb_u32(payload + off, (uint32_t) nlen);
    memcpy(payload + off, name, nlen);
    off += (uint32_t) nlen;

    uint8_t *resp;
    uint32_t rlen;
    if (ipc_sync(FD(ctx), IPC_OP_UNLINK, payload, off, &resp, &rlen) < 0) {
        fuse_reply_err(req, EIO);
        return;
    }
    int32_t e = 0;
    if (rlen >= 4)
        e = (int32_t) ((uint32_t) resp[0] << 24 | (uint32_t) resp[1] << 16 |
                       (uint32_t) resp[2] << 8 | (uint32_t) resp[3]);
    free(resp);
    fuse_reply_err(req, e != 0 ? -e : 0);
}

static void ec_rmdir(fuse_req_t req, fuse_ino_t parent, const char *name)
{
    size_t nlen = strlen(name);
    uint8_t payload[12 + 256];
    uint32_t off = 0;
    off += wb_u64(payload + off, parent);
    off += wb_u32(payload + off, (uint32_t) nlen);
    memcpy(payload + off, name, nlen);
    off += (uint32_t) nlen;

    uint8_t *resp;
    uint32_t rlen;
    if (ipc_sync(FD(ctx), IPC_OP_RMDIR, payload, off, &resp, &rlen) < 0) {
        fuse_reply_err(req, EIO);
        return;
    }
    int32_t e = 0;
    if (rlen >= 4)
        e = (int32_t) ((uint32_t) resp[0] << 24 | (uint32_t) resp[1] << 16 |
                       (uint32_t) resp[2] << 8 | (uint32_t) resp[3]);
    free(resp);
    fuse_reply_err(req, e != 0 ? -e : 0);
}

static void ec_rename(fuse_req_t req, fuse_ino_t old_parent, const char *old_name,
                      fuse_ino_t new_parent, const char *new_name, unsigned int flags)
{
    size_t olen = strlen(old_name), nlen = strlen(new_name);
    uint8_t payload[40 + 512];
    uint32_t off = 0;
    off += wb_u64(payload + off, old_parent);
    off += wb_u32(payload + off, (uint32_t) olen);
    memcpy(payload + off, old_name, olen);
    off += (uint32_t) olen;
    off += wb_u64(payload + off, new_parent);
    off += wb_u32(payload + off, (uint32_t) nlen);
    memcpy(payload + off, new_name, nlen);
    off += (uint32_t) nlen;
    off += wb_u32(payload + off, flags);

    uint8_t *resp;
    uint32_t rlen;
    if (ipc_sync(FD(ctx), IPC_OP_RENAME, payload, off, &resp, &rlen) < 0) {
        fuse_reply_err(req, EIO);
        return;
    }
    int32_t e = 0;
    if (rlen >= 4)
        e = (int32_t) ((uint32_t) resp[0] << 24 | (uint32_t) resp[1] << 16 |
                       (uint32_t) resp[2] << 8 | (uint32_t) resp[3]);
    free(resp);
    fuse_reply_err(req, e != 0 ? -e : 0);
}

static void ec_write(fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size, off_t off,
                     struct fuse_file_info *fi)
{
    (void) fi;
    uint8_t *payload = malloc(20 + size);
    uint32_t pos = 0;
    pos += wb_u64(payload + pos, ino);
    pos += wb_u64(payload + pos, (uint64_t) off);
    pos += wb_u32(payload + pos, (uint32_t) size);
    memcpy(payload + pos, buf, size);
    pos += (uint32_t) size;

    uint8_t *resp;
    uint32_t rlen;
    int ret = ipc_sync(FD(ctx), IPC_OP_WRITE, payload, pos, &resp, &rlen);
    free(payload);
    if (ret < 0) {
        fuse_reply_err(req, EIO);
        return;
    }
    uint32_t p = 0;
    int32_t e = rb_i32(resp, &p);
    uint32_t written = rb_u32(resp, &p);
    free(resp);
    if (e != 0) {
        fuse_reply_err(req, -e);
        return;
    }
    fuse_reply_write(req, (size_t) written);
}

/* Every field SETATTR can change goes over the wire; to_set says which of them
 * the kernel actually means.  Sending only st_size is what made chmod, chown
 * and utimensat succeed while changing nothing. */
static void ec_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr, int to_set,
                       struct fuse_file_info *fi)
{
    uint8_t payload[64];
    uint32_t off = 0;
    off += wb_u64(payload + off, ino);
    off += wb_u64(payload + off, (fi ? fi->fh : 0));
    off += wb_u32(payload + off, (uint32_t) to_set);
    off += wb_u64(payload + off, (uint64_t) attr->st_size);
    off += wb_u32(payload + off, (uint32_t) attr->st_mode);
    off += wb_u32(payload + off, (uint32_t) attr->st_uid);
    off += wb_u32(payload + off, (uint32_t) attr->st_gid);
    /* Only whole seconds are stored, so the nanosecond halves are dropped
     * here rather than sent and discarded on the other side. */
    off += wb_u64(payload + off, (uint64_t) attr->st_atime);
    off += wb_u64(payload + off, (uint64_t) attr->st_mtime);
    off += wb_u64(payload + off, (uint64_t) attr->st_ctime);

    uint8_t *resp;
    uint32_t rlen;
    if (ipc_sync(FD(ctx), IPC_OP_SETATTR, payload, off, &resp, &rlen) < 0) {
        fuse_reply_err(req, EIO);
        return;
    }
    uint32_t pos = 0;
    int32_t e = rb_i32(resp, &pos);
    if (e != 0) {
        fuse_reply_err(req, -e);
        free(resp);
        return;
    }
    struct etcfs_attr a;
    rb_attr(resp, &pos, &a);
    uint32_t to = rb_u32(resp, &pos);
    free(resp);
    struct stat st;
    fill_stat(&st, &a);
    fuse_reply_attr(req, &st, (double) to);
}

static void ec_symlink(fuse_req_t req, const char *target, fuse_ino_t parent, const char *name)
{
    size_t nlen = strlen(name), tlen = strlen(target);
    uint8_t payload[20 + 512];
    uint32_t off = 0;
    off += wb_u64(payload + off, parent);
    off += wb_u32(payload + off, (uint32_t) nlen);
    memcpy(payload + off, name, nlen);
    off += (uint32_t) nlen;
    off += wb_u32(payload + off, (uint32_t) tlen);
    memcpy(payload + off, target, tlen);
    off += (uint32_t) tlen;

    uint8_t *resp;
    uint32_t rlen;
    if (ipc_sync(FD(ctx), IPC_OP_SYMLINK, payload, off, &resp, &rlen) < 0) {
        fuse_reply_err(req, EIO);
        return;
    }
    uint32_t pos = 0;
    int32_t e = rb_i32(resp, &pos);
    if (e != 0) {
        fuse_reply_err(req, -e);
        free(resp);
        return;
    }
    struct fuse_entry_param ep;
    memset(&ep, 0, sizeof(ep));
    ep.ino = rb_u64(resp, &pos);
    struct etcfs_attr a;
    rb_attr(resp, &pos, &a);
    ep.entry_timeout = (double) rb_u32(resp, &pos);
    ep.attr_timeout = (double) rb_u32(resp, &pos);
    fill_stat(&ep.attr, &a);
    free(resp);
    fuse_reply_entry(req, &ep);
}

static void ec_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t new_parent, const char *new_name)
{
    size_t nlen = strlen(new_name);
    uint8_t payload[24 + 256];
    uint32_t off = 0;
    off += wb_u64(payload + off, ino);
    off += wb_u64(payload + off, new_parent);
    off += wb_u32(payload + off, (uint32_t) nlen);
    memcpy(payload + off, new_name, nlen);
    off += (uint32_t) nlen;

    uint8_t *resp;
    uint32_t rlen;
    if (ipc_sync(FD(ctx), IPC_OP_LINK, payload, off, &resp, &rlen) < 0) {
        fuse_reply_err(req, EIO);
        return;
    }
    uint32_t pos = 0;
    int32_t e = rb_i32(resp, &pos);
    if (e != 0) {
        fuse_reply_err(req, -e);
        free(resp);
        return;
    }
    struct fuse_entry_param ep;
    memset(&ep, 0, sizeof(ep));
    ep.ino = rb_u64(resp, &pos);
    struct etcfs_attr a;
    rb_attr(resp, &pos, &a);
    ep.entry_timeout = (double) rb_u32(resp, &pos);
    ep.attr_timeout = (double) rb_u32(resp, &pos);
    fill_stat(&ep.attr, &a);
    free(resp);
    fuse_reply_entry(req, &ep);
}

static void ec_mknod(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, dev_t rdev)
{
    size_t nlen = strlen(name);
    uint8_t payload[24 + 256];
    uint32_t off = 0;
    off += wb_u64(payload + off, parent);
    off += wb_u32(payload + off, (uint32_t) nlen);
    memcpy(payload + off, name, nlen);
    off += (uint32_t) nlen;
    off += wb_u32(payload + off, (uint32_t) mode);
    off += wb_u32(payload + off, (uint32_t) rdev);

    uint8_t *resp;
    uint32_t rlen;
    if (ipc_sync(FD(ctx), IPC_OP_MKNOD, payload, off, &resp, &rlen) < 0) {
        fuse_reply_err(req, EIO);
        return;
    }
    uint32_t pos = 0;
    int32_t e = rb_i32(resp, &pos);
    if (e != 0) {
        fuse_reply_err(req, -e);
        free(resp);
        return;
    }
    struct fuse_entry_param ep;
    memset(&ep, 0, sizeof(ep));
    ep.ino = rb_u64(resp, &pos);
    struct etcfs_attr a;
    rb_attr(resp, &pos, &a);
    ep.entry_timeout = (double) rb_u32(resp, &pos);
    ep.attr_timeout = (double) rb_u32(resp, &pos);
    fill_stat(&ep.attr, &a);
    free(resp);
    fuse_reply_entry(req, &ep);
}

/* ---- no-op handlers ---- */

static void ec_release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    (void) ino;
    (void) fi;
    fuse_reply_err(req, 0);
}
static void ec_releasedir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    (void) ino;
    (void) fi;
    fuse_reply_err(req, 0);
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
static void ec_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                    struct fuse_file_info *fi)
{
    (void) fi;
    uint8_t payload[24];
    uint32_t p = 0;
    p += wb_u64(payload + p, ino);
    p += wb_u64(payload + p, (uint64_t) off);
    p += wb_u32(payload + p, (uint32_t) size);

    uint8_t *resp;
    uint32_t rlen;
    if (ipc_sync(FD(ctx), IPC_OP_READ, payload, p, &resp, &rlen) < 0) {
        fuse_reply_err(req, EIO);
        return;
    }
    uint32_t pos = 0;
    int32_t e = rb_i32(resp, &pos);
    if (e != 0) {
        fuse_reply_err(req, -e);
        free(resp);
        return;
    }
    uint32_t dataLen = rb_u32(resp, &pos);
    fuse_reply_buf(req, (const char *) (resp + pos), dataLen);
    free(resp);
}
static void ec_fallocate(fuse_req_t req, fuse_ino_t ino, int mode, off_t offset, off_t length,
                         struct fuse_file_info *fi)
{
    (void) ino;
    (void) mode;
    (void) offset;
    (void) length;
    (void) fi;
    fuse_reply_err(req, ENOSYS);
}

/* ---- op table ---- */

struct fuse_lowlevel_ops *etcfs_fuse_ops(void)
{
    static struct fuse_lowlevel_ops ops;
    memset(&ops, 0, sizeof(ops));
    ops.lookup = ec_lookup;
    ops.getattr = ec_getattr;
    ops.readdir = ec_readdir;
    ops.readdirplus = ec_readdirplus;
    ops.readlink = ec_readlink;
    ops.statfs = ec_statfs;
    ops.open = ec_open;
    ops.opendir = ec_opendir;
    ops.create = ec_create;
    ops.mkdir = ec_mkdir;
    ops.unlink = ec_unlink;
    ops.rmdir = ec_rmdir;
    ops.rename = ec_rename;
    ops.symlink = ec_symlink;
    ops.link = ec_link;
    ops.setattr = ec_setattr;
    ops.write = ec_write;
    ops.mknod = ec_mknod;
    ops.release = ec_release;
    ops.releasedir = ec_releasedir;
    ops.flush = ec_flush;
    ops.fsync = ec_fsync;
    ops.fsyncdir = ec_fsyncdir;
    ops.read = ec_read;
    ops.fallocate = ec_fallocate;
    /* getlk/setlk are deliberately left unset. libfuse: "if the locking
     * methods are not implemented, the kernel will still allow file locking
     * to work locally." Implementing them takes that job away from the
     * kernel, and the daemon granted every request, so fcntl() locks
     * excluded nothing -- not even two processes on the same node. Unset,
     * fcntl() gets the node-local enforcement flock() already had.
     * See docs/architecture/metadata/posix-lock-operations.md. */
    return &ops;
}
