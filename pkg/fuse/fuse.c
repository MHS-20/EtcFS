/*
 * fuse.c — FUSE daemon core: session lifecycle, signal handling, IPC setup.
 *
 * Initialises the FUSE session, creates the Unix socket connection to the Go
 * metadata backend, and enters the libfuse event loop.  Actual FUSE operation
 * handlers live in ops.c.
 */

#include "fuse.h"
#include "ops.h"
#include "pool.h"
#include "../block/block.h"

#include <errno.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <fuse3/fuse_lowlevel.h>

/* ---- logging ---- */

static int etcfs_log_level = ETCFS_LOG_INFO;

void etcfs_set_log_level(int level);
void etcfs_set_log_level(int level)
{
    etcfs_log_level = level;
}

void etcfs_log(int level, const char *fmt, ...)
{
    if (level > etcfs_log_level)
        return;

    const char *prefix;
    switch (level) {
    case ETCFS_LOG_ERROR:
        prefix = "ERROR";
        break;
    case ETCFS_LOG_WARN:
        prefix = "WARN";
        break;
    case ETCFS_LOG_INFO:
        prefix = "INFO";
        break;
    case ETCFS_LOG_DEBUG:
        prefix = "DEBUG";
        break;
    default:
        prefix = "???";
        break;
    }

    fprintf(stderr, "[etcfuse] %s: ", prefix);
    va_list ap;
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
    fprintf(stderr, "\n");
}

/* ---- IPC connection to Go backend ---- */

static int connect_to_meta(const char *socket_path)
{
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        etcfs_log(ETCFS_LOG_ERROR, "socket: %s", strerror(errno));
        return -1;
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, socket_path, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *) &addr, sizeof(addr)) < 0) {
        etcfs_log(ETCFS_LOG_ERROR, "connect to %s: %s", socket_path, strerror(errno));
        close(fd);
        return -1;
    }

    etcfs_log(ETCFS_LOG_INFO, "connected to metadata backend at %s fd=%d", socket_path, fd);
    return fd;
}

/* ---- FUSE init callback ---- */

static void etcfs_init(void *userdata, struct fuse_conn_info *conn)
{
    (void) userdata;
    (void) conn;
}

/* ---- main entry ---- */

int etcfs_run(struct etcfs_context *ctx)
{
    struct fuse_args args = FUSE_ARGS_INIT(0, NULL);
    struct fuse_session *se;
    char *mountpoint;
    int ipc_fd;
    int ret;

    /* get the FUSE op table (populated in ops.c) */
    struct fuse_lowlevel_ops *ops = etcfs_fuse_ops();

    /* connect to Go metadata backend */
    const char *socket_path = getenv("ETCFS_IPC_SOCKET");
    if (!socket_path)
        socket_path = "/tmp/etcfuse.sock";

    ipc_fd = connect_to_meta(socket_path);
    if (ipc_fd < 0)
        return -1;

    ctx->ipc_fd = ipc_fd;
    ctx->ipc_sync = 1;  /* synchronous IPC mode */

    mountpoint = ctx->mountpoint;
    if (!mountpoint) {
        etcfs_log(ETCFS_LOG_ERROR, "mountpoint not set");
        return -1;
    }

    /* open block device if specified */
    if (ctx->volume_id) {
        ctx->block_fd = -1;
        struct etcfs_block_dev *block_dev = etcfs_block_open(ctx->volume_id);
        (void) block_dev; /* Phase 0: access through ctx->block_fd when I/O is implemented */
        etcfs_log(ETCFS_LOG_WARN, "block device not available (read-only mode)");
    }

    /* build FUSE args — mountpoint passed to fuse_session_mount, not in args */
    if (fuse_opt_add_arg(&args, "etcfuse") < 0)
        return -1;

    /* register init callback */
    ops->init = etcfs_init;

    /* create session */
    se = fuse_session_new(&args, ops, sizeof(*ops), ctx);
    if (!se) {
        etcfs_log(ETCFS_LOG_ERROR, "fuse_session_new failed");
        fuse_opt_free_args(&args);
        return -1;
    }

    fuse_opt_free_args(&args);

    /* mount */
    if (fuse_session_mount(se, mountpoint) != 0) {
        etcfs_log(ETCFS_LOG_ERROR, "fuse_session_mount failed");
        fuse_remove_signal_handlers(se);
        fuse_session_destroy(se);
        return -1;
    }

    etcfs_log(ETCFS_LOG_INFO, "EtcFS mounted at %s", mountpoint);

    /* enter event loop (single-threaded with synchronous IPC) */
    ret = fuse_session_loop(se);

    /* cleanup */
    fuse_session_unmount(se);
    fuse_session_destroy(se);
    close(ipc_fd);
    etcfs_log(ETCFS_LOG_INFO, "EtcFS unmounted");
    return ret;
}
