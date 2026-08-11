/*
 * fuse.c — FUSE daemon core: session lifecycle, signal handling, IPC setup.
 *
 * Initialises the FUSE session, creates the Unix socket connection to the Go
 * metadata backend, and enters the libfuse event loop.  Actual FUSE operation
 * handlers live in ops.c.
 */

#include "fuse.h"
#include "ops.h"
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
#include <pthread.h>

#include <fuse3/fuse_lowlevel.h>

static void *notify_thread(void *arg)
{
    struct etcfs_context *ctx = (struct etcfs_context *) arg;
    const char *path = getenv("ETCFS_NOTIFY_SOCKET");
    if (!path)
        path = "/run/etcfuse/etcfuse-notify.sock";
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0)
        return NULL;

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, path, sizeof(addr.sun_path) - 1);
    if (connect(fd, (struct sockaddr *) &addr, sizeof(addr)) < 0) {
        close(fd);
        return NULL;
    }

    uint8_t hdr[12];
    while (read(fd, hdr, 12) == 12) {
        uint32_t typ = ((uint32_t) hdr[0] << 24) | ((uint32_t) hdr[1] << 16) |
                       ((uint32_t) hdr[2] << 8) | (uint32_t) hdr[3];
        uint64_t parent = ((uint64_t) hdr[4] << 56) | ((uint64_t) hdr[5] << 48) |
                          ((uint64_t) hdr[6] << 40) | ((uint64_t) hdr[7] << 32) |
                          ((uint64_t) hdr[8] << 24) | ((uint64_t) hdr[9] << 16) |
                          ((uint64_t) hdr[10] << 8) | (uint64_t) hdr[11];
        if (typ == 1) {
            char name[256];
            ssize_t n = read(fd, name, sizeof(name) - 1);
            if (n > 0) {
                name[n] = '\0';
                fuse_lowlevel_notify_inval_entry(ctx->notify_se, parent, name, strlen(name));
            }
        }
    }
    close(fd);
    return NULL;
}

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

/* ---- per-thread IPC connections ---- */

/*
 * Every FUSE worker thread gets its own connection to the metadata backend.
 * The wire protocol carries no request identifiers — a reply is whatever
 * arrives next on the socket — so a shared fd would let two threads interleave
 * their frames and take each other's replies.  One fd per thread keeps every
 * exchange in ops.c exactly as synchronous as it was when the daemon ran one
 * request at a time; the Go side already serves a goroutine per connection.
 */
static pthread_key_t ipc_fd_key;
static pthread_once_t ipc_fd_once = PTHREAD_ONCE_INIT;
static char ipc_socket_path[108];

/* The path every connection is made to: what etcfs_run resolved, falling back
 * to the environment so a worker can connect before (or without) the daemon
 * having run. */
static const char *ipc_socket(void)
{
    if (ipc_socket_path[0] != '\0')
        return ipc_socket_path;
    const char *env = getenv("ETCFS_IPC_SOCKET");
    return env ? env : "/tmp/etcfuse.sock";
}

static void ipc_fd_destroy(void *value)
{
    int fd = (int) (intptr_t) value;
    if (fd >= 0)
        close(fd);
}

static void ipc_fd_key_create(void)
{
    if (pthread_key_create(&ipc_fd_key, ipc_fd_destroy) != 0)
        etcfs_log(ETCFS_LOG_ERROR, "pthread_key_create for IPC connections failed");
}

int etcfs_ipc_fd(void)
{
    pthread_once(&ipc_fd_once, ipc_fd_key_create);

    /* A stored fd is offset by one so that a thread that has not connected yet
     * (NULL) stays distinguishable from one holding fd 0. */
    intptr_t stored = (intptr_t) pthread_getspecific(ipc_fd_key);
    if (stored > 0)
        return (int) (stored - 1);

    int fd = connect_to_meta(ipc_socket());
    if (fd < 0)
        return -1;
    if (pthread_setspecific(ipc_fd_key, (void *) (intptr_t) (fd + 1)) != 0) {
        etcfs_log(ETCFS_LOG_ERROR, "cannot store this thread's IPC connection");
        close(fd);
        return -1;
    }
    return fd;
}

/* ---- FUSE init callback ---- */

static void etcfs_init(void *userdata, struct fuse_conn_info *conn)
{
    (void) userdata;
    conn->want |= FUSE_CAP_READDIRPLUS;
    conn->want |= FUSE_CAP_ASYNC_READ;
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

    /* Kept for every worker thread to connect with; the one opened here only
     * proves the backend is reachable before the mountpoint is taken over. */
    snprintf(ipc_socket_path, sizeof(ipc_socket_path), "%s", socket_path);

    ipc_fd = connect_to_meta(socket_path);
    if (ipc_fd < 0)
        return -1;

    ctx->ipc_fd = ipc_fd;
    ctx->ipc_sync = 1; /* synchronous IPC mode */

    mountpoint = ctx->mountpoint;
    if (!mountpoint) {
        etcfs_log(ETCFS_LOG_ERROR, "mountpoint not set");
        return -1;
    }

    /* open block device if specified */
    if (ctx->volume_id) {
        ctx->block_fd = -1;
        struct etcfs_block_dev *block_dev = etcfs_block_open(ctx->volume_id);
        (void) block_dev; /* access through ctx->block_fd when I/O is implemented */
        etcfs_log(ETCFS_LOG_WARN, "block device not available (read-only mode)");
    }

    /* build FUSE args — mountpoint passed to fuse_session_mount, not in args */
    if (fuse_opt_add_arg(&args, "etcfuse") < 0)
        return -1;

    /* default_permissions hands permission checking to the kernel, which
     * evaluates the mode, uid and gid this daemon reports against the calling
     * process before the request is ever sent.  EtcFS deliberately implements
     * no access checks of its own: duplicating them in the daemon would mean a
     * second, divergent copy of rules the kernel already applies correctly.
     *
     * allow_other is required alongside it: the daemon runs as root, and
     * without allow_other FUSE restricts the mount to the mounting user only
     * — every other user, including one connecting over SSH to use the mount,
     * gets EACCES on the mountpoint itself regardless of default_permissions.
     * It is safe together with default_permissions because the kernel still
     * enforces per-file mode/uid/gid on every access; allow_other only lifts
     * the FUSE-level single-user restriction, it does not bypass Unix
     * permissions. */
    if (fuse_opt_add_arg(&args, "-o") < 0)
        return -1;
    if (fuse_opt_add_arg(&args, "default_permissions,allow_other") < 0)
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

    /* check for a stale FUSE mount from a previous daemon instance.
     * if the mountpoint is already occupied by a dead FUSE mount,
     * the kernel will reject or corrupt the new session.
     * scan /proc/mounts to detect this and force-unmount if needed. */
    {
        FILE *fp = fopen("/proc/mounts", "r");
        if (fp) {
            char line[1024];
            while (fgets(line, sizeof(line), fp)) {
                char dev[256], mp[256], fst[256];
                if (sscanf(line, "%255s %255s %255s", dev, mp, fst) == 3) {
                    if (strcmp(mp, mountpoint) == 0 &&
                        (strcmp(fst, "fuse") == 0 || strcmp(fst, "fuseblk") == 0)) {
                        etcfs_log(ETCFS_LOG_WARN,
                                  "stale FUSE mount at %s (previous daemon crash?), cleaning up",
                                  mountpoint);
                        fclose(fp);
                        /* force-unmount the stale mount */
                        char cmd[512];
                        snprintf(cmd, sizeof(cmd), "fusermount -uz %s 2>/dev/null", mountpoint);
                        {
                            int rc = system(cmd);
                            (void) rc;
                        }
                        snprintf(cmd, sizeof(cmd), "umount -l %s 2>/dev/null", mountpoint);
                        {
                            int rc = system(cmd);
                            (void) rc;
                        }
                        goto after_cleanup;
                    }
                }
            }
            fclose(fp);
        }
    }
after_cleanup:

    /* mount — retry up to 5 times if the kernel hasn't released a stale
     * FUSE superblock from a just-cleaned-up previous daemon instance. */
    {
        int mounted = -1;
        for (int attempt = 0; attempt < 5; attempt++) {
            if (fuse_session_mount(se, mountpoint) == 0) {
                mounted = 0;
                break;
            }
            etcfs_log(ETCFS_LOG_WARN, "fuse_session_mount attempt %d failed, retrying (2s)",
                      attempt + 1);
            sleep(2);
        }
        if (mounted != 0) {
            etcfs_log(ETCFS_LOG_ERROR, "fuse_session_mount failed after 5 attempts");
            fuse_remove_signal_handlers(se);
            fuse_session_destroy(se);
            return -1;
        }
    }

    etcfs_log(ETCFS_LOG_INFO, "EtcFS mounted at %s", mountpoint);

    ctx->notify_se = se;
    pthread_t ntid;
    pthread_create(&ntid, NULL, notify_thread, ctx);

    /*
     * Multi-threaded: each worker takes its own IPC connection from
     * etcfs_ipc_fd(), so the synchronous exchanges in ops.c never share a
     * socket.  One slow etcd operation now blocks the request that issued it
     * rather than the whole mount.
     *
     * clone_fd gives every worker its own /dev/fuse descriptor, which is what
     * stops the kernel-side read of the request queue from becoming the next
     * serialisation point once the IPC one is gone.
     */
    {
        struct fuse_loop_config loop_config;
        memset(&loop_config, 0, sizeof(loop_config));
        loop_config.clone_fd = 1;
        loop_config.max_idle_threads = ETCFS_MAX_THREADS;
        ret = fuse_session_loop_mt(se, &loop_config);
    }

    /* cleanup */
    fuse_session_unmount(se);
    fuse_session_destroy(se);
    close(ipc_fd);
    etcfs_log(ETCFS_LOG_INFO, "EtcFS unmounted");
    return ret;
}
