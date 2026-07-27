/*
 * etcfuse — EtcFS FUSE daemon (C component).
 *
 * This binary handles:
 *   - FUSE kernel protocol (via libfuse low-level API)
 *   - Raw block device I/O (O_DIRECT)
 *   - Communication with the Go metadata backend (Unix socket + protobuf)
 *
 * The Go binary (etcfuse-meta) handles all etcd operations.
 *
 * Usage:
 *   etcfuse [--socket=/tmp/etcfuse.sock] [--volume-id=vol-xxx] <mountpoint>
 */

#include <errno.h>
#include <getopt.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define ETCFS_VERSION "0.1.0"

#include "pkg/fuse/fuse.h"
#include "pkg/fuse/ops.h"

static void
print_usage(const char *prog)
{
    fprintf(stderr,
        "EtcFS FUSE daemon v" ETCFS_VERSION "\n"
        "\n"
        "Usage: %s [options] <mountpoint>\n"
        "\n"
        "Options:\n"
        "  --socket=PATH      Unix socket path for Go metadata backend\n"
        "                     (default: /tmp/etcfuse.sock)\n"
        "  --volume-id=ID     EBS volume ID or block device path\n"
        "                     (example: vol-0abcdef1234567890 or /dev/nvme1n1)\n"
        "  --node-id=NAME     Node identifier (default: hostname)\n"
        "  --log-level=N      Log level: 0=error, 1=warn, 2=info, 3=debug\n"
        "                     (default: 2)\n"
        "  -h, --help         Show this help\n"
        "\n"
        "The Go metadata backend (etcfuse-meta) must be running before\n"
        "this daemon is started.\n",
        prog);
}

int
main(int argc, char *argv[])
{
    struct etcfs_context ctx;
    const char *socket_path = "/tmp/etcfuse.sock";
    const char *volume_id = NULL;
    const char *node_id = NULL;
    const char *mountpoint = NULL;
    int         log_level = ETCFS_LOG_INFO;

    memset(&ctx, 0, sizeof(ctx));

    /* parse arguments */
    static struct option long_opts[] = {
        {"socket",    required_argument, 0, 's'},
        {"volume-id", required_argument, 0, 'v'},
        {"node-id",   required_argument, 0, 'n'},
        {"log-level", required_argument, 0, 'l'},
        {"help",      no_argument,       0, 'h'},
        {0, 0, 0, 0}
    };

    int opt;
    while ((opt = getopt_long(argc, argv, "s:v:n:l:h", long_opts, NULL)) != -1) {
        switch (opt) {
        case 's': socket_path = optarg; break;
        case 'v': volume_id = optarg;   break;
        case 'n': node_id = optarg;     break;
        case 'l': log_level = atoi(optarg); break;
        case 'h': print_usage(argv[0]); return 0;
        default:  print_usage(argv[0]); return 1;
        }
    }

    if (optind >= argc) {
        fprintf(stderr, "ERROR: mountpoint required\n");
        print_usage(argv[0]);
        return 1;
    }
    mountpoint = argv[optind];

    /* node ID: use --node-id, then hostname */
    if (!node_id) {
        static char host[256];
        if (gethostname(host, sizeof(host)) == 0)
            node_id = host;
        else
            node_id = "unknown";
    }

    etcfs_set_log_level(log_level);
    etcfs_log(ETCFS_LOG_INFO, "EtcFS FUSE daemon v" ETCFS_VERSION " starting");
    etcfs_log(ETCFS_LOG_INFO, "  mount point: %s", mountpoint);
    etcfs_log(ETCFS_LOG_INFO, "  ipc socket:  %s", socket_path);
    etcfs_log(ETCFS_LOG_INFO, "  volume id:   %s", volume_id ? volume_id : "(none)");
    etcfs_log(ETCFS_LOG_INFO, "  node id:     %s", node_id);

    /* set environment for IPC socket (read by fuse.c) */
    setenv("ETCFS_IPC_SOCKET", socket_path, 1);

    /* populate context */
    ctx.mountpoint = (char *)mountpoint;
    ctx.volume_id  = (char *)volume_id;
    ctx.node_id    = (char *)node_id;

    /* run the FUSE daemon */
    int ret = etcfs_run(&ctx);
    if (ret != 0)
        etcfs_log(ETCFS_LOG_ERROR, "daemon exited with error %d", ret);

    return ret;
}
