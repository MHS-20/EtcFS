/*
 * pool.c — async IPC worker thread for EtcFS.
 *
 * A single dedicated thread processes IPC requests sequentially.  FUSE
 * worker threads submit requests and return immediately.  The IPC worker
 * does the socket I/O and invokes the callback, which calls fuse_reply_*.
 */

#include "pool.h"
#include "fuse.h"

#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* ---- queue structures ---- */

struct ipc_req {
    fuse_req_t   req;       /* saved FUSE request */
    uint16_t     opcode;
    uint8_t     *payload;
    uint32_t     plen;
    ipc_resp_cb  cb;
    void        *cb_data;
    struct ipc_req *next;
};

struct ipc_worker {
    int              fd;          /* Unix socket to Go backend */
    pthread_t        thread;
    pthread_mutex_t  mu;
    pthread_cond_t   cond;
    struct ipc_req  *head;        /* request queue */
    struct ipc_req  *tail;
    int              stop;         /* shutdown flag */
};

/* ---- raw socket I/O (single-threaded — no mutex needed) ---- */

static int send_full(int fd, const void *buf, size_t len)
{
    const char *p = buf;
    while (len > 0) {
        ssize_t n = write(fd, p, len);
        if (n < 0) { if (errno == EINTR) continue; etcfs_log(ETCFS_LOG_ERROR, "send_full write error fd=%d errno=%d", fd, errno); return -1; }
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

/* ---- IPC exchange (runs on worker thread) ---- */

static int do_ipc_exchange(int fd, uint16_t opcode,
                           const void *payload, uint32_t plen,
                           uint8_t **resp_payload, uint32_t *resp_len)
{
    uint8_t hdr[6];
    hdr[0] = (uint8_t)(opcode >> 8);
    hdr[1] = (uint8_t)opcode;
    hdr[2] = (uint8_t)(plen >> 24);
    hdr[3] = (uint8_t)(plen >> 16);
    hdr[4] = (uint8_t)(plen >> 8);
    hdr[5] = (uint8_t)plen;

    if (send_full(fd, hdr, 6) < 0) return -1;
    if (plen > 0 && send_full(fd, payload, plen) < 0) return -1;

    uint8_t rhdr[4];
    if (recv_full(fd, rhdr, 4) < 0) return -1;
    uint32_t rlen = ((uint32_t)rhdr[0] << 24) | ((uint32_t)rhdr[1] << 16) |
                    ((uint32_t)rhdr[2] << 8)  |  (uint32_t)rhdr[3];

    uint8_t *rbuf = malloc(rlen > 0 ? rlen : 1);
    if (!rbuf) return -1;
    if (rlen > 0 && recv_full(fd, rbuf, rlen) < 0) { free(rbuf); return -1; }

    *resp_payload = rbuf;
    *resp_len     = rlen;
    return 0;
}

/* ---- worker thread loop ---- */

static void *ipc_worker_thread(void *arg)
{
    struct ipc_worker *w = (struct ipc_worker *)arg;
    etcfs_log(ETCFS_LOG_INFO, "IPC worker thread started");

    for (;;) {
        pthread_mutex_lock(&w->mu);

        /* wait for work or shutdown */
        while (w->head == NULL && !w->stop)
            pthread_cond_wait(&w->cond, &w->mu);

        if (w->stop && w->head == NULL) {
            pthread_mutex_unlock(&w->mu);
            break;
        }

        /* dequeue one request */
        struct ipc_req *req = w->head;
        w->head = req->next;
        if (!w->head) w->tail = NULL;
        pthread_mutex_unlock(&w->mu);

        etcfs_log(ETCFS_LOG_DEBUG, "IPC worker processing op=%u plen=%u", req->opcode, req->plen);

    /* do IPC */
    uint8_t  *resp = NULL;
    uint32_t  rlen = 0;
    etcfs_log(ETCFS_LOG_INFO, "IPC worker exchange start op=%u fd=%d plen=%u",
              req->opcode, w->fd, req->plen);
    int ret = do_ipc_exchange(w->fd, req->opcode, req->payload, req->plen, &resp, &rlen);
    etcfs_log(ETCFS_LOG_INFO, "IPC worker exchange done ret=%d rlen=%u", ret, rlen);

        /* invoke callback */
        int32_t err = 0;
        if (ret < 0)
            err = -(int32_t)EIO;
        else if (rlen >= 4) {
            /* first 4 bytes of every response is an error code */
            err = (int32_t)((uint32_t)resp[0] << 24 | (uint32_t)resp[1] << 16 |
                            (uint32_t)resp[2] << 8  | (uint32_t)resp[3]);
        }

        req->cb(req->req, err, resp, rlen, req->cb_data);

        /* cleanup */
        free(req->payload);
        free(req);
    }

    return NULL;
}

/* ---- public API ---- */

struct ipc_worker *ipc_worker_new(int fd)
{
    struct ipc_worker *w = calloc(1, sizeof(*w));
    if (!w) return NULL;

    w->fd = fd;
    pthread_mutex_init(&w->mu, NULL);
    pthread_cond_init(&w->cond, NULL);

    if (pthread_create(&w->thread, NULL, ipc_worker_thread, w) != 0) {
        pthread_mutex_destroy(&w->mu);
        pthread_cond_destroy(&w->cond);
        free(w);
        return NULL;
    }

    return w;
}

int ipc_worker_submit(struct ipc_worker *w, fuse_req_t req,
                      uint16_t opcode, uint8_t *payload, uint32_t plen,
                      ipc_resp_cb cb, void *cb_data)
{
    struct ipc_req *r = calloc(1, sizeof(*r));
    if (!r) return -1;

    r->req     = req;
    r->opcode  = opcode;
    r->payload = payload;
    r->plen    = plen;
    r->cb      = cb;
    r->cb_data = cb_data;

    pthread_mutex_lock(&w->mu);

    /* enqueue */
    if (w->tail) {
        w->tail->next = r;
    } else {
        w->head = r;
    }
    w->tail = r;

    pthread_cond_signal(&w->cond);
    pthread_mutex_unlock(&w->mu);
    return 0;
}

void ipc_worker_destroy(struct ipc_worker *w)
{
    if (!w) return;

    pthread_mutex_lock(&w->mu);
    w->stop = 1;
    pthread_cond_signal(&w->cond);
    pthread_mutex_unlock(&w->mu);

    pthread_join(w->thread, NULL);

    /* drain remaining requests (shouldn't be any) */
    while (w->head) {
        struct ipc_req *r = w->head;
        w->head = r->next;
        free(r->payload);
        free(r);
    }

    pthread_mutex_destroy(&w->mu);
    pthread_cond_destroy(&w->cond);
    free(w);
}
