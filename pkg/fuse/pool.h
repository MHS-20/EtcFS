#ifndef ETCFS_FUSE_POOL_H
#define ETCFS_FUSE_POOL_H

#define FUSE_USE_VERSION FUSE_MAKE_VERSION(3, 12)

/*
 * pool.h — async IPC worker for EtcFS FUSE daemon.
 *
 * FUSE worker threads must never block on external I/O.  Instead of calling
 * the synchronous ipc_call() from within a FUSE handler, each handler
 * submits a request to the IPC worker thread and returns immediately.
 * The IPC worker processes the request (send → receive on the Unix socket)
 * and invokes the response callback, which calls the appropriate
 * fuse_reply_* function on the saved fuse_req_t.
 */

#include <fuse3/fuse_lowlevel.h>
#include <stdint.h>

/* Opaque IPC worker handle */
struct ipc_worker;

/* Response callback: called by the IPC worker when a response arrives.
 *   req   — the original FUSE request
 *   error — 0 on success, negative errno on failure
 *   resp  — response data (only valid if error==0), caller must free
 *   rlen  — response data length
 *   data  — user data passed when submitting the request
 */
typedef void (*ipc_resp_cb)(fuse_req_t req, int32_t error,
                            uint8_t *resp, uint32_t rlen, void *data);

/*
 * Create an IPC worker thread connected to the given socket.
 * The worker owns exclusive access to the socket — no mutex needed.
 */
struct ipc_worker *ipc_worker_new(int fd);

/*
 * Submit a request to the IPC worker.
 * The callback will be invoked from the IPC worker thread when the
 * response arrives.  Ownership of payload is transferred to the worker
 * (it will be freed).  The callback takes ownership of resp.
 */
int ipc_worker_submit(struct ipc_worker *w, fuse_req_t req,
                      uint16_t opcode, uint8_t *payload, uint32_t plen,
                      ipc_resp_cb cb, void *cb_data);

/*
 * Shut down the IPC worker.  Blocks until pending requests are drained
 * or the worker thread exits cleanly.
 */
void ipc_worker_destroy(struct ipc_worker *w);

#endif /* ETCFS_FUSE_POOL_H */
