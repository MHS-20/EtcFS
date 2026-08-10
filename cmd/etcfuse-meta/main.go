/*
Package main is the EtcFS metadata backend binary.

It serves a binary protocol over a Unix domain socket, receiving FUSE
operation requests from the C daemon (etcfuse) and translating them into etcd
transactions, leases, and watches.

The binary also runs:
  - The membership lease heartbeat (keepalive to etcd)
  - The self-fencing watchdog (lease health → stop if expired)
  - The etcd watch multiplexer (fan-out directory watches to the C side)

Usage:

	etcfuse-meta \
	  --listen=unix:///tmp/etcfuse.sock \
	  --etcd-endpoints=https://10.0.0.1:2379,https://10.0.0.2:2379,https://10.0.0.3:2379 \
	  --etcd-cert=/etc/etcfuse/client.crt \
	  --etcd-key=/etc/etcfuse/client.key \
	  --etcd-ca=/etc/etcfuse/ca.crt \
	  --node-id=etcfuse-node-1 \
	  --lease-ttl=10s
*/
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/MHS-20/EtcFS/internal/config"
	"github.com/MHS-20/EtcFS/internal/ipc"
	"github.com/MHS-20/EtcFS/pkg/blockio"
	"github.com/MHS-20/EtcFS/pkg/fencing"
	"github.com/MHS-20/EtcFS/pkg/fsck"
	"github.com/MHS-20/EtcFS/pkg/fsinfo"
	"github.com/MHS-20/EtcFS/pkg/metadata"
	"github.com/MHS-20/EtcFS/pkg/metrics"
	"github.com/MHS-20/EtcFS/pkg/scrub"
)

func main() {
	cfg := config.Parse()

	if cfg.ShowVersion {
		fmt.Println("etcfuse-meta", config.Version)
		os.Exit(0)
	}

	log := config.NewLogger(cfg.LogLevel)

	log.Info("etcfuse-meta starting", "version", config.Version)
	log.Info("listening", "socket", cfg.ListenAddr)
	log.Info("etcd", "endpoints", cfg.EtcdEndpoints)
	log.Info("node", "id", cfg.NodeID)
	log.Info("lease", "ttl", cfg.LeaseTTL)

	// Resolve before anything opens the device — both the data path and the
	// NVMe fencer must agree on which device this volume is right now, and a
	// stale path from a previous attachment would point at the wrong disk.
	// Fatal rather than falling back to --block-device: the fallback is what
	// silently opens someone else's volume.
	if cfg.VolumeID != "" {
		path, rerr := blockio.ResolvePath(cfg.VolumeID)
		if rerr != nil {
			log.Fatal("cannot resolve volume to a block device",
				"volume_id", cfg.VolumeID, "error", rerr)
		}
		log.Info("volume resolved", "volume_id", cfg.VolumeID, "path", path)
		cfg.BlockDevice = path
	}

	// Connect to etcd with aggressive failover
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:            cfg.EtcdEndpoints,
		DialTimeout:          3 * time.Second,
		DialKeepAliveTime:    1 * time.Second,
		DialKeepAliveTimeout: 1 * time.Second,
		PermitWithoutStream:  true,
		AutoSyncInterval:     30 * time.Second,
		TLS:                  cfg.EtcdTLSConfig(),
	})
	if err != nil {
		log.Fatal("cannot connect to etcd", "error", err)
	}
	defer func() { _ = etcdCli.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Membership: register this node with a lease-backed key
	membership := metadata.NewMembership(etcdCli, cfg.NodeID, cfg.ClusterName, cfg.LeaseTTL)
	// Recorded in the membership key so a peer can detach the shared volume
	// from this node after its lease expires — at which point the key is gone
	// and this node can no longer be asked for it.
	membership.SetInstanceID(cfg.EC2InstanceID)
	membership.SetLogger(log)

	// Metadata store: wraps etcd client with schema-aware helpers
	store := metadata.NewStore(etcdCli, cfg.NodeID)

	if cfg.RunFsck {
		chk := fsck.New(store)
		findings := chk.Run(ctx)
		fmt.Printf("fsck: %d errors, %d warnings\n", chk.ErrorCount(), chk.WarningCount())
		for _, f := range findings {
			fmt.Printf("  [%s] %s\n", f.Level, f.Message)
		}
		os.Exit(0)
	}
	if cfg.RunInfo {
		info, _ := fsinfo.Collect(ctx, store)
		fmt.Println(info.String())
		os.Exit(0)
	}

	// Self-fencing watchdog
	watchdog := fencing.NewWatchdog(membership, cfg.LeaseTTL)

	// IPC service: handles FUSE op requests from the C daemon
	svc := ipc.NewService(store, membership, watchdog, log)

	// Establish this node's fencing generation before serving any request, then
	// install it as the store-wide guard so namespace mutations are covered too,
	// not only extent writes.  A later bump by the fencing controller stops this
	// node from mutating anything.
	//
	// Fatal on failure: without the generation, every guarded transaction fails
	// closed, so the daemon could not serve writes anyway — exiting reports the
	// real cause instead of an unexplained EIO on every mutation.
	if err := svc.InitGeneration(ctx); err != nil {
		log.Fatal("cannot initialise fencing generation", "error", err)
	}
	svc.InstallStoreGuard()

	if cfg.BlockDevice != "" {
		openDevice := blockio.Open
		if cfg.AllowBufferedIO {
			openDevice = blockio.OpenBuffered
		}
		dev, err := openDevice(cfg.BlockDevice)
		if err != nil {
			log.Fatal("cannot open block device", "path", cfg.BlockDevice, "error", err)
		}
		if !dev.IsDirect() {
			log.Warn("block device opened WITHOUT O_DIRECT: writes are served from this node's "+
				"page cache and are not proven visible to other attachers; safe only on an "+
				"unshared device", "path", cfg.BlockDevice)
		}
		defer func() { _ = dev.Close() }()
		svc.SetBlockDevice(dev)
		_ = svc.ReconstructArenas(ctx)

		log.Info("block device opened", "path", cfg.BlockDevice,
			"sector_size", dev.SectorSize(), "total_size", dev.TotalSize(),
			"direct_io", dev.IsDirect())
	}

	// Start membership heartbeat
	go membership.Run(ctx)

	// Start self-fencing watchdog
	go watchdog.Run(ctx)

	// Start external fencing controller
	controller := fencing.NewController(store, membership, log)
	// Fatal rather than degrading silently in either branch: an operator who
	// asked for device-enforced or dual-confirmed fencing and quietly got the
	// weaker single-signal guarantee has a gap that only shows up as
	// corruption during an incident.
	switch {
	case cfg.NVMeReservations:
		fencer, ferr := fencing.NewNVMeFencer(cfg.BlockDevice, cfg.NodeID)
		if ferr != nil {
			log.Fatal("cannot initialise NVMe reservation fencing",
				"device", cfg.BlockDevice, "error", ferr)
		}
		controller.SetFencer(fencer)
		log.Info("external fencing: device-enforced (NVMe reservation preempt)",
			"device", cfg.BlockDevice)
	case cfg.EBSVolumeID != "":
		detacher, derr := fencing.NewEBSDetacher(ctx, cfg.EBSVolumeID)
		if derr != nil {
			log.Fatal("cannot initialise EBS fencing", "volume", cfg.EBSVolumeID, "error", derr)
		}
		controller.SetFencer(detacher)
		log.Info("external fencing: dual-confirmed (EBS detach + poll)", "volume", cfg.EBSVolumeID)
	default:
		log.Warn("external fencing: single-signal (generation bump on lease expiry only); " +
			"pass --ebs-volume-id to detach the shared volume before bumping")
	}
	go controller.Run(ctx)

	// POSIX fcntl/flock locks are node-local: neither daemon implements
	// GETLK/SETLK, so the kernel enforces them within this node and nothing
	// enforces them between nodes.  Warned at startup because a workload
	// relying on cross-node locking gets no other signal — the calls succeed,
	// they simply exclude nothing on other nodes.
	log.Warn("POSIX file locks (fcntl/flock) are enforced within this node only, NOT across the " +
		"cluster; see docs/architecture/metadata/posix-lock-operations.md")

	// Start background scrubber (checks every 30s)
	scrubber := scrub.New(store, cfg.NodeID, 30*time.Second, log)
	// Without this the scrubber deletes an unlinked file's dangling extent
	// records but never returns their blocks, so disk space leaks on every
	// deletion.
	scrubber.SetReclaimer(svc.Allocator())
	// The range check compares against the real device rather than a hardcoded
	// ceiling; without a device attached it is skipped.
	scrubber.SetDeviceSize(svc.Allocator().DeviceSize())
	go scrubber.Run(ctx)

	// Return arenas this node has emptied to the global free pool.  Without
	// this a node keeps every arena it ever acquired, so space freed by deletes
	// and truncates stays reserved to this node and no peer can ever use it.
	go svc.Allocator().ReapEmptyArenas(ctx, time.Minute)

	// Start Prometheus metrics server if configured
	if cfg.MetricsAddr != "" {
		reg := metrics.NewRegistry()
		go func() { _ = metrics.StartServer(cfg.MetricsAddr, reg) }()
		log.Info("metrics server listening", "addr", cfg.MetricsAddr)
	}

	// Signal handling: graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// A self-fence shuts the node down through the same path a signal does, so
	// the arena release below still runs.  The watchdog used to call os.Exit
	// itself, which skipped it: a self-fenced node's arenas leaked, permanently
	// in single-signal mode, where no fencing controller reclaims them either.
	var selfFenced atomic.Bool
	go func() {
		select {
		case sig := <-sigCh:
			log.Info("received signal, shutting down", "signal", sig)
		case <-watchdog.Fenced():
			selfFenced.Store(true)
			log.Error("self-fenced, shutting down")
		}
		cancel()
	}()

	log.Info("binary IPC server starting")
	svc.StartNotificationServer(ctx)
	go func() {
		if err := ipc.StartNotifyServer(svc, cfg.NotifyAddr); err != nil {
			// Not fatal: the mount works without it, but every node's caches
			// then rely on their timeouts alone, so it must not be silent.
			log.Error("cache-invalidation notify server stopped; peers will not be "+
				"invalidated until it is restarted", "path", cfg.NotifyAddr, "error", err)
		}
	}()
	if err := ipc.StartSocketServer(ctx, svc, cfg.ListenAddr, log); err != nil {
		log.Fatal("IPC server failed", "error", err)
	}

	// Leave the cluster now that this node is serving nothing.  A departing
	// node is its own proof of quiescence — the IPC server has stopped, so no
	// further write can be issued from here — which is what a fenced node needs
	// an external Fencer to establish.  Skipping this is what made arena space
	// leak on every departure, graceful or not.
	//
	// A context of its own: ctx is already cancelled by the time this runs.
	relCtx, relCancel := context.WithTimeout(context.Background(), 5*time.Second)
	released, rerr := membership.Leave(relCtx, store)
	relCancel()
	switch {
	case rerr != nil:
		log.Warn("arenas not all released on shutdown, that space stays leaked",
			"node", cfg.NodeID, "released", released, "error", rerr)
	case len(released) > 0:
		log.Info("released arenas on shutdown", "node", cfg.NodeID, "arenas", released)
	}

	log.Info("etcfuse-meta stopped")
	if selfFenced.Load() {
		os.Exit(fencing.SelfFenceExitCode)
	}
}
