/*
Package main is the EtcFS metadata backend binary.

It runs a gRPC server on a Unix domain socket, receiving FUSE operation
requests from the C daemon (etcfuse) and translating them into etcd
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
	"syscall"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/MHS-20/EtcFS/internal/config"
	"github.com/MHS-20/EtcFS/internal/ipc"
	"github.com/MHS-20/EtcFS/pkg/blockio"
	"github.com/MHS-20/EtcFS/pkg/fencing"
	"github.com/MHS-20/EtcFS/pkg/metadata"
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

	// Connect to etcd
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.EtcdEndpoints,
		DialTimeout: 5 * time.Second,
		TLS:         cfg.EtcdTLSConfig(),
	})
	if err != nil {
		log.Fatal("cannot connect to etcd", "error", err)
	}
	defer func() { _ = etcdCli.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Membership: register this node with a lease-backed key
	membership := metadata.NewMembership(etcdCli, cfg.NodeID, cfg.ClusterName, cfg.LeaseTTL)

	// Metadata store: wraps etcd client with schema-aware helpers
	store := metadata.NewStore(etcdCli, cfg.NodeID)

	// Self-fencing watchdog
	watchdog := fencing.NewWatchdog(membership, cfg.LeaseTTL)

	// IPC service: handles FUSE op requests from the C daemon
	svc := ipc.NewService(store, membership, watchdog, log)

	if cfg.BlockDevice != "" {
		dev, err := blockio.Open(cfg.BlockDevice)
		if err != nil {
			log.Fatal("cannot open block device", "path", cfg.BlockDevice, "error", err)
		}
		defer func() { _ = dev.Close() }()
		svc.SetBlockDevice(dev)
		log.Info("block device opened", "path", cfg.BlockDevice,
			"sector_size", dev.SectorSize(), "total_size", dev.TotalSize())
	}

	// Start membership heartbeat
	go membership.Run(ctx)

	// Start self-fencing watchdog
	go watchdog.Run(ctx)

	// Start external fencing controller
	controller := fencing.NewController(store, membership, log)
	go controller.Run(ctx)

	// Signal handling: graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		log.Info("received signal, shutting down", "signal", sig)
		cancel()
	}()

	log.Info("binary IPC server starting")
	if err := ipc.StartSocketServer(svc, cfg.ListenAddr, log); err != nil {
		log.Fatal("IPC server failed", "error", err)
	}

	log.Info("etcfuse-meta stopped")
}
