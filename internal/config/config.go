// Package config defines CLI flags and configuration for etcfuse-meta.
package config

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

// Version is stamped at build time.
var Version = "0.1.0"

// Config holds all parsed configuration.
type Config struct {
	ListenAddr    string
	EtcdEndpoints []string
	EtcdCertFile  string
	EtcdKeyFile   string
	EtcdCAFile    string
	NodeID        string
	ClusterName   string
	LeaseTTL      time.Duration
	LogLevel      int
	ShowVersion   bool
	BlockDevice   string

	// VolumeID identifies the shared data volume by its cloud volume ID.  When
	// set it takes precedence over BlockDevice, whose literal path does not
	// survive a detach/reattach cycle: the path is re-derived from the volume's
	// serial on every start.
	VolumeID    string
	MetricsAddr string
	RunFsck     bool
	RunInfo     bool

	// External fencing (AWS). When EBSVolumeID is set the fencing controller
	// detaches the shared volume from an expired node and waits for the
	// detachment to be confirmed before bumping its generation.  Unset (the
	// default, and the only option off EC2) leaves fencing single-signal.
	EBSVolumeID   string
	EC2InstanceID string

	// NVMeReservations selects device-enforced fencing: peers preempt an
	// expired node's NVMe reservation key on BlockDevice, and the device
	// itself rejects that node's writes.  Takes precedence over EBSVolumeID,
	// which is the weaker control-plane fallback.
	NVMeReservations bool

	// AllowBufferedIO permits the data device to be opened without O_DIRECT.
	// On a shared device that is a correctness change, not a fallback — a
	// write served back out of this node's page cache never proves it reached
	// the other attachers — so it is off by default and meant for single-node
	// mounts and file-backed test devices.
	AllowBufferedIO bool

	// WriteBarriers restores the device flush, range sync and readback after
	// every write, and the device flush before every read.  Off by default:
	// with O_DIRECT on a volume that acknowledges a write only once it is
	// durable, they are three device round trips per write that publish
	// nothing the write itself has not already published.  A device with a
	// volatile write cache needs them; buffered mode turns them on regardless.
	WriteBarriers bool

	// NotifyAddr is the socket the C daemon connects to for cache-invalidation
	// notifications.  Configurable for the same reason ListenAddr is: two
	// daemons on one host need two paths.
	NotifyAddr string

	// ReadOnly rejects every mutating FUSE operation with EROFS. Lets a node
	// mount the shared filesystem for backup or inspection while another node
	// writes, and gives fsck a safe way to run against a live volume.
	ReadOnly bool
}

// Default socket paths.
//
// Under /run rather than /tmp: both sockets are removed and re-bound at
// startup, and anything able to write the directory can win that race or
// occupy the path first.  /run is root-writable only, which /tmp is not.
const (
	DefaultSocket       = "/run/etcfuse/etcfuse.sock"
	DefaultNotifySocket = "/run/etcfuse/etcfuse-notify.sock"
)

// RequestTimeout bounds the etcd work behind a single FUSE request.
//
// It lives here rather than in the IPC package because it is one half of a
// constraint on configuration: it must sit below the self-fencing window, which
// is derived from the membership lease TTL.  A TTL that inverts the two makes
// the daemon exit before the request deadline can ever fire — the situation the
// deadline exists to avoid — so Parse rejects it, and it needs the number to do
// so.
//
// Above it, the value has to clear a routine leader election (~1-2 s) plus the
// several sequential store calls a handler makes, or an otherwise healthy
// cluster returns EIO during ordinary failover.
const RequestTimeout = 10 * time.Second

// SelfFenceWindow is how long a node keeps serving after its membership lease
// stops being renewed, before the watchdog declares it fenced and exits.  It
// mirrors the watchdog's own 2x rule.
func SelfFenceWindow(leaseTTL time.Duration) time.Duration {
	return 2 * leaseTTL
}

// Parse reads CLI flags and returns a Config.
func Parse() *Config {
	cfg := &Config{}

	var etcdEndpoints string
	var leaseTTL string

	flag.StringVar(&cfg.NotifyAddr, "notify-socket", DefaultNotifySocket,
		"Unix socket the C daemon connects to for cache-invalidation notifications")
	flag.StringVar(&cfg.ListenAddr, "listen", DefaultSocket,
		"Unix domain socket path for C daemon IPC")
	flag.StringVar(&etcdEndpoints, "etcd-endpoints", "http://localhost:2379",
		"Comma-separated etcd client endpoints")
	flag.StringVar(&cfg.EtcdCertFile, "etcd-cert", "",
		"Path to etcd client certificate")
	flag.StringVar(&cfg.EtcdKeyFile, "etcd-key", "",
		"Path to etcd client key")
	flag.StringVar(&cfg.EtcdCAFile, "etcd-ca", "",
		"Path to etcd CA certificate")
	flag.StringVar(&cfg.NodeID, "node-id", "",
		"Node identifier (default: hostname)")
	flag.StringVar(&cfg.ClusterName, "cluster-name", "etcfuse",
		"EtcFS cluster name")
	flag.StringVar(&leaseTTL, "lease-ttl", "10s",
		"Membership lease TTL (e.g., 10s, 30s, 1m)")
	flag.IntVar(&cfg.LogLevel, "log-level", 1,
		"Log level: 0=error, 1=info, 2=debug")
	flag.BoolVar(&cfg.ShowVersion, "version", false, "Show version and exit")
	flag.StringVar(&cfg.BlockDevice, "block-device", "",
		"Block device path for data I/O (e.g., /dev/nvme1n1); prefer --volume-id, which survives a detach/reattach")
	flag.StringVar(&cfg.VolumeID, "volume-id", "",
		"Cloud volume ID of the shared data volume (e.g., vol-0abcdef1234567890); its device path is resolved at every start and overrides --block-device")
	flag.StringVar(&cfg.MetricsAddr, "metrics-addr", "",
		"Prometheus metrics HTTP listen address (e.g., :9090)")
	flag.BoolVar(&cfg.RunFsck, "fsck", false,
		"Run offline filesystem check and exit")
	flag.BoolVar(&cfg.RunInfo, "info", false,
		"Print filesystem statistics and exit")
	flag.StringVar(&cfg.EBSVolumeID, "ebs-volume-id", "",
		"Shared EBS volume ID; enables dual-confirmed external fencing (detach + poll) when set")
	flag.BoolVar(&cfg.NVMeReservations, "nvme-reservations", false,
		"Fence peers by preempting their NVMe reservation key on --block-device (requires a device supporting NVMe reservations, e.g. an EBS io2 Multi-Attach volume)")
	flag.BoolVar(&cfg.AllowBufferedIO, "allow-buffered-io", false,
		"Permit opening the data device without O_DIRECT; unsafe on a device attached to more than one node")
	flag.BoolVar(&cfg.WriteBarriers, "write-barriers", false,
		"Flush the device cache, sync the range and read it back after every write; needed only on a device with a volatile write cache that does not publish an acknowledged O_DIRECT write to its other attachers (always on without O_DIRECT)")
	flag.StringVar(&cfg.EC2InstanceID, "ec2-instance-id", "",
		"This node's EC2 instance ID, recorded in its membership key so peers can detach the volume when it expires")
	flag.BoolVar(&cfg.ReadOnly, "read-only", false,
		"Reject every mutating FUSE operation with EROFS; for backup/inspection mounts and running fsck against a live volume")

	flag.Parse()

	cfg.EtcdEndpoints = strings.Split(etcdEndpoints, ",")

	if cfg.NodeID == "" {
		hostname, _ := os.Hostname()
		cfg.NodeID = hostname
	}

	if cfg.NVMeReservations && cfg.BlockDevice == "" && cfg.VolumeID == "" {
		fmt.Fprintln(os.Stderr, "-nvme-reservations requires -volume-id or -block-device")
		os.Exit(1)
	}

	d, err := time.ParseDuration(leaseTTL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid lease-ttl %q: %v\n", leaseTTL, err)
		os.Exit(1)
	}
	// The self-fencing watchdog declares the node fenced once its lease has
	// been dead for 2x the TTL, and exits.  If that window closes before the
	// request deadline can fire, the daemon dies with requests still waiting
	// for the deadline that would have failed them cleanly — which is the
	// situation the deadline exists to avoid.
	if SelfFenceWindow(d) <= RequestTimeout {
		fmt.Fprintf(os.Stderr,
			"lease-ttl %s gives a %s self-fencing window, at or below the %s request timeout: "+
				"the daemon would exit before a stalled request could fail\n",
			d, SelfFenceWindow(d), RequestTimeout)
		os.Exit(1)
	}
	cfg.LeaseTTL = d

	return cfg
}

// EtcdTLSConfig returns a tls.Config from the configured cert files.
// Returns nil if no cert files are specified (plaintext connection).
func (c *Config) EtcdTLSConfig() *tls.Config {
	if c.EtcdCertFile == "" && c.EtcdCAFile == "" {
		return nil
	}

	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	if c.EtcdCertFile != "" && c.EtcdKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(c.EtcdCertFile, c.EtcdKeyFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to load etcd client cert: %v\n", err)
			os.Exit(1)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	if c.EtcdCAFile != "" {
		caCert, err := os.ReadFile(c.EtcdCAFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to read etcd CA cert: %v\n", err)
			os.Exit(1)
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		tlsCfg.RootCAs = caCertPool
	}

	return tlsCfg
}
