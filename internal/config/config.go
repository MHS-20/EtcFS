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
}

// Parse reads CLI flags and returns a Config.
func Parse() *Config {
	cfg := &Config{}

	var etcdEndpoints string
	var leaseTTL string

	flag.StringVar(&cfg.ListenAddr, "listen", "/tmp/etcfuse.sock",
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

	flag.Parse()

	cfg.EtcdEndpoints = strings.Split(etcdEndpoints, ",")

	if cfg.NodeID == "" {
		hostname, _ := os.Hostname()
		cfg.NodeID = hostname
	}

	d, err := time.ParseDuration(leaseTTL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid lease-ttl %q: %v\n", leaseTTL, err)
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
