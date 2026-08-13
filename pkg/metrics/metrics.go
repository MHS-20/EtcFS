// Package metrics defines the Prometheus metrics EtcFS exposes on
// --metrics-addr, and the /metrics server that serves them.
//
// The metrics are package-level variables registered with the default
// Prometheus registry rather than values threaded through every constructor:
// a subsystem instruments itself by referring to the metric it owns, and
// nothing has to be wired for a metric to appear. Metric names are an API —
// dashboards and alert rules are written against them — so they are declared
// here, in one place, instead of being spelled out at each call site.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// FuseOps counts FUSE operations served, by operation name.
	FuseOps = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "etcfuse_fuse_ops_total",
		Help: "FUSE operations served, by operation.",
	}, []string{"op"})

	// FuseErrors counts FUSE operations that returned an errno, by operation.
	FuseErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "etcfuse_fuse_errors_total",
		Help: "FUSE operations that failed, by operation.",
	}, []string{"op"})

	// FuseOpDuration observes end-to-end handler latency, by operation.
	FuseOpDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "etcfuse_fuse_op_duration_seconds",
		Help:    "FUSE operation handler latency.",
		Buckets: prometheus.ExponentialBuckets(0.0001, 3, 10),
	}, []string{"op"})

	// EtcdTxns counts etcd transactions, by outcome (committed, rejected, error).
	EtcdTxns = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "etcfuse_etcd_txn_total",
		Help: "etcd transactions attempted, by outcome.",
	}, []string{"outcome"})

	// EtcdTxnDuration observes etcd transaction round-trip latency. This is
	// the metric an operator wants percentiles on: it is the dominant term in
	// every metadata operation's latency.
	EtcdTxnDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "etcfuse_etcd_txn_duration_seconds",
		Help:    "etcd transaction round-trip latency.",
		Buckets: prometheus.ExponentialBuckets(0.0005, 3, 10),
	})

	// EtcdReadDuration observes etcd read round-trip latency (Get/prefix
	// reads), separate from EtcdTxnDuration's guarded-commit path — the read
	// and write paths spend this budget differently and item 0 of the
	// round-trip-reduction plan needs them decomposed.
	EtcdReadDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "etcfuse_etcd_read_duration_seconds",
		Help:    "etcd read round-trip latency.",
		Buckets: prometheus.ExponentialBuckets(0.0005, 3, 10),
	})

	// BlockIO counts block-device operations, by direction (read, write).
	BlockIO = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "etcfuse_block_io_total",
		Help: "Block device operations, by direction.",
	}, []string{"op"})

	// BlockIOBytes counts bytes transferred to and from the block device.
	BlockIOBytes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "etcfuse_block_io_bytes_total",
		Help: "Bytes transferred to and from the block device, by direction.",
	}, []string{"op"})

	// BlockIODuration observes block device operation latency, by direction.
	BlockIODuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "etcfuse_block_io_duration_seconds",
		Help:    "Block device operation latency, by direction.",
		Buckets: prometheus.ExponentialBuckets(0.0001, 3, 10),
	}, []string{"op"})

	// ScrubAnomalies counts anomalies found by the scrubber, by type
	// (collision, orphan, dead, range, generation, nlink).
	ScrubAnomalies = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "etcfuse_scrub_anomalies_total",
		Help: "Anomalies detected by the scrubber, by type.",
	}, []string{"type"})

	// ScrubPasses counts completed scrub passes.
	ScrubPasses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "etcfuse_scrub_passes_total",
		Help: "Completed scrub passes.",
	})

	// ScrubLastRun holds the Unix timestamp of the last completed scrub pass.
	// A scrubber that has stopped is invisible in the counters above, which
	// simply stop rising; this makes staleness directly alertable.
	ScrubLastRun = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "etcfuse_scrub_last_run_seconds",
		Help: "Unix timestamp of the last completed scrub pass.",
	})

	// ArenaUtilization is the fraction of blocks in use across the arenas this
	// node owns, between 0 and 1.
	ArenaUtilization = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "etcfuse_arena_utilization",
		Help: "Fraction of blocks in use across arenas owned by this node.",
	})

	// ArenasOwned is the number of arenas this node currently owns.
	ArenasOwned = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "etcfuse_arenas_owned",
		Help: "Arenas currently owned by this node.",
	})

	// MembershipCount is the number of live members in the cluster as last
	// observed by this node.
	MembershipCount = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "etcfuse_membership_count",
		Help: "Live cluster members as last observed by this node.",
	})

	// FencingGeneration is this node's current fencing generation. A step in
	// this series is a fence, which is the single most important event an
	// operator can alert on.
	FencingGeneration = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "etcfuse_fencing_generation",
		Help: "This node's current fencing generation.",
	})

	// FencedNodes counts nodes this node's fencing controller has fenced, by
	// outcome (fenced, failed).
	FencedNodes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "etcfuse_fenced_nodes_total",
		Help: "Nodes fenced by this node's fencing controller, by outcome.",
	}, []string{"outcome"})
)

// StartServer serves /metrics on addr. It blocks until the server stops.
func StartServer(addr string) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	return http.ListenAndServe(addr, mux)
}
