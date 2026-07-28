package harness

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/MHS-20/EtcFS/pkg/metrics"
)

// ---- C11.3: Metrics completeness ----

func TestMetrics_Completeness(t *testing.T) {
	reg := metrics.NewRegistry()

	namedCounters := []string{
		"etcfuse_fuse_ops_total",
		"etcfuse_etcd_txn_total",
		"etcfuse_block_io_total",
		"etcfuse_scrub_anomalies_total",
		"etcfuse_etcd_txn_duration_seconds",
		"etcfuse_etcd_txn_duration_seconds_count",
	}
	for _, name := range namedCounters {
		reg.IncCounter(name, "test")
		assert.True(t, reg.HasCounter(name), "counter %s should exist", name)
	}

	namedGauges := []string{
		"etcfuse_arena_utilization",
		"etcfuse_inode_count",
		"etcfuse_membership_count",
		"etcfuse_lock_count",
		"etcfuse_dirent_count",
	}
	for _, name := range namedGauges {
		reg.SetGauge(name, 42)
		assert.True(t, reg.HasGauge(name), "gauge %s should exist", name)
	}
}

func TestMetrics_IncrementAndRead(t *testing.T) {
	reg := metrics.NewRegistry()

	reg.IncCounter("etcfuse_fuse_ops_total", "lookup")
	reg.IncCounter("etcfuse_fuse_ops_total", "lookup")
	reg.IncCounter("etcfuse_fuse_ops_total", "getattr")
	reg.IncCounter("etcfuse_fuse_ops_total", "create")

	assert.Equal(t, float64(4), reg.CounterValue("etcfuse_fuse_ops_total")) // sum across all labels

	labels := reg.CounterLabels("etcfuse_fuse_ops_total")
	assert.Subset(t, labels, []string{"lookup", "getattr", "create"})
}

func TestMetrics_DurationTracking(t *testing.T) {
	reg := metrics.NewRegistry()

	reg.TrackDuration("etcfuse_etcd_txn_duration_seconds", "txn_commit", 150*time.Millisecond)
	reg.TrackDuration("etcfuse_etcd_txn_duration_seconds", "txn_commit", 250*time.Millisecond)

	assert.Equal(t, float64(2), reg.CounterValue("etcfuse_etcd_txn_duration_seconds_count"))
	assert.Greater(t, reg.CounterValue("etcfuse_etcd_txn_duration_seconds"), 0.35)
	assert.Less(t, reg.CounterValue("etcfuse_etcd_txn_duration_seconds"), 0.45)
}

func TestMetrics_ScrubAnomalies(t *testing.T) {
	reg := metrics.NewRegistry()

	reg.IncCounter("etcfuse_scrub_anomalies_total", "collision")
	reg.IncCounter("etcfuse_scrub_anomalies_total", "collision")
	reg.IncCounter("etcfuse_scrub_anomalies_total", "orphan")
	reg.IncCounter("etcfuse_scrub_anomalies_total", "generation")
	reg.IncCounter("etcfuse_scrub_anomalies_total", "nlink")

	assert.Greater(t, reg.CounterValue("etcfuse_scrub_anomalies_total"), float64(0))
	labels := reg.CounterLabels("etcfuse_scrub_anomalies_total")
	assert.Subset(t, labels, []string{"collision", "orphan", "generation", "nlink"})
}

func TestMetrics_GaugeFluctuation(t *testing.T) {
	reg := metrics.NewRegistry()

	reg.SetGauge("etcfuse_arena_utilization", 0.75)
	assert.Equal(t, 0.75, reg.GaugeValue("etcfuse_arena_utilization"))

	reg.SetGauge("etcfuse_arena_utilization", 0.30)
	assert.Equal(t, 0.30, reg.GaugeValue("etcfuse_arena_utilization"))

	reg.SetGauge("etcfuse_membership_count", 3)
	assert.Equal(t, float64(3), reg.GaugeValue("etcfuse_membership_count"))
}
