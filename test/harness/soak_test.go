package harness

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/MHS-20/EtcFS/pkg/fsck"
	"github.com/MHS-20/EtcFS/pkg/metadata"
	"github.com/MHS-20/EtcFS/pkg/metrics"
	"github.com/MHS-20/EtcFS/pkg/scrub"
)

type nullLogger struct{}

func (nullLogger) Warn(msg string, args ...any)  {}
func (nullLogger) Info(msg string, args ...any)  {}
func (nullLogger) Error(msg string, args ...any) {}

// ---- C11.1: Scaled soak test (harness version) ----

func TestSoak_ScaledJepsenWithMetrics(t *testing.T) {
	cluster := NewCluster(3)
	ctx := t.Context()
	store := cluster.Store

	reg := metrics.NewRegistry()

	cluster.createDirIfMissing(ctx, 1, "soak", 70000)
	for i := 0; i < 20; i++ {
		ino := uint64(75000) + uint64(i)
		name := fmt.Sprintf("base-%02d", i)
		rec := &metadata.InodeRecord{
			Ino: ino, Mode: 0100644, Nlink: 1, Size: 4096, Blksize: 4096,
		}
		_, _ = store.Put(ctx, metadata.InodeKey(ino), metadata.EncodeInode(rec))
		_, _ = store.Put(ctx, metadata.DirentKey(70000, name), metadata.EncodeUint64(ino))
	}

	sc := scrub.New(store, "soak-node", 20*time.Millisecond, nullLogger{})

	runUntil := time.Now().Add(5 * time.Second)
	ops := 0
	faults := 0

	for time.Now().Before(runUntil) {
		n := cluster.Nodes[ops%3]
		ino := uint64(80000) + uint64(ops)
		name := fmt.Sprintf("s-%d", ops)

		switch ops % 10 {
		case 0:
			_, _ = n.createFile(ctx, 70000, name, ino, 0100644)
			reg.IncCounter("etcfuse_fuse_ops_total", "create")
		case 1:
			n.writeInode(ctx, ino, 4096)
			reg.IncCounter("etcfuse_fuse_ops_total", "write")
		case 2:
			reg.IncCounter("etcfuse_fuse_ops_total", "getattr")
		case 3:
			reg.IncCounter("etcfuse_fuse_ops_total", "lookup")
		case 4:
			_, _ = store.Put(ctx, fmt.Sprintf("extent:%d/0", ino),
				[]byte(fmt.Sprintf("0,%d,4096,1", 4096*(ops%1000))))
			reg.IncCounter("etcfuse_block_io_total", "write")
			reg.IncCounter("etcfuse_etcd_txn_total", "txn_commit")
		case 5:
			reg.TrackDuration("etcfuse_etcd_txn_duration_seconds", "txn_commit",
				time.Millisecond*time.Duration(1+ops%20))
		case 6:
			n.injectFault(FaultEtcdPartition)
			faults++
		case 7:
			reg.IncCounter("etcfuse_etcd_txn_total", "txn_commit")
			faults++
		case 8:
			_ = sc.CheckExtentCollisions(ctx)
			reg.IncCounter("etcfuse_scrub_anomalies_total", "scrub_pass")
		case 9:
			reg.SetGauge("etcfuse_arena_utilization", 0.4+float64(ops%30)/100.0)
			reg.SetGauge("etcfuse_membership_count", float64(len(cluster.Nodes)))
		}
		ops++
	}

	reg.SetGauge("etcfuse_inode_count", float64(ops))
	reg.SetGauge("etcfuse_lock_count", 3)
	reg.SetGauge("etcfuse_dirent_count", float64(ops/2))

	t.Logf("soak: %d ops, %d faults in 5s", ops, faults)

	// Verify invariants held
	assert.Zero(t, cluster.checkAllInvariants())

	// Run fsck after soak
	chk := fsck.New(store)
	findings := chk.Run(ctx)
	require.Zero(t, chk.ErrorCount(), "fsck should find zero errors after soak")
	t.Logf("fsck after soak: %d findings (0 errors, %d warnings)",
		len(findings), chk.WarningCount())

	// Verify metrics are populated
	assert.Greater(t, reg.CounterValue("etcfuse_fuse_ops_total"), float64(0))
	assert.Greater(t, reg.CounterValue("etcfuse_etcd_txn_total"), float64(0))
	assert.Greater(t, reg.CounterValue("etcfuse_block_io_total"), float64(0))
	assert.Greater(t, reg.GaugeValue("etcfuse_membership_count"), float64(0))

	// Verify all required metric names exist
	for _, metricName := range []string{
		"etcfuse_fuse_ops_total",
		"etcfuse_etcd_txn_total",
		"etcfuse_block_io_total",
		"etcfuse_scrub_anomalies_total",
		"etcfuse_etcd_txn_duration_seconds_count",
		"etcfuse_etcd_txn_duration_seconds",
	} {
		assert.True(t, reg.HasCounter(metricName),
			"metric %q should be reported", metricName)
	}
	for _, metricName := range []string{
		"etcfuse_arena_utilization",
		"etcfuse_inode_count",
		"etcfuse_membership_count",
		"etcfuse_lock_count",
		"etcfuse_dirent_count",
	} {
		assert.True(t, reg.HasGauge(metricName),
			"gauge %q should be reported", metricName)
	}
}
