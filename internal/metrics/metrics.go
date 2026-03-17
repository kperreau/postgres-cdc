// Package metrics defines and registers all Prometheus metrics centrally using promauto.
// Business code receives typed metric structs — it never instantiates metrics directly.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds all application-level Prometheus metrics.
// Pass this struct (or pointers to sub-structs) into components.
type Metrics struct {
	PG       PGMetrics
	CDC      CDCMetrics
	Snapshot SnapshotMetrics
}

// PGMetrics covers PostgreSQL replication source metrics.
type PGMetrics struct {
	WALMessagesTotal     prometheus.Counter
	TransactionsTotal    prometheus.Counter
	TransactionBytes     prometheus.Histogram
	RelationCacheEntries prometheus.Gauge
	ReconnectsTotal      prometheus.Counter
}

// CDCMetrics covers the publish/pipeline side.
type CDCMetrics struct {
	EventsTotal              *prometheus.CounterVec
	PublishRetriesTotal      prometheus.Counter
	PublishFailuresTotal     prometheus.Counter
	QueueDepth               prometheus.Gauge
	LastReadLSN              prometheus.Gauge
	LastCheckpointLSN        prometheus.Gauge
	CommitLagSeconds         prometheus.Gauge
	BackpressureSeconds      prometheus.Counter
	OversizedTransTotal      prometheus.Counter
}

// SnapshotMetrics covers snapshot progress.
type SnapshotMetrics struct {
	RowsTotal       *prometheus.CounterVec
	DurationSeconds prometheus.Histogram
}

// New creates and registers all metrics under the given namespace.
// The returned handler serves /metrics for promhttp scraping.
func New(namespace string) (*Metrics, http.Handler) {
	m := &Metrics{
		PG: PGMetrics{
			WALMessagesTotal: promauto.NewCounter(prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "pg_wal_messages_total",
				Help:      "Total WAL messages received from PostgreSQL.",
			}),
			TransactionsTotal: promauto.NewCounter(prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "pg_transactions_total",
				Help:      "Total committed transactions observed.",
			}),
			TransactionBytes: promauto.NewHistogram(prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "pg_transaction_bytes",
				Help:      "Approximate byte size of assembled transactions.",
				Buckets:   prometheus.ExponentialBuckets(1024, 4, 10), // 1KiB .. ~256MiB
			}),
			RelationCacheEntries: promauto.NewGauge(prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "pg_relation_cache_entries",
				Help:      "Number of entries in the relation metadata cache.",
			}),
			ReconnectsTotal: promauto.NewCounter(prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "cdc_reconnects_total",
				Help:      "Total number of replication reconnect attempts.",
			}),
		},
		CDC: CDCMetrics{
			EventsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "cdc_events_total",
				Help:      "Total CDC events published, by operation type.",
			}, []string{"op"}),
			PublishRetriesTotal: promauto.NewCounter(prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "cdc_publish_retries_total",
				Help:      "Total publish retry attempts to Redpanda.",
			}),
			PublishFailuresTotal: promauto.NewCounter(prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "cdc_publish_failures_total",
				Help:      "Total publish failures to Redpanda.",
			}),
			QueueDepth: promauto.NewGauge(prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "cdc_queue_depth",
				Help:      "Current depth of the internal pipeline queue.",
			}),
			LastReadLSN: promauto.NewGauge(prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "cdc_last_read_lsn",
				Help:      "Last WAL LSN read from PostgreSQL (as uint64).",
			}),
			LastCheckpointLSN: promauto.NewGauge(prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "cdc_last_checkpoint_lsn",
				Help:      "Last safely checkpointed LSN (as uint64).",
			}),
			CommitLagSeconds: promauto.NewGauge(prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "cdc_commit_lag_seconds",
				Help:      "Seconds between commit timestamp and now.",
			}),
			BackpressureSeconds: promauto.NewCounter(prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "cdc_backpressure_seconds",
				Help:      "Cumulative seconds spent blocked due to backpressure.",
			}),
			OversizedTransTotal: promauto.NewCounter(prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "cdc_oversized_transactions_total",
				Help:      "Total transactions dropped/skipped due to exceeding max_tx_bytes.",
			}),
		},
		Snapshot: SnapshotMetrics{
			RowsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "cdc_snapshot_rows_total",
				Help:      "Total rows read during snapshot, by table.",
			}, []string{"table"}),
			DurationSeconds: promauto.NewHistogram(prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "cdc_snapshot_duration_seconds",
				Help:      "Duration of snapshot operations.",
				Buckets:   prometheus.ExponentialBuckets(1, 2, 15), // 1s .. ~8h
			}),
		},
	}

	handler := promhttp.Handler()
	return m, handler
}
