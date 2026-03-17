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
	EventsTotal         *prometheus.CounterVec
	PublishRetriesTotal prometheus.Counter
	PublishFailuresTotal prometheus.Counter
	QueueDepth          prometheus.Gauge
	LastReadLSN         prometheus.Gauge
	LastCheckpointLSN   prometheus.Gauge
	CommitLagSeconds    prometheus.Gauge
	BackpressureSeconds prometheus.Counter
	OversizedTransTotal prometheus.Counter
}

// SnapshotMetrics covers snapshot progress.
type SnapshotMetrics struct {
	RowsTotal       *prometheus.CounterVec
	DurationSeconds prometheus.Histogram
}

// New creates and registers all metrics under the given namespace using the
// default global registry. The returned handler serves /metrics for promhttp.
// This should be called once in the application entrypoint.
func New(namespace string) (*Metrics, http.Handler) {
	factory := promauto.With(prometheus.DefaultRegisterer)
	m := newWithRegisterer(namespace, factory)
	handler := promhttp.Handler()
	return m, handler
}

// NewWithRegistry creates metrics registered to a custom registry.
// Useful for tests to avoid global state collisions.
func NewWithRegistry(namespace string, reg *prometheus.Registry) *Metrics {
	return newWithRegisterer(namespace, promauto.With(reg))
}

func newWithRegisterer(namespace string, factory promauto.Factory) *Metrics {
	return &Metrics{
		PG: PGMetrics{
			WALMessagesTotal: factory.NewCounter(prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "pg_wal_messages_total",
				Help:      "Total WAL messages received from PostgreSQL.",
			}),
			TransactionsTotal: factory.NewCounter(prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "pg_transactions_total",
				Help:      "Total committed transactions observed.",
			}),
			TransactionBytes: factory.NewHistogram(prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "pg_transaction_bytes",
				Help:      "Approximate byte size of assembled transactions.",
				Buckets:   prometheus.ExponentialBuckets(1024, 4, 10),
			}),
			RelationCacheEntries: factory.NewGauge(prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "pg_relation_cache_entries",
				Help:      "Number of entries in the relation metadata cache.",
			}),
			ReconnectsTotal: factory.NewCounter(prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "cdc_reconnects_total",
				Help:      "Total number of replication reconnect attempts.",
			}),
		},
		CDC: CDCMetrics{
			EventsTotal: factory.NewCounterVec(prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "cdc_events_total",
				Help:      "Total CDC events published, by operation type.",
			}, []string{"op"}),
			PublishRetriesTotal: factory.NewCounter(prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "cdc_publish_retries_total",
				Help:      "Total publish retry attempts to Redpanda.",
			}),
			PublishFailuresTotal: factory.NewCounter(prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "cdc_publish_failures_total",
				Help:      "Total publish failures to Redpanda.",
			}),
			QueueDepth: factory.NewGauge(prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "cdc_queue_depth",
				Help:      "Current depth of the internal pipeline queue.",
			}),
			LastReadLSN: factory.NewGauge(prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "cdc_last_read_lsn",
				Help:      "Last WAL LSN read from PostgreSQL (as uint64).",
			}),
			LastCheckpointLSN: factory.NewGauge(prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "cdc_last_checkpoint_lsn",
				Help:      "Last safely checkpointed LSN (as uint64).",
			}),
			CommitLagSeconds: factory.NewGauge(prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "cdc_commit_lag_seconds",
				Help:      "Seconds between commit timestamp and now.",
			}),
			BackpressureSeconds: factory.NewCounter(prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "cdc_backpressure_seconds",
				Help:      "Cumulative seconds spent blocked due to backpressure.",
			}),
			OversizedTransTotal: factory.NewCounter(prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "cdc_oversized_transactions_total",
				Help:      "Total transactions dropped/skipped due to exceeding max_tx_bytes.",
			}),
		},
		Snapshot: SnapshotMetrics{
			RowsTotal: factory.NewCounterVec(prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "cdc_snapshot_rows_total",
				Help:      "Total rows read during snapshot, by table.",
			}, []string{"table"}),
			DurationSeconds: factory.NewHistogram(prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "cdc_snapshot_duration_seconds",
				Help:      "Duration of snapshot operations.",
				Buckets:   prometheus.ExponentialBuckets(1, 2, 15),
			}),
		},
	}
}
