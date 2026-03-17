// Package snapshot performs an initial consistent snapshot of configured tables
// using a PostgreSQL transaction with REPEATABLE READ isolation. Snapshot rows
// are emitted as CDC events with op='r' (read) through the same publish path.
package snapshot

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"

	"github.com/kperreau/postgres-cdc/internal/encoder"
	"github.com/kperreau/postgres-cdc/internal/metrics"
	"github.com/kperreau/postgres-cdc/internal/model"
	"github.com/kperreau/postgres-cdc/internal/producer"
	"github.com/kperreau/postgres-cdc/internal/topic"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Config holds snapshot settings.
type Config struct {
	Tables    []string // fully qualified: schema.table
	FetchSize int
	Database  string
	Source    string
}

// Runner executes initial table snapshots.
type Runner struct {
	cfg      Config
	pool     *pgxpool.Pool
	enc      *encoder.Encoder
	prod     *producer.Producer
	resolver *topic.Resolver
	log      zerolog.Logger
	metrics  *metrics.SnapshotMetrics
}

// NewRunner creates a snapshot Runner.
func NewRunner(
	cfg Config,
	pool *pgxpool.Pool,
	prod *producer.Producer,
	resolver *topic.Resolver,
	log zerolog.Logger,
	m *metrics.SnapshotMetrics,
) *Runner {
	return &Runner{
		cfg:      cfg,
		pool:     pool,
		enc:      encoder.New(encoder.Config{SourceName: cfg.Source, Database: cfg.Database}),
		prod:     prod,
		resolver: resolver,
		log:      log.With().Str("component", "snapshot").Logger(),
		metrics:  m,
	}
}

// Run executes snapshots for all configured tables sequentially.
// Each table is snapshotted in a REPEATABLE READ transaction.
func (r *Runner) Run(ctx context.Context) error {
	if len(r.cfg.Tables) == 0 {
		r.log.Info().Msg("no tables configured for snapshot")
		return nil
	}

	start := time.Now()
	for _, tbl := range r.cfg.Tables {
		if err := r.snapshotTable(ctx, tbl); err != nil {
			return fmt.Errorf("snapshot %s: %w", tbl, err)
		}
	}
	r.metrics.DurationSeconds.Observe(time.Since(start).Seconds())
	r.log.Info().Int("tables", len(r.cfg.Tables)).Msg("snapshot complete")
	return nil
}

// snapshotTable reads all rows from a single table in a consistent snapshot.
func (r *Runner) snapshotTable(ctx context.Context, qualifiedName string) error {
	schema, table := parseQualifiedName(qualifiedName)
	r.log.Info().Str("table", qualifiedName).Msg("starting table snapshot")

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Use a cursor for bounded memory.
	cursorName := "cdc_snapshot_cursor"
	query := fmt.Sprintf(
		"DECLARE %s CURSOR FOR SELECT * FROM %s",
		cursorName,
		pgx.Identifier{schema, table}.Sanitize(),
	)
	if _, err := tx.Exec(ctx, query); err != nil {
		return fmt.Errorf("declare cursor: %w", err)
	}

	fetchQuery := fmt.Sprintf("FETCH %d FROM %s", r.cfg.FetchSize, cursorName)
	var rowCount int64

	for {
		rows, err := tx.Query(ctx, fetchQuery)
		if err != nil {
			return fmt.Errorf("fetch: %w", err)
		}

		descs := rows.FieldDescriptions()
		batchRecords := make([]*kgo.Record, 0, r.cfg.FetchSize)
		fetched := 0

		for rows.Next() {
			values, err := rows.Values()
			if err != nil {
				rows.Close()
				return fmt.Errorf("scan values: %w", err)
			}

			cols := make([]model.ColumnValue, len(descs))
			for i, desc := range descs {
				cols[i] = model.ColumnValue{
					Name:  desc.Name,
					Value: values[i],
				}
			}

			ev := &model.TxEvent{
				Change: model.Change{
					Op: model.OpSnapshot,
					Relation: &model.Relation{
						Namespace: schema,
						Name:      table,
					},
					After: cols,
				},
				CommitTS: time.Now().UTC(),
			}

			_, _, key, value, err := r.enc.Encode(ev, true)
			if err != nil {
				rows.Close()
				return fmt.Errorf("encode: %w", err)
			}

			topicName := r.resolver.Resolve(r.cfg.Database, schema, table)
			if err := r.prod.EnsureTopic(ctx, topicName); err != nil {
				rows.Close()
				return fmt.Errorf("ensure topic: %w", err)
			}
			batchRecords = append(batchRecords, &kgo.Record{
				Topic: topicName,
				Key:   key,
				Value: value,
			})
			fetched++
		}
		rows.Close()
		if rows.Err() != nil {
			return fmt.Errorf("rows error: %w", rows.Err())
		}

		if fetched == 0 {
			break
		}

		if err := r.prod.PublishBatch(ctx, batchRecords); err != nil {
			return fmt.Errorf("publish batch: %w", err)
		}

		rowCount += int64(fetched)
		r.metrics.RowsTotal.WithLabelValues(qualifiedName).Add(float64(fetched))
	}

	r.log.Info().Str("table", qualifiedName).Int64("rows", rowCount).Msg("table snapshot done")
	return nil
}

// parseQualifiedName splits "schema.table" into (schema, table).
// Defaults to "public" if no schema is specified.
func parseQualifiedName(name string) (string, string) {
	for i := 0; i < len(name); i++ {
		if name[i] == '.' {
			return name[:i], name[i+1:]
		}
	}
	return "public", name
}
