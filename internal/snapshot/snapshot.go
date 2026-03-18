// Package snapshot performs an initial consistent snapshot of configured tables
// using a PostgreSQL transaction with REPEATABLE READ isolation. Snapshot rows
// are emitted as CDC events with op='r' (read) through the same publish path.
//
// When primary key columns are detected, the cursor uses ORDER BY pk to ensure
// deterministic, resumable scanning. Tables without a primary key fall back to
// unordered scanning.
//
// Multiple tables can be snapshotted concurrently via MaxParallelTables. Each
// parallel table opens its own REPEATABLE READ transaction and encoder.
package snapshot

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"

	"github.com/kperreau/postgres-cdc/internal/encoder"
	"github.com/kperreau/postgres-cdc/internal/metrics"
	"github.com/kperreau/postgres-cdc/internal/model"
	"github.com/kperreau/postgres-cdc/internal/producer"
	"github.com/kperreau/postgres-cdc/internal/topic"
)

// Config holds snapshot settings.
type Config struct {
	Tables            []string // fully qualified: schema.table
	FetchSize         int
	MaxParallelTables int
	Database          string
	Source            string
	ToastStrategy     string
}

// Runner executes initial table snapshots.
type Runner struct {
	cfg      Config
	pool     *pgxpool.Pool
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
	if cfg.MaxParallelTables <= 0 {
		cfg.MaxParallelTables = 1
	}
	return &Runner{
		cfg:      cfg,
		pool:     pool,
		prod:     prod,
		resolver: resolver,
		log:      log.With().Str("component", "snapshot").Logger(),
		metrics:  m,
	}
}

// Run executes snapshots for all configured tables with bounded parallelism.
// Each table is snapshotted in its own REPEATABLE READ transaction.
func (r *Runner) Run(ctx context.Context) error {
	if len(r.cfg.Tables) == 0 {
		r.log.Info().Msg("no tables configured for snapshot")
		return nil
	}

	start := time.Now()

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(r.cfg.MaxParallelTables)

	for _, tbl := range r.cfg.Tables {
		g.Go(func() error {
			// Each goroutine gets its own encoder (not safe for concurrent use).
			enc := encoder.New(encoder.Config{
				SourceName:    r.cfg.Source,
				Database:      r.cfg.Database,
				ToastStrategy: r.cfg.ToastStrategy,
			})
			if err := r.snapshotTable(gCtx, tbl, enc); err != nil {
				return fmt.Errorf("snapshot %s: %w", tbl, err)
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	r.metrics.DurationSeconds.Observe(time.Since(start).Seconds())
	r.log.Info().Int("tables", len(r.cfg.Tables)).Msg("snapshot complete")
	return nil
}

// snapshotTable reads all rows from a single table in a consistent snapshot.
// It detects primary key columns and uses ORDER BY pk for deterministic scanning.
func (r *Runner) snapshotTable(ctx context.Context, qualifiedName string, enc *encoder.Encoder) error {
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

	// Detect primary key columns for ordered scanning.
	pkCols, err := r.detectPrimaryKey(ctx, tx, schema, table)
	if err != nil {
		r.log.Warn().Err(err).Str("table", qualifiedName).Msg("failed to detect primary key; using unordered scan")
		pkCols = nil
	}

	// Build cursor query with optional ORDER BY.
	tableName := pgx.Identifier{schema, table}.Sanitize()
	cursorName := "cdc_snapshot_cursor"
	cursorQuery := fmt.Sprintf("DECLARE %s CURSOR FOR SELECT * FROM %s", cursorName, tableName)
	if len(pkCols) > 0 {
		cursorQuery += " ORDER BY " + strings.Join(pkCols, ", ")
		r.log.Debug().Str("table", qualifiedName).Strs("pk", pkCols).Msg("using PK-ordered scan")
	}

	if _, err := tx.Exec(ctx, cursorQuery); err != nil {
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

			_, _, key, value, err := enc.Encode(ev, true)
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

// detectPrimaryKey queries pg_index to find primary key column names for a table.
// Returns nil if no primary key exists or on error.
func (r *Runner) detectPrimaryKey(ctx context.Context, tx pgx.Tx, schema, table string) ([]string, error) {
	const query = `
		SELECT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		WHERE i.indrelid = ($1 || '.' || $2)::regclass
		  AND i.indisprimary
		ORDER BY array_position(i.indkey, a.attnum)`

	rows, err := tx.Query(ctx, query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("query pg_index: %w", err)
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, fmt.Errorf("scan pk column: %w", err)
		}
		cols = append(cols, col)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return cols, nil
}

// parseQualifiedName splits "schema.table" into (schema, table).
// Defaults to "public" if no schema is specified.
func parseQualifiedName(name string) (string, string) {
	for i := range len(name) {
		if name[i] == '.' {
			return name[:i], name[i+1:]
		}
	}
	return "public", name
}
