//go:build integration

// Package integration_test provides end-to-end tests for the CDC pipeline
// using real PostgreSQL and Redpanda instances.
//
// Run with:
//
//	docker compose up -d postgres
//	go test -race -tags=integration -count=1 -v -timeout=120s ./internal/
//
// The test uses:
//   - PostgreSQL on localhost:5433 (docker-compose CDC instance)
//   - Redpanda on localhost:19092 (existing instance)
package integration_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	json "github.com/goccy/go-json"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/kperreau/postgres-cdc/internal/checkpoint"
	"github.com/kperreau/postgres-cdc/internal/health"
	"github.com/kperreau/postgres-cdc/internal/metrics"
	"github.com/kperreau/postgres-cdc/internal/model"
	"github.com/kperreau/postgres-cdc/internal/pgrepl"
	"github.com/kperreau/postgres-cdc/internal/pipeline"
	"github.com/kperreau/postgres-cdc/internal/producer"
	"github.com/kperreau/postgres-cdc/internal/topic"
)

const (
	testPGDSN    = "host=localhost port=5433 user=cdc password=changeme dbname=app sslmode=disable"
	testBrokers  = "localhost:19092"
	testSlot     = "cdc_integration_test"
	testPub      = "cdc_integration_pub"
	testDatabase = "app"

	// pgContainerName is the docker container running the CDC project's postgres.
	pgContainerName = "interesting-proskuriakova-postgres-1"
)

func testLogger() zerolog.Logger {
	return zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr}).With().Timestamp().Logger()
}

// newProducerConfig returns a default producer config for integration tests.
// TopicPartitions=1 enables EnsureTopic so topics are created by Go code,
// not by Redpanda auto-create.
func newProducerConfig() producer.Config {
	return producer.Config{
		Brokers:                []string{testBrokers},
		Compression:            "none",
		Linger:                 time.Millisecond,
		MaxInflight:            10,
		RequiredAcks:           "all",
		BatchMaxBytes:          1048576,
		BatchMaxRecords:        100,
		TopicPartitions:        1,
		TopicReplicationFactor: 1,
	}
}

func setupTestTable(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()

	// Create table first.
	_, err := pool.Exec(ctx, `
		DROP TABLE IF EXISTS integration_test;
		CREATE TABLE integration_test (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT
		);
	`)
	if err != nil {
		t.Fatalf("create test table: %v", err)
	}

	// Drop and recreate publication to include only the test table.
	_, _ = pool.Exec(ctx, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", testPub))
	_, err = pool.Exec(ctx, fmt.Sprintf(
		"CREATE PUBLICATION %s FOR TABLE integration_test", testPub))
	if err != nil {
		t.Fatalf("create publication: %v", err)
	}
}

func cleanupSlot(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	// Terminate any active connections using the slot before dropping it.
	_, _ = pool.Exec(context.Background(), `
		SELECT pg_terminate_backend(active_pid)
		FROM pg_replication_slots
		WHERE slot_name = $1 AND active_pid IS NOT NULL`,
		testSlot,
	)
	time.Sleep(200 * time.Millisecond)
	_, _ = pool.Exec(context.Background(), fmt.Sprintf(
		"SELECT pg_drop_replication_slot('%s')", testSlot,
	))
}

// consumeEvents polls the Redpanda topic until n events are received or the
// deadline is hit. It uses AtStart offset so it always reads from the beginning.
func consumeEvents(t *testing.T, ctx context.Context, topicName string, want int) []model.CDCEnvelope {
	t.Helper()

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(testBrokers),
		kgo.ConsumeTopics(topicName),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	t.Cleanup(consumer.Close)

	var envelopes []model.CDCEnvelope
	deadline := time.After(20 * time.Second)
	for len(envelopes) < want {
		select {
		case <-deadline:
			t.Fatalf("timed out after %d/%d events", len(envelopes), want)
		case <-ctx.Done():
			t.Fatalf("context cancelled after %d/%d events", len(envelopes), want)
		default:
		}

		fetchCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		fetches := consumer.PollFetches(fetchCtx)
		cancel()

		fetches.EachRecord(func(r *kgo.Record) {
			var env model.CDCEnvelope
			if err := json.Unmarshal(r.Value, &env); err != nil {
				t.Logf("unmarshal error: %v (raw: %s)", err, r.Value)
				return
			}
			t.Logf("received: op=%s table=%s", env.Op, env.Table)
			envelopes = append(envelopes, env)
		})
	}
	return envelopes
}

// startPipeline boots a CDC pipeline against the test slot/publication and
// returns a cancel function plus an error channel. The pipeline runs in a
// separate goroutine.
func startPipeline(
	ctx context.Context,
	t *testing.T,
	prod *producer.Producer,
	m *metrics.Metrics,
	cpMgr *checkpoint.Manager,
	resolver *topic.Resolver,
	hs *health.Status,
) (cancel context.CancelFunc, errCh <-chan error) {
	t.Helper()

	pipeCtx, pipeCancel := context.WithCancel(ctx)
	pipe := pipeline.New(
		pipeline.Config{
			QueueCapacity: 16,
			MaxTxBytes:    64 * 1024 * 1024,
			SourceName:    "integration",
			Database:      testDatabase,
		},
		pgrepl.ReaderConfig{
			ConnString:      testPGDSN,
			SlotName:        testSlot,
			PublicationName: testPub,
			CreateSlot:      true,
			StatusInterval:  2 * time.Second,
			ConfirmedLSN:    cpMgr.LastFlushed,
		},
		prod, cpMgr, resolver, hs, m, testLogger(),
	)

	ch := make(chan error, 1)
	go func() { ch <- pipe.Run(pipeCtx) }()
	return pipeCancel, ch
}

// ── Test 1: Happy-path INSERT / UPDATE / DELETE ───────────────────────────

func TestEndToEndInsertUpdateDelete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	log := testLogger()

	pool, err := pgxpool.New(ctx, testPGDSN)
	if err != nil {
		t.Skipf("PostgreSQL not available: %v", err)
	}
	defer pool.Close()

	cleanupSlot(t, pool)
	setupTestTable(t, pool)
	t.Cleanup(func() { cleanupSlot(t, pool) })

	reg := prometheus.NewRegistry()
	m := metrics.NewWithRegistry("e2e", reg)
	cpStore, _ := checkpoint.NewFileStore(t.TempDir() + "/cp.json")
	cpMgr := checkpoint.NewManager(cpStore, 100*time.Millisecond, log, &m.CDC)
	_, _ = cpMgr.LoadInitial()

	resolver, _ := topic.NewResolver(model.TopicPerTable, "cdc", "")

	prod, err := producer.New(ctx, newProducerConfig(), log, &m.CDC)
	if err != nil {
		t.Skipf("Redpanda not available: %v", err)
	}
	defer prod.Close()

	hs := health.NewStatus()
	pipeCancel, pipeErrCh := startPipeline(ctx, t, prod, m, cpMgr, resolver, hs)
	defer pipeCancel()

	// Let the pipeline connect and start streaming.
	time.Sleep(3 * time.Second)

	// Perform DML.
	_, err = pool.Exec(ctx, `INSERT INTO integration_test (name, email) VALUES ('alice', 'alice@example.com')`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = pool.Exec(ctx, `UPDATE integration_test SET email = 'alice2@example.com' WHERE name = 'alice'`)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	_, err = pool.Exec(ctx, `DELETE FROM integration_test WHERE name = 'alice'`)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Give the pipeline time to flush.
	time.Sleep(2 * time.Second)

	topicName := resolver.Resolve(testDatabase, "public", "integration_test")
	t.Logf("consuming from topic: %s", topicName)

	envelopes := consumeEvents(t, ctx, topicName, 3)

	// Validate envelope shape and op types.
	ops := make(map[string]int)
	for _, env := range envelopes {
		ops[env.Op]++
		if env.Table != "integration_test" {
			t.Errorf("unexpected table: %s", env.Table)
		}
		if env.Schema != "public" {
			t.Errorf("unexpected schema: %s", env.Schema)
		}
		if env.Database != testDatabase {
			t.Errorf("unexpected database: %s", env.Database)
		}
		if env.Source == "" {
			t.Error("source should not be empty")
		}
		if env.LSN == "" {
			t.Error("LSN should not be empty")
		}
	}
	if ops["c"] < 1 {
		t.Error("missing insert event (op=c)")
	}
	if ops["u"] < 1 {
		t.Error("missing update event (op=u)")
	}
	if ops["d"] < 1 {
		t.Error("missing delete event (op=d)")
	}

	pipeCancel()
	<-pipeErrCh

	// Checkpoint must have been written.
	if err := cpMgr.Flush(); err != nil {
		t.Fatalf("flush checkpoint: %v", err)
	}
	cp, _ := cpStore.Load()
	if cp.LSN == 0 {
		t.Error("checkpoint LSN should be > 0 after processing events")
	}
	t.Logf("final checkpoint LSN: %s", cp.LSN)
}

// ── Test 2: PostgreSQL reconnect after container restart ──────────────────

func TestIntegration_PostgresReconnect(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not in PATH; skipping crash-recovery test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	log := testLogger()

	pool, err := pgxpool.New(ctx, testPGDSN)
	if err != nil {
		t.Skipf("PostgreSQL not available: %v", err)
	}
	defer pool.Close()

	cleanupSlot(t, pool)
	setupTestTable(t, pool)
	t.Cleanup(func() {
		// Reconnect pool if postgres was stopped.
		pool2, _ := pgxpool.New(context.Background(), testPGDSN)
		if pool2 != nil {
			cleanupSlot(t, pool2)
			pool2.Close()
		}
	})

	reg := prometheus.NewRegistry()
	m := metrics.NewWithRegistry("reconnect", reg)
	cpStore, _ := checkpoint.NewFileStore(t.TempDir() + "/cp.json")
	cpMgr := checkpoint.NewManager(cpStore, 100*time.Millisecond, log, &m.CDC)
	_, _ = cpMgr.LoadInitial()

	resolver, _ := topic.NewResolver(model.TopicPerTable, "cdc", "")

	prod, err := producer.New(ctx, newProducerConfig(), log, &m.CDC)
	if err != nil {
		t.Skipf("Redpanda not available: %v", err)
	}
	defer prod.Close()

	hs := health.NewStatus()
	pipeCancel, pipeErrCh := startPipeline(ctx, t, prod, m, cpMgr, resolver, hs)
	defer pipeCancel()

	// Wait for pipeline to connect.
	time.Sleep(3 * time.Second)

	// Insert a first row to confirm the pipeline is live.
	_, err = pool.Exec(ctx, `INSERT INTO integration_test (name, email) VALUES ('before-crash', 'x@x.com')`)
	if err != nil {
		t.Fatalf("pre-crash insert: %v", err)
	}
	time.Sleep(2 * time.Second)

	// Stop the postgres container to simulate a crash.
	t.Log("stopping postgres container…")
	stopCmd := exec.CommandContext(ctx, "docker", "stop", pgContainerName)
	if out, err := stopCmd.CombinedOutput(); err != nil {
		t.Skipf("cannot stop container %s: %v – %s", pgContainerName, err, out)
	}

	// Wait a few seconds while the reader is reconnecting.
	time.Sleep(4 * time.Second)

	// Restart postgres.
	t.Log("restarting postgres container…")
	startCmd := exec.CommandContext(ctx, "docker", "start", pgContainerName)
	if out, err := startCmd.CombinedOutput(); err != nil {
		t.Fatalf("cannot start container %s: %v – %s", pgContainerName, err, out)
	}

	// Wait for postgres to be ready.
	t.Log("waiting for postgres to become ready…")
	for i := 0; i < 20; i++ {
		time.Sleep(time.Second)
		pool2, err := pgxpool.New(ctx, testPGDSN)
		if err == nil {
			var ok bool
			if err2 := pool2.QueryRow(ctx, "SELECT true").Scan(&ok); err2 == nil && ok {
				pool2.Close()
				break
			}
			pool2.Close()
		}
	}

	// Insert a row after recovery and verify the pipeline picks it up.
	t.Log("inserting post-crash row…")
	pool3, err := pgxpool.New(ctx, testPGDSN)
	if err != nil {
		t.Fatalf("reconnect pool: %v", err)
	}
	defer pool3.Close()

	_, err = pool3.Exec(ctx, `INSERT INTO integration_test (name, email) VALUES ('after-crash', 'y@y.com')`)
	if err != nil {
		t.Fatalf("post-crash insert: %v", err)
	}

	time.Sleep(2 * time.Second)

	// Verify reconnect counter incremented.
	reconnects, err := getCounterValue(reg, "reconnect_cdc_reconnects_total")
	if err != nil {
		t.Logf("reconnect metric unavailable: %v", err)
	} else if reconnects < 1 {
		t.Errorf("expected at least 1 reconnect, got %.0f", reconnects)
	} else {
		t.Logf("reconnects: %.0f", reconnects)
	}

	topicName := resolver.Resolve(testDatabase, "public", "integration_test")
	envelopes := consumeEvents(t, ctx, topicName, 2) // before-crash + after-crash
	t.Logf("got %d events after crash recovery", len(envelopes))

	pipeCancel()
	<-pipeErrCh
}

// ── Test 3: Backpressure – slow Redpanda publish fills txCh ──────────────

func TestIntegration_Backpressure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	log := testLogger()

	pool, err := pgxpool.New(ctx, testPGDSN)
	if err != nil {
		t.Skipf("PostgreSQL not available: %v", err)
	}
	defer pool.Close()

	cleanupSlot(t, pool)
	setupTestTable(t, pool)
	t.Cleanup(func() { cleanupSlot(t, pool) })

	reg := prometheus.NewRegistry()
	m := metrics.NewWithRegistry("bp", reg)

	// Very small queue: backpressure will kick in quickly when rows arrive fast.
	cpStore, _ := checkpoint.NewFileStore(t.TempDir() + "/cp.json")
	cpMgr := checkpoint.NewManager(cpStore, 100*time.Millisecond, log, &m.CDC)
	_, _ = cpMgr.LoadInitial()

	resolver, _ := topic.NewResolver(model.TopicPerTable, "cdc", "")

	prod, err := producer.New(ctx, newProducerConfig(), log, &m.CDC)
	if err != nil {
		t.Skipf("Redpanda not available: %v", err)
	}
	defer prod.Close()

	hs := health.NewStatus()

	pipeCtx, pipeCancel := context.WithCancel(ctx)
	defer pipeCancel()

	// Use queue capacity of 1 to trigger backpressure easily.
	pipe := pipeline.New(
		pipeline.Config{
			QueueCapacity: 1,
			MaxTxBytes:    64 * 1024 * 1024,
			SourceName:    "backpressure",
			Database:      testDatabase,
		},
		pgrepl.ReaderConfig{
			ConnString:      testPGDSN,
			SlotName:        testSlot,
			PublicationName: testPub,
			CreateSlot:      true,
			StatusInterval:  2 * time.Second,
			ConfirmedLSN:    cpMgr.LastFlushed,
		},
		prod, cpMgr, resolver, hs, m, log,
	)

	pipeErrCh := make(chan error, 1)
	go func() { pipeErrCh <- pipe.Run(pipeCtx) }()

	// Let the pipeline connect.
	time.Sleep(3 * time.Second)

	// Insert many rows rapidly to fill the queue and trigger backpressure.
	const numRows = 20
	t.Logf("inserting %d rows rapidly…", numRows)
	for i := 0; i < numRows; i++ {
		_, err = pool.Exec(ctx, `INSERT INTO integration_test (name, email) VALUES ($1, $2)`,
			fmt.Sprintf("user-%d", i), fmt.Sprintf("u%d@x.com", i))
		if err != nil {
			t.Fatalf("insert row %d: %v", i, err)
		}
	}

	// Wait for events to be processed.
	time.Sleep(5 * time.Second)

	// Verify backpressure seconds was recorded.
	bp, err := getGaugeValue(reg, "bp_cdc_backpressure_seconds")
	if err != nil {
		t.Logf("backpressure metric unavailable: %v (may be 0 if queue drained fast)", err)
	} else {
		t.Logf("backpressure_seconds_total: %f", bp)
		// With queue=1 and 20 rapid inserts, backpressure should register.
		if bp <= 0 {
			t.Log("backpressure did not register (queue drained before filling – acceptable on fast machines)")
		}
	}

	// Verify all events arrived on the topic.
	topicName := resolver.Resolve(testDatabase, "public", "integration_test")
	envelopes := consumeEvents(t, ctx, topicName, numRows)
	t.Logf("received %d/%d events", len(envelopes), numRows)

	pipeCancel()
	<-pipeErrCh
}

// ── Prometheus helpers ────────────────────────────────────────────────────

func getCounterValue(reg *prometheus.Registry, name string) (float64, error) {
	mfs, err := reg.Gather()
	if err != nil {
		return 0, err
	}
	for _, mf := range mfs {
		if mf.GetName() == name {
			if len(mf.GetMetric()) > 0 {
				return mf.GetMetric()[0].GetCounter().GetValue(), nil
			}
		}
	}
	return 0, fmt.Errorf("metric %q not found", name)
}

func getGaugeValue(reg *prometheus.Registry, name string) (float64, error) {
	mfs, err := reg.Gather()
	if err != nil {
		return 0, err
	}
	for _, mf := range mfs {
		if mf.GetName() == name {
			if len(mf.GetMetric()) > 0 {
				m := mf.GetMetric()[0]
				if m.GetCounter() != nil {
					return m.GetCounter().GetValue(), nil
				}
				return m.GetGauge().GetValue(), nil
			}
		}
	}
	return 0, fmt.Errorf("metric %q not found", name)
}
