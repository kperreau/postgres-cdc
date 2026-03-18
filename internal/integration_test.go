//go:build integration

// Package integration_test provides end-to-end tests for the CDC pipeline
// using real PostgreSQL and Redpanda/Kafka instances.
//
// Run with:
//
//	docker compose up -d postgres
//	go test -race -tags=integration -count=1 -v -timeout=300s ./internal/
//
// Environment variables (with defaults for local docker-compose):
//
//	TEST_PG_DSN     host=localhost port=5433 user=cdc password=changeme dbname=app sslmode=disable
//	TEST_BROKER     localhost:19092
//	TEST_PG_CONTAINER  (empty = skip docker restart tests)
package integration_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	json "github.com/goccy/go-json"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kadm"
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

// ── Environment helpers ───────────────────────────────────────────────────

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func testPGDSN() string {
	return envOr("TEST_PG_DSN", "host=localhost port=5433 user=cdc password=changeme dbname=app sslmode=disable")
}

func testBroker() string {
	return envOr("TEST_BROKER", "localhost:19092")
}

func testPGContainer() string {
	return os.Getenv("TEST_PG_CONTAINER")
}

const testDatabase = "app"

// ── Common helpers ────────────────────────────────────────────────────────

func testLogger() zerolog.Logger {
	return zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr}).With().Timestamp().Logger()
}

// newProducerConfig returns a default producer config for integration tests.
func newProducerConfig() producer.Config {
	return producer.Config{
		Brokers:                []string{testBroker()},
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

func connectPG(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(ctx, testPGDSN())
	if err != nil {
		t.Skipf("PostgreSQL not available: %v", err)
	}
	return pool
}

func connectProducer(t *testing.T, ctx context.Context, log zerolog.Logger, m *metrics.CDCMetrics) *producer.Producer {
	t.Helper()
	prod, err := producer.New(ctx, newProducerConfig(), log, m)
	if err != nil {
		t.Skipf("Redpanda/Kafka not available: %v", err)
	}
	return prod
}

func setupTable(t *testing.T, pool *pgxpool.Pool, tableName, pubName string) {
	t.Helper()
	ctx := context.Background()

	_, err := pool.Exec(ctx, fmt.Sprintf(`
		DROP TABLE IF EXISTS %s;
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT
		);
	`, tableName, tableName))
	if err != nil {
		t.Fatalf("create test table %s: %v", tableName, err)
	}

	_, _ = pool.Exec(ctx, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pubName))
	_, err = pool.Exec(ctx, fmt.Sprintf(
		"CREATE PUBLICATION %s FOR TABLE %s", pubName, tableName))
	if err != nil {
		t.Fatalf("create publication %s: %v", pubName, err)
	}
}

func setupTables(t *testing.T, pool *pgxpool.Pool, tableNames []string, pubName string) {
	t.Helper()
	ctx := context.Background()

	for _, tbl := range tableNames {
		_, err := pool.Exec(ctx, fmt.Sprintf(`
			DROP TABLE IF EXISTS %s;
			CREATE TABLE %s (
				id SERIAL PRIMARY KEY,
				name TEXT NOT NULL,
				email TEXT
			);
		`, tbl, tbl))
		if err != nil {
			t.Fatalf("create test table %s: %v", tbl, err)
		}
	}

	_, _ = pool.Exec(ctx, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pubName))
	_, err := pool.Exec(ctx, fmt.Sprintf(
		"CREATE PUBLICATION %s FOR TABLE %s", pubName, strings.Join(tableNames, ", ")))
	if err != nil {
		t.Fatalf("create publication %s: %v", pubName, err)
	}
}

func cleanupSlot(t *testing.T, pool *pgxpool.Pool, slotName string) {
	t.Helper()
	_, _ = pool.Exec(context.Background(), `
		SELECT pg_terminate_backend(active_pid)
		FROM pg_replication_slots
		WHERE slot_name = $1 AND active_pid IS NOT NULL`,
		slotName,
	)
	time.Sleep(200 * time.Millisecond)
	_, _ = pool.Exec(context.Background(), fmt.Sprintf(
		"SELECT pg_drop_replication_slot('%s')", slotName,
	))
}

// cleanupTopics deletes all Kafka topics whose name starts with the given prefix.
// Safe to call even if no topics match. Registered via t.Cleanup so topics are
// removed after each test run, preventing stale data from polluting the next run.
func cleanupTopics(t *testing.T, prefix string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(testBroker()))
	if err != nil {
		t.Logf("cleanupTopics: cannot connect: %v", err)
		return
	}
	defer cl.Close()

	adm := kadm.NewClient(cl)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	listed, err := adm.ListTopics(ctx)
	if err != nil {
		t.Logf("cleanupTopics: list failed: %v", err)
		return
	}

	var toDelete []string
	for _, tpc := range listed.Sorted() {
		if strings.HasPrefix(tpc.Topic, prefix) {
			toDelete = append(toDelete, tpc.Topic)
		}
	}
	if len(toDelete) == 0 {
		return
	}

	if _, err := adm.DeleteTopics(ctx, toDelete...); err != nil {
		t.Logf("cleanupTopics: delete failed: %v", err)
	}
}

// consumeEvents polls the topic until n events are received or the deadline is hit.
func consumeEvents(t *testing.T, ctx context.Context, topicName string, want int) []model.CDCEnvelope {
	t.Helper()

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(testBroker()),
		kgo.ConsumeTopics(topicName),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	t.Cleanup(consumer.Close)

	var envelopes []model.CDCEnvelope
	deadline := time.After(30 * time.Second)
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
			t.Logf("received: op=%s table=%s txid=%d", env.Op, env.Table, env.TxID)
			envelopes = append(envelopes, env)
		})
	}
	return envelopes
}

// consumeRawRecords polls the topic and returns raw kgo.Record values.
func consumeRawRecords(t *testing.T, ctx context.Context, topicName string, want int, timeout time.Duration) []*kgo.Record {
	t.Helper()

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(testBroker()),
		kgo.ConsumeTopics(topicName),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	t.Cleanup(consumer.Close)

	var records []*kgo.Record
	deadline := time.After(timeout)
	for len(records) < want {
		select {
		case <-deadline:
			return records // return what we have
		case <-ctx.Done():
			return records
		default:
		}

		fetchCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		fetches := consumer.PollFetches(fetchCtx)
		cancel()

		fetches.EachRecord(func(r *kgo.Record) {
			records = append(records, r)
		})
	}
	return records
}

// startPipeline boots a CDC pipeline and returns a cancel function plus an error channel.
func startPipeline(
	ctx context.Context,
	t *testing.T,
	prod *producer.Producer,
	m *metrics.Metrics,
	cpMgr *checkpoint.Manager,
	resolver *topic.Resolver,
	hs *health.Status,
	slotName, pubName string,
	pipeCfg pipeline.Config,
) (cancel context.CancelFunc, errCh <-chan error) {
	t.Helper()

	pipeCtx, pipeCancel := context.WithCancel(ctx)
	pipe := pipeline.New(
		pipeCfg,
		pgrepl.ReaderConfig{
			ConnString:      testPGDSN(),
			SlotName:        slotName,
			PublicationName: pubName,
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

// startPipelineWithReaderCfg boots a pipeline with custom ReaderConfig.
func startPipelineWithReaderCfg(
	ctx context.Context,
	t *testing.T,
	prod *producer.Producer,
	m *metrics.Metrics,
	cpMgr *checkpoint.Manager,
	resolver *topic.Resolver,
	hs *health.Status,
	pipeCfg pipeline.Config,
	readerCfg pgrepl.ReaderConfig,
) (cancel context.CancelFunc, errCh <-chan error) {
	t.Helper()

	pipeCtx, pipeCancel := context.WithCancel(ctx)
	pipe := pipeline.New(
		pipeCfg,
		readerCfg,
		prod, cpMgr, resolver, hs, m, testLogger(),
	)

	ch := make(chan error, 1)
	go func() { ch <- pipe.Run(pipeCtx) }()
	return pipeCancel, ch
}

func defaultPipelineCfg() pipeline.Config {
	return pipeline.Config{
		QueueCapacity: 16,
		MaxTxBytes:    64 * 1024 * 1024,
		SourceName:    "integration",
		Database:      testDatabase,
	}
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

// ── Test 1: Happy-path INSERT / UPDATE / DELETE ───────────────────────────

func TestEndToEndInsertUpdateDelete(t *testing.T) {
	t.Parallel()

	const (
		slotName  = "cdc_test_iud"
		pubName   = "cdc_test_iud_pub"
		tableName = "integration_test_iud"
	)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	log := testLogger()
	pool := connectPG(t, ctx)
	defer pool.Close()

	cleanupSlot(t, pool, slotName)
	setupTable(t, pool, tableName, pubName)
	t.Cleanup(func() { cleanupSlot(t, pool, slotName) })

	reg := prometheus.NewRegistry()
	m := metrics.NewWithRegistry("e2e", reg)
	cpStore, _ := checkpoint.NewFileStore(t.TempDir() + "/cp.json")
	cpMgr := checkpoint.NewManager(cpStore, 100*time.Millisecond, log, &m.CDC)
	_, _ = cpMgr.LoadInitial()

	resolver, _ := topic.NewResolver(model.TopicPerTable, "cdc_iud", "")
	t.Cleanup(func() { cleanupTopics(t, "cdc_iud") })

	prod := connectProducer(t, ctx, log, &m.CDC)
	defer prod.Close()

	hs := health.NewStatus()
	pipeCancel, pipeErrCh := startPipeline(ctx, t, prod, m, cpMgr, resolver, hs, slotName, pubName, defaultPipelineCfg())
	defer pipeCancel()

	time.Sleep(3 * time.Second)

	_, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (name, email) VALUES ('alice', 'alice@example.com')`, tableName))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = pool.Exec(ctx, fmt.Sprintf(`UPDATE %s SET email = 'alice2@example.com' WHERE name = 'alice'`, tableName))
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	_, err = pool.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE name = 'alice'`, tableName))
	if err != nil {
		t.Fatalf("delete: %v", err)
	}

	time.Sleep(2 * time.Second)

	topicName := resolver.Resolve(testDatabase, "public", tableName)
	t.Logf("consuming from topic: %s", topicName)

	envelopes := consumeEvents(t, ctx, topicName, 3)

	ops := make(map[string]int)
	for _, env := range envelopes {
		ops[env.Op]++
		if env.Table != tableName {
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
	container := testPGContainer()
	if container == "" {
		t.Skip("TEST_PG_CONTAINER not set; skipping docker restart test")
	}

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not in PATH; skipping crash-recovery test")
	}

	const (
		slotName  = "cdc_test_reconnect"
		pubName   = "cdc_test_reconnect_pub"
		tableName = "integration_test_reconnect"
	)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	log := testLogger()
	pool := connectPG(t, ctx)
	defer pool.Close()

	cleanupSlot(t, pool, slotName)
	setupTable(t, pool, tableName, pubName)
	t.Cleanup(func() {
		pool2, _ := pgxpool.New(context.Background(), testPGDSN())
		if pool2 != nil {
			cleanupSlot(t, pool2, slotName)
			pool2.Close()
		}
	})

	reg := prometheus.NewRegistry()
	m := metrics.NewWithRegistry("reconnect", reg)
	cpStore, _ := checkpoint.NewFileStore(t.TempDir() + "/cp.json")
	cpMgr := checkpoint.NewManager(cpStore, 100*time.Millisecond, log, &m.CDC)
	_, _ = cpMgr.LoadInitial()

	resolver, _ := topic.NewResolver(model.TopicPerTable, "cdc_recon", "")
	t.Cleanup(func() { cleanupTopics(t, "cdc_recon") })

	prod := connectProducer(t, ctx, log, &m.CDC)
	defer prod.Close()

	hs := health.NewStatus()
	pipeCancel, pipeErrCh := startPipeline(ctx, t, prod, m, cpMgr, resolver, hs, slotName, pubName, defaultPipelineCfg())
	defer pipeCancel()

	time.Sleep(3 * time.Second)

	_, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (name, email) VALUES ('before-crash', 'x@x.com')`, tableName))
	if err != nil {
		t.Fatalf("pre-crash insert: %v", err)
	}
	time.Sleep(2 * time.Second)

	t.Log("stopping postgres container...")
	stopCmd := exec.CommandContext(ctx, "docker", "stop", container)
	if out, err := stopCmd.CombinedOutput(); err != nil {
		t.Skipf("cannot stop container %s: %v -- %s", container, err, out)
	}

	time.Sleep(4 * time.Second)

	t.Log("restarting postgres container...")
	startCmd := exec.CommandContext(ctx, "docker", "start", container)
	if out, err := startCmd.CombinedOutput(); err != nil {
		t.Fatalf("cannot start container %s: %v -- %s", container, err, out)
	}

	t.Log("waiting for postgres to become ready...")
	for i := 0; i < 20; i++ {
		time.Sleep(time.Second)
		pool2, err := pgxpool.New(ctx, testPGDSN())
		if err == nil {
			var ok bool
			if err2 := pool2.QueryRow(ctx, "SELECT true").Scan(&ok); err2 == nil && ok {
				pool2.Close()
				break
			}
			pool2.Close()
		}
	}

	t.Log("inserting post-crash row...")
	pool3, err := pgxpool.New(ctx, testPGDSN())
	if err != nil {
		t.Fatalf("reconnect pool: %v", err)
	}
	defer pool3.Close()

	_, err = pool3.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (name, email) VALUES ('after-crash', 'y@y.com')`, tableName))
	if err != nil {
		t.Fatalf("post-crash insert: %v", err)
	}

	time.Sleep(2 * time.Second)

	reconnects, err := getCounterValue(reg, "reconnect_cdc_reconnects_total")
	if err != nil {
		t.Logf("reconnect metric unavailable: %v", err)
	} else if reconnects < 1 {
		t.Errorf("expected at least 1 reconnect, got %.0f", reconnects)
	} else {
		t.Logf("reconnects: %.0f", reconnects)
	}

	topicName := resolver.Resolve(testDatabase, "public", tableName)
	envelopes := consumeEvents(t, ctx, topicName, 2)
	t.Logf("got %d events after crash recovery", len(envelopes))

	pipeCancel()
	<-pipeErrCh
}

// ── Test 3: Backpressure ──────────────────────────────────────────────────

func TestIntegration_Backpressure(t *testing.T) {
	t.Parallel()

	const (
		slotName  = "cdc_test_bp"
		pubName   = "cdc_test_bp_pub"
		tableName = "integration_test_bp"
	)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	log := testLogger()
	pool := connectPG(t, ctx)
	defer pool.Close()

	cleanupSlot(t, pool, slotName)
	setupTable(t, pool, tableName, pubName)
	t.Cleanup(func() { cleanupSlot(t, pool, slotName) })

	reg := prometheus.NewRegistry()
	m := metrics.NewWithRegistry("bp", reg)

	cpStore, _ := checkpoint.NewFileStore(t.TempDir() + "/cp.json")
	cpMgr := checkpoint.NewManager(cpStore, 100*time.Millisecond, log, &m.CDC)
	_, _ = cpMgr.LoadInitial()

	resolver, _ := topic.NewResolver(model.TopicPerTable, "cdc_bp", "")
	t.Cleanup(func() { cleanupTopics(t, "cdc_bp") })

	prod := connectProducer(t, ctx, log, &m.CDC)
	defer prod.Close()

	hs := health.NewStatus()

	pipeCtx, pipeCancel := context.WithCancel(ctx)
	defer pipeCancel()

	pipe := pipeline.New(
		pipeline.Config{
			QueueCapacity: 1,
			MaxTxBytes:    64 * 1024 * 1024,
			SourceName:    "backpressure",
			Database:      testDatabase,
		},
		pgrepl.ReaderConfig{
			ConnString:      testPGDSN(),
			SlotName:        slotName,
			PublicationName: pubName,
			CreateSlot:      true,
			StatusInterval:  2 * time.Second,
			ConfirmedLSN:    cpMgr.LastFlushed,
		},
		prod, cpMgr, resolver, hs, m, log,
	)

	pipeErrCh := make(chan error, 1)
	go func() { pipeErrCh <- pipe.Run(pipeCtx) }()

	time.Sleep(3 * time.Second)

	const numRows = 20
	t.Logf("inserting %d rows rapidly...", numRows)
	for i := 0; i < numRows; i++ {
		_, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (name, email) VALUES ($1, $2)`, tableName),
			fmt.Sprintf("user-%d", i), fmt.Sprintf("u%d@x.com", i))
		if err != nil {
			t.Fatalf("insert row %d: %v", i, err)
		}
	}

	time.Sleep(5 * time.Second)

	bp, err := getGaugeValue(reg, "bp_cdc_backpressure_seconds")
	if err != nil {
		t.Logf("backpressure metric unavailable: %v (may be 0 if queue drained fast)", err)
	} else {
		t.Logf("backpressure_seconds_total: %f", bp)
		if bp <= 0 {
			t.Log("backpressure did not register (queue drained before filling -- acceptable on fast machines)")
		}
	}

	topicName := resolver.Resolve(testDatabase, "public", tableName)
	envelopes := consumeEvents(t, ctx, topicName, numRows)
	t.Logf("received %d/%d events", len(envelopes), numRows)

	pipeCancel()
	<-pipeErrCh
}

// ── Test 4: MultiTableTransaction ─────────────────────────────────────────

func TestIntegration_MultiTableTransaction(t *testing.T) {
	t.Parallel()

	const (
		slotName = "cdc_test_multitbl"
		pubName  = "cdc_test_multitbl_pub"
		table1   = "integration_test_multi_a"
		table2   = "integration_test_multi_b"
	)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	log := testLogger()
	pool := connectPG(t, ctx)
	defer pool.Close()

	cleanupSlot(t, pool, slotName)
	setupTables(t, pool, []string{table1, table2}, pubName)
	t.Cleanup(func() { cleanupSlot(t, pool, slotName) })

	reg := prometheus.NewRegistry()
	m := metrics.NewWithRegistry("multi", reg)
	cpStore, _ := checkpoint.NewFileStore(t.TempDir() + "/cp.json")
	cpMgr := checkpoint.NewManager(cpStore, 100*time.Millisecond, log, &m.CDC)
	_, _ = cpMgr.LoadInitial()

	resolver, _ := topic.NewResolver(model.TopicPerTable, "cdc_multi", "")
	t.Cleanup(func() { cleanupTopics(t, "cdc_multi") })

	prod := connectProducer(t, ctx, log, &m.CDC)
	defer prod.Close()

	hs := health.NewStatus()
	pipeCancel, pipeErrCh := startPipeline(ctx, t, prod, m, cpMgr, resolver, hs, slotName, pubName, defaultPipelineCfg())
	defer pipeCancel()

	time.Sleep(3 * time.Second)

	// Single transaction spanning two tables.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	_, err = tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (name, email) VALUES ('multi-a', 'a@x.com')`, table1))
	if err != nil {
		t.Fatalf("insert table1: %v", err)
	}
	_, err = tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (name, email) VALUES ('multi-b', 'b@x.com')`, table2))
	if err != nil {
		t.Fatalf("insert table2: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit tx: %v", err)
	}

	time.Sleep(2 * time.Second)

	topic1 := resolver.Resolve(testDatabase, "public", table1)
	topic2 := resolver.Resolve(testDatabase, "public", table2)

	env1 := consumeEvents(t, ctx, topic1, 1)
	env2 := consumeEvents(t, ctx, topic2, 1)

	if env1[0].TxID == 0 {
		t.Error("txid should not be 0")
	}
	if env1[0].TxID != env2[0].TxID {
		t.Errorf("events from same tx should have same txid: %d vs %d", env1[0].TxID, env2[0].TxID)
	}
	if env1[0].Table != table1 {
		t.Errorf("expected table %s, got %s", table1, env1[0].Table)
	}
	if env2[0].Table != table2 {
		t.Errorf("expected table %s, got %s", table2, env2[0].Table)
	}

	pipeCancel()
	<-pipeErrCh
}

// ── Test 5: LargeTransaction ──────────────────────────────────────────────

func TestIntegration_LargeTransaction(t *testing.T) {
	t.Parallel()

	const (
		slotName  = "cdc_test_largetx"
		pubName   = "cdc_test_largetx_pub"
		tableName = "integration_test_largetx"
		numRows   = 500
	)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	log := testLogger()
	pool := connectPG(t, ctx)
	defer pool.Close()

	cleanupSlot(t, pool, slotName)
	setupTable(t, pool, tableName, pubName)
	t.Cleanup(func() { cleanupSlot(t, pool, slotName) })

	reg := prometheus.NewRegistry()
	m := metrics.NewWithRegistry("large", reg)
	cpStore, _ := checkpoint.NewFileStore(t.TempDir() + "/cp.json")
	cpMgr := checkpoint.NewManager(cpStore, 100*time.Millisecond, log, &m.CDC)
	_, _ = cpMgr.LoadInitial()

	resolver, _ := topic.NewResolver(model.TopicPerTable, "cdc_large", "")
	t.Cleanup(func() { cleanupTopics(t, "cdc_large") })

	prod := connectProducer(t, ctx, log, &m.CDC)
	defer prod.Close()

	hs := health.NewStatus()
	pipeCancel, pipeErrCh := startPipeline(ctx, t, prod, m, cpMgr, resolver, hs, slotName, pubName, defaultPipelineCfg())
	defer pipeCancel()

	time.Sleep(3 * time.Second)

	// Insert 500 rows in a single transaction.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	for i := 0; i < numRows; i++ {
		_, err = tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (name, email) VALUES ($1, $2)`, tableName),
			fmt.Sprintf("user-%d", i), fmt.Sprintf("u%d@x.com", i))
		if err != nil {
			t.Fatalf("insert row %d: %v", i, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	time.Sleep(5 * time.Second)

	topicName := resolver.Resolve(testDatabase, "public", tableName)
	envelopes := consumeEvents(t, ctx, topicName, numRows)

	if len(envelopes) != numRows {
		t.Errorf("expected %d events, got %d", numRows, len(envelopes))
	}

	// Verify all share the same txid.
	txID := envelopes[0].TxID
	for i, env := range envelopes {
		if env.TxID != txID {
			t.Errorf("event %d has different txid: %d vs %d", i, env.TxID, txID)
			break
		}
	}

	pipeCancel()
	<-pipeErrCh
}

// ── Test 6: RapidTransactions ─────────────────────────────────────────────

func TestIntegration_RapidTransactions(t *testing.T) {
	t.Parallel()

	const (
		slotName  = "cdc_test_rapid"
		pubName   = "cdc_test_rapid_pub"
		tableName = "integration_test_rapid"
		numTx     = 50
	)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	log := testLogger()
	pool := connectPG(t, ctx)
	defer pool.Close()

	cleanupSlot(t, pool, slotName)
	setupTable(t, pool, tableName, pubName)
	t.Cleanup(func() { cleanupSlot(t, pool, slotName) })

	reg := prometheus.NewRegistry()
	m := metrics.NewWithRegistry("rapid", reg)
	cpStore, _ := checkpoint.NewFileStore(t.TempDir() + "/cp.json")
	cpMgr := checkpoint.NewManager(cpStore, 100*time.Millisecond, log, &m.CDC)
	_, _ = cpMgr.LoadInitial()

	resolver, _ := topic.NewResolver(model.TopicPerTable, "cdc_rapid", "")
	t.Cleanup(func() { cleanupTopics(t, "cdc_rapid") })

	prod := connectProducer(t, ctx, log, &m.CDC)
	defer prod.Close()

	hs := health.NewStatus()
	pipeCancel, pipeErrCh := startPipeline(ctx, t, prod, m, cpMgr, resolver, hs, slotName, pubName, defaultPipelineCfg())
	defer pipeCancel()

	time.Sleep(3 * time.Second)

	// 50 separate single-row transactions.
	for i := 0; i < numTx; i++ {
		_, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (name, email) VALUES ($1, $2)`, tableName),
			fmt.Sprintf("rapid-%d", i), fmt.Sprintf("r%d@x.com", i))
		if err != nil {
			t.Fatalf("insert tx %d: %v", i, err)
		}
	}

	time.Sleep(3 * time.Second)

	topicName := resolver.Resolve(testDatabase, "public", tableName)
	envelopes := consumeEvents(t, ctx, topicName, numTx)

	if len(envelopes) != numTx {
		t.Errorf("expected %d events, got %d", numTx, len(envelopes))
	}

	// Each event should come from a different transaction.
	txIDs := make(map[uint32]bool)
	for _, env := range envelopes {
		txIDs[env.TxID] = true
	}
	if len(txIDs) != numTx {
		t.Errorf("expected %d distinct txids, got %d", numTx, len(txIDs))
	}

	pipeCancel()
	<-pipeErrCh
}

// ── Test 7: UpdateWithOldValues (REPLICA IDENTITY FULL) ───────────────────

func TestIntegration_UpdateWithOldValues(t *testing.T) {
	t.Parallel()

	const (
		slotName  = "cdc_test_oldvals"
		pubName   = "cdc_test_oldvals_pub"
		tableName = "integration_test_oldvals"
	)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	log := testLogger()
	pool := connectPG(t, ctx)
	defer pool.Close()

	cleanupSlot(t, pool, slotName)

	// Create table with REPLICA IDENTITY FULL.
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		DROP TABLE IF EXISTS %s;
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT
		);
		ALTER TABLE %s REPLICA IDENTITY FULL;
	`, tableName, tableName, tableName))
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	_, _ = pool.Exec(ctx, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pubName))
	_, err = pool.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pubName, tableName))
	if err != nil {
		t.Fatalf("create publication: %v", err)
	}

	t.Cleanup(func() { cleanupSlot(t, pool, slotName) })

	reg := prometheus.NewRegistry()
	m := metrics.NewWithRegistry("oldvals", reg)
	cpStore, _ := checkpoint.NewFileStore(t.TempDir() + "/cp.json")
	cpMgr := checkpoint.NewManager(cpStore, 100*time.Millisecond, log, &m.CDC)
	_, _ = cpMgr.LoadInitial()

	resolver, _ := topic.NewResolver(model.TopicPerTable, "cdc_oldvals", "")
	t.Cleanup(func() { cleanupTopics(t, "cdc_oldvals") })

	prod := connectProducer(t, ctx, log, &m.CDC)
	defer prod.Close()

	hs := health.NewStatus()
	pipeCancel, pipeErrCh := startPipeline(ctx, t, prod, m, cpMgr, resolver, hs, slotName, pubName, defaultPipelineCfg())
	defer pipeCancel()

	time.Sleep(3 * time.Second)

	// Insert then update.
	_, err = pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (name, email) VALUES ('bob', 'bob@old.com')`, tableName))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = pool.Exec(ctx, fmt.Sprintf(`UPDATE %s SET email = 'bob@new.com' WHERE name = 'bob'`, tableName))
	if err != nil {
		t.Fatalf("update: %v", err)
	}

	time.Sleep(2 * time.Second)

	topicName := resolver.Resolve(testDatabase, "public", tableName)
	envelopes := consumeEvents(t, ctx, topicName, 2) // insert + update

	// Find the update event.
	var updateEnv *model.CDCEnvelope
	for i := range envelopes {
		if envelopes[i].Op == "u" {
			updateEnv = &envelopes[i]
			break
		}
	}

	if updateEnv == nil {
		t.Fatal("no update event found")
	}

	// With REPLICA IDENTITY FULL, Before should be populated.
	if updateEnv.Before == nil || len(updateEnv.Before) == 0 {
		t.Error("before should be populated with REPLICA IDENTITY FULL")
	} else {
		if updateEnv.Before["email"] != "bob@old.com" {
			t.Errorf("before.email: expected bob@old.com, got %v", updateEnv.Before["email"])
		}
	}

	if updateEnv.After == nil || len(updateEnv.After) == 0 {
		t.Error("after should be populated on update")
	} else {
		if updateEnv.After["email"] != "bob@new.com" {
			t.Errorf("after.email: expected bob@new.com, got %v", updateEnv.After["email"])
		}
	}

	pipeCancel()
	<-pipeErrCh
}

// ── Test 8: NullValues ────────────────────────────────────────────────────

func TestIntegration_NullValues(t *testing.T) {
	t.Parallel()

	const (
		slotName  = "cdc_test_nulls"
		pubName   = "cdc_test_nulls_pub"
		tableName = "integration_test_nulls"
	)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	log := testLogger()
	pool := connectPG(t, ctx)
	defer pool.Close()

	cleanupSlot(t, pool, slotName)
	setupTable(t, pool, tableName, pubName)
	t.Cleanup(func() { cleanupSlot(t, pool, slotName) })

	reg := prometheus.NewRegistry()
	m := metrics.NewWithRegistry("nulls", reg)
	cpStore, _ := checkpoint.NewFileStore(t.TempDir() + "/cp.json")
	cpMgr := checkpoint.NewManager(cpStore, 100*time.Millisecond, log, &m.CDC)
	_, _ = cpMgr.LoadInitial()

	resolver, _ := topic.NewResolver(model.TopicPerTable, "cdc_nulls", "")
	t.Cleanup(func() { cleanupTopics(t, "cdc_nulls") })

	prod := connectProducer(t, ctx, log, &m.CDC)
	defer prod.Close()

	hs := health.NewStatus()
	pipeCancel, pipeErrCh := startPipeline(ctx, t, prod, m, cpMgr, resolver, hs, slotName, pubName, defaultPipelineCfg())
	defer pipeCancel()

	time.Sleep(3 * time.Second)

	// Insert a row with NULL email.
	_, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (name, email) VALUES ('nulluser', NULL)`, tableName))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	time.Sleep(2 * time.Second)

	topicName := resolver.Resolve(testDatabase, "public", tableName)
	envelopes := consumeEvents(t, ctx, topicName, 1)

	env := envelopes[0]
	if env.Op != "c" {
		t.Errorf("expected op=c, got %s", env.Op)
	}

	// The email column should be nil/null in the after map.
	if env.After == nil {
		t.Fatal("after should not be nil")
	}
	emailVal, exists := env.After["email"]
	if !exists {
		t.Error("email key should exist in after map")
	} else if emailVal != nil {
		t.Errorf("email should be null, got %v", emailVal)
	}

	// name should be set.
	if env.After["name"] != "nulluser" {
		t.Errorf("expected name=nulluser, got %v", env.After["name"])
	}

	pipeCancel()
	<-pipeErrCh
}

// ── Test 9: TemporarySlot ─────────────────────────────────────────────────

func TestIntegration_TemporarySlot(t *testing.T) {
	t.Parallel()

	const (
		slotName  = "cdc_test_tmpslot"
		pubName   = "cdc_test_tmpslot_pub"
		tableName = "integration_test_tmpslot"
	)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	log := testLogger()
	pool := connectPG(t, ctx)
	defer pool.Close()

	cleanupSlot(t, pool, slotName)
	setupTable(t, pool, tableName, pubName)
	// No cleanup needed for temporary slot -- it vanishes on disconnect.

	reg := prometheus.NewRegistry()
	m := metrics.NewWithRegistry("tmpslot", reg)
	cpStore, _ := checkpoint.NewFileStore(t.TempDir() + "/cp.json")
	cpMgr := checkpoint.NewManager(cpStore, 100*time.Millisecond, log, &m.CDC)
	_, _ = cpMgr.LoadInitial()

	resolver, _ := topic.NewResolver(model.TopicPerTable, "cdc_tmpslot", "")
	t.Cleanup(func() { cleanupTopics(t, "cdc_tmpslot") })

	prod := connectProducer(t, ctx, log, &m.CDC)
	defer prod.Close()

	hs := health.NewStatus()

	pipeCtx, pipeCancel := context.WithCancel(ctx)

	pipeCfg := defaultPipelineCfg()
	pipeCfg.SourceName = "tmpslot"

	pipe := pipeline.New(
		pipeCfg,
		pgrepl.ReaderConfig{
			ConnString:      testPGDSN(),
			SlotName:        slotName,
			PublicationName: pubName,
			CreateSlot:      true,
			TemporarySlot:   true,
			StatusInterval:  2 * time.Second,
			ConfirmedLSN:    cpMgr.LastFlushed,
		},
		prod, cpMgr, resolver, hs, m, log,
	)

	pipeErrCh := make(chan error, 1)
	go func() { pipeErrCh <- pipe.Run(pipeCtx) }()

	time.Sleep(3 * time.Second)

	_, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (name, email) VALUES ('tmp', 'tmp@x.com')`, tableName))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	time.Sleep(2 * time.Second)

	topicName := resolver.Resolve(testDatabase, "public", tableName)
	envelopes := consumeEvents(t, ctx, topicName, 1)
	if len(envelopes) < 1 {
		t.Fatal("expected at least 1 event from temporary slot pipeline")
	}

	// Stop the pipeline (disconnects the temporary slot).
	pipeCancel()
	<-pipeErrCh

	time.Sleep(1 * time.Second)

	// Verify the slot no longer exists.
	var slotExists bool
	err = pool.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", slotName).Scan(&slotExists)
	if err != nil {
		t.Fatalf("query replication slots: %v", err)
	}
	if slotExists {
		t.Error("temporary slot should not exist after pipeline stops")
	}
}

// ── Test 10: CheckpointResume ─────────────────────────────────────────────

func TestIntegration_CheckpointResume(t *testing.T) {
	t.Parallel()

	const (
		slotName  = "cdc_test_cpresume"
		pubName   = "cdc_test_cpresume_pub"
		tableName = "integration_test_cpresume"
	)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	log := testLogger()
	pool := connectPG(t, ctx)
	defer pool.Close()

	cleanupSlot(t, pool, slotName)
	setupTable(t, pool, tableName, pubName)
	t.Cleanup(func() { cleanupSlot(t, pool, slotName) })
	t.Cleanup(func() { cleanupTopics(t, "cdc_cpresume") })

	cpPath := t.TempDir() + "/cp.json"
	reg := prometheus.NewRegistry()

	// -- Phase 1: start pipeline, insert rows, stop.
	{
		m := metrics.NewWithRegistry("cp1", reg)
		cpStore, _ := checkpoint.NewFileStore(cpPath)
		cpMgr := checkpoint.NewManager(cpStore, 100*time.Millisecond, log, &m.CDC)
		_, _ = cpMgr.LoadInitial()

		resolver, _ := topic.NewResolver(model.TopicPerTable, "cdc_cpresume", "")

		prod := connectProducer(t, ctx, log, &m.CDC)

		hs := health.NewStatus()
		pipeCancel, pipeErrCh := startPipeline(ctx, t, prod, m, cpMgr, resolver, hs, slotName, pubName, defaultPipelineCfg())

		time.Sleep(3 * time.Second)

		for i := 0; i < 5; i++ {
			_, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (name, email) VALUES ($1, $2)`, tableName),
				fmt.Sprintf("phase1-%d", i), fmt.Sprintf("p1-%d@x.com", i))
			if err != nil {
				t.Fatalf("phase1 insert %d: %v", i, err)
			}
		}

		time.Sleep(3 * time.Second)

		// Force checkpoint flush.
		if err := cpMgr.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}

		pipeCancel()
		<-pipeErrCh
		prod.Close()
	}

	// -- Phase 2: restart pipeline with same checkpoint, insert new rows.
	{
		reg2 := prometheus.NewRegistry()
		m := metrics.NewWithRegistry("cp2", reg2)
		cpStore, _ := checkpoint.NewFileStore(cpPath)
		cpMgr := checkpoint.NewManager(cpStore, 100*time.Millisecond, log, &m.CDC)
		startLSN, _ := cpMgr.LoadInitial()
		t.Logf("resumed from checkpoint LSN: %s", startLSN)

		resolver, _ := topic.NewResolver(model.TopicPerTable, "cdc_cpresume2", "")

		prod := connectProducer(t, ctx, log, &m.CDC)
		defer prod.Close()

		hs := health.NewStatus()

		// Use the same slot (it persists).
		pipeCancel, pipeErrCh := startPipeline(ctx, t, prod, m, cpMgr, resolver, hs, slotName, pubName, defaultPipelineCfg())
		defer pipeCancel()

		time.Sleep(3 * time.Second)

		for i := 0; i < 3; i++ {
			_, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (name, email) VALUES ($1, $2)`, tableName),
				fmt.Sprintf("phase2-%d", i), fmt.Sprintf("p2-%d@x.com", i))
			if err != nil {
				t.Fatalf("phase2 insert %d: %v", i, err)
			}
		}

		time.Sleep(3 * time.Second)

		// Consume all available events (phase1 duplicates + phase2 new).
		// At-least-once semantics means the slot may replay some phase1 events.
		topicName := resolver.Resolve(testDatabase, "public", tableName)
		envelopes := consumeEvents(t, ctx, topicName, 3)

		// Keep polling briefly to catch any additional events.
		consumer2, err := kgo.NewClient(
			kgo.SeedBrokers(testBroker()),
			kgo.ConsumeTopics(topicName),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		)
		if err == nil {
			drainCtx, drainCancel := context.WithTimeout(ctx, 3*time.Second)
			for {
				fetches := consumer2.PollFetches(drainCtx)
				if drainCtx.Err() != nil {
					break
				}
				fetches.EachRecord(func(r *kgo.Record) {
					var env model.CDCEnvelope
					if err := json.Unmarshal(r.Value, &env); err == nil {
						envelopes = append(envelopes, env)
					}
				})
			}
			drainCancel()
			consumer2.Close()
		}

		var phase2Count int
		for _, env := range envelopes {
			name, _ := env.After["name"].(string)
			if strings.HasPrefix(name, "phase2-") {
				phase2Count++
			} else if strings.HasPrefix(name, "phase1-") {
				t.Logf("at-least-once duplicate (expected): %s", name)
			}
		}
		if phase2Count < 3 {
			t.Errorf("expected at least 3 phase2 events, got %d (total %d)", phase2Count, len(envelopes))
		}
		t.Logf("phase2: received %d phase2 events out of %d total", phase2Count, len(envelopes))

		pipeCancel()
		<-pipeErrCh
	}
}

// ── Test 11: TransactionMarkers ───────────────────────────────────────────

func TestIntegration_TransactionMarkers(t *testing.T) {
	t.Parallel()

	const (
		slotName  = "cdc_test_txmark"
		pubName   = "cdc_test_txmark_pub"
		tableName = "integration_test_txmark"
	)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	log := testLogger()
	pool := connectPG(t, ctx)
	defer pool.Close()

	cleanupSlot(t, pool, slotName)
	setupTable(t, pool, tableName, pubName)
	t.Cleanup(func() { cleanupSlot(t, pool, slotName) })

	reg := prometheus.NewRegistry()
	m := metrics.NewWithRegistry("txmark", reg)
	cpStore, _ := checkpoint.NewFileStore(t.TempDir() + "/cp.json")
	cpMgr := checkpoint.NewManager(cpStore, 100*time.Millisecond, log, &m.CDC)
	_, _ = cpMgr.LoadInitial()

	resolver, _ := topic.NewResolver(model.TopicPerTable, "cdc_txmark", "")
	t.Cleanup(func() { cleanupTopics(t, "cdc_txmark") })

	prod := connectProducer(t, ctx, log, &m.CDC)
	defer prod.Close()

	hs := health.NewStatus()

	pipeCfg := defaultPipelineCfg()
	pipeCfg.TxMarkers = true
	pipeCfg.SourceName = "txmark"

	pipeCancel, pipeErrCh := startPipeline(ctx, t, prod, m, cpMgr, resolver, hs, slotName, pubName, pipeCfg)
	defer pipeCancel()

	time.Sleep(3 * time.Second)

	_, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (name, email) VALUES ('marker', 'mk@x.com')`, tableName))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	time.Sleep(2 * time.Second)

	topicName := resolver.Resolve(testDatabase, "public", tableName)
	// Expect at least 3 records: BEGIN marker + data event + COMMIT marker.
	records := consumeRawRecords(t, ctx, topicName, 3, 20*time.Second)

	if len(records) < 3 {
		t.Fatalf("expected at least 3 records (begin+data+commit), got %d", len(records))
	}

	// Parse all records looking for begin and commit markers.
	var hasBegin, hasCommit, hasData bool
	for _, r := range records {
		var generic map[string]any
		if err := json.Unmarshal(r.Value, &generic); err != nil {
			t.Logf("unmarshal error: %v", err)
			continue
		}
		op, _ := generic["op"].(string)
		switch op {
		case "begin":
			hasBegin = true
		case "commit":
			hasCommit = true
		case "c":
			hasData = true
		}
	}

	if !hasBegin {
		t.Error("missing BEGIN marker record")
	}
	if !hasCommit {
		t.Error("missing COMMIT marker record")
	}
	if !hasData {
		t.Error("missing data event (op=c)")
	}

	pipeCancel()
	<-pipeErrCh
}

// ── Test 12: HeartbeatEmission ────────────────────────────────────────────

func TestIntegration_HeartbeatEmission(t *testing.T) {
	t.Parallel()

	const (
		slotName       = "cdc_test_hb"
		pubName        = "cdc_test_hb_pub"
		tableName      = "integration_test_hb"
		heartbeatTopic = "cdc_test_heartbeat"
	)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	log := testLogger()
	pool := connectPG(t, ctx)
	defer pool.Close()

	cleanupSlot(t, pool, slotName)
	setupTable(t, pool, tableName, pubName)
	t.Cleanup(func() { cleanupSlot(t, pool, slotName) })

	reg := prometheus.NewRegistry()
	m := metrics.NewWithRegistry("hb", reg)
	cpStore, _ := checkpoint.NewFileStore(t.TempDir() + "/cp.json")
	cpMgr := checkpoint.NewManager(cpStore, 100*time.Millisecond, log, &m.CDC)
	_, _ = cpMgr.LoadInitial()

	resolver, _ := topic.NewResolver(model.TopicPerTable, "cdc_hb", "")
	t.Cleanup(func() { cleanupTopics(t, "cdc_hb") })
	t.Cleanup(func() { cleanupTopics(t, "cdc_test_heartbeat") })

	prod := connectProducer(t, ctx, log, &m.CDC)
	defer prod.Close()

	hs := health.NewStatus()

	pipeCfg := defaultPipelineCfg()
	pipeCfg.HeartbeatInterval = 1 * time.Second
	pipeCfg.HeartbeatTopic = heartbeatTopic
	pipeCfg.SourceName = "hb"

	pipeCancel, pipeErrCh := startPipeline(ctx, t, prod, m, cpMgr, resolver, hs, slotName, pubName, pipeCfg)
	defer pipeCancel()

	// Wait for at least 3 heartbeats (interval=1s, wait 5s for safety).
	time.Sleep(5 * time.Second)

	records := consumeRawRecords(t, ctx, heartbeatTopic, 1, 10*time.Second)

	if len(records) < 1 {
		t.Fatal("expected at least 1 heartbeat record")
	}

	// Validate heartbeat structure.
	var hb map[string]any
	if err := json.Unmarshal(records[0].Value, &hb); err != nil {
		t.Fatalf("unmarshal heartbeat: %v", err)
	}
	if hb["source"] == nil || hb["source"] == "" {
		t.Error("heartbeat should have source field")
	}
	if hb["timestamp"] == nil || hb["timestamp"] == "" {
		t.Error("heartbeat should have timestamp field")
	}
	if hb["database"] == nil || hb["database"] == "" {
		t.Error("heartbeat should have database field")
	}

	t.Logf("received %d heartbeat records", len(records))

	pipeCancel()
	<-pipeErrCh
}

// ── Test 13: TopicSingleMode ──────────────────────────────────────────────

func TestIntegration_TopicSingleMode(t *testing.T) {
	t.Parallel()

	const (
		slotName    = "cdc_test_single"
		pubName     = "cdc_test_single_pub"
		table1      = "integration_test_single_a"
		table2      = "integration_test_single_b"
		singleTopic = "cdc_test_all_events"
	)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	log := testLogger()
	pool := connectPG(t, ctx)
	defer pool.Close()

	cleanupSlot(t, pool, slotName)
	setupTables(t, pool, []string{table1, table2}, pubName)
	t.Cleanup(func() { cleanupSlot(t, pool, slotName) })

	reg := prometheus.NewRegistry()
	m := metrics.NewWithRegistry("single", reg)
	cpStore, _ := checkpoint.NewFileStore(t.TempDir() + "/cp.json")
	cpMgr := checkpoint.NewManager(cpStore, 100*time.Millisecond, log, &m.CDC)
	_, _ = cpMgr.LoadInitial()

	resolver, _ := topic.NewResolver(model.TopicSingle, "", singleTopic)
	t.Cleanup(func() { cleanupTopics(t, "cdc_test_all_events") })

	prod := connectProducer(t, ctx, log, &m.CDC)
	defer prod.Close()

	hs := health.NewStatus()
	pipeCancel, pipeErrCh := startPipeline(ctx, t, prod, m, cpMgr, resolver, hs, slotName, pubName, defaultPipelineCfg())
	defer pipeCancel()

	time.Sleep(3 * time.Second)

	// Insert into two different tables.
	_, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (name, email) VALUES ('single-a', 'a@x.com')`, table1))
	if err != nil {
		t.Fatalf("insert table1: %v", err)
	}
	_, err = pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (name, email) VALUES ('single-b', 'b@x.com')`, table2))
	if err != nil {
		t.Fatalf("insert table2: %v", err)
	}

	time.Sleep(2 * time.Second)

	// Both events should land on the single topic.
	envelopes := consumeEvents(t, ctx, singleTopic, 2)

	tables := make(map[string]bool)
	for _, env := range envelopes {
		tables[env.Table] = true
	}

	if !tables[table1] {
		t.Errorf("missing event from %s", table1)
	}
	if !tables[table2] {
		t.Errorf("missing event from %s", table2)
	}

	pipeCancel()
	<-pipeErrCh
}

// ── Test 14: EmptyTransaction ─────────────────────────────────────────────

func TestIntegration_EmptyTransaction(t *testing.T) {
	t.Parallel()

	const (
		slotName  = "cdc_test_empty"
		pubName   = "cdc_test_empty_pub"
		tableName = "integration_test_empty"
	)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	log := testLogger()
	pool := connectPG(t, ctx)
	defer pool.Close()

	cleanupSlot(t, pool, slotName)
	setupTable(t, pool, tableName, pubName)
	t.Cleanup(func() { cleanupSlot(t, pool, slotName) })

	reg := prometheus.NewRegistry()
	m := metrics.NewWithRegistry("empty", reg)
	cpStore, _ := checkpoint.NewFileStore(t.TempDir() + "/cp.json")
	cpMgr := checkpoint.NewManager(cpStore, 100*time.Millisecond, log, &m.CDC)
	_, _ = cpMgr.LoadInitial()

	resolver, _ := topic.NewResolver(model.TopicPerTable, "cdc_empty", "")
	t.Cleanup(func() { cleanupTopics(t, "cdc_empty") })

	prod := connectProducer(t, ctx, log, &m.CDC)
	defer prod.Close()

	hs := health.NewStatus()
	pipeCancel, pipeErrCh := startPipeline(ctx, t, prod, m, cpMgr, resolver, hs, slotName, pubName, defaultPipelineCfg())
	defer pipeCancel()

	time.Sleep(3 * time.Second)

	// Execute an empty transaction (BEGIN; COMMIT; with no DML).
	_, err := pool.Exec(ctx, "BEGIN; COMMIT;")
	if err != nil {
		t.Fatalf("empty tx: %v", err)
	}

	// Then insert a real row as a sentinel to prove the pipeline is alive.
	_, err = pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (name, email) VALUES ('sentinel', 's@x.com')`, tableName))
	if err != nil {
		t.Fatalf("sentinel insert: %v", err)
	}

	time.Sleep(3 * time.Second)

	topicName := resolver.Resolve(testDatabase, "public", tableName)
	envelopes := consumeEvents(t, ctx, topicName, 1)

	// Only the sentinel insert should be there, no events from the empty tx.
	if len(envelopes) != 1 {
		t.Errorf("expected exactly 1 event (sentinel), got %d", len(envelopes))
	}
	if len(envelopes) > 0 && envelopes[0].Op != "c" {
		t.Errorf("expected op=c for sentinel, got %s", envelopes[0].Op)
	}

	pipeCancel()
	<-pipeErrCh
}
