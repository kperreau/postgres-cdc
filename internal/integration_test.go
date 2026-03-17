//go:build integration

// Package integration_test provides end-to-end tests for the CDC pipeline
// using real PostgreSQL and Redpanda instances via docker-compose.
//
// Run with: go test -race -tags=integration -count=1 ./internal/
//
// Prerequisites:
//
//	docker compose up -d
//	docker compose exec postgres psql -U cdc -d app -c \
//	  "CREATE PUBLICATION cdc_pub FOR ALL TABLES;"
package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

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
	testPGDSN    = "host=localhost port=5432 user=cdc password=changeme dbname=app"
	testBrokers  = "localhost:19092"
	testSlot     = "cdc_integration_test"
	testPub      = "cdc_pub"
	testDatabase = "app"
)

func setupTestTable(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
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
}

func cleanupSlot(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	_, _ = pool.Exec(ctx, fmt.Sprintf(
		"SELECT pg_drop_replication_slot('%s')", testSlot,
	))
}

func TestEndToEndInsertUpdateDelete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Connect to PostgreSQL.
	pool, err := pgxpool.New(ctx, testPGDSN)
	if err != nil {
		t.Skipf("PostgreSQL not available: %v", err)
	}
	defer pool.Close()

	cleanupSlot(t, pool)
	setupTestTable(t, pool)

	// Set up metrics with isolated registry.
	reg := prometheus.NewRegistry()
	m := metrics.NewWithRegistry("integration", reg)

	// Checkpoint in temp dir.
	cpStore, err := checkpoint.NewFileStore(t.TempDir() + "/cp.json")
	if err != nil {
		t.Fatal(err)
	}
	log := zerolog.Nop()
	cpMgr := checkpoint.NewManager(cpStore, 100*time.Millisecond, log, &m.CDC)
	_, _ = cpMgr.LoadInitial()

	// Topic resolver.
	resolver, err := topic.NewResolver(model.TopicPerTable, "cdc", "")
	if err != nil {
		t.Fatal(err)
	}

	// Producer.
	prod, err := producer.New(ctx, producer.Config{
		Brokers:         []string{testBrokers},
		Compression:     "none",
		Linger:          time.Millisecond,
		MaxInflight:     10,
		RequiredAcks:    "all",
		BatchMaxBytes:   1048576,
		BatchMaxRecords: 100,
	}, log, &m.CDC)
	if err != nil {
		t.Skipf("Redpanda not available: %v", err)
	}
	defer prod.Close()

	// Health.
	hs := health.NewStatus()

	// Pipeline.
	pipeCtx, pipeCancel := context.WithCancel(ctx)
	defer pipeCancel()

	pipe := pipeline.New(
		pipeline.Config{
			QueueCapacity: 100,
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
		},
		prod, cpMgr, resolver, hs, m, log,
	)

	pipeErrCh := make(chan error, 1)
	go func() { pipeErrCh <- pipe.Run(pipeCtx) }()

	// Wait for the pipeline to connect.
	time.Sleep(2 * time.Second)

	// Insert rows.
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

	// Consume from Redpanda and verify.
	topicName := resolver.Resolve(testDatabase, "public", "integration_test")
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(testBrokers),
		kgo.ConsumeTopics(topicName),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	defer consumer.Close()

	// Collect events with timeout.
	var envelopes []model.CDCEnvelope
	deadline := time.After(10 * time.Second)
	for len(envelopes) < 3 {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for events; got %d", len(envelopes))
		default:
		}

		fetches := consumer.PollFetches(ctx)
		fetches.EachRecord(func(r *kgo.Record) {
			var env model.CDCEnvelope
			if err := json.Unmarshal(r.Value, &env); err != nil {
				t.Logf("unmarshal error: %v", err)
				return
			}
			envelopes = append(envelopes, env)
		})
	}

	// Verify we got insert, update, delete.
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
	}
	if ops["c"] < 1 {
		t.Error("missing insert event")
	}
	if ops["u"] < 1 {
		t.Error("missing update event")
	}
	if ops["d"] < 1 {
		t.Error("missing delete event")
	}

	pipeCancel()
	<-pipeErrCh

	// Verify checkpoint was written.
	cp, _ := cpStore.Load()
	if cp.LSN == 0 {
		t.Error("checkpoint LSN should be > 0 after processing events")
	}

	// Cleanup.
	cleanupSlot(t, pool)
}
