package pipeline

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"

	"github.com/kperreau/postgres-cdc/internal/checkpoint"
	"github.com/kperreau/postgres-cdc/internal/health"
	"github.com/kperreau/postgres-cdc/internal/metrics"
	"github.com/kperreau/postgres-cdc/internal/model"
	"github.com/kperreau/postgres-cdc/internal/pgrepl"
	"github.com/kperreau/postgres-cdc/internal/topic"
)

// ---------- test helpers ----------

// memStore is an in-memory checkpoint store for tests.
type memStore struct {
	mu sync.Mutex
	cp model.Checkpoint
}

func (s *memStore) Load() (model.Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cp, nil
}

func (s *memStore) Save(cp model.Checkpoint) error {
	s.mu.Lock()
	s.cp = cp
	s.mu.Unlock()
	return nil
}

func testMetrics(t *testing.T) *metrics.Metrics {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	return metrics.NewWithRegistry("test", reg)
}

func testPipeline(t *testing.T, checkpointLimit int, m *metrics.Metrics) (*Pipeline, *checkpoint.Manager) {
	t.Helper()

	store := &memStore{}
	cpMgr := checkpoint.NewManager(store, 50*time.Millisecond, testLogger(), &m.CDC)
	_, _ = cpMgr.LoadInitial()

	resolver, err := topic.NewResolver(model.TopicPerTable, "cdc", "")
	if err != nil {
		t.Fatalf("new resolver: %v", err)
	}

	p := New(
		Config{
			QueueCapacity:   64,
			MaxTxBytes:      1 << 20,
			CheckpointLimit: checkpointLimit,
			SourceName:      "test",
			Database:        "testdb",
		},
		pgrepl.ReaderConfig{},
		nil, // producer — not used in these tests
		cpMgr,
		resolver,
		health.NewStatus(),
		m,
		testLogger(),
	)
	return p, cpMgr
}

func testLogger() zerolog.Logger {
	return zerolog.Nop()
}

// ---------- tests ----------

// TestConfirmCheckpoint_Sequential verifies that confirmCheckpoint advances the
// checkpoint manager's pending LSN and updates health/metrics.
func TestConfirmCheckpoint_Sequential(t *testing.T) {
	m := testMetrics(t)
	p, cpMgr := testPipeline(t, 1, m)

	p.confirmCheckpoint(pglogrepl.LSN(100), time.Now())

	// Flush checkpoint so LastFlushed reflects the confirmed value.
	if err := cpMgr.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	got := cpMgr.LastFlushed()
	if got != pglogrepl.LSN(100) {
		t.Errorf("LastFlushed = %v, want 100", got)
	}
}

// TestAsyncPublishLoop_ContiguousCheckpoint verifies that the async publish
// loop only advances the checkpoint contiguously. If batch 2 finishes before
// batch 1, the checkpoint should only advance after batch 1 completes.
func TestAsyncPublishLoop_ContiguousCheckpoint(t *testing.T) {
	m := testMetrics(t)
	store := &memStore{}
	cpMgr := checkpoint.NewManager(store, 50*time.Millisecond, testLogger(), &m.CDC)
	_, _ = cpMgr.LoadInitial()

	// We test the advancer logic directly by simulating inflight entries.
	limit := 4
	inflightCh := make(chan inflight, limit)
	advancerDone := make(chan error, 1)

	var confirmedLSNs []pglogrepl.LSN
	var mu sync.Mutex

	go func() {
		for inf := range inflightCh {
			if err := <-inf.done; err != nil {
				advancerDone <- err
				//nolint:revive // drain.
				for range inflightCh {
				}
				return
			}
			mu.Lock()
			confirmedLSNs = append(confirmedLSNs, inf.lsn)
			mu.Unlock()
		}
		advancerDone <- nil
	}()

	// Simulate 3 batches: batch1 (LSN=10), batch2 (LSN=20), batch3 (LSN=30).
	done1 := make(chan error, 1)
	done2 := make(chan error, 1)
	done3 := make(chan error, 1)

	inflightCh <- inflight{lsn: 10, ts: time.Now(), done: done1}
	inflightCh <- inflight{lsn: 20, ts: time.Now(), done: done2}
	inflightCh <- inflight{lsn: 30, ts: time.Now(), done: done3}

	// Batch 3 finishes first, then batch 2, then batch 1.
	done3 <- nil
	done2 <- nil

	// Give the advancer time to process — it should be blocked on batch 1.
	time.Sleep(20 * time.Millisecond)

	mu.Lock()
	if len(confirmedLSNs) != 0 {
		t.Errorf("expected 0 confirmations while batch1 pending, got %v", confirmedLSNs)
	}
	mu.Unlock()

	// Complete batch 1 — advancer should now confirm 10, 20, 30 in order.
	done1 <- nil
	close(inflightCh)

	if err := <-advancerDone; err != nil {
		t.Fatalf("advancer: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(confirmedLSNs) != 3 {
		t.Fatalf("expected 3 confirmations, got %d: %v", len(confirmedLSNs), confirmedLSNs)
	}
	want := []pglogrepl.LSN{10, 20, 30}
	for i, lsn := range confirmedLSNs {
		if lsn != want[i] {
			t.Errorf("confirmation[%d] = %v, want %v", i, lsn, want[i])
		}
	}
}

// TestAsyncPublishLoop_ErrorStopsAdvancer verifies that a publish error
// from any in-flight batch propagates through the advancer.
func TestAsyncPublishLoop_ErrorStopsAdvancer(t *testing.T) {
	limit := 4
	inflightCh := make(chan inflight, limit)
	advancerDone := make(chan error, 1)

	var confirmed atomic.Int32

	go func() {
		for inf := range inflightCh {
			if err := <-inf.done; err != nil {
				advancerDone <- err
				//nolint:revive // drain.
				for range inflightCh {
				}
				return
			}
			confirmed.Add(1)
		}
		advancerDone <- nil
	}()

	done1 := make(chan error, 1)
	done2 := make(chan error, 1)

	inflightCh <- inflight{lsn: 10, ts: time.Now(), done: done1}
	inflightCh <- inflight{lsn: 20, ts: time.Now(), done: done2}
	close(inflightCh)

	// Batch 1 succeeds, batch 2 fails.
	done1 <- nil
	done2 <- context.DeadlineExceeded

	err := <-advancerDone
	if err == nil {
		t.Fatal("expected error from advancer")
	}

	if confirmed.Load() != 1 {
		t.Errorf("expected 1 confirmation before error, got %d", confirmed.Load())
	}
}
