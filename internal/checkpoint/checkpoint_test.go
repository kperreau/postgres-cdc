package checkpoint

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"

	"github.com/kperreau/postgres-cdc/internal/metrics"
	"github.com/kperreau/postgres-cdc/internal/model"
)

func TestFileStoreRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cp.json")
	store, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}

	// Load with no file should return zero.
	cp, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cp.LSN != 0 {
		t.Errorf("expected LSN 0, got %s", cp.LSN)
	}

	// Save and reload.
	now := time.Now().UTC().Truncate(time.Second)
	want := model.Checkpoint{LSN: pglogrepl.LSN(0x16B6A30), Timestamp: now}
	if err := store.Save(want); err != nil {
		t.Fatal(err)
	}

	got, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.LSN != want.LSN {
		t.Errorf("LSN = %s, want %s", got.LSN, want.LSN)
	}
}

func TestFileStoreAtomicOverwrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", "cp.json")
	store, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}

	_ = store.Save(model.Checkpoint{LSN: 100, Timestamp: time.Now()})
	_ = store.Save(model.Checkpoint{LSN: 200, Timestamp: time.Now()})

	cp, _ := store.Load()
	if cp.LSN != 200 {
		t.Errorf("expected LSN 200, got %d", cp.LSN)
	}
}

func newTestCDCMetrics() *metrics.CDCMetrics {
	reg := prometheus.NewRegistry()
	m := metrics.NewWithRegistry("test_cp", reg)
	return &m.CDC
}

func TestManagerLoadInitialEmpty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cp.json")
	store, _ := NewFileStore(path)
	mgr := NewManager(store, time.Second, zerolog.Nop(), newTestCDCMetrics())

	lsn, err := mgr.LoadInitial()
	if err != nil {
		t.Fatal(err)
	}
	if lsn != 0 {
		t.Errorf("expected LSN 0 for empty checkpoint, got %s", lsn)
	}
}

func TestManagerLoadInitialExisting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cp.json")
	store, _ := NewFileStore(path)
	_ = store.Save(model.Checkpoint{LSN: pglogrepl.LSN(0x500), Timestamp: time.Now()})

	mgr := NewManager(store, time.Second, zerolog.Nop(), newTestCDCMetrics())
	lsn, err := mgr.LoadInitial()
	if err != nil {
		t.Fatal(err)
	}
	if lsn != pglogrepl.LSN(0x500) {
		t.Errorf("expected LSN 0/500, got %s", lsn)
	}
}

func TestManagerConfirmAndFlush(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cp.json")
	store, _ := NewFileStore(path)
	mgr := NewManager(store, time.Hour, zerolog.Nop(), newTestCDCMetrics()) // long interval, manual flush

	_, _ = mgr.LoadInitial()

	// Confirm several LSNs, only the highest matters.
	mgr.Confirm(pglogrepl.LSN(0x100))
	mgr.Confirm(pglogrepl.LSN(0x300))
	mgr.Confirm(pglogrepl.LSN(0x200)) // lower than 0x300, should be ignored

	if err := mgr.Flush(); err != nil {
		t.Fatal(err)
	}

	// Verify persisted value.
	cp, _ := store.Load()
	if cp.LSN != pglogrepl.LSN(0x300) {
		t.Errorf("expected LSN 0/300, got %s", cp.LSN)
	}

	if mgr.LastFlushed() != pglogrepl.LSN(0x300) {
		t.Errorf("LastFlushed = %s, want 0/300", mgr.LastFlushed())
	}
}

func TestManagerFlushNoOp(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cp.json")
	store, _ := NewFileStore(path)
	mgr := NewManager(store, time.Hour, zerolog.Nop(), newTestCDCMetrics())
	_, _ = mgr.LoadInitial()

	// Flush without any confirm should be a no-op.
	if err := mgr.Flush(); err != nil {
		t.Fatal(err)
	}
	if mgr.LastFlushed() != 0 {
		t.Errorf("LastFlushed should be 0, got %s", mgr.LastFlushed())
	}
}

func TestManagerRunPeriodicFlush(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cp.json")
	store, _ := NewFileStore(path)
	mgr := NewManager(store, 50*time.Millisecond, zerolog.Nop(), newTestCDCMetrics())
	_, _ = mgr.LoadInitial()

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() { errCh <- mgr.Run(ctx) }()

	mgr.Confirm(pglogrepl.LSN(0x999))

	// Wait for periodic flush to trigger.
	time.Sleep(150 * time.Millisecond)
	cancel()
	<-errCh

	cp, _ := store.Load()
	if cp.LSN != pglogrepl.LSN(0x999) {
		t.Errorf("expected periodic flush to write LSN 0/999, got %s", cp.LSN)
	}
}
