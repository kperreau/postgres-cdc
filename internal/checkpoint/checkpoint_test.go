package checkpoint

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"

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
