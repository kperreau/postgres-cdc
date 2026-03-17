// Package checkpoint persists the last safely published LSN to a local file.
// It ensures that on restart the replication reader can resume from the correct
// position without re-processing already-acknowledged transactions.
package checkpoint

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/rs/zerolog"

	"github.com/kperreau/postgres-cdc/internal/metrics"
	"github.com/kperreau/postgres-cdc/internal/model"
)

// Store persists and retrieves checkpoint state.
type Store interface {
	// Load reads the last persisted checkpoint. Returns zero value if none exists.
	Load() (model.Checkpoint, error)
	// Save atomically persists a checkpoint.
	Save(cp model.Checkpoint) error
}

// FileStore implements Store using a local JSON file with atomic writes.
type FileStore struct {
	path string
	mu   sync.Mutex
}

// NewFileStore creates a FileStore at the given path.
// The parent directory is created if it does not exist.
func NewFileStore(path string) (*FileStore, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("checkpoint: create dir %s: %w", dir, err)
	}
	return &FileStore{path: path}, nil
}

// Load reads the checkpoint from disk. If the file does not exist, returns a
// zero-value checkpoint (LSN 0).
func (f *FileStore) Load() (model.Checkpoint, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	data, err := os.ReadFile(f.path)
	if os.IsNotExist(err) {
		return model.Checkpoint{}, nil
	}
	if err != nil {
		return model.Checkpoint{}, fmt.Errorf("checkpoint: read %s: %w", f.path, err)
	}

	var cp model.Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return model.Checkpoint{}, fmt.Errorf("checkpoint: parse %s: %w", f.path, err)
	}
	return cp, nil
}

// Save writes the checkpoint atomically by writing to a temp file then renaming.
func (f *FileStore) Save(cp model.Checkpoint) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	data, err := json.Marshal(&cp)
	if err != nil {
		return fmt.Errorf("checkpoint: marshal: %w", err)
	}

	tmp := f.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o640); err != nil {
		return fmt.Errorf("checkpoint: write tmp: %w", err)
	}
	if err := os.Rename(tmp, f.path); err != nil {
		return fmt.Errorf("checkpoint: rename: %w", err)
	}
	return nil
}

// Manager coordinates periodic checkpoint flushing. It accepts LSN updates
// from the pipeline and periodically persists the latest confirmed LSN.
type Manager struct {
	store         Store
	log           zerolog.Logger
	metrics       *metrics.CDCMetrics
	flushInterval time.Duration

	mu      sync.Mutex
	pending pglogrepl.LSN // latest LSN ready to checkpoint
	flushed pglogrepl.LSN // last actually persisted LSN
}

// NewManager creates a checkpoint Manager.
func NewManager(store Store, flushInterval time.Duration, log zerolog.Logger, m *metrics.CDCMetrics) *Manager {
	return &Manager{
		store:         store,
		log:           log.With().Str("component", "checkpoint").Logger(),
		metrics:       m,
		flushInterval: flushInterval,
	}
}

// LoadInitial reads the persisted checkpoint and returns the start LSN.
func (m *Manager) LoadInitial() (pglogrepl.LSN, error) {
	cp, err := m.store.Load()
	if err != nil {
		return 0, err
	}
	m.mu.Lock()
	m.flushed = cp.LSN
	m.pending = cp.LSN
	m.mu.Unlock()

	m.metrics.LastCheckpointLSN.Set(float64(uint64(cp.LSN)))
	if cp.LSN > 0 {
		m.log.Info().Str("lsn", cp.LSN.String()).Msg("loaded checkpoint")
	} else {
		m.log.Info().Msg("no existing checkpoint; starting from beginning")
	}
	return cp.LSN, nil
}

// Confirm marks an LSN as safe to checkpoint (all records at or before this
// LSN have been durably published).
func (m *Manager) Confirm(lsn pglogrepl.LSN) {
	m.mu.Lock()
	if lsn > m.pending {
		m.pending = lsn
	}
	m.mu.Unlock()
}

// Run periodically flushes the confirmed LSN to the store.
// It blocks until ctx is cancelled.
func (m *Manager) Run(ctx context.Context) error {
	ticker := time.NewTicker(m.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Final flush on shutdown.
			return m.flush()
		case <-ticker.C:
			if err := m.flush(); err != nil {
				m.log.Error().Err(err).Msg("checkpoint flush failed")
			}
		}
	}
}

// Flush forces a checkpoint write now.
func (m *Manager) Flush() error {
	return m.flush()
}

func (m *Manager) flush() error {
	m.mu.Lock()
	pending := m.pending
	flushed := m.flushed
	m.mu.Unlock()

	if pending <= flushed {
		return nil // nothing new to flush
	}

	cp := model.Checkpoint{
		LSN:       pending,
		Timestamp: time.Now().UTC(),
	}
	if err := m.store.Save(cp); err != nil {
		return err
	}

	m.mu.Lock()
	m.flushed = pending
	m.mu.Unlock()

	m.metrics.LastCheckpointLSN.Set(float64(uint64(pending)))
	m.log.Debug().Str("lsn", pending.String()).Msg("checkpoint flushed")
	return nil
}

// LastFlushed returns the last persisted LSN.
func (m *Manager) LastFlushed() pglogrepl.LSN {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.flushed
}
