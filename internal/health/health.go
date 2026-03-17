// Package health exposes /livez and /readyz HTTP endpoints for Kubernetes probes.
package health

import (
	"net/http"
	"sync"
	"time"

	"github.com/goccy/go-json"

	"github.com/jackc/pglogrepl"
)

// Status holds the health state observed by pipeline components.
type Status struct {
	mu                sync.Mutex
	sourceConnected   bool
	producerHealthy   bool
	lastReadLSN       pglogrepl.LSN
	lastCheckpointLSN pglogrepl.LSN
	lastReadTime      time.Time
}

// NewStatus returns a new Status with default (unhealthy) state.
func NewStatus() *Status {
	return &Status{}
}

// SetSourceConnected marks the PostgreSQL source as connected or disconnected.
func (s *Status) SetSourceConnected(v bool) {
	s.mu.Lock()
	s.sourceConnected = v
	s.mu.Unlock()
}

// SetProducerHealthy marks the Redpanda producer as healthy or unhealthy.
func (s *Status) SetProducerHealthy(v bool) {
	s.mu.Lock()
	s.producerHealthy = v
	s.mu.Unlock()
}

// SetLastReadLSN records the last WAL LSN read.
func (s *Status) SetLastReadLSN(lsn pglogrepl.LSN) {
	s.mu.Lock()
	s.lastReadLSN = lsn
	s.lastReadTime = time.Now()
	s.mu.Unlock()
}

// SetLastCheckpointLSN records the last checkpointed LSN.
func (s *Status) SetLastCheckpointLSN(lsn pglogrepl.LSN) {
	s.mu.Lock()
	s.lastCheckpointLSN = lsn
	s.mu.Unlock()
}

// Handler returns an http.ServeMux with /livez and /readyz endpoints.
func (s *Status) Handler() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/livez", s.liveHandler)
	mux.HandleFunc("/readyz", s.readyHandler)
	return mux
}

type readyResponse struct {
	Ready             bool   `json:"ready"`
	SourceConnected   bool   `json:"source_connected"`
	ProducerHealthy   bool   `json:"producer_healthy"`
	LastReadLSN       string `json:"last_read_lsn"`
	LastCheckpointLSN string `json:"last_checkpoint_lsn"`
	LagBytes          uint64 `json:"lag_bytes"`
}

func (s *Status) liveHandler(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"live":true}`))
}

func (s *Status) readyHandler(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	resp := readyResponse{
		Ready:             s.sourceConnected && s.producerHealthy,
		SourceConnected:   s.sourceConnected,
		ProducerHealthy:   s.producerHealthy,
		LastReadLSN:       s.lastReadLSN.String(),
		LastCheckpointLSN: s.lastCheckpointLSN.String(),
		LagBytes:          uint64(s.lastReadLSN) - uint64(s.lastCheckpointLSN),
	}
	s.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	if !resp.Ready {
		w.WriteHeader(http.StatusServiceUnavailable)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	_ = json.NewEncoder(w).Encode(resp)
}
