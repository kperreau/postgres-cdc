package health

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/goccy/go-json"

	"github.com/jackc/pglogrepl"
)

func TestLivez(t *testing.T) {
	s := NewStatus()
	mux := s.Handler()

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/livez", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("livez status = %d, want 200", w.Code)
	}
	if w.Body.String() != `{"live":true}` {
		t.Errorf("livez body = %q", w.Body.String())
	}
}

func TestReadyzUnhealthy(t *testing.T) {
	s := NewStatus()
	mux := s.Handler()

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("readyz status = %d, want 503", w.Code)
	}

	var resp readyResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.Ready {
		t.Error("should not be ready when nothing is connected")
	}
}

func TestReadyzHealthy(t *testing.T) {
	s := NewStatus()
	s.SetSourceConnected(true)
	s.SetProducerHealthy(true)
	s.SetLastReadLSN(pglogrepl.LSN(0x200))
	s.SetLastCheckpointLSN(pglogrepl.LSN(0x100))

	mux := s.Handler()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("readyz status = %d, want 200", w.Code)
	}

	var resp readyResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if !resp.Ready {
		t.Error("should be ready")
	}
	if resp.LagBytes != 0x100 {
		t.Errorf("lag = %d, want %d", resp.LagBytes, 0x100)
	}
}

func TestReadyzLagCalculation(t *testing.T) {
	t.Parallel()
	s := NewStatus()
	s.SetSourceConnected(true)
	s.SetProducerHealthy(true)
	s.SetLastReadLSN(pglogrepl.LSN(1000))
	s.SetLastCheckpointLSN(pglogrepl.LSN(500))

	mux := s.Handler()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	var resp readyResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.LagBytes != 500 {
		t.Errorf("lag_bytes = %d, want 500", resp.LagBytes)
	}
}

func TestReadyzAfterLSNUpdate(t *testing.T) {
	t.Parallel()
	s := NewStatus()
	s.SetSourceConnected(true)
	s.SetProducerHealthy(true)
	s.SetLastReadLSN(pglogrepl.LSN(2000))
	s.SetLastCheckpointLSN(pglogrepl.LSN(1500))

	mux := s.Handler()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("readyz status = %d, want 200", w.Code)
	}

	var resp readyResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if !resp.Ready {
		t.Error("should be ready")
	}
	if resp.LastReadLSN != pglogrepl.LSN(2000).String() {
		t.Errorf("last_read_lsn = %s, want %s", resp.LastReadLSN, pglogrepl.LSN(2000).String())
	}
	if resp.LastCheckpointLSN != pglogrepl.LSN(1500).String() {
		t.Errorf("last_checkpoint_lsn = %s, want %s", resp.LastCheckpointLSN, pglogrepl.LSN(1500).String())
	}
}

func TestLivezAlwaysOK(t *testing.T) {
	t.Parallel()
	s := NewStatus()
	// Do not set any health state — it should still return 200.
	mux := s.Handler()

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/livez", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("livez status = %d, want 200", w.Code)
	}
	if w.Body.String() != `{"live":true}` {
		t.Errorf("livez body = %q", w.Body.String())
	}
}
