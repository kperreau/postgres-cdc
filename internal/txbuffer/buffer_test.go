package txbuffer

import (
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"

	"github.com/kperreau/postgres-cdc/internal/metrics"
	"github.com/kperreau/postgres-cdc/internal/model"
)

func newTestMetrics() (*metrics.PGMetrics, *metrics.CDCMetrics) {
	reg := prometheus.NewRegistry()
	m := metrics.NewWithRegistry("test_txbuf", reg)
	return &m.PG, &m.CDC
}

func TestBufferBasicTransaction(t *testing.T) {
	pgm, cdcm := newTestMetrics()
	var got *model.TxBatch
	buf := New(Config{MaxTxBytes: 1 << 20, InitialEventCap: 8}, func(b *model.TxBatch) error {
		// copy events since buffer reuses the slice
		events := make([]model.TxEvent, len(b.Events))
		copy(events, b.Events)
		got = &model.TxBatch{XID: b.XID, CommitTS: b.CommitTS, LSN: b.LSN, Events: events, ByteSize: b.ByteSize}
		return nil
	}, zerolog.Nop(), pgm, cdcm)

	rel := &model.Relation{ID: 1, Namespace: "public", Name: "users", Columns: []model.Column{{Name: "id", IsKey: true}}}
	_ = buf.OnRelation(rel)

	now := time.Now()
	_ = buf.OnBegin(100, 0x100, now)
	_ = buf.OnInsert(1, []model.ColumnValue{{Name: "id", Value: "1"}}, 0x101)
	_ = buf.OnInsert(1, []model.ColumnValue{{Name: "id", Value: "2"}}, 0x102)
	_ = buf.OnCommit(0x110, now)

	if got == nil {
		t.Fatal("expected onTx callback")
	}
	if got.XID != 100 {
		t.Errorf("XID = %d, want 100", got.XID)
	}
	if len(got.Events) != 2 {
		t.Errorf("events = %d, want 2", len(got.Events))
	}
	if got.Events[0].Change.Op != model.OpInsert {
		t.Errorf("op = %c, want %c", got.Events[0].Change.Op, model.OpInsert)
	}
}

func TestBufferOversizedTransaction(t *testing.T) {
	pgm, cdcm := newTestMetrics()
	called := false
	buf := New(Config{MaxTxBytes: 100, InitialEventCap: 4}, func(_ *model.TxBatch) error {
		called = true
		return nil
	}, zerolog.Nop(), pgm, cdcm)

	rel := &model.Relation{ID: 1, Namespace: "public", Name: "users", Columns: []model.Column{{Name: "id"}}}
	_ = buf.OnRelation(rel)

	bigRow := []model.ColumnValue{{Name: "data", Bytes: make([]byte, 200)}}
	_ = buf.OnBegin(200, 0x200, time.Now())
	_ = buf.OnInsert(1, bigRow, 0x201)
	_ = buf.OnCommit(0x210, time.Now())

	if called {
		t.Error("onTx should not be called for oversized tx")
	}
}

func TestBufferReusesSlice(t *testing.T) {
	pgm, cdcm := newTestMetrics()
	var counts []int
	buf := New(Config{MaxTxBytes: 1 << 20, InitialEventCap: 8}, func(b *model.TxBatch) error {
		counts = append(counts, len(b.Events))
		return nil
	}, zerolog.Nop(), pgm, cdcm)

	rel := &model.Relation{ID: 1, Namespace: "public", Name: "t"}
	_ = buf.OnRelation(rel)

	for i := 0; i < 3; i++ {
		_ = buf.OnBegin(uint32(i), pglogrepl.LSN(i*0x100), time.Now())
		_ = buf.OnInsert(1, []model.ColumnValue{{Name: "x"}}, pglogrepl.LSN(i*0x100+1))
		_ = buf.OnCommit(pglogrepl.LSN(i*0x100+0x10), time.Now())
	}

	if len(counts) != 3 {
		t.Fatalf("expected 3 tx callbacks, got %d", len(counts))
	}
	for i, c := range counts {
		if c != 1 {
			t.Errorf("tx %d: events = %d, want 1", i, c)
		}
	}
}

func TestBufferChangeOutsideTx(t *testing.T) {
	pgm, cdcm := newTestMetrics()
	buf := New(Config{MaxTxBytes: 1 << 20}, func(_ *model.TxBatch) error { return nil }, zerolog.Nop(), pgm, cdcm)

	err := buf.OnInsert(1, nil, 0)
	if err == nil {
		t.Error("expected error for change outside transaction")
	}
}
