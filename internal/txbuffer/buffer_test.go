package txbuffer

import (
	"errors"
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

func TestBufferUpdateEvent(t *testing.T) {
	t.Parallel()
	pgm, cdcm := newTestMetrics()
	var got *model.TxBatch
	buf := New(Config{MaxTxBytes: 1 << 20, InitialEventCap: 8}, func(b *model.TxBatch) error {
		events := make([]model.TxEvent, len(b.Events))
		copy(events, b.Events)
		got = &model.TxBatch{XID: b.XID, CommitTS: b.CommitTS, LSN: b.LSN, Events: events, ByteSize: b.ByteSize}
		return nil
	}, zerolog.Nop(), pgm, cdcm)

	rel := &model.Relation{ID: 1, Namespace: "public", Name: "users", Columns: []model.Column{{Name: "id", IsKey: true}, {Name: "email"}}}
	_ = buf.OnRelation(rel)

	now := time.Now()
	_ = buf.OnBegin(200, 0x200, now)
	_ = buf.OnUpdate(1,
		[]model.ColumnValue{{Name: "id", Value: "1"}, {Name: "email", Value: "old@example.com"}},
		[]model.ColumnValue{{Name: "id", Value: "1"}, {Name: "email", Value: "new@example.com"}},
		0x201,
	)
	_ = buf.OnCommit(0x210, now)

	if got == nil {
		t.Fatal("expected onTx callback")
	}
	if len(got.Events) != 1 {
		t.Fatalf("events = %d, want 1", len(got.Events))
	}
	ev := got.Events[0]
	if ev.Change.Op != model.OpUpdate {
		t.Errorf("op = %c, want %c", ev.Change.Op, model.OpUpdate)
	}
	if len(ev.Change.Before) != 2 {
		t.Errorf("before columns = %d, want 2", len(ev.Change.Before))
	}
	if len(ev.Change.After) != 2 {
		t.Errorf("after columns = %d, want 2", len(ev.Change.After))
	}
}

func TestBufferDeleteEvent(t *testing.T) {
	t.Parallel()
	pgm, cdcm := newTestMetrics()
	var got *model.TxBatch
	buf := New(Config{MaxTxBytes: 1 << 20, InitialEventCap: 8}, func(b *model.TxBatch) error {
		events := make([]model.TxEvent, len(b.Events))
		copy(events, b.Events)
		got = &model.TxBatch{XID: b.XID, CommitTS: b.CommitTS, LSN: b.LSN, Events: events, ByteSize: b.ByteSize}
		return nil
	}, zerolog.Nop(), pgm, cdcm)

	rel := &model.Relation{ID: 1, Namespace: "public", Name: "users", Columns: []model.Column{{Name: "id", IsKey: true}}}
	_ = buf.OnRelation(rel)

	now := time.Now()
	_ = buf.OnBegin(300, 0x300, now)
	_ = buf.OnDelete(1, []model.ColumnValue{{Name: "id", Value: "99"}}, 0x301)
	_ = buf.OnCommit(0x310, now)

	if got == nil {
		t.Fatal("expected onTx callback")
	}
	if len(got.Events) != 1 {
		t.Fatalf("events = %d, want 1", len(got.Events))
	}
	if got.Events[0].Change.Op != model.OpDelete {
		t.Errorf("op = %c, want %c", got.Events[0].Change.Op, model.OpDelete)
	}
}

func TestBufferMultipleEventsInTx(t *testing.T) {
	t.Parallel()
	pgm, cdcm := newTestMetrics()
	var got *model.TxBatch
	buf := New(Config{MaxTxBytes: 1 << 20, InitialEventCap: 8}, func(b *model.TxBatch) error {
		events := make([]model.TxEvent, len(b.Events))
		copy(events, b.Events)
		got = &model.TxBatch{XID: b.XID, CommitTS: b.CommitTS, LSN: b.LSN, Events: events, ByteSize: b.ByteSize}
		return nil
	}, zerolog.Nop(), pgm, cdcm)

	rel := &model.Relation{ID: 1, Namespace: "public", Name: "users", Columns: []model.Column{{Name: "id", IsKey: true}, {Name: "name"}}}
	_ = buf.OnRelation(rel)

	now := time.Now()
	_ = buf.OnBegin(400, 0x400, now)
	_ = buf.OnInsert(1, []model.ColumnValue{{Name: "id", Value: "1"}, {Name: "name", Value: "Alice"}}, 0x401)
	_ = buf.OnUpdate(1,
		[]model.ColumnValue{{Name: "id", Value: "1"}, {Name: "name", Value: "Alice"}},
		[]model.ColumnValue{{Name: "id", Value: "1"}, {Name: "name", Value: "Bob"}},
		0x402,
	)
	_ = buf.OnDelete(1, []model.ColumnValue{{Name: "id", Value: "1"}}, 0x403)
	_ = buf.OnCommit(0x410, now)

	if got == nil {
		t.Fatal("expected onTx callback")
	}
	if len(got.Events) != 3 {
		t.Fatalf("events = %d, want 3", len(got.Events))
	}
	if got.Events[0].Change.Op != model.OpInsert {
		t.Errorf("event[0].op = %c, want %c", got.Events[0].Change.Op, model.OpInsert)
	}
	if got.Events[1].Change.Op != model.OpUpdate {
		t.Errorf("event[1].op = %c, want %c", got.Events[1].Change.Op, model.OpUpdate)
	}
	if got.Events[2].Change.Op != model.OpDelete {
		t.Errorf("event[2].op = %c, want %c", got.Events[2].Change.Op, model.OpDelete)
	}
}

func TestBufferBeginWithoutCommit(t *testing.T) {
	t.Parallel()
	pgm, cdcm := newTestMetrics()
	var batches []*model.TxBatch
	buf := New(Config{MaxTxBytes: 1 << 20, InitialEventCap: 8}, func(b *model.TxBatch) error {
		events := make([]model.TxEvent, len(b.Events))
		copy(events, b.Events)
		batches = append(batches, &model.TxBatch{XID: b.XID, Events: events})
		return nil
	}, zerolog.Nop(), pgm, cdcm)

	rel := &model.Relation{ID: 1, Namespace: "public", Name: "t"}
	_ = buf.OnRelation(rel)

	now := time.Now()
	// First tx: begin + insert but no commit.
	_ = buf.OnBegin(500, 0x500, now)
	_ = buf.OnInsert(1, []model.ColumnValue{{Name: "x", Value: "discarded"}}, 0x501)

	// Second tx: begin again (discards first), insert, commit.
	_ = buf.OnBegin(600, 0x600, now)
	_ = buf.OnInsert(1, []model.ColumnValue{{Name: "x", Value: "kept"}}, 0x601)
	_ = buf.OnCommit(0x610, now)

	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}
	if batches[0].XID != 600 {
		t.Errorf("XID = %d, want 600", batches[0].XID)
	}
}

func TestBufferCommitWithoutBegin(t *testing.T) {
	t.Parallel()
	pgm, cdcm := newTestMetrics()
	called := false
	buf := New(Config{MaxTxBytes: 1 << 20}, func(_ *model.TxBatch) error {
		called = true
		return nil
	}, zerolog.Nop(), pgm, cdcm)

	// Commit without any prior Begin should not crash and not call callback.
	err := buf.OnCommit(0x100, time.Now())
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if called {
		t.Error("onTx should not be called for commit without begin")
	}
}

func TestBufferEmptyTransaction(t *testing.T) {
	t.Parallel()
	pgm, cdcm := newTestMetrics()
	var got *model.TxBatch
	buf := New(Config{MaxTxBytes: 1 << 20}, func(b *model.TxBatch) error {
		events := make([]model.TxEvent, len(b.Events))
		copy(events, b.Events)
		got = &model.TxBatch{XID: b.XID, Events: events}
		return nil
	}, zerolog.Nop(), pgm, cdcm)

	now := time.Now()
	_ = buf.OnBegin(700, 0x700, now)
	_ = buf.OnCommit(0x710, now)

	if got == nil {
		t.Fatal("expected onTx callback for empty transaction")
	}
	if len(got.Events) != 0 {
		t.Errorf("events = %d, want 0", len(got.Events))
	}
}

func TestBufferOverflowSkipsSubsequentChanges(t *testing.T) {
	t.Parallel()
	pgm, cdcm := newTestMetrics()
	called := false
	buf := New(Config{MaxTxBytes: 100, InitialEventCap: 4}, func(_ *model.TxBatch) error {
		called = true
		return nil
	}, zerolog.Nop(), pgm, cdcm)

	rel := &model.Relation{ID: 1, Namespace: "public", Name: "t", Columns: []model.Column{{Name: "data"}}}
	_ = buf.OnRelation(rel)

	bigRow := []model.ColumnValue{{Name: "data", Bytes: make([]byte, 200)}}
	smallRow := []model.ColumnValue{{Name: "data", Value: "small"}}

	_ = buf.OnBegin(800, 0x800, time.Now())
	// First change exceeds limit.
	_ = buf.OnInsert(1, bigRow, 0x801)
	// Subsequent changes should be silently skipped (no error).
	err := buf.OnInsert(1, smallRow, 0x802)
	if err != nil {
		t.Errorf("expected no error for skipped change after overflow, got: %v", err)
	}
	_ = buf.OnCommit(0x810, time.Now())

	if called {
		t.Error("onTx should not be called for oversized tx")
	}
}

func TestBufferRelationCache(t *testing.T) {
	t.Parallel()
	pgm, cdcm := newTestMetrics()
	buf := New(Config{MaxTxBytes: 1 << 20}, func(_ *model.TxBatch) error { return nil }, zerolog.Nop(), pgm, cdcm)

	rel1 := &model.Relation{ID: 1, Namespace: "public", Name: "users"}
	rel2 := &model.Relation{ID: 2, Namespace: "public", Name: "orders"}
	_ = buf.OnRelation(rel1)
	_ = buf.OnRelation(rel2)

	got1, ok1 := buf.GetRelation(1)
	if !ok1 || got1.Name != "users" {
		t.Errorf("GetRelation(1) = %v, %v", got1, ok1)
	}
	got2, ok2 := buf.GetRelation(2)
	if !ok2 || got2.Name != "orders" {
		t.Errorf("GetRelation(2) = %v, %v", got2, ok2)
	}
	_, ok3 := buf.GetRelation(999)
	if ok3 {
		t.Error("GetRelation(999) should return false")
	}
}

func TestBufferOnTxCallbackError(t *testing.T) {
	t.Parallel()
	pgm, cdcm := newTestMetrics()
	errExpected := errors.New("callback failed")
	buf := New(Config{MaxTxBytes: 1 << 20}, func(_ *model.TxBatch) error {
		return errExpected
	}, zerolog.Nop(), pgm, cdcm)

	rel := &model.Relation{ID: 1, Namespace: "public", Name: "t"}
	_ = buf.OnRelation(rel)

	_ = buf.OnBegin(900, 0x900, time.Now())
	_ = buf.OnInsert(1, []model.ColumnValue{{Name: "x", Value: "1"}}, 0x901)
	err := buf.OnCommit(0x910, time.Now())

	if !errors.Is(err, errExpected) {
		t.Errorf("expected callback error, got: %v", err)
	}
}
