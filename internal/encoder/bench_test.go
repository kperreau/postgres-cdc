package encoder

import (
	"testing"
	"time"

	"github.com/kperreau/postgres-cdc/internal/model"
)

func BenchmarkEncodeSmall(b *testing.B) {
	enc := New(Config{SourceName: "pg-main", Database: "app"})
	rel := &model.Relation{
		ID: 1, Namespace: "public", Name: "users",
		Columns: []model.Column{{Name: "id", IsKey: true}},
		KeyCols: []int{0},
	}
	ev := &model.TxEvent{
		Change: model.Change{
			Op: model.OpInsert, Relation: rel,
			After: []model.ColumnValue{{Name: "id", Value: "1"}},
			LSN:   0x100,
		},
		TxID: 1, CommitTS: time.Now(),
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, _, _, _ = enc.Encode(ev, false)
	}
}

func BenchmarkEncodeLarge(b *testing.B) {
	enc := New(Config{SourceName: "pg-main", Database: "app"})
	cols := make([]model.Column, 20)
	after := make([]model.ColumnValue, 20)
	before := make([]model.ColumnValue, 20)
	for i := range cols {
		name := "column_" + string(rune('a'+i))
		cols[i] = model.Column{Name: name, IsKey: i == 0}
		after[i] = model.ColumnValue{Name: name, Value: "some_value_that_is_relatively_long_for_benchmarking"}
		before[i] = model.ColumnValue{Name: name, Value: "old_value_that_is_also_relatively_long_for_benchmarking"}
	}
	rel := &model.Relation{
		ID: 1, Namespace: "public", Name: "wide_table",
		Columns: cols, KeyCols: []int{0},
	}
	ev := &model.TxEvent{
		Change: model.Change{
			Op: model.OpUpdate, Relation: rel,
			Before: before, After: after, LSN: 0x100,
		},
		TxID: 1, CommitTS: time.Now(),
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, _, _, _ = enc.Encode(ev, false)
	}
}

func BenchmarkDeterministicKeyString(b *testing.B) {
	key := map[string]any{"id": "42", "tenant_id": "org_123"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = DeterministicKeyString(key)
	}
}
