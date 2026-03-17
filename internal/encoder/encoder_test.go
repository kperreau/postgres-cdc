package encoder

import (
	"testing"
	"time"

	"github.com/goccy/go-json"

	"github.com/jackc/pglogrepl"

	"github.com/kperreau/postgres-cdc/internal/model"
)

func TestEncodeInsert(t *testing.T) {
	enc := New(Config{SourceName: "pg-main", Database: "app"})
	rel := &model.Relation{
		ID:        1,
		Namespace: "public",
		Name:      "users",
		Columns: []model.Column{
			{Name: "id", IsKey: true},
			{Name: "email"},
		},
		KeyCols: []int{0},
	}
	ev := &model.TxEvent{
		Change: model.Change{
			Op:       model.OpInsert,
			Relation: rel,
			After: []model.ColumnValue{
				{Name: "id", Value: "42"},
				{Name: "email", Value: "test@example.com"},
			},
			LSN: pglogrepl.LSN(0x16B6A30),
		},
		TxID:     123,
		CommitTS: time.Date(2026, 3, 17, 15, 0, 0, 0, time.UTC),
	}

	schema, table, key, value, err := enc.Encode(ev, false)
	if err != nil {
		t.Fatal(err)
	}
	if schema != "public" || table != "users" {
		t.Errorf("schema=%s table=%s", schema, table)
	}

	// Check key contains only PK.
	var keyMap map[string]any
	if err := json.Unmarshal(key, &keyMap); err != nil {
		t.Fatal(err)
	}
	if len(keyMap) != 1 {
		t.Errorf("key should have 1 entry, got %d", len(keyMap))
	}
	if keyMap["id"] != "42" {
		t.Errorf("key[id] = %v", keyMap["id"])
	}

	// Check envelope.
	var env model.CDCEnvelope
	if err := json.Unmarshal(value, &env); err != nil {
		t.Fatal(err)
	}
	if env.Source != "pg-main" {
		t.Errorf("source = %s", env.Source)
	}
	if env.Op != "c" {
		t.Errorf("op = %s, want c", env.Op)
	}
	if env.Database != "app" {
		t.Errorf("database = %s", env.Database)
	}
	if env.Before != nil {
		t.Error("before should be nil for insert")
	}
	if env.After["email"] != "test@example.com" {
		t.Errorf("after.email = %v", env.After["email"])
	}
}

func TestEncodeDelete(t *testing.T) {
	enc := New(Config{SourceName: "pg-main", Database: "app"})
	rel := &model.Relation{
		ID: 1, Namespace: "public", Name: "users",
		Columns: []model.Column{{Name: "id", IsKey: true}},
		KeyCols: []int{0},
	}
	ev := &model.TxEvent{
		Change: model.Change{
			Op: model.OpDelete, Relation: rel,
			Before: []model.ColumnValue{{Name: "id", Value: "99"}},
			LSN:    0x200,
		},
		TxID:     456,
		CommitTS: time.Now(),
	}

	_, _, key, _, err := enc.Encode(ev, false)
	if err != nil {
		t.Fatal(err)
	}
	var keyMap map[string]any
	_ = json.Unmarshal(key, &keyMap)
	if keyMap["id"] != "99" {
		t.Errorf("delete key: id = %v", keyMap["id"])
	}
}

func TestDeterministicKeyString(t *testing.T) {
	key := map[string]any{"b": "2", "a": "1"}
	got := DeterministicKeyString(key)
	want := `a="1"|b="2"`
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestDeterministicKeyStringEmpty(t *testing.T) {
	got := DeterministicKeyString(nil)
	if got != "" {
		t.Errorf("expected empty, got %q", got)
	}
}

func BenchmarkEncode(b *testing.B) {
	enc := New(Config{SourceName: "pg-main", Database: "app"})
	rel := &model.Relation{
		ID: 1, Namespace: "public", Name: "users",
		Columns: []model.Column{{Name: "id", IsKey: true}, {Name: "email"}, {Name: "name"}},
		KeyCols: []int{0},
	}
	ev := &model.TxEvent{
		Change: model.Change{
			Op: model.OpUpdate, Relation: rel,
			Before: []model.ColumnValue{
				{Name: "id", Value: "42"}, {Name: "email", Value: "old@x.com"}, {Name: "name", Value: "Old"},
			},
			After: []model.ColumnValue{
				{Name: "id", Value: "42"}, {Name: "email", Value: "new@x.com"}, {Name: "name", Value: "New"},
			},
			LSN: 0x100,
		},
		TxID: 1, CommitTS: time.Now(),
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, _, _, _ = enc.Encode(ev, false)
	}
}
