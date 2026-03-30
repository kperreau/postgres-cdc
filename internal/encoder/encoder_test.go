package encoder

import (
	"fmt"
	"sync"
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

func TestToastOmitStrategy(t *testing.T) {
	enc := New(Config{SourceName: "pg-main", Database: "app", ToastStrategy: "omit"})
	ev := &model.TxEvent{
		Change: model.Change{
			Op: model.OpUpdate,
			Relation: &model.Relation{
				ID: 1, Namespace: "public", Name: "users",
				Columns: []model.Column{{Name: "id", IsKey: true}, {Name: "bio"}},
				KeyCols: []int{0},
			},
			Before: []model.ColumnValue{
				{Name: "id", Value: "1"},
				{Name: "bio", Value: model.ToastUnchanged{}},
			},
			After: []model.ColumnValue{
				{Name: "id", Value: "1"},
				{Name: "bio", Value: model.ToastUnchanged{}},
			},
			LSN: 0x100,
		},
		TxID: 1, CommitTS: time.Now(),
	}

	_, _, _, value, err := enc.Encode(ev, false)
	if err != nil {
		t.Fatal(err)
	}

	var env model.CDCEnvelope
	if err := json.Unmarshal(value, &env); err != nil {
		t.Fatal(err)
	}

	// TOAST columns should be omitted from before/after.
	if _, ok := env.Before["bio"]; ok {
		t.Error("omit strategy: before should not contain toast column 'bio'")
	}
	if _, ok := env.After["bio"]; ok {
		t.Error("omit strategy: after should not contain toast column 'bio'")
	}
}

func TestToastSentinelStrategy(t *testing.T) {
	enc := New(Config{SourceName: "pg-main", Database: "app", ToastStrategy: "sentinel"})
	ev := &model.TxEvent{
		Change: model.Change{
			Op: model.OpUpdate,
			Relation: &model.Relation{
				ID: 1, Namespace: "public", Name: "users",
				Columns: []model.Column{{Name: "id", IsKey: true}, {Name: "bio"}},
				KeyCols: []int{0},
			},
			After: []model.ColumnValue{
				{Name: "id", Value: "1"},
				{Name: "bio", Value: model.ToastUnchanged{}},
			},
			LSN: 0x100,
		},
		TxID: 1, CommitTS: time.Now(),
	}

	_, _, _, value, err := enc.Encode(ev, false)
	if err != nil {
		t.Fatal(err)
	}

	var env model.CDCEnvelope
	if err := json.Unmarshal(value, &env); err != nil {
		t.Fatal(err)
	}

	// TOAST columns should have the sentinel value.
	if env.After["bio"] != ToastSentinelValue {
		t.Errorf("sentinel strategy: after.bio = %v, want %q", env.After["bio"], ToastSentinelValue)
	}
}

func TestEncodeUpdate(t *testing.T) {
	t.Parallel()
	enc := New(Config{SourceName: "pg-main", Database: "app"})
	rel := &model.Relation{
		ID: 1, Namespace: "public", Name: "users",
		Columns: []model.Column{{Name: "id", IsKey: true}, {Name: "email"}},
		KeyCols: []int{0},
	}
	ev := &model.TxEvent{
		Change: model.Change{
			Op:       model.OpUpdate,
			Relation: rel,
			Before: []model.ColumnValue{
				{Name: "id", Value: "42"},
				{Name: "email", Value: "old@example.com"},
			},
			After: []model.ColumnValue{
				{Name: "id", Value: "42"},
				{Name: "email", Value: "new@example.com"},
			},
			LSN: 0x300,
		},
		TxID:     789,
		CommitTS: time.Date(2026, 3, 17, 15, 0, 0, 0, time.UTC),
	}

	_, _, _, value, err := enc.Encode(ev, false)
	if err != nil {
		t.Fatal(err)
	}

	var env model.CDCEnvelope
	if err := json.Unmarshal(value, &env); err != nil {
		t.Fatal(err)
	}
	if env.Op != "u" {
		t.Errorf("op = %s, want u", env.Op)
	}
	if env.Before == nil {
		t.Error("before should not be nil for update")
	}
	if env.After == nil {
		t.Error("after should not be nil for update")
	}
	if env.Before["email"] != "old@example.com" {
		t.Errorf("before.email = %v", env.Before["email"])
	}
	if env.After["email"] != "new@example.com" {
		t.Errorf("after.email = %v", env.After["email"])
	}
}

func TestEncodeSnapshot(t *testing.T) {
	t.Parallel()
	enc := New(Config{SourceName: "pg-main", Database: "app"})
	rel := &model.Relation{
		ID: 1, Namespace: "public", Name: "users",
		Columns: []model.Column{{Name: "id", IsKey: true}},
		KeyCols: []int{0},
	}
	ev := &model.TxEvent{
		Change: model.Change{
			Op:       model.OpSnapshot,
			Relation: rel,
			After:    []model.ColumnValue{{Name: "id", Value: "1"}},
			LSN:      0x400,
		},
		TxID:     0,
		CommitTS: time.Now(),
	}

	_, _, _, value, err := enc.Encode(ev, true)
	if err != nil {
		t.Fatal(err)
	}

	var env model.CDCEnvelope
	if err := json.Unmarshal(value, &env); err != nil {
		t.Fatal(err)
	}
	if env.Op != "r" {
		t.Errorf("op = %s, want r", env.Op)
	}
	if !env.Snapshot {
		t.Error("snapshot should be true")
	}
}

func TestEncodeNilRelation(t *testing.T) {
	t.Parallel()
	enc := New(Config{SourceName: "pg-main", Database: "app"})
	ev := &model.TxEvent{
		Change: model.Change{
			Op:       model.OpInsert,
			Relation: nil,
			After:    []model.ColumnValue{{Name: "id", Value: "1"}},
			LSN:      0x500,
		},
		TxID:     1,
		CommitTS: time.Now(),
	}

	schema, table, _, value, err := enc.Encode(ev, false)
	if err != nil {
		t.Fatal(err)
	}
	if schema != "unknown" {
		t.Errorf("schema = %s, want unknown", schema)
	}
	if table != "unknown" {
		t.Errorf("table = %s, want unknown", table)
	}

	var env model.CDCEnvelope
	if err := json.Unmarshal(value, &env); err != nil {
		t.Fatal(err)
	}
	if env.Schema != "unknown" {
		t.Errorf("env.Schema = %s, want unknown", env.Schema)
	}
	if env.Table != "unknown" {
		t.Errorf("env.Table = %s, want unknown", env.Table)
	}
}

func TestEncodeNoKeyColumns(t *testing.T) {
	t.Parallel()
	enc := New(Config{SourceName: "pg-main", Database: "app"})
	rel := &model.Relation{
		ID: 1, Namespace: "public", Name: "users",
		Columns: []model.Column{{Name: "id"}, {Name: "email"}},
		KeyCols: []int{}, // empty — no explicit key columns
	}
	ev := &model.TxEvent{
		Change: model.Change{
			Op:       model.OpInsert,
			Relation: rel,
			After: []model.ColumnValue{
				{Name: "id", Value: "42"},
				{Name: "email", Value: "test@example.com"},
			},
			LSN: 0x600,
		},
		TxID:     1,
		CommitTS: time.Now(),
	}

	_, _, key, _, err := enc.Encode(ev, false)
	if err != nil {
		t.Fatal(err)
	}

	var keyMap map[string]any
	if err := json.Unmarshal(key, &keyMap); err != nil {
		t.Fatal(err)
	}
	// All columns should be used as key fallback.
	if len(keyMap) != 2 {
		t.Errorf("key should have 2 entries (all columns), got %d", len(keyMap))
	}
	if keyMap["id"] != "42" {
		t.Errorf("key[id] = %v", keyMap["id"])
	}
	if keyMap["email"] != "test@example.com" {
		t.Errorf("key[email] = %v", keyMap["email"])
	}
}

func TestEncodeSnapshotUUIDColumnsUseStringsAndPKOnly(t *testing.T) {
	t.Parallel()
	enc := New(Config{SourceName: "pg-main", Database: "app"})
	orgID := []byte{84, 245, 17, 55, 211, 251, 69, 160, 190, 222, 222, 224, 148, 95, 149, 154}
	subscriptionOfferID := []byte{8, 161, 172, 57, 95, 148, 65, 251, 151, 249, 238, 81, 113, 138, 189, 42}

	ev := &model.TxEvent{
		Change: model.Change{
			Op: model.OpSnapshot,
			Relation: &model.Relation{
				Namespace: "public",
				Name:      "billing_credit_balances",
				Columns: []model.Column{
					{Name: "organization_id", TypeOID: uuidOID, IsKey: true},
					{Name: "subscription_offer_id", TypeOID: uuidOID, IsKey: true},
					{Name: "balance"},
				},
				KeyCols: []int{0, 1},
			},
			After: []model.ColumnValue{
				{Name: "organization_id", TypeOID: uuidOID, Value: orgID},
				{Name: "subscription_offer_id", TypeOID: uuidOID, Value: subscriptionOfferID},
				{Name: "balance", Value: int64(9245)},
			},
			LSN: 0x900,
		},
		CommitTS: time.Now(),
	}

	_, _, key, value, err := enc.Encode(ev, true)
	if err != nil {
		t.Fatal(err)
	}

	var keyMap map[string]any
	if err := json.Unmarshal(key, &keyMap); err != nil {
		t.Fatal(err)
	}
	if len(keyMap) != 2 {
		t.Fatalf("snapshot key should include only PK columns, got %d entries", len(keyMap))
	}
	if keyMap["organization_id"] != "54f51137-d3fb-45a0-bede-dee0945f959a" {
		t.Fatalf("organization_id key = %v", keyMap["organization_id"])
	}
	if keyMap["subscription_offer_id"] != "08a1ac39-5f94-41fb-97f9-ee51718abd2a" {
		t.Fatalf("subscription_offer_id key = %v", keyMap["subscription_offer_id"])
	}

	var env model.CDCEnvelope
	if err := json.Unmarshal(value, &env); err != nil {
		t.Fatal(err)
	}
	if env.After["organization_id"] != "54f51137-d3fb-45a0-bede-dee0945f959a" {
		t.Fatalf("after.organization_id = %v", env.After["organization_id"])
	}
	if env.After["subscription_offer_id"] != "08a1ac39-5f94-41fb-97f9-ee51718abd2a" {
		t.Fatalf("after.subscription_offer_id = %v", env.After["subscription_offer_id"])
	}
	if env.After["balance"] != float64(9245) {
		t.Fatalf("after.balance = %v", env.After["balance"])
	}
}

func TestEncodeDeleteNullAfter(t *testing.T) {
	t.Parallel()
	enc := New(Config{SourceName: "pg-main", Database: "app"})
	rel := &model.Relation{
		ID: 1, Namespace: "public", Name: "users",
		Columns: []model.Column{{Name: "id", IsKey: true}, {Name: "email"}},
		KeyCols: []int{0},
	}
	ev := &model.TxEvent{
		Change: model.Change{
			Op:       model.OpDelete,
			Relation: rel,
			Before: []model.ColumnValue{
				{Name: "id", Value: "55"},
				{Name: "email", Value: "del@example.com"},
			},
			// After is nil for deletes.
			LSN: 0x700,
		},
		TxID:     2,
		CommitTS: time.Now(),
	}

	_, _, key, _, err := enc.Encode(ev, false)
	if err != nil {
		t.Fatal(err)
	}

	var keyMap map[string]any
	if err := json.Unmarshal(key, &keyMap); err != nil {
		t.Fatal(err)
	}
	if keyMap["id"] != "55" {
		t.Errorf("delete key should be extracted from Before: id = %v", keyMap["id"])
	}
}

func TestEncoderConcurrentSafety(t *testing.T) {
	t.Parallel()
	enc := New(Config{SourceName: "pg-main", Database: "app"})
	rel := &model.Relation{
		ID: 1, Namespace: "public", Name: "users",
		Columns: []model.Column{{Name: "id", IsKey: true}},
		KeyCols: []int{0},
	}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			ev := &model.TxEvent{
				Change: model.Change{
					Op:       model.OpInsert,
					Relation: rel,
					After:    []model.ColumnValue{{Name: "id", Value: fmt.Sprintf("%d", n)}},
					LSN:      pglogrepl.LSN(n),
				},
				TxID:     uint32(n),
				CommitTS: time.Now(),
			}
			_, _, _, _, err := enc.Encode(ev, false)
			if err != nil {
				t.Errorf("goroutine %d: %v", n, err)
			}
		}(i)
	}
	wg.Wait()
}

func TestToastMixedColumns(t *testing.T) {
	t.Parallel()
	enc := New(Config{SourceName: "pg-main", Database: "app", ToastStrategy: "omit"})
	ev := &model.TxEvent{
		Change: model.Change{
			Op: model.OpUpdate,
			Relation: &model.Relation{
				ID: 1, Namespace: "public", Name: "posts",
				Columns: []model.Column{{Name: "id", IsKey: true}, {Name: "title"}, {Name: "body"}},
				KeyCols: []int{0},
			},
			After: []model.ColumnValue{
				{Name: "id", Value: "1"},
				{Name: "title", Value: "Updated Title"},       // normal column
				{Name: "body", Value: model.ToastUnchanged{}}, // TOAST unchanged
			},
			LSN: 0x800,
		},
		TxID: 1, CommitTS: time.Now(),
	}

	_, _, _, value, err := enc.Encode(ev, false)
	if err != nil {
		t.Fatal(err)
	}

	var env model.CDCEnvelope
	if err := json.Unmarshal(value, &env); err != nil {
		t.Fatal(err)
	}

	// Normal column should be present.
	if env.After["title"] != "Updated Title" {
		t.Errorf("after.title = %v, want 'Updated Title'", env.After["title"])
	}
	// TOAST column should be omitted.
	if _, ok := env.After["body"]; ok {
		t.Error("omit strategy: after should not contain toast column 'body'")
	}
	// Non-toast columns should still be present.
	if env.After["id"] != "1" {
		t.Errorf("after.id = %v, want '1'", env.After["id"])
	}
}

func TestColumnsToMapNil(t *testing.T) {
	t.Parallel()
	enc := New(Config{SourceName: "pg-main", Database: "app"})
	result := enc.columnsToMap(nil)
	if result != nil {
		t.Errorf("expected nil for nil input, got %v", result)
	}
	result2 := enc.columnsToMap([]model.ColumnValue{})
	if result2 != nil {
		t.Errorf("expected nil for empty slice, got %v", result2)
	}
}

func TestDeterministicKeyStringMultipleTypes(t *testing.T) {
	t.Parallel()
	key := map[string]any{
		"name":   "alice",
		"age":    float64(30),
		"active": true,
	}
	got := DeterministicKeyString(key)
	// Keys sorted: active, age, name. Values JSON-encoded.
	want := `active=true|age=30|name="alice"`
	if got != want {
		t.Errorf("got %q, want %q", got, want)
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
