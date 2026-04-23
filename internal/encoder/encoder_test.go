package encoder

import (
	"fmt"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/goccy/go-json"
	"github.com/google/uuid"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"

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

// TestNormalizeUUIDCanonicalString exercises every UUID value shape that the
// snapshot or WAL paths may surface, to guarantee the encoder always emits the
// canonical 36-char string (Debezium / Kafka Connect convention).
func TestNormalizeUUIDCanonicalString(t *testing.T) {
	t.Parallel()
	want := "54f51137-d3fb-45a0-bede-dee0945f959a"
	raw := [16]byte{0x54, 0xf5, 0x11, 0x37, 0xd3, 0xfb, 0x45, 0xa0, 0xbe, 0xde, 0xde, 0xe0, 0x94, 0x5f, 0x95, 0x9a}

	cases := []struct {
		name  string
		value any
	}{
		{"string", want},
		{"[16]byte", raw},
		{"[]byte", raw[:]},
		{"google/uuid.UUID", uuid.UUID(raw)},
		{"pgtype.UUID", pgtype.UUID{Bytes: raw, Valid: true}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			col := model.ColumnValue{Name: "id", TypeOID: uuidOID, Value: tc.value}
			got := normalizeColumnValue(col)
			if got != want {
				t.Fatalf("got %v (%T), want %q", got, got, want)
			}
		})
	}
}

// TestNormalizeUUIDArray verifies uuid[] columns (OID 2951) are emitted as a
// slice of canonical strings, not a slice of byte arrays.
func TestNormalizeUUIDArray(t *testing.T) {
	t.Parallel()
	a := [16]byte{0x54, 0xf5, 0x11, 0x37, 0xd3, 0xfb, 0x45, 0xa0, 0xbe, 0xde, 0xde, 0xe0, 0x94, 0x5f, 0x95, 0x9a}
	b := [16]byte{0x08, 0xa1, 0xac, 0x39, 0x5f, 0x94, 0x41, 0xfb, 0x97, 0xf9, 0xee, 0x51, 0x71, 0x8a, 0xbd, 0x2a}
	wantA := "54f51137-d3fb-45a0-bede-dee0945f959a"
	wantB := "08a1ac39-5f94-41fb-97f9-ee51718abd2a"

	cases := []struct {
		name  string
		value any
		want  []any
	}{
		{"[]any of [16]byte", []any{a, b}, []any{wantA, wantB}},
		{"[]any of string", []any{wantA, wantB}, []any{wantA, wantB}},
		{"[][16]byte", [][16]byte{a, b}, nil}, // handled below (returns []string)
		{"[]uuid.UUID", []uuid.UUID{uuid.UUID(a), uuid.UUID(b)}, nil},
		{"[]string", []string{wantA, wantB}, nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			col := model.ColumnValue{Name: "ids", TypeOID: uuidArrayOID, Value: tc.value}
			got := normalizeColumnValue(col)

			// Serialize through JSON to validate the wire format regardless of
			// the concrete Go slice type.
			raw, err := json.Marshal(got)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			expected := fmt.Sprintf(`["%s","%s"]`, wantA, wantB)
			if string(raw) != expected {
				t.Fatalf("uuid[] wire format = %s, want %s", raw, expected)
			}
		})
	}
}

// TestEncodeSnapshotPKUUIDKey is the end-to-end check for Debezium parity: a
// snapshot row whose PK is a [16]byte UUID must produce a Kafka key with the
// canonical string, not a byte array.
func TestEncodeSnapshotPKUUIDKey(t *testing.T) {
	t.Parallel()
	enc := New(Config{SourceName: "pg-main", Database: "app"})
	raw := [16]byte{0xf7, 0x8e, 0x7f, 0xa7, 0xb1, 0x1b, 0x4b, 0x06, 0x90, 0x06, 0xea, 0xd0, 0x0e, 0xcd, 0xe0, 0xb9}
	want := "f78e7fa7-b11b-4b06-9006-ead00ecde0b9"

	ev := &model.TxEvent{
		Change: model.Change{
			Op: model.OpSnapshot,
			Relation: &model.Relation{
				Namespace: "public",
				Name:      "orders",
				Columns:   []model.Column{{Name: "id", TypeOID: uuidOID, IsKey: true}},
				KeyCols:   []int{0},
			},
			After: []model.ColumnValue{{Name: "id", TypeOID: uuidOID, Value: raw}},
			LSN:   0xA00,
		},
		CommitTS: time.Now(),
	}

	_, _, key, _, err := enc.Encode(ev, true)
	if err != nil {
		t.Fatal(err)
	}
	if string(key) != fmt.Sprintf(`{"id":"%s"}`, want) {
		t.Fatalf("pk key = %s, want {\"id\":\"%s\"}", key, want)
	}
}

// TestNormalizeInterval covers the pgtype.Interval → ISO 8601 duration path.
func TestNormalizeInterval(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		iv   pgtype.Interval
		want any
	}{
		{"invalid", pgtype.Interval{Valid: false}, nil},
		{"zero", pgtype.Interval{Valid: true}, "PT0S"},
		{"1y2m3d", pgtype.Interval{Months: 14, Days: 3, Valid: true}, "P1Y2M3D"},
		{"days_only", pgtype.Interval{Days: 7, Valid: true}, "P7D"},
		{"time_only", pgtype.Interval{Microseconds: 4*3_600_000_000 + 5*60_000_000 + 6*1_000_000, Valid: true}, "PT4H5M6S"},
		{"fractional_seconds", pgtype.Interval{Microseconds: 6_789_000, Valid: true}, "PT6.789S"},
		{"negative_micros", pgtype.Interval{Microseconds: -3_600_000_000, Valid: true}, "PT-1H"},
		{"mixed", pgtype.Interval{Months: 14, Days: 3, Microseconds: 14_706_789_000, Valid: true}, "P1Y2M3DT4H5M6.789S"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			col := model.ColumnValue{Name: "dur", TypeOID: 1186, Value: tc.iv}
			got := normalizeColumnValue(col)
			if got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// TestNormalizeBits covers the pgtype.Bits → bitstring path.
func TestNormalizeBits(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   pgtype.Bits
		want any
	}{
		{"invalid", pgtype.Bits{Valid: false}, nil},
		{"single_byte_full", pgtype.Bits{Bytes: []byte{0xB5}, Len: 8, Valid: true}, "10110101"},
		{"partial_bits", pgtype.Bits{Bytes: []byte{0xA0}, Len: 4, Valid: true}, "1010"},
		{"multi_byte", pgtype.Bits{Bytes: []byte{0xFF, 0x00}, Len: 12, Valid: true}, "111111110000"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			col := model.ColumnValue{Name: "b", TypeOID: 1560, Value: tc.in}
			got := normalizeColumnValue(col)
			if got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// TestNormalizeRange covers pgtype.Range[any] → "[lo,hi)" notation for the
// common range types. The bound values mimic what pgx surfaces via
// rows.Values() — concrete pgtype.* structs with MarshalJSON support.
func TestNormalizeRange(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		r    pgtype.Range[any]
		want any
	}{
		{
			"invalid",
			pgtype.Range[any]{Valid: false},
			nil,
		},
		{
			"empty",
			pgtype.Range[any]{LowerType: pgtype.Empty, UpperType: pgtype.Empty, Valid: true},
			"empty",
		},
		{
			"int4_inclusive_exclusive",
			pgtype.Range[any]{
				Lower:     pgtype.Int4{Int32: 1, Valid: true},
				Upper:     pgtype.Int4{Int32: 10, Valid: true},
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Valid:     true,
			},
			"[1,10)",
		},
		{
			"unbounded_lower",
			pgtype.Range[any]{
				Upper:     pgtype.Int4{Int32: 5, Valid: true},
				LowerType: pgtype.Unbounded,
				UpperType: pgtype.Exclusive,
				Valid:     true,
			},
			"(,5)",
		},
		{
			"date_range",
			pgtype.Range[any]{
				Lower:     pgtype.Date{Time: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true},
				Upper:     pgtype.Date{Time: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC), Valid: true},
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Valid:     true,
			},
			"[2026-01-01,2026-02-01)",
		},
		{
			"numeric_range",
			pgtype.Range[any]{
				Lower:     pgtype.Numeric{Int: big.NewInt(125), Exp: -1, Valid: true}, // 12.5
				Upper:     pgtype.Numeric{Int: big.NewInt(200), Exp: -1, Valid: true}, // 20.0
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Inclusive,
				Valid:     true,
			},
			"[12.5,20.0]",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			col := model.ColumnValue{Name: "r", TypeOID: 3904, Value: tc.r}
			got := normalizeColumnValue(col)
			if got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// TestNormalizeMultirange covers pgtype.Multirange → "{[a,b),[c,d)}".
func TestNormalizeMultirange(t *testing.T) {
	t.Parallel()

	mkRange := func(lo, hi int32) pgtype.Range[any] {
		return pgtype.Range[any]{
			Lower:     pgtype.Int4{Int32: lo, Valid: true},
			Upper:     pgtype.Int4{Int32: hi, Valid: true},
			LowerType: pgtype.Inclusive,
			UpperType: pgtype.Exclusive,
			Valid:     true,
		}
	}

	cases := []struct {
		name string
		mr   pgtype.Multirange[pgtype.Range[any]]
		want any
	}{
		{"null", nil, nil},
		{"empty_slice", pgtype.Multirange[pgtype.Range[any]]{}, "{}"},
		{"two_ranges", pgtype.Multirange[pgtype.Range[any]]{mkRange(1, 5), mkRange(10, 15)}, "{[1,5),[10,15)}"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			col := model.ColumnValue{Name: "mr", TypeOID: 4451, Value: tc.mr}
			got := normalizeColumnValue(col)
			if got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// TestNormalizeIntervalArray verifies _interval arrays (surfaced as []any of
// pgtype.Interval by ArrayCodec) are recursively normalized.
func TestNormalizeIntervalArray(t *testing.T) {
	t.Parallel()
	arr := []any{
		pgtype.Interval{Months: 1, Valid: true},
		pgtype.Interval{Days: 2, Valid: true},
	}
	col := model.ColumnValue{Name: "ds", TypeOID: 1187, Value: arr}
	got := normalizeColumnValue(col)
	raw, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != `["P1M","P2D"]` {
		t.Fatalf("interval array = %s, want [\"P1M\",\"P2D\"]", raw)
	}
}

func BenchmarkNormalizeInterval(b *testing.B) {
	col := model.ColumnValue{Name: "dur", TypeOID: 1186, Value: pgtype.Interval{
		Months: 14, Days: 3, Microseconds: 14_706_789_000, Valid: true,
	}}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = normalizeColumnValue(col)
	}
}

func BenchmarkNormalizeBits(b *testing.B) {
	col := model.ColumnValue{Name: "b", TypeOID: 1560, Value: pgtype.Bits{
		Bytes: []byte{0xB5, 0xA0}, Len: 12, Valid: true,
	}}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = normalizeColumnValue(col)
	}
}

func BenchmarkNormalizeRangeInt4(b *testing.B) {
	col := model.ColumnValue{Name: "r", TypeOID: 3904, Value: pgtype.Range[any]{
		Lower:     pgtype.Int4{Int32: 1, Valid: true},
		Upper:     pgtype.Int4{Int32: 1000, Valid: true},
		LowerType: pgtype.Inclusive,
		UpperType: pgtype.Exclusive,
		Valid:     true,
	}}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = normalizeColumnValue(col)
	}
}

func BenchmarkNormalizeRangeTstz(b *testing.B) {
	col := model.ColumnValue{Name: "r", TypeOID: 3910, Value: pgtype.Range[any]{
		Lower:     pgtype.Timestamptz{Time: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true},
		Upper:     pgtype.Timestamptz{Time: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC), Valid: true},
		LowerType: pgtype.Inclusive,
		UpperType: pgtype.Exclusive,
		Valid:     true,
	}}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = normalizeColumnValue(col)
	}
}

func BenchmarkNormalizeUUID(b *testing.B) {
	col := model.ColumnValue{Name: "id", TypeOID: uuidOID, Value: [16]byte{
		0xf7, 0x8e, 0x7f, 0xa7, 0xb1, 0x1b, 0x4b, 0x06, 0x90, 0x06, 0xea, 0xd0, 0x0e, 0xcd, 0xe0, 0xb9,
	}}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = normalizeColumnValue(col)
	}
}

// BenchmarkNormalizeStringPassthrough measures the cost of the hot path when
// the column is a non-exotic type (typical CDC traffic). This should be
// effectively free — one type-switch miss.
func BenchmarkNormalizeStringPassthrough(b *testing.B) {
	col := model.ColumnValue{Name: "s", TypeOID: 25, Value: "hello world"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = normalizeColumnValue(col)
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
