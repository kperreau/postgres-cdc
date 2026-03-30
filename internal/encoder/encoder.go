// Package encoder converts internal Change/TxEvent types into CDCEnvelope
// values and serializes them to JSON. It produces deterministic message keys
// for Kafka partitioning.
//
// The ToastStrategy setting controls how unchanged TOAST columns appear:
//   - "omit"     — excluded from the map entirely (default).
//   - "sentinel" — present with the string value "__toast_unchanged".
package encoder

import (
	"sort"
	"strings"
	"time"

	"github.com/goccy/go-json"
	"github.com/google/uuid"

	"github.com/kperreau/postgres-cdc/internal/model"
)

// ToastSentinelValue is the placeholder used when ToastStrategy is "sentinel".
const ToastSentinelValue = "__toast_unchanged"

const uuidOID uint32 = 2950

// Config holds encoder settings.
type Config struct {
	SourceName    string // e.g. "postgres-main"
	Database      string // e.g. "app"
	ToastStrategy string // "omit" (default) or "sentinel"
}

// Encoder builds CDCEnvelope values and serializes them to JSON.
// It is safe for concurrent use: all fields are read-only after construction.
type Encoder struct {
	cfg           Config
	toastSentinel bool
}

// New creates a new Encoder.
func New(cfg Config) *Encoder {
	return &Encoder{
		cfg:           cfg,
		toastSentinel: cfg.ToastStrategy == "sentinel",
	}
}

// Encode converts a TxEvent into a JSON-encoded CDCEnvelope.
// It returns the topic-routing metadata, the deterministic key bytes,
// and the value bytes.
func (e *Encoder) Encode(ev *model.TxEvent, snapshot bool) (schema, table string, key, value []byte, err error) {
	rel := ev.Change.Relation
	if rel == nil {
		schema = "unknown"
		table = "unknown"
	} else {
		schema = rel.Namespace
		table = rel.Name
	}

	envelope := model.CDCEnvelope{
		Source:   e.cfg.SourceName,
		Database: e.cfg.Database,
		Schema:   schema,
		Table:    table,
		Op:       string(ev.Change.Op),
		TxID:     ev.TxID,
		LSN:      ev.Change.LSN.String(),
		CommitTS: ev.CommitTS.UTC().Format(time.RFC3339),
		Snapshot: snapshot,
		Key:      buildKey(rel, ev),
		Before:   e.columnsToMap(ev.Change.Before),
		After:    e.columnsToMap(ev.Change.After),
	}

	// Serialize value (full envelope).
	value, err = json.Marshal(&envelope)
	if err != nil {
		return "", "", nil, nil, err
	}

	// Serialize key (deterministic, sorted).
	key, err = json.Marshal(envelope.Key)
	if err != nil {
		return "", "", nil, nil, err
	}

	return schema, table, key, value, nil
}

// buildKey extracts the primary key columns into a deterministic map.
// If no key columns are defined, falls back to using all after columns.
func buildKey(rel *model.Relation, ev *model.TxEvent) map[string]any {
	if rel == nil {
		return nil
	}

	// Determine which row to extract keys from: prefer after, fall back to before (deletes).
	row := ev.Change.After
	if len(row) == 0 {
		row = ev.Change.Before
	}
	if len(row) == 0 {
		return nil
	}

	// If relation has explicit key columns, use only those.
	if len(rel.KeyCols) > 0 {
		m := make(map[string]any, len(rel.KeyCols))
		for _, idx := range rel.KeyCols {
			if idx < len(row) {
				m[row[idx].Name] = normalizeColumnValue(row[idx])
			}
		}
		return m
	}

	// Fallback: use all columns as key (not ideal but deterministic).
	m := make(map[string]any, len(row))
	for i := range row {
		m[row[i].Name] = normalizeColumnValue(row[i])
	}
	return m
}

// columnsToMap converts a ColumnValue slice to a map. Returns nil for empty slices.
// Unchanged TOAST columns are handled according to the encoder's ToastStrategy.
func (e *Encoder) columnsToMap(cols []model.ColumnValue) map[string]any {
	if len(cols) == 0 {
		return nil
	}
	m := make(map[string]any, len(cols))
	for i := range cols {
		v := cols[i].Value
		if _, ok := v.(model.ToastUnchanged); ok {
			if e.toastSentinel {
				m[cols[i].Name] = ToastSentinelValue
			}
			// omit strategy: skip the column entirely.
			continue
		}
		m[cols[i].Name] = normalizeColumnValue(cols[i])
	}
	return m
}

func normalizeColumnValue(col model.ColumnValue) any {
	if col.Value == nil {
		return nil
	}
	if _, ok := col.Value.(model.ToastUnchanged); ok {
		return col.Value
	}

	switch col.TypeOID {
	case uuidOID:
		switch v := col.Value.(type) {
		case string:
			return v
		case [16]byte:
			if u, err := uuid.FromBytes(v[:]); err == nil {
				return u.String()
			}
		case []byte:
			if u, err := uuid.FromBytes(v); err == nil {
				return u.String()
			}
		}
	}

	return col.Value
}

// DeterministicKeyString returns a stable string representation of a key map,
// suitable for use as a Kafka message key when byte-level determinism is needed.
// Keys are sorted alphabetically.
func DeterministicKeyString(key map[string]any) string {
	if len(key) == 0 {
		return ""
	}
	keys := make([]string, 0, len(key))
	for k := range key {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte('|')
		}
		b.WriteString(k)
		b.WriteByte('=')
		// Use JSON encoding for the value to handle all types.
		v, _ := json.Marshal(key[k])
		b.Write(v)
	}
	return b.String()
}
