// Package encoder converts internal Change/TxEvent types into CDCEnvelope
// values and serializes them to JSON. It produces deterministic message keys
// for Kafka partitioning.
//
// The ToastStrategy setting controls how unchanged TOAST columns appear:
//   - "omit"     — excluded from the map entirely (default).
//   - "sentinel" — present with the string value "__toast_unchanged".
package encoder

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/goccy/go-json"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/kperreau/postgres-cdc/internal/model"
)

// ToastSentinelValue is the placeholder used when ToastStrategy is "sentinel".
const ToastSentinelValue = "__toast_unchanged"

// PostgreSQL type OIDs for uuid and uuid[].
const (
	uuidOID      uint32 = 2950
	uuidArrayOID uint32 = 2951
)

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
		return normalizeUUID(col.Value)
	case uuidArrayOID:
		return normalizeUUIDArray(col.Value)
	}

	// Type-based normalization catches pgtype structs (Interval, Bits, Range,
	// Multirange) that would otherwise serialize as {"Field": …} objects.
	// Keyed on the Go concrete type rather than the PG OID so it applies to
	// every instance regardless of how pgx was configured.
	return normalizePGType(col.Value)
}

// normalizePGType walks a value produced by pgx and rewrites the pgtype
// structs that lack MarshalJSON into their canonical Postgres text form.
// Arrays ([]any) are recursively normalized element-by-element.
func normalizePGType(v any) any {
	switch val := v.(type) {
	case pgtype.Interval:
		return formatInterval(val)
	case pgtype.Bits:
		return formatBits(val)
	case pgtype.Range[any]:
		return formatRange(val)
	case pgtype.Multirange[pgtype.Range[any]]:
		return formatMultirange(val)
	case []any:
		// Arrays (e.g. _interval, _bit, _tsrange) come through as []any from
		// pgx's ArrayCodec. Recurse so each element hits the type switch.
		out := make([]any, len(val))
		for i, e := range val {
			out[i] = normalizePGType(e)
		}
		return out
	}
	return v
}

// formatInterval renders a pgtype.Interval as an ISO 8601 duration string
// (e.g. "P1Y2M3DT4H5M6.789S"). ISO 8601 is platform-neutral (no Java-specific
// encoding like Debezium's MicroDuration) and round-trips through Postgres'
// own interval text parser.
func formatInterval(iv pgtype.Interval) any {
	if !iv.Valid {
		return nil
	}

	var b strings.Builder
	b.WriteByte('P')

	months := iv.Months
	years := months / 12
	months %= 12

	if years != 0 {
		fmt.Fprintf(&b, "%dY", years)
	}
	if months != 0 {
		fmt.Fprintf(&b, "%dM", months)
	}
	if iv.Days != 0 {
		fmt.Fprintf(&b, "%dD", iv.Days)
	}

	micros := iv.Microseconds
	if micros != 0 {
		b.WriteByte('T')
		neg := micros < 0
		if neg {
			micros = -micros
		}
		hours := micros / 3_600_000_000
		micros %= 3_600_000_000
		mins := micros / 60_000_000
		micros %= 60_000_000
		secs := micros / 1_000_000
		frac := micros % 1_000_000

		sign := ""
		if neg {
			sign = "-"
		}
		if hours != 0 {
			fmt.Fprintf(&b, "%s%dH", sign, hours)
		}
		if mins != 0 {
			fmt.Fprintf(&b, "%s%dM", sign, mins)
		}
		if secs != 0 || frac != 0 {
			if frac != 0 {
				fracStr := strings.TrimRight(fmt.Sprintf("%06d", frac), "0")
				fmt.Fprintf(&b, "%s%d.%sS", sign, secs, fracStr)
			} else {
				fmt.Fprintf(&b, "%s%dS", sign, secs)
			}
		}
	}

	if b.Len() == 1 { // only "P" → zero interval
		return "PT0S"
	}
	return b.String()
}

// formatBits renders a pgtype.Bits (bit/varbit column) as a string of '0' and
// '1' characters — the canonical Postgres text representation.
func formatBits(bits pgtype.Bits) any {
	if !bits.Valid {
		return nil
	}
	var b strings.Builder
	b.Grow(int(bits.Len))
	for i := int32(0); i < bits.Len; i++ {
		byteIdx := i / 8
		bitMask := byte(128 >> byte(i%8))
		if bits.Bytes[byteIdx]&bitMask > 0 {
			b.WriteByte('1')
		} else {
			b.WriteByte('0')
		}
	}
	return b.String()
}

// formatRange renders a pgtype.Range as its Postgres text form:
// "[lower,upper)", "(,upper]", "empty", etc.
func formatRange(r pgtype.Range[any]) any {
	if !r.Valid {
		return nil
	}
	if r.LowerType == pgtype.Empty || r.UpperType == pgtype.Empty {
		return "empty"
	}

	var b strings.Builder
	if r.LowerType == pgtype.Inclusive {
		b.WriteByte('[')
	} else {
		b.WriteByte('(')
	}
	if r.LowerType != pgtype.Unbounded {
		b.WriteString(formatRangeBound(r.Lower))
	}
	b.WriteByte(',')
	if r.UpperType != pgtype.Unbounded {
		b.WriteString(formatRangeBound(r.Upper))
	}
	if r.UpperType == pgtype.Inclusive {
		b.WriteByte(']')
	} else {
		b.WriteByte(')')
	}
	return b.String()
}

// formatMultirange renders a pgtype.Multirange as "{range1,range2,…}".
func formatMultirange(mr pgtype.Multirange[pgtype.Range[any]]) any {
	if mr.IsNull() {
		return nil
	}
	var b strings.Builder
	b.WriteByte('{')
	for i, r := range mr {
		if i > 0 {
			b.WriteByte(',')
		}
		s, ok := formatRange(r).(string)
		if !ok {
			continue
		}
		b.WriteString(s)
	}
	b.WriteByte('}')
	return b.String()
}

// formatRangeBound renders a range bound value using the element type's
// canonical string form. pgtype element types (Int4, Int8, Date, Timestamp,
// Timestamptz, Numeric, Float8) all implement MarshalJSON, so we route
// through JSON and strip surrounding quotes for string-formatted values.
func formatRangeBound(v any) string {
	if v == nil {
		return ""
	}
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	s := string(b)
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}

// normalizeUUID converts a single uuid column value into its canonical string
// representation (e.g. "f78e7fa7-b11b-4b06-9006-ead00ecde0b9"). This matches
// the Debezium/Kafka Connect convention (io.debezium.data.Uuid → STRING) so
// downstream consumers see UUIDs as strings rather than byte arrays.
func normalizeUUID(v any) any {
	switch val := v.(type) {
	case string:
		return val
	case [16]byte:
		u, err := uuid.FromBytes(val[:])
		if err != nil {
			return fmt.Sprintf("%v", val)
		}
		return u.String()
	case []byte:
		if len(val) == 16 {
			if u, err := uuid.FromBytes(val); err == nil {
				return u.String()
			}
		}
		// Non-16-byte slice: assume it's already a text representation.
		return string(val)
	case fmt.Stringer:
		// Catches pgtype.UUID, google/uuid.UUID, and other string-convertible
		// UUID types without hard-coding an import on pgx pgtype.
		return val.String()
	}
	// Unexpected type: fall back to a string representation rather than
	// letting encoding/json emit a raw byte array like [247,142,…].
	return fmt.Sprintf("%v", v)
}

// normalizeUUIDArray converts a uuid[] column value into a slice of canonical
// strings. Handles the types pgx v5 may surface from rows.Values(): []any
// (the default path), []string, [][16]byte, and []uuid.UUID.
func normalizeUUIDArray(v any) any {
	switch arr := v.(type) {
	case []string:
		return arr
	case []any:
		out := make([]any, len(arr))
		for i, elem := range arr {
			out[i] = normalizeUUID(elem)
		}
		return out
	case [][16]byte:
		out := make([]string, len(arr))
		for i, b := range arr {
			if u, err := uuid.FromBytes(b[:]); err == nil {
				out[i] = u.String()
			} else {
				out[i] = fmt.Sprintf("%v", b)
			}
		}
		return out
	case []uuid.UUID:
		out := make([]string, len(arr))
		for i, u := range arr {
			out[i] = u.String()
		}
		return out
	}
	return v
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
