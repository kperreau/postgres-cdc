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
	"strconv"
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
//
// time.Duration is not usable here: interval has variable-length components
// (months, days) that a fixed-nanosecond duration cannot represent. We build
// the string directly with strconv.AppendInt on a pre-sized []byte to avoid
// the fmt package's format-string parsing allocations.
func formatInterval(iv pgtype.Interval) any {
	if !iv.Valid {
		return nil
	}

	// Worst case: "-P-NNNNY-NNNNM-NNNNDT-NNNNH-NNNNM-NNNN.NNNNNNS" — ~48 bytes.
	buf := make([]byte, 0, 48)
	buf = append(buf, 'P')

	months := iv.Months
	years := months / 12
	months %= 12

	if years != 0 {
		buf = strconv.AppendInt(buf, int64(years), 10)
		buf = append(buf, 'Y')
	}
	if months != 0 {
		buf = strconv.AppendInt(buf, int64(months), 10)
		buf = append(buf, 'M')
	}
	if iv.Days != 0 {
		buf = strconv.AppendInt(buf, int64(iv.Days), 10)
		buf = append(buf, 'D')
	}

	micros := iv.Microseconds
	if micros != 0 {
		buf = append(buf, 'T')
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

		if hours != 0 {
			if neg {
				buf = append(buf, '-')
			}
			buf = strconv.AppendInt(buf, hours, 10)
			buf = append(buf, 'H')
		}
		if mins != 0 {
			if neg {
				buf = append(buf, '-')
			}
			buf = strconv.AppendInt(buf, mins, 10)
			buf = append(buf, 'M')
		}
		if secs != 0 || frac != 0 {
			if neg {
				buf = append(buf, '-')
			}
			buf = strconv.AppendInt(buf, secs, 10)
			if frac != 0 {
				buf = appendFracMicros(buf, frac)
			}
			buf = append(buf, 'S')
		}
	}

	if len(buf) == 1 { // only "P" → zero interval
		return "PT0S"
	}
	return string(buf)
}

// appendFracMicros writes ".NNN" (fractional microseconds, trailing zeros
// trimmed) onto buf. frac must be in [1, 999_999].
func appendFracMicros(buf []byte, frac int64) []byte {
	var digits [6]byte
	for i := 5; i >= 0; i-- {
		digits[i] = byte('0' + frac%10)
		frac /= 10
	}
	end := 6
	for end > 1 && digits[end-1] == '0' {
		end--
	}
	buf = append(buf, '.')
	return append(buf, digits[:end]...)
}

// formatBits renders a pgtype.Bits (bit/varbit column) as a string of '0' and
// '1' characters — the canonical Postgres text representation. Builds the
// output in a single pre-sized []byte (one allocation: the final string copy).
func formatBits(bits pgtype.Bits) any {
	if !bits.Valid {
		return nil
	}
	buf := make([]byte, bits.Len)
	for i := int32(0); i < bits.Len; i++ {
		byteIdx := i / 8
		bitMask := byte(128 >> byte(i%8))
		if bits.Bytes[byteIdx]&bitMask > 0 {
			buf[i] = '1'
		} else {
			buf[i] = '0'
		}
	}
	return string(buf)
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
	// Cap sized for a typical integer range "[NNNN,NNNN)".
	buf := make([]byte, 0, 32)
	buf = appendRange(buf, r)
	return string(buf)
}

// formatMultirange renders a pgtype.Multirange as "{range1,range2,…}".
func formatMultirange(mr pgtype.Multirange[pgtype.Range[any]]) any {
	if mr.IsNull() {
		return nil
	}
	buf := make([]byte, 0, 2+len(mr)*16)
	buf = append(buf, '{')
	for i, r := range mr {
		if i > 0 {
			buf = append(buf, ',')
		}
		if r.LowerType == pgtype.Empty || r.UpperType == pgtype.Empty {
			buf = append(buf, "empty"...)
			continue
		}
		buf = appendRange(buf, r)
	}
	buf = append(buf, '}')
	return string(buf)
}

// appendRange writes one range literal ("[1,5)", "(,5]", …) into buf. The
// caller is responsible for Empty / Valid pre-checks.
func appendRange(buf []byte, r pgtype.Range[any]) []byte {
	if r.LowerType == pgtype.Inclusive {
		buf = append(buf, '[')
	} else {
		buf = append(buf, '(')
	}
	if r.LowerType != pgtype.Unbounded {
		buf = appendRangeBound(buf, r.Lower)
	}
	buf = append(buf, ',')
	if r.UpperType != pgtype.Unbounded {
		buf = appendRangeBound(buf, r.Upper)
	}
	if r.UpperType == pgtype.Inclusive {
		buf = append(buf, ']')
	} else {
		buf = append(buf, ')')
	}
	return buf
}

// appendRangeBound appends the canonical text form of a range bound. Fast
// paths for the pgtype element types that pgx's default codecs produce; falls
// back to json.Marshal + quote stripping for anything exotic.
func appendRangeBound(buf []byte, v any) []byte {
	switch b := v.(type) {
	case nil:
		return buf
	case pgtype.Int4:
		if !b.Valid {
			return buf
		}
		return strconv.AppendInt(buf, int64(b.Int32), 10)
	case pgtype.Int8:
		if !b.Valid {
			return buf
		}
		return strconv.AppendInt(buf, b.Int64, 10)
	case pgtype.Float8:
		if !b.Valid {
			return buf
		}
		return strconv.AppendFloat(buf, b.Float64, 'g', -1, 64)
	case pgtype.Date:
		if !b.Valid {
			return buf
		}
		return b.Time.UTC().AppendFormat(buf, "2006-01-02")
	case pgtype.Timestamp:
		if !b.Valid {
			return buf
		}
		return b.Time.UTC().AppendFormat(buf, "2006-01-02T15:04:05.999999999")
	case pgtype.Timestamptz:
		if !b.Valid {
			return buf
		}
		return b.Time.UTC().AppendFormat(buf, time.RFC3339Nano)
	}
	// Unknown bound type: route through MarshalJSON (covers pgtype.Numeric
	// and future types) and strip surrounding quotes. One allocation here,
	// unavoidable without per-type support.
	out, err := json.Marshal(v)
	if err != nil {
		return append(buf, fmt.Sprintf("%v", v)...)
	}
	if len(out) >= 2 && out[0] == '"' && out[len(out)-1] == '"' {
		return append(buf, out[1:len(out)-1]...)
	}
	return append(buf, out...)
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
