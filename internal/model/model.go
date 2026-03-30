// Package model defines the core typed data structures used throughout the CDC pipeline.
// These types avoid map[string]any on the hot path and use concrete, preallocated structures.
package model

import (
	"time"

	"github.com/jackc/pglogrepl"
)

// OpType represents a CDC operation type.
type OpType byte

// CDC operation types published in the envelope "op" field.
const (
	OpInsert   OpType = 'c' // insert
	OpUpdate   OpType = 'u' // update
	OpDelete   OpType = 'd' // delete
	OpSnapshot OpType = 'r' // snapshot read
)

// String returns the single-character string representation.
func (o OpType) String() string {
	return string(o)
}

// Column describes a single column in a relation.
type Column struct {
	Name     string
	TypeOID  uint32
	TypeName string
	IsKey    bool
}

// Relation describes a PostgreSQL relation (table) and its columns.
// Populated from pgoutput RelationMessage and cached by relation ID.
type Relation struct {
	ID        uint32
	Namespace string // schema name
	Name      string // table name
	Columns   []Column
	KeyCols   []int // indices into Columns for primary key columns
}

// ToastUnchanged is a sentinel value indicating a TOAST column whose value
// was not modified by this change. Downstream consumers can distinguish this
// from an explicit NULL by checking for this exact type.
type ToastUnchanged struct{}

// ColumnValue holds a decoded column value for a single row change.
type ColumnValue struct {
	Name    string
	TypeOID uint32
	Value   any    // decoded Go value (nil=NULL, ToastUnchanged=TOAST unchanged)
	Bytes   []byte // raw bytes from pgoutput, for zero-copy encoding paths
}

// Change represents a single row-level change within a transaction.
type Change struct {
	Op         OpType
	RelationID uint32
	Relation   *Relation // pointer to cached relation; set during assembly
	Before     []ColumnValue
	After      []ColumnValue
	LSN        pglogrepl.LSN
}

// TxEvent wraps a Change with transaction-level metadata, populated at COMMIT.
type TxEvent struct {
	Change   Change
	TxID     uint32
	CommitTS time.Time
}

// TxBatch holds all changes for a single committed transaction.
// The pipeline processes entire transactions atomically.
type TxBatch struct {
	XID      uint32
	CommitTS time.Time
	LSN      pglogrepl.LSN // commit LSN
	Events   []TxEvent
	ByteSize int // approximate byte size for memory accounting
}

// Checkpoint represents the last safely published position.
type Checkpoint struct {
	LSN       pglogrepl.LSN `json:"lsn"`
	Timestamp time.Time     `json:"timestamp"`
}

// CDCEnvelope is the published event envelope sent to Redpanda.
// Fields are ordered for JSON marshal efficiency (fixed fields first).
type CDCEnvelope struct {
	Source   string         `json:"source"`
	Database string         `json:"database"`
	Schema   string         `json:"schema"`
	Table    string         `json:"table"`
	Op       string         `json:"op"`
	TxID     uint32         `json:"txid"`
	LSN      string         `json:"lsn"`
	CommitTS string         `json:"commit_ts"` // RFC3339
	Snapshot bool           `json:"snapshot"`
	Key      map[string]any `json:"key"`
	Before   map[string]any `json:"before"`
	After    map[string]any `json:"after"`
}

// TopicMode determines how topics are resolved.
type TopicMode string

// Topic routing modes.
const (
	TopicPerTable TopicMode = "per_table" // one topic per table: {prefix}.{db}.{schema}.{table}
	TopicSingle   TopicMode = "single"    // all events go to a single configured topic
)
