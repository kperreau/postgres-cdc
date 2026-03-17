// Package txbuffer assembles individual WAL row changes (between BEGIN and COMMIT)
// into complete TxBatch values. It enforces bounded memory per transaction and
// preserves strict transaction ordering.
package txbuffer

import (
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/rs/zerolog"

	"github.com/kperreau/postgres-cdc/internal/metrics"
	"github.com/kperreau/postgres-cdc/internal/model"
)

// Buffer collects row changes between BEGIN and COMMIT into complete transactions.
// It implements pgrepl.MessageHandler.
type Buffer struct {
	log        zerolog.Logger
	pgMetrics  *metrics.PGMetrics
	cdcMetrics *metrics.CDCMetrics
	maxTxBytes int
	relations  map[uint32]*model.Relation

	// current transaction state (only one tx at a time from a single replication slot)
	inTx     bool
	xid      uint32
	commitTS time.Time
	lsn      pglogrepl.LSN
	events   []model.TxEvent
	byteSize int
	overflow bool // true if tx exceeded maxTxBytes

	// onTx is called with a complete transaction batch. It must not retain
	// the slice — the buffer reuses it.
	onTx func(batch *model.TxBatch) error
}

// Config holds txbuffer configuration.
type Config struct {
	MaxTxBytes      int
	InitialEventCap int // preallocated event slice capacity
}

// New creates a new transaction buffer.
// onTx is called synchronously for each committed transaction.
func New(cfg Config, onTx func(*model.TxBatch) error, log zerolog.Logger, pgm *metrics.PGMetrics, cdcm *metrics.CDCMetrics) *Buffer {
	if cfg.InitialEventCap <= 0 {
		cfg.InitialEventCap = 128
	}
	return &Buffer{
		log:        log.With().Str("component", "txbuffer").Logger(),
		pgMetrics:  pgm,
		cdcMetrics: cdcm,
		maxTxBytes: cfg.MaxTxBytes,
		relations:  make(map[uint32]*model.Relation, 64),
		events:     make([]model.TxEvent, 0, cfg.InitialEventCap),
		onTx:       onTx,
	}
}

// OnBegin starts accumulating a new transaction.
func (b *Buffer) OnBegin(xid uint32, lsn pglogrepl.LSN, commitTS time.Time) error {
	if b.inTx {
		b.log.Warn().Uint32("prev_xid", b.xid).Uint32("new_xid", xid).Msg("BEGIN without COMMIT; discarding previous tx")
		b.reset()
	}
	b.inTx = true
	b.xid = xid
	b.lsn = lsn
	b.commitTS = commitTS
	b.overflow = false
	return nil
}

// OnRelation caches relation metadata for column decoding.
func (b *Buffer) OnRelation(rel *model.Relation) error {
	b.relations[rel.ID] = rel
	return nil
}

// OnInsert appends an insert change to the current transaction.
func (b *Buffer) OnInsert(relID uint32, newRow []model.ColumnValue, lsn pglogrepl.LSN) error {
	return b.addChange(model.OpInsert, relID, nil, newRow, lsn)
}

// OnUpdate appends an update change to the current transaction.
func (b *Buffer) OnUpdate(relID uint32, oldRow, newRow []model.ColumnValue, lsn pglogrepl.LSN) error {
	return b.addChange(model.OpUpdate, relID, oldRow, newRow, lsn)
}

// OnDelete appends a delete change to the current transaction.
func (b *Buffer) OnDelete(relID uint32, oldRow []model.ColumnValue, lsn pglogrepl.LSN) error {
	return b.addChange(model.OpDelete, relID, oldRow, nil, lsn)
}

// OnCommit finalizes the current transaction and delivers it via onTx.
func (b *Buffer) OnCommit(lsn pglogrepl.LSN, commitTS time.Time) error {
	if !b.inTx {
		b.log.Warn().Msg("COMMIT without BEGIN; ignoring")
		return nil
	}
	defer b.reset()

	if b.overflow {
		b.cdcMetrics.OversizedTransTotal.Inc()
		b.log.Warn().
			Uint32("xid", b.xid).
			Int("byte_size", b.byteSize).
			Int("max_tx_bytes", b.maxTxBytes).
			Msg("dropping oversized transaction")
		return nil
	}

	b.pgMetrics.TransactionsTotal.Inc()
	b.pgMetrics.TransactionBytes.Observe(float64(b.byteSize))

	batch := &model.TxBatch{
		XID:      b.xid,
		CommitTS: commitTS,
		LSN:      lsn,
		Events:   b.events,
		ByteSize: b.byteSize,
	}

	return b.onTx(batch)
}

// GetRelation returns cached relation metadata.
func (b *Buffer) GetRelation(id uint32) (*model.Relation, bool) {
	rel, ok := b.relations[id]
	return rel, ok
}

// addChange appends a row change, tracking byte size and enforcing the limit.
func (b *Buffer) addChange(op model.OpType, relID uint32, before, after []model.ColumnValue, lsn pglogrepl.LSN) error {
	if !b.inTx {
		return fmt.Errorf("txbuffer: change outside transaction (relID=%d)", relID)
	}
	if b.overflow {
		return nil // already oversized, skip silently
	}

	rel := b.relations[relID]

	change := model.Change{
		Op:         op,
		RelationID: relID,
		Relation:   rel,
		Before:     before,
		After:      after,
		LSN:        lsn,
	}

	size := estimateChangeBytes(before, after)
	if b.maxTxBytes > 0 && b.byteSize+size > b.maxTxBytes {
		b.overflow = true
		b.log.Warn().
			Uint32("xid", b.xid).
			Int("byte_size", b.byteSize+size).
			Int("max_tx_bytes", b.maxTxBytes).
			Msg("transaction exceeds max bytes; will be dropped at COMMIT")
		return nil
	}

	b.byteSize += size
	b.events = append(b.events, model.TxEvent{
		Change:   change,
		TxID:     b.xid,
		CommitTS: b.commitTS,
	})
	return nil
}

// reset clears the current transaction state, reusing the events slice.
func (b *Buffer) reset() {
	b.inTx = false
	b.xid = 0
	b.commitTS = time.Time{}
	b.lsn = 0
	b.events = b.events[:0]
	b.byteSize = 0
	b.overflow = false
}

// estimateChangeBytes returns a rough byte size for memory accounting.
func estimateChangeBytes(before, after []model.ColumnValue) int {
	size := 64 // base overhead per change
	for i := range before {
		size += len(before[i].Name) + len(before[i].Bytes) + 32
	}
	for i := range after {
		size += len(after[i].Name) + len(after[i].Bytes) + 32
	}
	return size
}
