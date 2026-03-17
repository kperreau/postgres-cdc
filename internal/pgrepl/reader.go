// Package pgrepl manages the PostgreSQL logical replication connection,
// WAL read loop, pgoutput message parsing, relation caching, and standby
// status heartbeats.
package pgrepl

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/rs/zerolog"

	"github.com/kperreau/postgres-cdc/internal/metrics"
	"github.com/kperreau/postgres-cdc/internal/model"
)

// ReaderConfig holds parameters for the replication reader.
type ReaderConfig struct {
	ConnString      string
	SlotName        string
	PublicationName string
	CreateSlot      bool
	StatusInterval  time.Duration
	StartLSN        pglogrepl.LSN
}

// MessageHandler is called for each parsed WAL message.
// Implementations must not retain msg pointers after returning.
type MessageHandler interface {
	OnBegin(xid uint32, lsn pglogrepl.LSN, commitTS time.Time) error
	OnRelation(rel *model.Relation) error
	OnInsert(relID uint32, newRow []model.ColumnValue, lsn pglogrepl.LSN) error
	OnUpdate(relID uint32, oldRow, newRow []model.ColumnValue, lsn pglogrepl.LSN) error
	OnDelete(relID uint32, oldRow []model.ColumnValue, lsn pglogrepl.LSN) error
	OnCommit(lsn pglogrepl.LSN, commitTS time.Time) error
}

// Reader manages a single replication connection and WAL read loop.
type Reader struct {
	cfg     ReaderConfig
	log     zerolog.Logger
	metrics *metrics.PGMetrics
	handler MessageHandler

	mu        sync.Mutex // guards relations
	relations map[uint32]*model.Relation

	conn    *pgconn.PgConn
	lastLSN pglogrepl.LSN
}

// NewReader creates a new replication reader. Call Run to start streaming.
func NewReader(cfg ReaderConfig, handler MessageHandler, log zerolog.Logger, m *metrics.PGMetrics) *Reader {
	return &Reader{
		cfg:       cfg,
		log:       log.With().Str("component", "pgrepl").Logger(),
		metrics:   m,
		handler:   handler,
		relations: make(map[uint32]*model.Relation, 64),
		lastLSN:   cfg.StartLSN,
	}
}

// Run connects, optionally creates the replication slot, and enters the WAL
// read loop. It blocks until ctx is cancelled or a fatal error occurs.
func (r *Reader) Run(ctx context.Context) error {
	if err := r.connect(ctx); err != nil {
		return fmt.Errorf("pgrepl connect: %w", err)
	}
	defer r.close()

	if r.cfg.CreateSlot {
		if err := r.ensureSlot(ctx); err != nil {
			return fmt.Errorf("pgrepl ensure slot: %w", err)
		}
	}

	if err := r.startReplication(ctx); err != nil {
		return fmt.Errorf("pgrepl start replication: %w", err)
	}

	return r.readLoop(ctx)
}

// LastLSN returns the last WAL LSN processed by the reader.
func (r *Reader) LastLSN() pglogrepl.LSN {
	return r.lastLSN
}

// connect establishes a replication-mode connection to PostgreSQL.
func (r *Reader) connect(ctx context.Context) error {
	connStr := r.cfg.ConnString + " replication=database"
	conn, err := pgconn.Connect(ctx, connStr)
	if err != nil {
		return err
	}
	r.conn = conn
	r.log.Info().Msg("connected to PostgreSQL replication")
	return nil
}

// close shuts down the connection.
func (r *Reader) close() {
	if r.conn != nil {
		_ = r.conn.Close(context.Background())
		r.conn = nil
	}
}

// ensureSlot creates the replication slot if it does not exist.
func (r *Reader) ensureSlot(ctx context.Context) error {
	_, err := pglogrepl.CreateReplicationSlot(ctx, r.conn, r.cfg.SlotName, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{
			Temporary: false,
			Mode:      pglogrepl.LogicalReplication,
		})
	if err != nil {
		// Slot already exists is not an error for our purposes.
		// pglogrepl returns a PgError with code 42710 (duplicate_object).
		if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == "42710" {
			r.log.Info().Str("slot", r.cfg.SlotName).Msg("replication slot already exists")
			return nil
		}
		return err
	}
	r.log.Info().Str("slot", r.cfg.SlotName).Msg("created replication slot")
	return nil
}

// startReplication begins WAL streaming from the configured LSN.
func (r *Reader) startReplication(ctx context.Context) error {
	pluginArgs := []string{
		"proto_version '2'",
		fmt.Sprintf("publication_names '%s'", r.cfg.PublicationName),
	}
	err := pglogrepl.StartReplication(ctx, r.conn, r.cfg.SlotName, r.lastLSN,
		pglogrepl.StartReplicationOptions{
			PluginArgs: pluginArgs,
		})
	if err != nil {
		return err
	}
	r.log.Info().
		Str("slot", r.cfg.SlotName).
		Str("lsn", r.lastLSN.String()).
		Msg("started WAL streaming")
	return nil
}

// readLoop processes WAL messages until the context is cancelled.
func (r *Reader) readLoop(ctx context.Context) error {
	statusTicker := time.NewTicker(r.cfg.StatusInterval)
	defer statusTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-statusTicker.C:
			if err := r.sendStandbyStatus(ctx); err != nil {
				r.log.Warn().Err(err).Msg("failed to send standby status")
			}
		default:
		}

		rawMsg, err := r.conn.ReceiveMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("receive message: %w", err)
		}

		switch msg := rawMsg.(type) {
		case *pgproto3.CopyData:
			if err := r.handleCopyData(msg.Data); err != nil {
				return fmt.Errorf("handle copy data: %w", err)
			}
		case *pgproto3.ErrorResponse:
			return fmt.Errorf("postgres error: %s (code %s)", msg.Message, msg.Code)
		default:
			r.log.Debug().Str("type", fmt.Sprintf("%T", msg)).Msg("ignoring message")
		}
	}
}

// handleCopyData dispatches a CopyData payload to the appropriate handler.
func (r *Reader) handleCopyData(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	switch data[0] {
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		return r.handleKeepalive(data[1:])
	case pglogrepl.XLogDataByteID:
		return r.handleXLogData(data[1:])
	default:
		r.log.Debug().Uint8("type", data[0]).Msg("unknown CopyData sub-message")
		return nil
	}
}

// handleKeepalive processes a primary keepalive message.
func (r *Reader) handleKeepalive(data []byte) error {
	pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(data)
	if err != nil {
		return fmt.Errorf("parse keepalive: %w", err)
	}
	if pkm.ReplyRequested {
		return r.sendStandbyStatus(context.Background())
	}
	return nil
}

// handleXLogData parses the XLogData header and dispatches the WAL message.
func (r *Reader) handleXLogData(data []byte) error {
	xld, err := pglogrepl.ParseXLogData(data)
	if err != nil {
		return fmt.Errorf("parse xlog data: %w", err)
	}

	r.metrics.WALMessagesTotal.Inc()
	r.lastLSN = xld.WALStart + pglogrepl.LSN(len(xld.WALData))

	return r.parseWALMessage(xld.WALData, xld.WALStart)
}

// parseWALMessage dispatches a pgoutput-encoded WAL message to the handler.
func (r *Reader) parseWALMessage(data []byte, walStart pglogrepl.LSN) error {
	logicalMsg, err := pglogrepl.ParseV2(data, false)
	if err != nil {
		return fmt.Errorf("parse pgoutput: %w", err)
	}

	switch msg := logicalMsg.(type) {
	case *pglogrepl.RelationMessageV2:
		return r.handleRelation(msg)
	case *pglogrepl.BeginMessage:
		return r.handler.OnBegin(msg.Xid, pglogrepl.LSN(msg.FinalLSN), msg.CommitTime)
	case *pglogrepl.CommitMessage:
		return r.handler.OnCommit(pglogrepl.LSN(msg.CommitLSN), msg.CommitTime)
	case *pglogrepl.InsertMessageV2:
		cols := r.decodeColumns(msg.RelationID, msg.Tuple)
		return r.handler.OnInsert(msg.RelationID, cols, walStart)
	case *pglogrepl.UpdateMessageV2:
		var oldCols []model.ColumnValue
		if msg.OldTuple != nil {
			oldCols = r.decodeColumns(msg.RelationID, msg.OldTuple)
		}
		newCols := r.decodeColumns(msg.RelationID, msg.NewTuple)
		return r.handler.OnUpdate(msg.RelationID, oldCols, newCols, walStart)
	case *pglogrepl.DeleteMessageV2:
		oldCols := r.decodeColumns(msg.RelationID, msg.OldTuple)
		return r.handler.OnDelete(msg.RelationID, oldCols, walStart)
	case *pglogrepl.TruncateMessageV2:
		r.log.Warn().Msg("TRUNCATE message received; not propagated")
		return nil
	case *pglogrepl.TypeMessageV2:
		return nil // type messages are informational
	case *pglogrepl.OriginMessage:
		return nil
	default:
		r.log.Debug().Str("type", fmt.Sprintf("%T", msg)).Msg("unhandled pgoutput message")
		return nil
	}
}

// handleRelation caches relation metadata and notifies the handler.
func (r *Reader) handleRelation(msg *pglogrepl.RelationMessageV2) error {
	cols := make([]model.Column, len(msg.Columns))
	var keyCols []int
	for i, c := range msg.Columns {
		cols[i] = model.Column{
			Name:    c.Name,
			TypeOID: c.DataType,
			IsKey:   c.Flags == 1,
		}
		if c.Flags == 1 {
			keyCols = append(keyCols, i)
		}
	}

	rel := &model.Relation{
		ID:        msg.RelationID,
		Namespace: msg.Namespace,
		Name:      msg.RelationName,
		Columns:   cols,
		KeyCols:   keyCols,
	}

	r.mu.Lock()
	r.relations[msg.RelationID] = rel
	r.metrics.RelationCacheEntries.Set(float64(len(r.relations)))
	r.mu.Unlock()

	return r.handler.OnRelation(rel)
}

// GetRelation returns cached relation metadata for the given relation ID.
func (r *Reader) GetRelation(id uint32) (*model.Relation, bool) {
	r.mu.Lock()
	rel, ok := r.relations[id]
	r.mu.Unlock()
	return rel, ok
}

// decodeColumns converts a pglogrepl TupleData into typed ColumnValues.
func (r *Reader) decodeColumns(relID uint32, tuple *pglogrepl.TupleData) []model.ColumnValue {
	if tuple == nil {
		return nil
	}

	r.mu.Lock()
	rel, ok := r.relations[relID]
	r.mu.Unlock()
	if !ok {
		r.log.Warn().Uint32("relation_id", relID).Msg("unknown relation; columns returned with indices only")
		cols := make([]model.ColumnValue, len(tuple.Columns))
		for i, col := range tuple.Columns {
			cols[i] = model.ColumnValue{
				Name:  fmt.Sprintf("col_%d", i),
				Bytes: col.Data,
			}
		}
		return cols
	}

	cols := make([]model.ColumnValue, len(tuple.Columns))
	for i, col := range tuple.Columns {
		name := ""
		if i < len(rel.Columns) {
			name = rel.Columns[i].Name
		}
		cols[i] = model.ColumnValue{
			Name:  name,
			Bytes: col.Data,
			Value: decodeColumnValue(col),
		}
	}
	return cols
}

// decodeColumnValue converts a single pglogrepl TupleDataColumn to a Go value.
// For the MVP, text-format values are returned as strings. Null and TOAST
// unchanged columns are handled explicitly.
func decodeColumnValue(col *pglogrepl.TupleDataColumn) any {
	switch col.DataType {
	case 'n': // null
		return nil
	case 'u': // TOAST unchanged
		return nil
	case 't': // text
		return string(col.Data)
	default:
		return string(col.Data)
	}
}

// sendStandbyStatus sends a standby status update to PostgreSQL,
// confirming the last processed LSN.
func (r *Reader) sendStandbyStatus(ctx context.Context) error {
	return pglogrepl.SendStandbyStatusUpdate(ctx, r.conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: r.lastLSN,
		WALFlushPosition: r.lastLSN,
		WALApplyPosition: r.lastLSN,
	})
}
