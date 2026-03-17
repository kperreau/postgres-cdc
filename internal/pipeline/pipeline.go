// Package pipeline wires the CDC stages together: WAL reader → txbuffer →
// encode → publish → checkpoint. All channels are bounded, backpressure
// propagates upstream, and transaction ordering is preserved.
package pipeline

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/kperreau/postgres-cdc/internal/checkpoint"
	"github.com/kperreau/postgres-cdc/internal/encoder"
	"github.com/kperreau/postgres-cdc/internal/health"
	"github.com/kperreau/postgres-cdc/internal/metrics"
	"github.com/kperreau/postgres-cdc/internal/model"
	"github.com/kperreau/postgres-cdc/internal/pgrepl"
	"github.com/kperreau/postgres-cdc/internal/producer"
	"github.com/kperreau/postgres-cdc/internal/topic"
	"github.com/kperreau/postgres-cdc/internal/txbuffer"
)

// Config holds pipeline configuration.
type Config struct {
	QueueCapacity int
	MaxTxBytes    int
	SourceName    string
	Database      string
}

// Pipeline coordinates the CDC stages.
type Pipeline struct {
	cfg       Config
	log       zerolog.Logger
	metrics   *metrics.Metrics
	health    *health.Status
	producer  *producer.Producer
	cpMgr     *checkpoint.Manager
	resolver  *topic.Resolver
	encoder   *encoder.Encoder
	readerCfg pgrepl.ReaderConfig

	// txCh carries assembled transactions from the buffer to the publish stage.
	txCh chan *model.TxBatch
}

// New creates a Pipeline.
func New(
	cfg Config,
	readerCfg pgrepl.ReaderConfig,
	prod *producer.Producer,
	cpMgr *checkpoint.Manager,
	resolver *topic.Resolver,
	healthStatus *health.Status,
	m *metrics.Metrics,
	log zerolog.Logger,
) *Pipeline {
	return &Pipeline{
		cfg:       cfg,
		log:       log.With().Str("component", "pipeline").Logger(),
		metrics:   m,
		health:    healthStatus,
		producer:  prod,
		cpMgr:     cpMgr,
		resolver:  resolver,
		encoder:   encoder.New(encoder.Config{SourceName: cfg.SourceName, Database: cfg.Database}),
		readerCfg: readerCfg,
		txCh:      make(chan *model.TxBatch, cfg.QueueCapacity),
	}
}

// SetReaderConfig updates the reader config (used when wiring is two-phase).
func (p *Pipeline) SetReaderConfig(cfg pgrepl.ReaderConfig) {
	p.readerCfg = cfg
}

// Run starts the pipeline stages and blocks until ctx is cancelled or a fatal
// error occurs.
func (p *Pipeline) Run(ctx context.Context) error {
	// Create the txbuffer whose onTx callback sends to txCh.
	buf := txbuffer.New(
		txbuffer.Config{
			MaxTxBytes:      p.cfg.MaxTxBytes,
			InitialEventCap: 256,
		},
		p.enqueueTx(ctx),
		p.log,
		&p.metrics.PG,
		&p.metrics.CDC,
	)

	// Create the WAL reader with the txbuffer as its message handler.
	reader := pgrepl.NewReader(p.readerCfg, buf, p.log, &p.metrics.PG)

	// Start checkpoint manager.
	cpCtx, cpCancel := context.WithCancel(ctx)
	defer cpCancel()
	cpErrCh := make(chan error, 1)
	go func() {
		cpErrCh <- p.cpMgr.Run(cpCtx)
	}()

	// Start WAL reader in a goroutine.
	readerErrCh := make(chan error, 1)
	go func() {
		p.health.SetSourceConnected(true)
		err := reader.Run(ctx)
		p.health.SetSourceConnected(false)
		readerErrCh <- err
	}()

	// Publish loop: process transactions sequentially, preserving order.
	for {
		select {
		case batch := <-p.txCh:
			if err := p.publishTx(ctx, batch); err != nil {
				cpCancel()
				return fmt.Errorf("pipeline: publish: %w", err)
			}
		case err := <-readerErrCh:
			cpCancel()
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("pipeline: reader: %w", err)
		case <-ctx.Done():
			p.drainTxCh(ctx)
			cpCancel()
			<-cpErrCh
			return ctx.Err()
		}
	}
}

// enqueueTx returns the onTx callback for the txbuffer. It copies events
// (since the buffer reuses its slice) and sends to txCh with backpressure.
func (p *Pipeline) enqueueTx(ctx context.Context) func(*model.TxBatch) error {
	return func(batch *model.TxBatch) error {
		events := make([]model.TxEvent, len(batch.Events))
		copy(events, batch.Events)
		copied := &model.TxBatch{
			XID:      batch.XID,
			CommitTS: batch.CommitTS,
			LSN:      batch.LSN,
			Events:   events,
			ByteSize: batch.ByteSize,
		}

		bpStart := time.Now()
		select {
		case p.txCh <- copied:
		case <-ctx.Done():
			return ctx.Err()
		}
		if elapsed := time.Since(bpStart); elapsed > 10*time.Millisecond {
			p.metrics.CDC.BackpressureSeconds.Add(elapsed.Seconds())
		}
		return nil
	}
}

// publishTx encodes and publishes all events in a transaction, then confirms
// the checkpoint. The LSN is only confirmed after all records are acknowledged.
func (p *Pipeline) publishTx(ctx context.Context, batch *model.TxBatch) error {
	records := make([]*kgo.Record, 0, len(batch.Events))

	for i := range batch.Events {
		ev := &batch.Events[i]
		schema, table, key, value, err := p.encoder.Encode(ev, false)
		if err != nil {
			return fmt.Errorf("encode event: %w", err)
		}

		topicName := p.resolver.Resolve(p.cfg.Database, schema, table)
		records = append(records, &kgo.Record{
			Topic: topicName,
			Key:   key,
			Value: value,
		})

		p.metrics.CDC.EventsTotal.WithLabelValues(string(ev.Change.Op)).Inc()
	}

	if err := p.producer.PublishBatch(ctx, records); err != nil {
		return err
	}

	// Only after all records are acked do we confirm the checkpoint.
	p.cpMgr.Confirm(batch.LSN)

	p.metrics.CDC.LastReadLSN.Set(float64(uint64(batch.LSN)))
	p.metrics.CDC.LastCheckpointLSN.Set(float64(uint64(batch.LSN)))
	p.health.SetLastReadLSN(batch.LSN)
	p.health.SetLastCheckpointLSN(batch.LSN)
	p.health.SetProducerHealthy(true)

	if lag := time.Since(batch.CommitTS).Seconds(); lag > 0 {
		p.metrics.CDC.CommitLagSeconds.Set(lag)
	}
	p.metrics.CDC.QueueDepth.Set(float64(len(p.txCh)))

	return nil
}

// drainTxCh processes remaining transactions in the channel during shutdown.
func (p *Pipeline) drainTxCh(ctx context.Context) {
	for {
		select {
		case batch := <-p.txCh:
			if err := p.publishTx(ctx, batch); err != nil {
				p.log.Warn().Err(err).Msg("error publishing during drain")
				return
			}
		default:
			return
		}
	}
}
