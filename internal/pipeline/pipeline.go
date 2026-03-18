// Package pipeline wires the CDC stages together: WAL reader → txbuffer →
// encode → publish → checkpoint. All channels are bounded, backpressure
// propagates upstream, and transaction ordering is preserved.
//
// When CheckpointLimit > 1, batches are published concurrently up to the
// configured limit. The checkpoint LSN only advances when all prior batches
// in the ordered sequence are fully acknowledged (contiguous ack window),
// preserving at-least-once semantics without unbounded memory growth.
package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/goccy/go-json"
	"github.com/jackc/pglogrepl"
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
	QueueCapacity   int
	MaxTxBytes      int
	CheckpointLimit int
	SourceName      string
	Database        string

	// Heartbeat settings. HeartbeatInterval <= 0 disables heartbeat emission.
	HeartbeatInterval time.Duration
	HeartbeatTopic    string
}

// Pipeline coordinates the CDC stages.
type Pipeline struct {
	cfg         Config
	log         zerolog.Logger
	metrics     *metrics.Metrics
	health      *health.Status
	producer    *producer.Producer
	cpMgr       *checkpoint.Manager
	resolver    *topic.Resolver
	encoderPool sync.Pool
	readerCfg   pgrepl.ReaderConfig

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
	if cfg.CheckpointLimit <= 0 {
		cfg.CheckpointLimit = 1
	}

	p := &Pipeline{
		cfg:       cfg,
		log:       log.With().Str("component", "pipeline").Logger(),
		metrics:   m,
		health:    healthStatus,
		producer:  prod,
		cpMgr:     cpMgr,
		resolver:  resolver,
		readerCfg: readerCfg,
		txCh:      make(chan *model.TxBatch, cfg.QueueCapacity),
	}
	p.encoderPool = sync.Pool{
		New: func() any {
			return encoder.New(encoder.Config{
				SourceName: cfg.SourceName,
				Database:   cfg.Database,
			})
		},
	}
	return p
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

	// Start heartbeat emitter.
	if p.cfg.HeartbeatInterval > 0 && p.cfg.HeartbeatTopic != "" {
		go p.heartbeatLoop(ctx)
	}

	// Publish loop: sequential or async depending on checkpoint_limit.
	var publishErr error
	if p.cfg.CheckpointLimit > 1 {
		publishErr = p.asyncPublishLoop(ctx, readerErrCh)
	} else {
		publishErr = p.sequentialPublishLoop(ctx, readerErrCh)
	}

	cpCancel()
	<-cpErrCh
	return publishErr
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

// ---------- sequential publish loop (checkpoint_limit = 1) ----------

// sequentialPublishLoop processes transactions one at a time, preserving order.
func (p *Pipeline) sequentialPublishLoop(ctx context.Context, readerErrCh <-chan error) error {
	for {
		select {
		case batch := <-p.txCh:
			if err := p.publishAndConfirm(ctx, batch); err != nil {
				return fmt.Errorf("pipeline: publish: %w", err)
			}
		case err := <-readerErrCh:
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("pipeline: reader: %w", err)
		case <-ctx.Done():
			p.drainTxCh(ctx)
			return ctx.Err()
		}
	}
}

// ---------- async publish loop (checkpoint_limit > 1) ----------

// inflight tracks a single in-flight transaction batch.
type inflight struct {
	lsn  pglogrepl.LSN
	ts   time.Time
	done chan error
}

// asyncPublishLoop publishes up to CheckpointLimit batches concurrently.
// Checkpoint advancement is contiguous: the advancer goroutine waits for
// completions in order and only confirms each LSN after all prior LSNs
// have been confirmed.
func (p *Pipeline) asyncPublishLoop(ctx context.Context, readerErrCh <-chan error) error {
	limit := p.cfg.CheckpointLimit
	sem := make(chan struct{}, limit)
	inflightCh := make(chan inflight, limit)

	// Advancer goroutine: processes completions in order, advances checkpoint.
	advancerDone := make(chan error, 1)
	go func() {
		for inf := range inflightCh {
			if err := <-inf.done; err != nil {
				advancerDone <- err
				//nolint:revive // drain remaining entries so the channel can be GC'd.
				for range inflightCh {
				}
				return
			}
			p.confirmCheckpoint(inf.lsn, inf.ts)
			p.metrics.CDC.InflightBatches.Dec()
		}
		advancerDone <- nil
	}()

	publishCtx, publishCancel := context.WithCancel(ctx)
	defer publishCancel()

	var loopErr error
loop:
	for {
		select {
		case batch := <-p.txCh:
			// Acquire semaphore slot (backpressure when window is full).
			select {
			case sem <- struct{}{}:
			case <-publishCtx.Done():
				loopErr = publishCtx.Err()
				break loop
			}

			inf := inflight{
				lsn:  batch.LSN,
				ts:   batch.CommitTS,
				done: make(chan error, 1),
			}
			inflightCh <- inf
			p.metrics.CDC.InflightBatches.Inc()

			go func(b *model.TxBatch, i inflight) {
				defer func() { <-sem }()
				i.done <- p.publishBatch(publishCtx, b)
			}(batch, inf)

		case err := <-readerErrCh:
			if ctx.Err() != nil {
				loopErr = ctx.Err()
			} else {
				loopErr = fmt.Errorf("pipeline: reader: %w", err)
			}
			break loop

		case err := <-advancerDone:
			loopErr = fmt.Errorf("pipeline: inflight: %w", err)
			break loop

		case <-ctx.Done():
			loopErr = ctx.Err()
			break loop
		}
	}

	// Shutdown: cancel all in-flight publishes.
	publishCancel()
	close(inflightCh)

	// Wait for all in-flight goroutines to release their semaphore slots.
	for range limit {
		sem <- struct{}{}
	}

	// Wait for advancer to finish processing.
	<-advancerDone

	return loopErr
}

// ---------- shared publish helpers ----------

// publishAndConfirm encodes, publishes, and confirms the checkpoint (sequential mode).
func (p *Pipeline) publishAndConfirm(ctx context.Context, batch *model.TxBatch) error {
	if err := p.publishBatch(ctx, batch); err != nil {
		return err
	}
	p.confirmCheckpoint(batch.LSN, batch.CommitTS)
	return nil
}

// publishBatch encodes and publishes all events in a transaction batch.
// It does NOT confirm the checkpoint — the caller is responsible for that.
func (p *Pipeline) publishBatch(ctx context.Context, batch *model.TxBatch) error {
	enc := p.encoderPool.Get().(*encoder.Encoder)
	defer p.encoderPool.Put(enc)

	records := make([]*kgo.Record, 0, len(batch.Events))

	for i := range batch.Events {
		ev := &batch.Events[i]
		schema, table, key, value, err := enc.Encode(ev, false)
		if err != nil {
			return fmt.Errorf("encode event: %w", err)
		}

		topicName := p.resolver.Resolve(p.cfg.Database, schema, table)

		if err := p.producer.EnsureTopic(ctx, topicName); err != nil {
			return fmt.Errorf("ensure topic: %w", err)
		}

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

	// Update read-position metrics (these are safe from concurrent goroutines
	// because Prometheus operations and health.Status are thread-safe).
	p.metrics.CDC.LastReadLSN.Set(float64(uint64(batch.LSN)))
	p.health.SetLastReadLSN(batch.LSN)
	p.health.SetProducerHealthy(true)
	p.metrics.CDC.QueueDepth.Set(float64(len(p.txCh)))

	return nil
}

// confirmCheckpoint marks an LSN as safe and updates checkpoint metrics/health.
func (p *Pipeline) confirmCheckpoint(lsn pglogrepl.LSN, commitTS time.Time) {
	p.cpMgr.Confirm(lsn)
	p.metrics.CDC.LastCheckpointLSN.Set(float64(uint64(lsn)))
	p.health.SetLastCheckpointLSN(lsn)

	if lag := time.Since(commitTS).Seconds(); lag > 0 {
		p.metrics.CDC.CommitLagSeconds.Set(lag)
	}
}

// ---------- heartbeat ----------

// heartbeatRecord is the JSON structure emitted to the heartbeat topic.
type heartbeatRecord struct {
	Source    string `json:"source"`
	Database  string `json:"database"`
	Timestamp string `json:"timestamp"`
	LSN       string `json:"lsn"`
}

// heartbeatLoop periodically emits a heartbeat record to the heartbeat topic.
// It runs until ctx is cancelled and logs errors without stopping the pipeline.
func (p *Pipeline) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(p.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := p.emitHeartbeat(ctx); err != nil {
				p.log.Warn().Err(err).Msg("heartbeat emit failed")
			}
		}
	}
}

// emitHeartbeat publishes a single heartbeat record.
func (p *Pipeline) emitHeartbeat(ctx context.Context) error {
	hb := heartbeatRecord{
		Source:    p.cfg.SourceName,
		Database:  p.cfg.Database,
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		LSN:       p.cpMgr.LastFlushed().String(),
	}
	value, err := json.Marshal(&hb)
	if err != nil {
		return fmt.Errorf("marshal heartbeat: %w", err)
	}

	topic := p.cfg.HeartbeatTopic
	if err := p.producer.EnsureTopic(ctx, topic); err != nil {
		return fmt.Errorf("ensure heartbeat topic: %w", err)
	}

	if err := p.producer.PublishRecord(ctx, topic, nil, value); err != nil {
		return fmt.Errorf("publish heartbeat: %w", err)
	}

	p.metrics.CDC.HeartbeatsTotal.Inc()
	return nil
}

// ---------- drain ----------

// drainTxCh processes remaining transactions in the channel during shutdown.
func (p *Pipeline) drainTxCh(ctx context.Context) {
	for {
		select {
		case batch := <-p.txCh:
			if err := p.publishAndConfirm(ctx, batch); err != nil {
				p.log.Warn().Err(err).Msg("error publishing during drain")
				return
			}
		default:
			return
		}
	}
}
