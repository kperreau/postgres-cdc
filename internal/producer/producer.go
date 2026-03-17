// Package producer wraps the franz-go Kafka client for publishing CDC events
// to Redpanda with batching, compression, retries with exponential backoff,
// and bounded in-flight records.
package producer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/kperreau/postgres-cdc/internal/metrics"
)

// Config holds producer settings.
type Config struct {
	Brokers         []string
	Compression     string
	Linger          time.Duration
	MaxInflight     int
	RequiredAcks    string
	BatchMaxBytes   int
	BatchMaxRecords int
}

// Producer wraps a franz-go client for CDC event publishing.
type Producer struct {
	client  *kgo.Client
	log     zerolog.Logger
	metrics *metrics.CDCMetrics

	mu      sync.Mutex
	healthy bool
}

// New creates and returns a connected Producer.
func New(ctx context.Context, cfg Config, log zerolog.Logger, m *metrics.CDCMetrics) (*Producer, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ProducerLinger(cfg.Linger),
		kgo.MaxBufferedRecords(cfg.MaxInflight),
		kgo.ProducerBatchMaxBytes(int32(cfg.BatchMaxBytes)),
		kgo.MaxProduceRequestsInflightPerBroker(cfg.MaxInflight),
	}

	// Compression.
	switch cfg.Compression {
	case "snappy":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.SnappyCompression()))
	case "lz4":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.Lz4Compression()))
	case "zstd":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.ZstdCompression()))
	case "gzip":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.GzipCompression()))
	case "none", "":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.NoCompression()))
	default:
		return nil, fmt.Errorf("producer: unsupported compression %q", cfg.Compression)
	}

	// Required acks.
	switch cfg.RequiredAcks {
	case "all":
		opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
	case "leader":
		opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()))
	case "none":
		opts = append(opts, kgo.RequiredAcks(kgo.NoAck()))
	default:
		return nil, fmt.Errorf("producer: unsupported required_acks %q", cfg.RequiredAcks)
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("producer: create client: %w", err)
	}

	// Verify connectivity.
	if err := client.Ping(ctx); err != nil {
		client.Close()
		return nil, fmt.Errorf("producer: ping brokers: %w", err)
	}

	p := &Producer{
		client:  client,
		log:     log.With().Str("component", "producer").Logger(),
		metrics: m,
		healthy: true,
	}
	return p, nil
}

// PublishRecord publishes a single record synchronously with retry on transient
// failures. It blocks until the record is acknowledged or the context is cancelled.
func (p *Producer) PublishRecord(ctx context.Context, topic string, key, value []byte) error {
	rec := &kgo.Record{
		Topic: topic,
		Key:   key,
		Value: value,
	}
	return p.publishWithRetry(ctx, []*kgo.Record{rec})
}

// PublishBatch publishes a slice of records synchronously with retry on transient
// failures. All records must be acknowledged before this returns.
func (p *Producer) PublishBatch(ctx context.Context, records []*kgo.Record) error {
	return p.publishWithRetry(ctx, records)
}

// publishWithRetry wraps the publish call with exponential backoff. On transient
// broker failures, it retries the entire batch. On context cancellation, it returns
// immediately.
func (p *Producer) publishWithRetry(ctx context.Context, records []*kgo.Record) error {
	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = 200 * time.Millisecond
	bo.MaxInterval = 10 * time.Second

	_, err := backoff.Retry(ctx, func() (struct{}, error) {
		results := p.client.ProduceSync(ctx, records...)
		for _, r := range results {
			if r.Err != nil {
				p.metrics.PublishRetriesTotal.Inc()
				p.setHealthy(false)
				p.log.Warn().Err(r.Err).Msg("publish failed; retrying")
				return struct{}{}, r.Err
			}
		}
		p.setHealthy(true)
		return struct{}{}, nil
	},
		backoff.WithBackOff(bo),
		backoff.WithMaxElapsedTime(0), // retry indefinitely until ctx cancelled
	)

	if err != nil {
		p.metrics.PublishFailuresTotal.Inc()
		return fmt.Errorf("producer: publish: %w", err)
	}
	return nil
}

// Flush blocks until all buffered records are flushed or the context expires.
func (p *Producer) Flush(ctx context.Context) error {
	return p.client.Flush(ctx)
}

// Close flushes and closes the producer.
func (p *Producer) Close() {
	p.client.Close()
}

// IsHealthy returns whether the last publish succeeded.
func (p *Producer) IsHealthy() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.healthy
}

func (p *Producer) setHealthy(v bool) {
	p.mu.Lock()
	p.healthy = v
	p.mu.Unlock()
}
