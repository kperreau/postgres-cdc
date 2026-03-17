// Package producer wraps the franz-go Kafka client for publishing CDC events
// to Redpanda with batching, compression, retries, and bounded in-flight records.
package producer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/kperreau/postgres-cdc/internal/metrics"
)

// Config holds producer settings.
type Config struct {
	Brokers        []string
	Compression    string
	Linger         time.Duration
	MaxInflight    int
	RequiredAcks   string
	BatchMaxBytes  int
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

// PublishRecord publishes a single record synchronously and returns after ack.
// It blocks until the record is acknowledged or the context is cancelled.
func (p *Producer) PublishRecord(ctx context.Context, topic string, key, value []byte) error {
	rec := &kgo.Record{
		Topic: topic,
		Key:   key,
		Value: value,
	}

	result := p.client.ProduceSync(ctx, rec)
	if err := result.FirstErr(); err != nil {
		p.metrics.PublishFailuresTotal.Inc()
		p.setHealthy(false)
		return fmt.Errorf("producer: publish to %s: %w", topic, err)
	}

	p.setHealthy(true)
	return nil
}

// PublishBatch publishes a slice of records synchronously. All records must be
// acknowledged before this returns. On partial failure, the first error is returned.
func (p *Producer) PublishBatch(ctx context.Context, records []*kgo.Record) error {
	results := p.client.ProduceSync(ctx, records...)
	for _, r := range results {
		if r.Err != nil {
			p.metrics.PublishFailuresTotal.Inc()
			p.setHealthy(false)
			return fmt.Errorf("producer: batch publish: %w", r.Err)
		}
	}
	p.setHealthy(true)
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
