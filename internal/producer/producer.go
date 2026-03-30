// Package producer wraps the franz-go Kafka client for publishing CDC events
// to Redpanda with batching, compression, retries with exponential backoff,
// and bounded in-flight records.
package producer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/kperreau/postgres-cdc/internal/metrics"
)

// Config holds producer settings.
type Config struct {
	Brokers     []string
	Compression string
	Linger      time.Duration
	// MaxInflight is the max produce requests in flight per broker when
	// EnableIdempotence is false. With idempotence (franz-go default), the
	// broker limit is fixed at 1; this value still sets MaxBufferedRecords.
	MaxInflight     int
	RequiredAcks    string
	BatchMaxBytes   int
	BatchMaxRecords int

	// TopicAutoCreate controls automatic topic creation via the Kafka Admin API.
	// When Partitions > 0, EnsureTopic will create topics that don't exist yet.
	// ReplicationFactor defaults to 1 when unset.
	// EnableIdempotence enables the Kafka idempotent producer (EOS stage 1).
	// When true, the broker deduplicates retries, preventing duplicate records
	// on transient failures. Requires acks=all. Enabled by default.
	EnableIdempotence bool

	TopicPartitions        int32
	TopicReplicationFactor int16
}

// Producer wraps a franz-go client for CDC event publishing.
type Producer struct {
	client  *kgo.Client
	admin   *kadm.Client
	cfg     Config
	log     zerolog.Logger
	metrics *metrics.CDCMetrics

	mu      sync.Mutex
	healthy bool

	// ensured tracks topic names that have already been created/verified,
	// avoiding repeated admin RPCs for the same topic.
	ensured sync.Map
}

// New creates and returns a connected Producer.
func New(ctx context.Context, cfg Config, log zerolog.Logger, m *metrics.CDCMetrics) (*Producer, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ProducerLinger(cfg.Linger),
		kgo.MaxBufferedRecords(cfg.MaxInflight),
		kgo.ProducerBatchMaxBytes(int32(cfg.BatchMaxBytes)),
	}
	// Idempotent producer requires max in-flight produce requests per broker == 1
	// (franz-go default). MaxProduceRequestsInflightPerBroker may only be set when
	// idempotency is disabled.
	if !cfg.EnableIdempotence {
		opts = append(opts, kgo.MaxProduceRequestsInflightPerBroker(cfg.MaxInflight))
		opts = append(opts, kgo.DisableIdempotentWrite())
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
		admin:   kadm.NewClient(client),
		cfg:     cfg,
		log:     log.With().Str("component", "producer").Logger(),
		metrics: m,
		healthy: true,
	}
	return p, nil
}

// EnsureTopic creates the topic if it does not already exist. It is a no-op
// when TopicPartitions is 0 (auto-create disabled) or after the first
// successful call for a given topic name (result is cached in-process).
func (p *Producer) EnsureTopic(ctx context.Context, topic string) error {
	if p.cfg.TopicPartitions == 0 {
		return nil
	}
	if _, ok := p.ensured.Load(topic); ok {
		return nil
	}

	replFactor := p.cfg.TopicReplicationFactor
	if replFactor == 0 {
		replFactor = 1
	}

	resp, err := p.admin.CreateTopics(ctx, p.cfg.TopicPartitions, replFactor, nil, topic)
	if err != nil {
		return fmt.Errorf("ensure topic %q: %w", topic, err)
	}
	if tr, ok := resp[topic]; ok {
		if tr.Err != nil && !errors.Is(tr.Err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("ensure topic %q: %w", topic, tr.Err)
		}
	}

	p.ensured.Store(topic, struct{}{})
	p.log.Debug().Str("topic", topic).Msg("topic ensured")
	return nil
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
