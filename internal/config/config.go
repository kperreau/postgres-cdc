// Package config loads, validates, and exposes the application configuration.
// It uses koanf to merge YAML files, environment variables, and defaults.
package config

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
	"github.com/kperreau/postgres-cdc/internal/model"
)

// Config is the root configuration struct.
type Config struct {
	Postgres    PostgresConfig    `koanf:"postgres"`
	Replication ReplicationConfig `koanf:"replication"`
	Snapshot    SnapshotConfig    `koanf:"snapshot"`
	Redpanda    RedpandaConfig    `koanf:"redpanda"`
	Topic       TopicConfig       `koanf:"topic"`
	Checkpoint  CheckpointConfig  `koanf:"checkpoint"`
	Runtime     RuntimeConfig     `koanf:"runtime"`
	Logging     LoggingConfig     `koanf:"logging"`
	Metrics     MetricsConfig     `koanf:"metrics"`
	Health      HealthConfig      `koanf:"health"`
	Tuning      TuningConfig      `koanf:"tuning"`
}

// PostgresConfig holds PostgreSQL connection parameters.
type PostgresConfig struct {
	DSN      string `koanf:"dsn"`
	Host     string `koanf:"host"`
	Port     int    `koanf:"port"`
	User     string `koanf:"user"`
	Password string `koanf:"password"`
	DBName   string `koanf:"dbname"`
}

// ConnString returns a connection string, preferring DSN if set.
func (p *PostgresConfig) ConnString() string {
	if p.DSN != "" {
		return p.DSN
	}
	host := p.Host
	if host == "" {
		host = "localhost"
	}
	port := p.Port
	if port == 0 {
		port = 5432
	}
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s",
		host, port, p.User, p.Password, p.DBName)
}

// ReplicationConfig holds logical replication settings.
type ReplicationConfig struct {
	SlotName            string        `koanf:"slot_name"`
	PublicationName     string        `koanf:"publication_name"`
	CreateSlotIfMissing bool          `koanf:"create_slot_if_missing"`
	StatusInterval      time.Duration `koanf:"status_interval"`
}

// SnapshotMode controls initial snapshot behavior.
type SnapshotMode string

const (
	SnapshotNever   SnapshotMode = "never"
	SnapshotInitial SnapshotMode = "initial"
)

// SnapshotConfig holds snapshot settings.
type SnapshotConfig struct {
	Mode      SnapshotMode `koanf:"mode"`
	FetchSize int          `koanf:"fetch_size"`
	Tables    []string     `koanf:"tables"`
}

// RedpandaConfig holds Redpanda/Kafka producer settings.
type RedpandaConfig struct {
	Brokers      []string      `koanf:"brokers"`
	Compression  string        `koanf:"compression"`
	Linger       time.Duration `koanf:"linger"`
	MaxInflight  int           `koanf:"max_inflight"`
	RequiredAcks string        `koanf:"required_acks"`
}

// TopicConfig holds topic routing settings.
type TopicConfig struct {
	Mode            model.TopicMode `koanf:"mode"`
	Prefix          string          `koanf:"prefix"`
	SingleTopicName string          `koanf:"single_topic_name"`
}

// CheckpointConfig holds checkpoint persistence settings.
type CheckpointConfig struct {
	Backend       string        `koanf:"backend"`
	FilePath      string        `koanf:"file_path"`
	FlushInterval time.Duration `koanf:"flush_interval"`
}

// RuntimeConfig holds pipeline runtime settings.
type RuntimeConfig struct {
	QueueCapacity   int           `koanf:"queue_capacity"`
	MaxTxBytes      int           `koanf:"max_tx_bytes"`
	ShutdownTimeout time.Duration `koanf:"shutdown_timeout"`
}

// LoggingConfig holds structured logging settings.
type LoggingConfig struct {
	Level string `koanf:"level"`
}

// MetricsConfig holds Prometheus metrics settings.
type MetricsConfig struct {
	Enabled    bool   `koanf:"enabled"`
	ListenAddr string `koanf:"listen_addr"`
	Path       string `koanf:"path"`
	Namespace  string `koanf:"namespace"`
	Subsystem  string `koanf:"subsystem"`
}

// HealthConfig holds health endpoint settings.
type HealthConfig struct {
	ListenAddr string `koanf:"listen_addr"`
}

// TuningConfig holds producer tuning knobs.
type TuningConfig struct {
	ProducerBatchMaxBytes   int `koanf:"producer_batch_max_bytes"`
	ProducerBatchMaxRecords int `koanf:"producer_batch_max_records"`
}

// Load reads configuration from a YAML file and environment variables.
// Environment variables are prefixed with CDC_ and use __ as separator.
func Load(path string) (*Config, error) {
	k := koanf.New(".")

	// Load YAML file if provided.
	if path != "" {
		if err := k.Load(file.Provider(path), yaml.Parser()); err != nil {
			return nil, fmt.Errorf("load config file: %w", err)
		}
	}

	// Overlay environment variables: CDC__POSTGRES__HOST -> postgres.host
	if err := k.Load(env.Provider("CDC_", ".", func(s string) string {
		s = strings.TrimPrefix(s, "CDC_")
		s = strings.ToLower(s)
		s = strings.ReplaceAll(s, "__", ".")
		return s
	}), nil); err != nil {
		return nil, fmt.Errorf("load env config: %w", err)
	}

	cfg := DefaultConfig()
	if err := k.Unmarshal("", cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}
	return cfg, nil
}

// DefaultConfig returns a Config populated with safe defaults.
func DefaultConfig() *Config {
	return &Config{
		Postgres: PostgresConfig{
			Host: "localhost",
			Port: 5432,
		},
		Replication: ReplicationConfig{
			SlotName:            "cdc_slot",
			PublicationName:     "cdc_pub",
			CreateSlotIfMissing: true,
			StatusInterval:      10 * time.Second,
		},
		Snapshot: SnapshotConfig{
			Mode:      SnapshotNever,
			FetchSize: 10000,
		},
		Redpanda: RedpandaConfig{
			Brokers:      []string{"localhost:9092"},
			Compression:  "snappy",
			Linger:       5 * time.Millisecond,
			MaxInflight:  5,
			RequiredAcks: "all",
		},
		Topic: TopicConfig{
			Mode:   model.TopicPerTable,
			Prefix: "cdc",
		},
		Checkpoint: CheckpointConfig{
			Backend:       "file",
			FilePath:      "/var/lib/cdc/checkpoint.json",
			FlushInterval: 5 * time.Second,
		},
		Runtime: RuntimeConfig{
			QueueCapacity:   4096,
			MaxTxBytes:      64 * 1024 * 1024, // 64 MiB
			ShutdownTimeout: 30 * time.Second,
		},
		Logging: LoggingConfig{
			Level: "info",
		},
		Metrics: MetricsConfig{
			Enabled:    true,
			ListenAddr: ":9090",
			Path:       "/metrics",
			Namespace:  "cdc",
			Subsystem:  "",
		},
		Health: HealthConfig{
			ListenAddr: ":8080",
		},
		Tuning: TuningConfig{
			ProducerBatchMaxBytes:   1048576, // 1 MiB
			ProducerBatchMaxRecords: 1000,
		},
	}
}

// Validate checks invariants and rejects unsafe or inconsistent configurations.
func (c *Config) Validate() error {
	var errs []error

	// Postgres
	if c.Postgres.DSN == "" && c.Postgres.User == "" {
		errs = append(errs, errors.New("postgres: either dsn or user must be set"))
	}

	// Replication
	if c.Replication.SlotName == "" {
		errs = append(errs, errors.New("replication: slot_name is required"))
	}
	if c.Replication.PublicationName == "" {
		errs = append(errs, errors.New("replication: publication_name is required"))
	}
	if c.Replication.StatusInterval < time.Second {
		errs = append(errs, errors.New("replication: status_interval must be >= 1s"))
	}

	// Snapshot
	switch c.Snapshot.Mode {
	case SnapshotNever, SnapshotInitial:
	default:
		errs = append(errs, fmt.Errorf("snapshot: invalid mode %q (want never|initial)", c.Snapshot.Mode))
	}
	if c.Snapshot.FetchSize <= 0 {
		errs = append(errs, errors.New("snapshot: fetch_size must be > 0"))
	}

	// Redpanda
	if len(c.Redpanda.Brokers) == 0 {
		errs = append(errs, errors.New("redpanda: at least one broker is required"))
	}
	switch c.Redpanda.RequiredAcks {
	case "all", "none", "leader":
	default:
		errs = append(errs, fmt.Errorf("redpanda: invalid required_acks %q (want all|none|leader)", c.Redpanda.RequiredAcks))
	}
	if c.Redpanda.MaxInflight <= 0 {
		errs = append(errs, errors.New("redpanda: max_inflight must be > 0"))
	}

	// Topic
	switch c.Topic.Mode {
	case model.TopicPerTable:
		if c.Topic.Prefix == "" {
			errs = append(errs, errors.New("topic: prefix is required for per_table mode"))
		}
	case model.TopicSingle:
		if c.Topic.SingleTopicName == "" {
			errs = append(errs, errors.New("topic: single_topic_name is required for single mode"))
		}
	default:
		errs = append(errs, fmt.Errorf("topic: invalid mode %q (want per_table|single)", c.Topic.Mode))
	}

	// Checkpoint
	if c.Checkpoint.Backend != "file" {
		errs = append(errs, fmt.Errorf("checkpoint: unsupported backend %q (only file is supported)", c.Checkpoint.Backend))
	}
	if c.Checkpoint.FilePath == "" {
		errs = append(errs, errors.New("checkpoint: file_path is required"))
	}
	if c.Checkpoint.FlushInterval <= 0 {
		errs = append(errs, errors.New("checkpoint: flush_interval must be > 0"))
	}

	// Runtime
	if c.Runtime.QueueCapacity <= 0 {
		errs = append(errs, errors.New("runtime: queue_capacity must be > 0"))
	}
	if c.Runtime.MaxTxBytes <= 0 {
		errs = append(errs, errors.New("runtime: max_tx_bytes must be > 0"))
	}
	if c.Runtime.ShutdownTimeout <= 0 {
		errs = append(errs, errors.New("runtime: shutdown_timeout must be > 0"))
	}

	// Metrics
	if c.Metrics.Enabled && c.Metrics.ListenAddr == "" {
		errs = append(errs, errors.New("metrics: listen_addr is required when metrics are enabled"))
	}

	// Health
	if c.Health.ListenAddr == "" {
		errs = append(errs, errors.New("health: listen_addr is required"))
	}

	return errors.Join(errs...)
}
