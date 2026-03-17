// Command cdc is the PostgreSQL CDC -> Redpanda service entrypoint.
// It bootstraps configuration, logging, metrics, health probes, and the
// replication pipeline, then blocks until a termination signal is received.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"

	"github.com/kperreau/postgres-cdc/internal/checkpoint"
	"github.com/kperreau/postgres-cdc/internal/config"
	"github.com/kperreau/postgres-cdc/internal/health"
	"github.com/kperreau/postgres-cdc/internal/metrics"
	"github.com/kperreau/postgres-cdc/internal/pgrepl"
	"github.com/kperreau/postgres-cdc/internal/pipeline"
	"github.com/kperreau/postgres-cdc/internal/producer"
	"github.com/kperreau/postgres-cdc/internal/snapshot"
	"github.com/kperreau/postgres-cdc/internal/topic"
)

func main() {
	os.Exit(run())
}

func run() int {
	configPath := flag.String("config", "config.yaml", "path to configuration file")
	flag.Parse()

	// --- Config ---
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config: %v\n", err)
		return 1
	}

	// --- Logger ---
	level, err := zerolog.ParseLevel(cfg.Logging.Level)
	if err != nil {
		level = zerolog.InfoLevel
	}
	log := zerolog.New(os.Stdout).With().Timestamp().Logger().Level(level)
	log.Info().Str("config", *configPath).Msg("starting CDC service")

	// --- Metrics ---
	m, metricsHandler := metrics.New(cfg.Metrics.Namespace)

	// --- Health ---
	healthStatus := health.NewStatus()

	// --- Checkpoint ---
	cpStore, err := checkpoint.NewFileStore(cfg.Checkpoint.FilePath)
	if err != nil {
		log.Error().Err(err).Msg("failed to create checkpoint store")
		return 1
	}
	cpMgr := checkpoint.NewManager(cpStore, cfg.Checkpoint.FlushInterval, log, &m.CDC)

	startLSN, err := cpMgr.LoadInitial()
	if err != nil {
		log.Error().Err(err).Msg("failed to load checkpoint")
		return 1
	}

	// --- Topic resolver ---
	resolver, err := topic.NewResolver(cfg.Topic.Mode, cfg.Topic.Prefix, cfg.Topic.SingleTopicName)
	if err != nil {
		log.Error().Err(err).Msg("failed to create topic resolver")
		return 1
	}

	// --- Context + signal handling ---
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Info().Str("signal", sig.String()).Msg("shutdown requested")
		cancel()
	}()

	// --- Redpanda producer ---
	prod, err := producer.New(ctx, producer.Config{
		Brokers:                cfg.Redpanda.Brokers,
		Compression:            cfg.Redpanda.Compression,
		Linger:                 cfg.Redpanda.Linger,
		MaxInflight:            cfg.Redpanda.MaxInflight,
		RequiredAcks:           cfg.Redpanda.RequiredAcks,
		BatchMaxBytes:          cfg.Tuning.ProducerBatchMaxBytes,
		BatchMaxRecords:        cfg.Tuning.ProducerBatchMaxRecords,
		TopicPartitions:        cfg.Redpanda.TopicAutoCreate.Partitions,
		TopicReplicationFactor: cfg.Redpanda.TopicAutoCreate.ReplicationFactor,
	}, log, &m.CDC)
	if err != nil {
		log.Error().Err(err).Msg("failed to create producer")
		return 1
	}
	defer prod.Close()
	healthStatus.SetProducerHealthy(true)

	// --- Snapshot (if configured) ---
	if cfg.Snapshot.Mode == config.SnapshotInitial && len(cfg.Snapshot.Tables) > 0 {
		if err := runSnapshot(ctx, cfg, prod, resolver, log, m); err != nil {
			log.Error().Err(err).Msg("snapshot failed")
			return 1
		}
	}

	// --- HTTP servers ---
	healthSrv := &http.Server{Addr: cfg.Health.ListenAddr, Handler: healthStatus.Handler()}
	go func() {
		log.Info().Str("addr", cfg.Health.ListenAddr).Msg("health server listening")
		if err := healthSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("health server error")
		}
	}()

	var metricsSrv *http.Server
	if cfg.Metrics.Enabled {
		mux := http.NewServeMux()
		mux.Handle(cfg.Metrics.Path, metricsHandler)
		metricsSrv = &http.Server{Addr: cfg.Metrics.ListenAddr, Handler: mux}
		go func() {
			log.Info().Str("addr", cfg.Metrics.ListenAddr).Str("path", cfg.Metrics.Path).Msg("metrics server listening")
			if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Error().Err(err).Msg("metrics server error")
			}
		}()
	}

	// --- Pipeline ---
	readerCfg := pgrepl.ReaderConfig{
		ConnString:      cfg.Postgres.ConnString(),
		SlotName:        cfg.Replication.SlotName,
		PublicationName: cfg.Replication.PublicationName,
		CreateSlot:      cfg.Replication.CreateSlotIfMissing,
		StatusInterval:  cfg.Replication.StatusInterval,
		StartLSN:        startLSN,
	}

	pipe := pipeline.New(
		pipeline.Config{
			QueueCapacity: cfg.Runtime.QueueCapacity,
			MaxTxBytes:    cfg.Runtime.MaxTxBytes,
			SourceName:    cfg.Metrics.Namespace,
			Database:      cfg.Postgres.DBName,
		},
		readerCfg, prod, cpMgr, resolver, healthStatus, m, log,
	)

	log.Info().Msg("starting CDC pipeline")
	if err := pipe.Run(ctx); err != nil && ctx.Err() == nil {
		log.Error().Err(err).Msg("pipeline error")
		cancel()
	}

	// --- Graceful shutdown ---
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.Runtime.ShutdownTimeout)
	defer shutdownCancel()

	if err := prod.Flush(shutdownCtx); err != nil {
		log.Warn().Err(err).Msg("producer flush error during shutdown")
	}
	if err := cpMgr.Flush(); err != nil {
		log.Warn().Err(err).Msg("checkpoint flush error during shutdown")
	}
	if metricsSrv != nil {
		_ = metricsSrv.Shutdown(shutdownCtx)
	}
	_ = healthSrv.Shutdown(shutdownCtx)

	log.Info().Msg("CDC service stopped")
	return 0
}

func runSnapshot(ctx context.Context, cfg *config.Config, prod *producer.Producer, resolver *topic.Resolver, log zerolog.Logger, m *metrics.Metrics) error {
	pool, err := pgxpool.New(ctx, cfg.Postgres.ConnString())
	if err != nil {
		return fmt.Errorf("snapshot pool: %w", err)
	}
	defer pool.Close()

	runner := snapshot.NewRunner(
		snapshot.Config{
			Tables:    cfg.Snapshot.Tables,
			FetchSize: cfg.Snapshot.FetchSize,
			Database:  cfg.Postgres.DBName,
			Source:    cfg.Metrics.Namespace,
		},
		pool, prod, resolver, log, &m.Snapshot,
	)

	log.Info().Strs("tables", cfg.Snapshot.Tables).Msg("running initial snapshot")
	return runner.Run(ctx)
}
