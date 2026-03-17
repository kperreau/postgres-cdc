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
	"time"

	"github.com/rs/zerolog"

	"github.com/kperreau/postgres-cdc/internal/config"
	"github.com/kperreau/postgres-cdc/internal/health"
	"github.com/kperreau/postgres-cdc/internal/metrics"
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
	var metricsHandler http.Handler
	if cfg.Metrics.Enabled {
		_, metricsHandler = metrics.New(cfg.Metrics.Namespace)
	}

	// --- Health ---
	healthStatus := health.NewStatus()

	// --- HTTP servers ---
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Health server.
	healthSrv := &http.Server{
		Addr:    cfg.Health.ListenAddr,
		Handler: healthStatus.Handler(),
	}
	go func() {
		log.Info().Str("addr", cfg.Health.ListenAddr).Msg("health server listening")
		if err := healthSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("health server error")
		}
	}()

	// Metrics server (may share port with health or be separate).
	var metricsSrv *http.Server
	if cfg.Metrics.Enabled {
		mux := http.NewServeMux()
		mux.Handle(cfg.Metrics.Path, metricsHandler)
		metricsSrv = &http.Server{
			Addr:    cfg.Metrics.ListenAddr,
			Handler: mux,
		}
		go func() {
			log.Info().Str("addr", cfg.Metrics.ListenAddr).Str("path", cfg.Metrics.Path).Msg("metrics server listening")
			if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Error().Err(err).Msg("metrics server error")
			}
		}()
	}

	// --- Signal handling ---
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// TODO: start pipeline (pgrepl -> txbuffer -> encoder -> producer -> checkpoint)
	_ = ctx
	_ = healthStatus
	log.Info().Msg("pipeline not yet wired; waiting for signal")

	sig := <-sigCh
	log.Info().Str("signal", sig.String()).Msg("shutdown requested")
	cancel()

	// --- Graceful shutdown ---
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.Runtime.ShutdownTimeout)
	defer shutdownCancel()

	if metricsSrv != nil {
		_ = metricsSrv.Shutdown(shutdownCtx)
	}
	_ = healthSrv.Shutdown(shutdownCtx)

	// Allow a beat for flush.
	time.Sleep(200 * time.Millisecond)
	log.Info().Msg("CDC service stopped")
	return 0
}
