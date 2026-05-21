package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"emeland.io/modelsrv-oci-registry-sensor/internal/config"
	"emeland.io/modelsrv-oci-registry-sensor/internal/sensor"
)

func main() {
	configPath := flag.String("config", "config/sensor.yaml", "Path to YAML config")
	flag.Parse()

	log := zap.Must(zap.NewDevelopmentConfig().Build(zap.AddStacktrace(zap.ErrorLevel)))
	slog := log.Sugar()
	defer func() { _ = log.Sync() }()

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Fatalw("failed to load config", "path", *configPath, "error", err)
	}

	srv, err := sensor.New(cfg, slog)
	if err != nil {
		slog.Fatalw("failed to create sensor", "error", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := srv.Run(ctx); err != nil {
		slog.Fatalw("sensor exited with error", "error", err)
	}
}
