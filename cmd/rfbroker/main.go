// rfbroker is the RelayForge broker server.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"relayforge/internal/broker"
	"relayforge/internal/version"
)

func main() {
	addr := flag.String("addr", ":7000", "gRPC listen address")
	dataDir := flag.String("data-dir", "data", "broker data directory")
	maxRecordSize := flag.Int("max-record-size", 0, "max full record size in bytes (0 = engine default)")
	logLevel := flag.String("log-level", "info", "log level: debug, info, warn, error")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(version.Get())
		return
	}

	level := slog.LevelInfo
	switch *logLevel {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))
	log := slog.Default()
	info := version.Get()
	log.Info("starting rfbroker", "version", info.String(), "data-dir", *dataDir, "addr", *addr)

	b, err := broker.Open(broker.Options{DataDir: *dataDir, MaxRecordSize: *maxRecordSize})
	if err != nil {
		log.Error("open broker", "error", err)
		os.Exit(1)
	}
	srv, hs := broker.NewServer(b, log)
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Error("listen", "error", err)
		os.Exit(1)
	}
	fmt.Printf("listening on %s\n", ln.Addr())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()

	select {
	case <-ctx.Done():
		log.Info("shutdown signal received")
	case err := <-serveErr:
		log.Error("serve failed", "error", err)
		b.Close()
		os.Exit(1)
	}

	// Graceful stop: stop accepting, drain in-flight RPCs with a bound.
	done := make(chan struct{})
	go func() { srv.GracefulStop(); close(done) }()
	select {
	case <-done:
		log.Info("graceful stop complete")
	case <-time.After(10 * time.Second):
		log.Warn("graceful stop timed out; forcing stop")
		srv.Stop()
	}
	if err := b.Close(); err != nil {
		log.Error("close broker", "error", err)
		os.Exit(1)
	}
	log.Info("shutdown complete", "version", info.Version, "commit", info.Commit)
}
