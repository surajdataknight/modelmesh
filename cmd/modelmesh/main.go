// Command modelmesh runs the ModelMesh LLM gateway.
//
//	MODELMESH_API_KEYS=dev:sk-dev-local-key modelmesh -config configs/local.json
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/modelmesh/modelmesh/internal/config"
	"github.com/modelmesh/modelmesh/internal/gateway"
	"github.com/modelmesh/modelmesh/internal/metrics"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "modelmesh:", err)
		os.Exit(1)
	}
}

func run() error {
	cfgPath := flag.String("config", "configs/local.json", "path to the JSON config file")
	check := flag.Bool("check", false, "validate the config and exit")
	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("modelmesh %s (%s)\n", version, runtime.Version())
		return nil
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	if *check {
		fmt.Printf("config %s is valid: %d providers, %d models\n", *cfgPath, len(cfg.Providers), len(cfg.Models))
		return nil
	}

	var level slog.Level
	_ = level.UnmarshalText([]byte(cfg.LogLevel))
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	reg := metrics.NewRegistry()
	metrics.RegisterRuntime(reg)
	gw, err := gateway.New(cfg, log, reg)
	if err != nil {
		return err
	}
	srv := gateway.NewServer(gw)
	pub, adm, err := srv.Listen()
	if err != nil {
		return err
	}

	if cfg.Auth.Mode == "none" {
		log.Warn("authentication is disabled (auth.mode = none); do not expose this listener")
	}
	if cfg.AdminListen != "" && !config.IsLoopback(cfg.AdminListen) {
		log.Warn("admin listener is not bound to loopback; metrics and pprof are exposed", "admin_listen", cfg.AdminListen)
	}
	log.Info("modelmesh started", "version", version, "listen", pub.Addr().String(),
		"admin", cfg.AdminListen, "providers", len(cfg.Providers), "models", len(cfg.Models), "tls", cfg.TLS.Enabled())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(pub, adm) }()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	stop() // a second signal now terminates immediately
	log.Info("shutdown started", "grace", cfg.Timeouts.ShutdownGrace.D().String())
	if err := srv.Shutdown(context.Background()); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	if err := <-errc; err != nil {
		return err
	}
	log.Info("shutdown complete")
	return nil
}
