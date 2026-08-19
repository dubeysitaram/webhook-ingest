package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/convin/webhook-ingest/internal/config"
	"github.com/convin/webhook-ingest/internal/httpapi"
	"github.com/convin/webhook-ingest/internal/ingest"
	"github.com/convin/webhook-ingest/internal/redisclient"
	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

const (
	// warmupTimeout bounds the startup read of the durable aggregates.
	warmupTimeout = 15 * time.Second
	// shutdownTimeout bounds draining in-flight HTTP requests.
	shutdownTimeout = 10 * time.Second
	// drainTimeout bounds waiting for background recording work, separately
	// from shutdownTimeout so a slow HTTP drain cannot consume it.
	drainTimeout = 10 * time.Second
)

// main keeps only the exit code, so that run's deferred cleanup -- closing the
// Postgres pool and the Redis client -- always executes. Calling os.Exit from
// inside the body, or from the listener goroutine as this previously did,
// skips every pending defer.
func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("exiting", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg := config.Load()
	ctx := context.Background()

	st, err := store.New(ctx, cfg.PostgresDSN, cfg.DBMaxConns)
	if err != nil {
		return err
	}
	defer st.Close()

	rdb, err := redisclient.New(ctx, cfg.RedisAddr)
	if err != nil {
		return err
	}
	defer func() { _ = rdb.Close() }()

	svc := ingest.New(st, stats.NewCache(), rdb, log)

	// Load the durable totals before serving, otherwise the stats endpoint
	// reports zero for every account until new webhooks rebuild the numbers.
	// Bounded, so an unreachable database fails the boot instead of hanging it
	// forever with no listener and no error.
	warmCtx, cancelWarm := context.WithTimeout(ctx, warmupTimeout)
	err = svc.WarmCache(warmCtx)
	cancelWarm()
	if err != nil {
		return err
	}

	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: httpapi.NewRouter(svc, log)}

	// Buffered, so the goroutine never blocks if nobody is receiving.
	serverErr := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	// A failed listener -- an address already in use, say -- has to end the
	// process as surely as a signal does, but by unwinding rather than by
	// exiting from inside the goroutine.
	var runErr error
	select {
	case runErr = <-serverErr:
		log.Error("server stopped", "err", runErr)
	case <-stop:
		log.Info("shutting down")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("shutdown", "err", err)
	}

	// Shutdown only drains in-flight HTTP handlers. Recording work
	// deliberately outlives its handler, so it has to be waited for
	// separately or a deploy discards it.
	//
	// It gets a fresh budget rather than the remainder of shutdownCtx: a slow
	// HTTP drain would otherwise leave the recording work a few milliseconds,
	// discarding exactly the work this wait exists to preserve.
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), drainTimeout)
	defer cancelDrain()
	if err := svc.Wait(drainCtx); err != nil {
		log.Error("in-flight work did not drain before the deadline", "err", err)
	}

	log.Info("stopped")
	return runErr
}
