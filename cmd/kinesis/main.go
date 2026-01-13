// Command kinesis runs the ledger API, the outbox/reconciliation worker, or a
// one-off reconciliation, depending on its first argument.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"github.com/Cyber2905s/kinesis/internal/api"
	"github.com/Cyber2905s/kinesis/internal/config"
	"github.com/Cyber2905s/kinesis/internal/ledger"
	"github.com/Cyber2905s/kinesis/internal/metrics"
	"github.com/Cyber2905s/kinesis/internal/outbox"
	"github.com/Cyber2905s/kinesis/migrations"
)

const usage = "usage: kinesis [api|worker|reconcile|migrate]"

func main() {
	cmd := "api"
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel})).With("cmd", cmd)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cmd, cfg, log); err != nil {
		log.Error("exiting", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cmd string, cfg config.Config, log *slog.Logger) error {
	pcfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return err
	}
	pcfg.MaxConns = cfg.DBMaxConns
	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := migrations.Apply(ctx, pool); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	switch cmd {
	case "migrate":
		log.Info("migrations applied")
		return nil

	case "reconcile":
		rep, err := ledger.Reconcile(ctx, pool)
		if err != nil {
			return err
		}
		log.Info("reconciliation", "report", rep)
		if !rep.OK {
			return errors.New("ledger invariants violated")
		}
		return nil

	case "api":
		srv := api.New(pool, log, cfg.DemoEnabled)
		return serve(ctx, cfg, log, srv.Handler())

	case "worker":
		opts, err := redis.ParseURL(cfg.RedisURL)
		if err != nil {
			return err
		}
		rdb := redis.NewClient(opts)
		defer rdb.Close()
		relay := &outbox.Relay{Pool: pool, Redis: rdb, Stream: cfg.OutboxStream, Batch: cfg.OutboxBatch, Interval: cfg.OutboxInterval, Log: log}
		go relay.Run(ctx)
		go every(ctx, cfg.ReconInterval, func() { reconcile(ctx, pool, log) })
		go every(ctx, time.Hour, func() {
			tag, err := pool.Exec(ctx, `DELETE FROM idempotency_keys WHERE created_at < now() - $1::interval`, cfg.IdempotencyTTL.String())
			if err != nil && ctx.Err() == nil {
				log.Error("idempotency cleanup failed", "err", err)
				return
			}
			log.Info("idempotency keys expired", "deleted", tag.RowsAffected())
		})
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
		return serve(ctx, cfg, log, mux)
	}
	return errors.New(usage)
}

func reconcile(ctx context.Context, pool *pgxpool.Pool, log *slog.Logger) {
	rep, err := ledger.Reconcile(ctx, pool)
	if err != nil {
		if ctx.Err() == nil {
			log.Error("reconciliation failed", "err", err)
		}
		return
	}
	if rep.OK {
		metrics.ReconOK.Set(1)
		log.Info("reconciliation ok", "transactions", rep.Transactions, "duration_ms", rep.DurationMS)
		return
	}
	metrics.ReconOK.Set(0)
	log.Error("RECONCILIATION MISMATCH", "report", rep)
}

func every(ctx context.Context, d time.Duration, f func()) {
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		f()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// serve runs h until ctx is cancelled, then drains in-flight requests.
func serve(ctx context.Context, cfg config.Config, log *slog.Logger, h http.Handler) error {
	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: h, ReadHeaderTimeout: 5 * time.Second}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	log.Info("listening", "addr", cfg.HTTPAddr)
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	log.Info("shutting down", "timeout", cfg.ShutdownTimeout)
	sctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	return srv.Shutdown(sctx)
}
