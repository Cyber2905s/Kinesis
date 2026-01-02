// Package config loads runtime settings from the environment.
package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"
)

type Config struct {
	DatabaseURL     string
	DBMaxConns      int32
	RedisURL        string
	OutboxStream    string
	OutboxBatch     int
	OutboxInterval  time.Duration
	HTTPAddr        string
	LogLevel        slog.Level
	ShutdownTimeout time.Duration
	ReconInterval   time.Duration
	IdempotencyTTL  time.Duration
	DemoEnabled     bool
}

// Load reads the environment. Every value has a default suitable for local
// development and none are secret, so a bare `kinesis api` works against
// `docker compose up postgres redis`.
func Load() (Config, error) {
	var errs []error
	str := func(k, def string) string {
		if v, ok := os.LookupEnv(k); ok && v != "" {
			return v
		}
		return def
	}
	num := func(k string, def int) int {
		n, err := strconv.Atoi(str(k, strconv.Itoa(def)))
		if err != nil || n <= 0 {
			errs = append(errs, fmt.Errorf("%s: want positive integer", k))
		}
		return n
	}
	dur := func(k string, def time.Duration) time.Duration {
		d, err := time.ParseDuration(str(k, def.String()))
		if err != nil || d <= 0 {
			errs = append(errs, fmt.Errorf("%s: want positive duration", k))
		}
		return d
	}
	c := Config{
		DatabaseURL:     str("DATABASE_URL", "postgres://kinesis:kinesis@localhost:5432/kinesis?sslmode=disable"),
		DBMaxConns:      int32(num("DB_MAX_CONNS", 32)),
		RedisURL:        str("REDIS_URL", "redis://localhost:6379/0"),
		OutboxStream:    str("OUTBOX_STREAM", "ledger.events"),
		OutboxBatch:     num("OUTBOX_BATCH", 500),
		OutboxInterval:  dur("OUTBOX_INTERVAL", 100*time.Millisecond),
		HTTPAddr:        str("HTTP_ADDR", ":8080"),
		ShutdownTimeout: dur("SHUTDOWN_TIMEOUT", 15*time.Second),
		ReconInterval:   dur("RECON_INTERVAL", 30*time.Second),
		IdempotencyTTL:  dur("IDEMPOTENCY_TTL", 24*time.Hour),
	}
	if err := c.LogLevel.UnmarshalText([]byte(str("LOG_LEVEL", "info"))); err != nil {
		errs = append(errs, fmt.Errorf("LOG_LEVEL: %w", err))
	}
	demo, err := strconv.ParseBool(str("DEMO_ENABLED", "false"))
	if err != nil {
		errs = append(errs, fmt.Errorf("DEMO_ENABLED: %w", err))
	}
	c.DemoEnabled = demo
	if len(errs) > 0 {
		return c, fmt.Errorf("config: %v", errs)
	}
	return c, nil
}
