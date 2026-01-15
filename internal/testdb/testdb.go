// Package testdb starts throwaway Postgres and Redis containers for
// integration tests. One container of each is shared per test binary;
// testcontainers' reaper removes them when the process exits.
package testdb

import (
	"context"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/Cyber2905s/kinesis/migrations"
)

var (
	pgOnce, redisOnce sync.Once
	pgURL, redisURL   string
	pgErr, redisErr   error
)

// Postgres returns a pool to a migrated database. Skipped under -short.
func Postgres(t testing.TB) *pgxpool.Pool {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test: needs Docker")
	}
	ctx := context.Background()
	pgOnce.Do(func() {
		var c *postgres.PostgresContainer
		c, pgErr = postgres.Run(ctx, "postgres:17-alpine",
			postgres.WithDatabase("kinesis"), postgres.WithUsername("kinesis"), postgres.WithPassword("kinesis"),
			postgres.BasicWaitStrategies())
		if pgErr == nil {
			pgURL, pgErr = c.ConnectionString(ctx, "sslmode=disable")
		}
	})
	if pgErr != nil {
		t.Fatalf("start postgres: %v", pgErr)
	}
	cfg, err := pgxpool.ParseConfig(pgURL)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 64
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := migrations.Apply(ctx, pool); err != nil {
		t.Fatal(err)
	}
	return pool
}

// Redis returns a client to a shared Redis. Skipped under -short.
func Redis(t testing.TB) *redis.Client {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test: needs Docker")
	}
	ctx := context.Background()
	redisOnce.Do(func() {
		var c *tcredis.RedisContainer
		c, redisErr = tcredis.Run(ctx, "redis:7-alpine")
		if redisErr == nil {
			redisURL, redisErr = c.ConnectionString(ctx)
		}
	})
	if redisErr != nil {
		t.Fatalf("start redis: %v", redisErr)
	}
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(opts)
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}
