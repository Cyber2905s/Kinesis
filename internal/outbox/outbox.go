// Package outbox relays ledger events from the outbox table to Redis Streams.
package outbox

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/Cyber2905s/kinesis/internal/metrics"
)

type Relay struct {
	Pool     *pgxpool.Pool
	Redis    *redis.Client
	Stream   string
	Batch    int
	Interval time.Duration
	Log      *slog.Logger
}

// Run polls until ctx is cancelled, draining full batches back to back.
func (r *Relay) Run(ctx context.Context) {
	t := time.NewTicker(r.Interval)
	defer t.Stop()
	for {
		n, err := r.PublishBatch(ctx)
		if err != nil && ctx.Err() == nil {
			r.Log.Error("outbox publish failed", "err", err)
		}
		if n == r.Batch {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// PublishBatch claims up to Batch events, XADDs them in one pipeline and
// deletes them in the same transaction. If publishing fails the transaction
// rolls back and the events stay put; if the process dies after XADD but
// before COMMIT they are published again. Delivery is therefore
// at-least-once: consumers dedupe on the "outbox_id" field. SKIP LOCKED lets several relays run at once, at
// the cost of global ordering across relays.
func (r *Relay) PublishBatch(ctx context.Context) (int, error) {
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit

	rows, err := tx.Query(ctx, `
		DELETE FROM outbox WHERE id IN (
			SELECT id FROM outbox ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED)
		RETURNING id, payload, created_at`, r.Batch)
	if err != nil {
		return 0, err
	}
	pipe := r.Redis.Pipeline()
	var oldest time.Time
	n := 0
	for rows.Next() {
		var id int64
		var payload []byte
		var created time.Time
		if err := rows.Scan(&id, &payload, &created); err != nil {
			rows.Close()
			return 0, err
		}
		if n == 0 || created.Before(oldest) {
			oldest = created
		}
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: r.Stream,
			MaxLen: 1_000_000, // ponytail: fixed cap; make configurable if consumers lag that far
			Approx: true,
			Values: map[string]any{"outbox_id": strconv.FormatInt(id, 10), "payload": payload},
		})
		n++
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if n == 0 {
		return 0, nil
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	metrics.OutboxPublished.Add(float64(n))
	metrics.OutboxLag.Observe(time.Since(oldest).Seconds())
	return n, nil
}
