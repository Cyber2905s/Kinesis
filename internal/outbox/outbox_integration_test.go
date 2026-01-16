package outbox_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Cyber2905s/kinesis/internal/ledger"
	"github.com/Cyber2905s/kinesis/internal/outbox"
	"github.com/Cyber2905s/kinesis/internal/testdb"
)

func TestRelayPublishesCommittedEvents(t *testing.T) {
	ctx := context.Background()
	pool, rdb := testdb.Postgres(t), testdb.Redis(t)
	acct, err := ledger.CreateAccount(ctx, pool, "outbox", false, 1)
	if err != nil {
		t.Fatal(err)
	}
	var posted ledger.Txn
	if err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		posted, err = ledger.Post(ctx, tx, ledger.Deposit, ledger.WorldAccountID, acct.ID, 42)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	// A rolled-back posting must never produce an event.
	_ = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		_, _ = ledger.Post(ctx, tx, ledger.Deposit, ledger.WorldAccountID, acct.ID, 7)
		return context.Canceled
	})

	relay := &outbox.Relay{Pool: pool, Redis: rdb, Stream: "test.events", Batch: 100, Interval: time.Second, Log: slog.New(slog.DiscardHandler)}
	n, err := relay.PublishBatch(ctx)
	if err != nil || n != 1 {
		t.Fatalf("PublishBatch = %d, %v; want 1 event", n, err)
	}
	msgs, err := rdb.XRange(ctx, "test.events", "-", "+").Result()
	if err != nil || len(msgs) != 1 {
		t.Fatalf("stream has %d messages (%v), want 1", len(msgs), err)
	}
	var ev struct {
		Type        string     `json:"type"`
		Transaction ledger.Txn `json:"transaction"`
	}
	if err := json.Unmarshal([]byte(msgs[0].Values["payload"].(string)), &ev); err != nil {
		t.Fatal(err)
	}
	if ev.Type != "transaction.posted" || ev.Transaction.ID != posted.ID || ev.Transaction.Amount != 42 {
		t.Fatalf("unexpected event %+v", ev)
	}
	if n, _ := relay.PublishBatch(ctx); n != 0 {
		t.Fatalf("second batch republished %d events", n)
	}
}
