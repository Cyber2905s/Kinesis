package ledger_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Cyber2905s/kinesis/internal/ledger"
	"github.com/Cyber2905s/kinesis/internal/testdb"
)

var ctx = context.Background()

func post(t *testing.T, pool *pgxpool.Pool, kind ledger.Kind, debit, credit, amount int64) (ledger.Txn, error) {
	t.Helper()
	var txn ledger.Txn
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		var err error
		txn, err = ledger.Post(ctx, tx, kind, debit, credit, amount)
		return err
	})
	return txn, err
}

func mustAccount(t *testing.T, pool *pgxpool.Pool, name string, overdraft bool, shards int32) ledger.Account {
	t.Helper()
	a, err := ledger.CreateAccount(ctx, pool, name, overdraft, shards)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func balance(t *testing.T, pool *pgxpool.Pool, id int64) int64 {
	t.Helper()
	a, err := ledger.GetAccount(ctx, pool, id)
	if err != nil {
		t.Fatal(err)
	}
	return a.Balance
}

func assertReconciled(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	rep, err := ledger.Reconcile(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.OK {
		t.Fatalf("reconciliation failed: %+v", rep)
	}
}

func TestPostingLifecycle(t *testing.T) {
	pool := testdb.Postgres(t)
	alice := mustAccount(t, pool, "alice", false, 1)
	bob := mustAccount(t, pool, "bob", false, 1)

	if _, err := post(t, pool, ledger.Deposit, ledger.WorldAccountID, alice.ID, 1000); err != nil {
		t.Fatal(err)
	}
	if _, err := post(t, pool, ledger.Transfer, alice.ID, bob.ID, 300); err != nil {
		t.Fatal(err)
	}
	if _, err := post(t, pool, ledger.Transfer, alice.ID, bob.ID, 701); !errors.Is(err, ledger.ErrInsufficientFunds) {
		t.Fatalf("overspend: got %v, want ErrInsufficientFunds", err)
	}
	if _, err := post(t, pool, ledger.Withdrawal, bob.ID, ledger.WorldAccountID, 300); err != nil {
		t.Fatal(err)
	}
	if _, err := post(t, pool, ledger.Transfer, alice.ID, 999999, 1); !errors.Is(err, ledger.ErrNotFound) {
		t.Fatalf("unknown account: got %v, want ErrNotFound", err)
	}
	if a, b := balance(t, pool, alice.ID), balance(t, pool, bob.ID); a != 700 || b != 0 {
		t.Fatalf("balances alice=%d bob=%d, want 700/0", a, b)
	}
	assertReconciled(t, pool)
}

func TestOverdraftAccount(t *testing.T) {
	pool := testdb.Postgres(t)
	credit := mustAccount(t, pool, "credit-line", true, 1)
	shop := mustAccount(t, pool, "shop", false, 1)
	if _, err := post(t, pool, ledger.Transfer, credit.ID, shop.ID, 500); err != nil {
		t.Fatal(err)
	}
	if got := balance(t, pool, credit.ID); got != -500 {
		t.Fatalf("overdraft balance = %d, want -500", got)
	}
	assertReconciled(t, pool)
}

func TestShardedAccount(t *testing.T) {
	pool := testdb.Postgres(t)
	merchant := mustAccount(t, pool, "merchant", false, 8)
	payer := mustAccount(t, pool, "payer", false, 1)
	if _, err := post(t, pool, ledger.Deposit, ledger.WorldAccountID, payer.ID, 10_000); err != nil {
		t.Fatal(err)
	}
	for range 100 {
		if _, err := post(t, pool, ledger.Transfer, payer.ID, merchant.ID, 10); err != nil {
			t.Fatal(err)
		}
	}
	var used int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM account_shards WHERE account_id = $1 AND balance > 0`, merchant.ID).Scan(&used); err != nil {
		t.Fatal(err)
	}
	if used < 2 {
		t.Fatalf("credits landed on %d shard(s), want them spread", used)
	}
	// A debit larger than any single shard must span shards.
	if _, err := post(t, pool, ledger.Withdrawal, merchant.ID, ledger.WorldAccountID, 1000); err != nil {
		t.Fatal(err)
	}
	if got := balance(t, pool, merchant.ID); got != 0 {
		t.Fatalf("merchant balance = %d, want 0", got)
	}
	if _, err := post(t, pool, ledger.Withdrawal, merchant.ID, ledger.WorldAccountID, 1); !errors.Is(err, ledger.ErrInsufficientFunds) {
		t.Fatalf("got %v, want ErrInsufficientFunds", err)
	}
	assertReconciled(t, pool)
}

func TestListTxnsPagination(t *testing.T) {
	pool := testdb.Postgres(t)
	a := mustAccount(t, pool, "pager", false, 1)
	b := mustAccount(t, pool, "other", false, 1)
	if _, err := post(t, pool, ledger.Deposit, ledger.WorldAccountID, a.ID, 100); err != nil {
		t.Fatal(err)
	}
	for range 6 { // 6 outgoing + 1 deposit = 7 rows touching a
		if _, err := post(t, pool, ledger.Transfer, a.ID, b.ID, 1); err != nil {
			t.Fatal(err)
		}
	}
	var ids []int64
	var cursor int64
	for {
		page, err := ledger.ListTxns(ctx, pool, a.ID, cursor, 3)
		if err != nil {
			t.Fatal(err)
		}
		for _, tx := range page {
			ids = append(ids, tx.ID)
		}
		if len(page) < 3 {
			break
		}
		cursor = page[len(page)-1].ID
	}
	if len(ids) != 7 {
		t.Fatalf("paged %d txns, want 7", len(ids))
	}
	for i := 1; i < len(ids); i++ {
		if ids[i] >= ids[i-1] {
			t.Fatalf("not strictly descending: %v", ids)
		}
	}
}

func TestReconcileDetectsDrift(t *testing.T) {
	pool := testdb.Postgres(t)
	a := mustAccount(t, pool, "drift", false, 1)
	assertReconciled(t, pool)
	if _, err := pool.Exec(ctx, `UPDATE account_shards SET balance = balance + 1 WHERE account_id = $1`, a.ID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `UPDATE account_shards SET balance = balance - 1 WHERE account_id = $1`, a.ID)
	})
	rep, err := ledger.Reconcile(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if rep.OK || rep.ShardMismatches != 1 {
		t.Fatalf("expected one shard mismatch, got %+v", rep)
	}
}
