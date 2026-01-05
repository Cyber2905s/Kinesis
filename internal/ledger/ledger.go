// Package ledger implements a double-entry ledger on PostgreSQL.
//
// Every posting moves an amount from a debit account to a credit account and
// writes one entry per touched shard, all inside the caller's transaction:
// balances, entries, the transaction row and its outbox event commit or roll
// back together.
package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/Cyber2905s/kinesis/internal/metrics"
)

// WorldAccountID is the seeded external account that funds deposits and
// receives withdrawals.
const WorldAccountID int64 = 1

// MaxAmount bounds a single posting so balances cannot overflow int64 in
// any realistic lifetime.
const MaxAmount int64 = 1e15

type Kind string

const (
	Deposit    Kind = "deposit"
	Withdrawal Kind = "withdrawal"
	Transfer   Kind = "transfer"
)

var (
	ErrNotFound          = errors.New("account not found")
	ErrInsufficientFunds = errors.New("insufficient funds")
)

// DB is satisfied by pgx.Tx, *pgx.Conn and *pgxpool.Pool.
type DB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type Account struct {
	ID             int64     `json:"id"`
	Name           string    `json:"name"`
	AllowOverdraft bool      `json:"allow_overdraft"`
	Shards         int32     `json:"shards"`
	Balance        int64     `json:"balance"`
	CreatedAt      time.Time `json:"created_at"`
}

type Txn struct {
	ID            int64     `json:"id"`
	Kind          Kind      `json:"kind"`
	DebitAccount  int64     `json:"debit_account_id"`
	CreditAccount int64     `json:"credit_account_id"`
	Amount        int64     `json:"amount"`
	CreatedAt     time.Time `json:"created_at"`
}

// CreateAccount inserts an account and its zero-balance shards.
func CreateAccount(ctx context.Context, db DB, name string, allowOverdraft bool, shards int32) (Account, error) {
	a := Account{Name: name, AllowOverdraft: allowOverdraft, Shards: shards}
	err := db.QueryRow(ctx, `
		WITH a AS (
			INSERT INTO accounts (name, allow_overdraft, shards) VALUES ($1, $2, $3)
			RETURNING id, created_at, shards
		), s AS (
			INSERT INTO account_shards (account_id, shard)
			SELECT a.id, g FROM a, generate_series(0, a.shards - 1) g
		)
		SELECT id, created_at FROM a`, name, allowOverdraft, shards).Scan(&a.ID, &a.CreatedAt)
	return a, err
}

const accountCols = `a.id, a.name, a.allow_overdraft, a.shards, a.created_at, sum(s.balance)::bigint`

func scanAccount(row pgx.Row) (Account, error) {
	var a Account
	err := row.Scan(&a.ID, &a.Name, &a.AllowOverdraft, &a.Shards, &a.CreatedAt, &a.Balance)
	if errors.Is(err, pgx.ErrNoRows) {
		return a, ErrNotFound
	}
	return a, err
}

// GetAccount returns the account with its balance (the sum of its shards).
func GetAccount(ctx context.Context, db DB, id int64) (Account, error) {
	return scanAccount(db.QueryRow(ctx, `
		SELECT `+accountCols+` FROM accounts a JOIN account_shards s ON s.account_id = a.id
		WHERE a.id = $1 GROUP BY a.id`, id))
}

// ListAccounts returns up to limit accounts ordered by id, starting after afterID.
func ListAccounts(ctx context.Context, db DB, afterID int64, limit int) ([]Account, error) {
	rows, err := db.Query(ctx, `
		SELECT `+accountCols+` FROM accounts a JOIN account_shards s ON s.account_id = a.id
		WHERE a.id > $1 GROUP BY a.id ORDER BY a.id LIMIT $2`, afterID, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Account, error) { return scanAccount(r) })
}

// ListTxns pages through transactions newest first. accountID 0 lists all
// accounts; before 0 starts from the newest. The caller passes the last
// returned ID as before to fetch the next page.
func ListTxns(ctx context.Context, db DB, accountID, before int64, limit int) ([]Txn, error) {
	if before <= 0 {
		before = 1<<63 - 1
	}
	q := `SELECT id, kind, debit_account, credit_account, amount, created_at FROM transactions
		WHERE id < $1 ORDER BY id DESC LIMIT $2`
	args := []any{before, limit}
	if accountID != 0 {
		// Two index-friendly branches instead of an OR the planner can't use well.
		q = `(SELECT id, kind, debit_account, credit_account, amount, created_at FROM transactions
			WHERE debit_account = $3 AND id < $1 ORDER BY id DESC LIMIT $2)
		UNION ALL
		(SELECT id, kind, debit_account, credit_account, amount, created_at FROM transactions
			WHERE credit_account = $3 AND id < $1 ORDER BY id DESC LIMIT $2)
		ORDER BY id DESC LIMIT $2`
		args = append(args, accountID)
	}
	rows, err := db.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[Txn])
}

type shardBal struct {
	shard   int32
	balance int64
}

type leg struct {
	shard  int32
	amount int64
}

// planDebit decides which of the locked shards pay for amount. Overdraft
// accounts only ever lock one shard and take the whole amount from it;
// everyone else drains shards in order and must cover the amount in total,
// which keeps every shard (and therefore the account) non-negative.
func planDebit(shards []shardBal, amount int64, overdraft bool) ([]leg, error) {
	if overdraft {
		return []leg{{shards[0].shard, amount}}, nil
	}
	var legs []leg
	remaining := amount
	for _, s := range shards {
		if remaining == 0 {
			break
		}
		if take := min(s.balance, remaining); take > 0 {
			legs = append(legs, leg{s.shard, take})
			remaining -= take
		}
	}
	if remaining > 0 {
		return nil, ErrInsufficientFunds
	}
	return legs, nil
}

// Post moves amount from debit to credit inside tx. It must be called in a
// transaction; the caller commits.
//
// Locking: one statement locks every shard row the posting touches, sorted by
// (account_id, shard). All postings acquire locks in that global order, so
// two postings can never wait on each other in a cycle — no deadlocks. The
// debit side locks all its shards when it must prove sufficient funds, or
// one random shard when overdraft is allowed; the credit side locks one
// random shard. That is what makes sharding help hot accounts: concurrent
// credits to a 16-shard account usually hit different rows.
func Post(ctx context.Context, tx pgx.Tx, kind Kind, debit, credit, amount int64) (Txn, error) {
	if debit == credit || amount <= 0 || amount > MaxAmount {
		return Txn{}, fmt.Errorf("ledger: invalid posting %d->%d amount %d", debit, credit, amount)
	}
	start := time.Now()
	rows, err := tx.Query(ctx, `
		SELECT s.account_id, s.shard, s.balance, a.allow_overdraft
		FROM account_shards s JOIN accounts a ON a.id = s.account_id
		WHERE (s.account_id = $1 AND (NOT a.allow_overdraft OR s.shard = $3 % a.shards))
		   OR (s.account_id = $2 AND s.shard = $4 % a.shards)
		ORDER BY s.account_id, s.shard
		FOR NO KEY UPDATE OF s`, debit, credit, rand.Int32(), rand.Int32())
	if err != nil {
		return Txn{}, err
	}
	var debitShards []shardBal
	var creditShard *int32
	var overdraft bool
	for rows.Next() {
		var acct int64
		var s shardBal
		var od bool
		if err := rows.Scan(&acct, &s.shard, &s.balance, &od); err != nil {
			rows.Close()
			return Txn{}, err
		}
		if acct == debit {
			debitShards = append(debitShards, s)
			overdraft = od
		} else {
			creditShard = &s.shard
		}
	}
	if err := rows.Err(); err != nil {
		return Txn{}, err
	}
	metrics.LockWait.Observe(time.Since(start).Seconds())
	if len(debitShards) == 0 || creditShard == nil {
		return Txn{}, ErrNotFound
	}
	legs, err := planDebit(debitShards, amount, overdraft)
	if err != nil {
		return Txn{}, err
	}

	t := Txn{Kind: kind, DebitAccount: debit, CreditAccount: credit, Amount: amount}
	if err := tx.QueryRow(ctx, `
		INSERT INTO transactions (kind, debit_account, credit_account, amount)
		VALUES ($1, $2, $3, $4) RETURNING id, created_at`,
		kind, debit, credit, amount).Scan(&t.ID, &t.CreatedAt); err != nil {
		return Txn{}, err
	}
	event, err := json.Marshal(map[string]any{"type": "transaction.posted", "transaction": t})
	if err != nil {
		return Txn{}, err
	}

	b := &pgx.Batch{}
	move := func(acct int64, shard int32, delta int64) {
		b.Queue(`UPDATE account_shards SET balance = balance + $3 WHERE account_id = $1 AND shard = $2`, acct, shard, delta)
		b.Queue(`INSERT INTO entries (tx_id, account_id, shard, amount) VALUES ($1, $2, $3, $4)`, t.ID, acct, shard, delta)
	}
	for _, l := range legs {
		move(debit, l.shard, -l.amount)
	}
	move(credit, *creditShard, amount)
	b.Queue(`INSERT INTO outbox (payload) VALUES ($1)`, event)
	if err := tx.SendBatch(ctx, b).Close(); err != nil {
		return Txn{}, err
	}
	return t, nil
}
