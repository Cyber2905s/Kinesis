package ledger

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Report is the outcome of a reconciliation run. OK is true only when every
// check found zero discrepancies.
type Report struct {
	OK               bool      `json:"ok"`
	EntriesSum       int64     `json:"entries_sum"`       // must be 0
	UnbalancedTxns   int64     `json:"unbalanced_txns"`   // txns whose entries don't sum to 0
	ShardMismatches  int64     `json:"shard_mismatches"`  // shard balance != sum of its entries
	NegativeBalances int64     `json:"negative_balances"` // non-overdraft shards below 0
	Transactions     int64     `json:"transactions"`
	CheckedAt        time.Time `json:"checked_at"`
	DurationMS       int64     `json:"duration_ms"`
}

// Reconcile verifies the ledger invariants on one consistent snapshot.
// ponytail: full scans of entries; fine to tens of millions of rows. Beyond
// that, checkpoint per-shard sums and only scan entries after the checkpoint.
func Reconcile(ctx context.Context, pool *pgxpool.Pool) (Report, error) {
	r := Report{CheckedAt: time.Now().UTC()}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return r, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // read-only

	checks := []struct {
		dst *int64
		sql string
	}{
		{&r.EntriesSum, `SELECT COALESCE(sum(amount), 0)::bigint FROM entries`},
		{&r.UnbalancedTxns, `SELECT count(*) FROM (
			SELECT tx_id FROM entries GROUP BY tx_id HAVING sum(amount) <> 0) x`},
		{&r.ShardMismatches, `SELECT count(*) FROM account_shards s
			LEFT JOIN (SELECT account_id, shard, sum(amount) AS total FROM entries GROUP BY 1, 2) e
			USING (account_id, shard)
			WHERE s.balance <> COALESCE(e.total, 0)`},
		{&r.NegativeBalances, `SELECT count(*) FROM account_shards s JOIN accounts a ON a.id = s.account_id
			WHERE NOT a.allow_overdraft AND s.balance < 0`},
		{&r.Transactions, `SELECT count(*) FROM transactions`},
	}
	for _, c := range checks {
		if err := tx.QueryRow(ctx, c.sql).Scan(c.dst); err != nil {
			return r, err
		}
	}
	r.OK = r.EntriesSum == 0 && r.UnbalancedTxns == 0 && r.ShardMismatches == 0 && r.NegativeBalances == 0
	r.DurationMS = time.Since(r.CheckedAt).Milliseconds()
	return r, nil
}
