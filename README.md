# Kinesis

Kinesis is a double-entry ledger and wallet service in Go, built so that correctness holds under concurrency. Every deposit, withdrawal and transfer writes balanced debit and credit entries inside one PostgreSQL transaction. That same transaction also writes the idempotency record for the request and the event the worker later publishes to Redis Streams, so a transfer can't be applied twice, lost, or announced without having happened. Hot accounts, like a merchant that thousands of wallets pay at once, are split into sub-account shards so they stop serializing every writer. A reconciliation job continuously proves the books balance, and the repo includes a live dashboard, Prometheus metrics, a k6 load test, and benchmark numbers that were measured rather than assumed.

![Kinesis dashboard: live TPS, reconciliation status, recent transactions and balances](docs/dashboard.png)

> 🎬 _GIF placeholder: clicking "Run load burst" and watching TPS spike while invariants stay green._

## Contents

- [Architecture](#architecture)
- [Quick start](#quick-start)
- [Design decisions and tradeoffs](#design-decisions-and-tradeoffs)
- [API reference](#api-reference)
- [Configuration](#configuration)
- [Testing](#testing)
- [Benchmarks](#benchmarks)
- [What I'd do next](#what-id-do-next)

## Architecture

```mermaid
flowchart LR
    clients["Clients / k6"] -->|"HTTP + Idempotency-Key"| api
    browser["Dashboard (browser)"] -->|"polls /v1/*"| api

    subgraph api["kinesis api"]
        handlers["chi handlers"] --> idem["idempotency wrapper"] --> post["ledger.Post"]
    end

    post -->|"one transaction: lock shards, update balances,<br/>insert txn + entries + outbox row + idempotency key"| pg[("PostgreSQL<br/>source of truth")]

    subgraph worker["kinesis worker"]
        relay["outbox relay"]
        recon["reconciler (every 30s)"]
    end

    relay -->|"DELETE … FOR UPDATE SKIP LOCKED RETURNING"| pg
    relay -->|"XADD ledger.events"| redis[("Redis Streams")]
    recon -->|"REPEATABLE READ snapshot checks"| pg
    prom["Prometheus"] -.->|"/metrics"| api
    prom -.->|"/metrics"| worker
```

A single binary (`cmd/kinesis`) runs in one of four modes:

| Mode | What it does |
|---|---|
| `kinesis api` | HTTP API, dashboard at `/`, `/metrics`, `/healthz` |
| `kinesis worker` | Outbox relay to Redis Streams, periodic reconciliation, idempotency-key expiry, `/metrics` |
| `kinesis reconcile` | One-off reconciliation; exits non-zero if an invariant is violated (cron/CI friendly) |
| `kinesis migrate` | Apply migrations and exit (the api and worker also migrate on start, under an advisory lock) |

```
cmd/kinesis/          entrypoint, graceful shutdown, mode switch
internal/ledger/      posting, locking, shard planning, reconciliation
internal/api/         HTTP handlers, idempotency, demo burst, embedded dashboard
internal/outbox/      outbox → Redis Streams relay
internal/metrics/     Prometheus collectors
internal/config/      env config
internal/testdb/      testcontainers helpers
migrations/           embedded SQL schema
loadtest/             k6 script, benchmark runner, tmpfs override
```

## Quick start

```bash
docker compose up --build        # Postgres, Redis, api (+ dashboard), worker
open http://localhost:8080       # dashboard → "Run load burst"
```

If ports 5432/6379 are taken on your machine, run `PG_PORT=5433 REDIS_PORT=6380 docker compose up --build`. Postgres and Redis are only published on `127.0.0.1`, for local tooling.

```bash
# create two accounts, fund one, transfer
curl -s -XPOST localhost:8080/v1/accounts  -H 'Idempotency-Key: a-1' -d '{"name":"alice"}'
curl -s -XPOST localhost:8080/v1/accounts  -H 'Idempotency-Key: a-2' -d '{"name":"shop","shards":16}'
curl -s -XPOST localhost:8080/v1/deposits  -H 'Idempotency-Key: d-1' -d '{"account_id":2,"amount":10000}'
curl -s -XPOST localhost:8080/v1/transfers -H 'Idempotency-Key: t-1' -d '{"from_account_id":2,"to_account_id":3,"amount":2500}'
curl -s localhost:8080/v1/accounts/2
curl -s localhost:8080/v1/reconcile

# watch ledger events
docker compose exec redis redis-cli XRANGE ledger.events - + COUNT 5

make test        # unit + integration (needs Docker)
make lint
make reconcile   # run a reconciliation inside the compose stack
```

## Design decisions and tradeoffs

### Data model: double entry and a world account

- **Money is an `int64` in minor units** (cents). There are no floats anywhere. A single posting is capped at 10¹⁵, so no balance can overflow.
- **`transactions`** holds one row per business event (deposit, withdrawal, transfer) with its debit account, credit account and amount.
- **`entries`** holds the double-entry legs, signed (negative = debit, positive = credit), one per touched `(account, shard)`. Every transaction's entries sum to zero, so all entries in the ledger sum to zero.
- **`account_shards`** holds the balances, one row per `(account, shard)`. An account's balance is the sum of its shards. Account metadata (`allow_overdraft`, `shards`) is immutable after creation.
- **The world account (id 1)** is the counterparty for money entering and leaving the system: a deposit is `world → account`, and a withdrawal is `account → world`. This keeps deposits and withdrawals double-entry too. The world account is the only seeded overdraft account, and its (negative) balance mirrors total customer funds. The API won't let you use it directly in a transfer.
- **Single currency.** Multi-currency needs one world account per currency plus FX postings. I left that out deliberately.

### Concurrency: pessimistic locking with a global lock order

A posting runs one statement that locks every shard row it will touch:

```sql
SELECT … FROM account_shards s JOIN accounts a ON a.id = s.account_id
WHERE (s.account_id = $debit  AND (NOT a.allow_overdraft OR s.shard = $r1 % a.shards))
   OR (s.account_id = $credit AND s.shard = $r2 % a.shards)
ORDER BY s.account_id, s.shard
FOR NO KEY UPDATE OF s
```

**Why pessimistic (`SELECT … FOR UPDATE`) instead of optimistic version columns?**

- Ledgers have hot rows. Under optimistic concurrency, every conflicting writer does all its work, fails the version check, and retries. As contention rises, most work gets thrown away and throughput collapses exactly when load peaks, and every caller needs a retry loop. Row locks make contending writers queue inside Postgres instead: no wasted work, no retry storms, and the "check balance, then write" step is naturally atomic.
- **Deadlocks are avoided by construction, not by retrying.** One statement acquires every lock the posting needs, in a single global order `(account_id, shard)`. Postgres locks rows in the order the sort emits them, since `LockRows` sits above the `Sort` node. Two postings therefore can't each hold a lock the other wants. The concurrency test fires opposite-direction transfers between the same pairs, which is the classic deadlock shape, and sees zero deadlock errors.
- **`FOR NO KEY UPDATE` rather than `FOR UPDATE`:** we never change key columns. The weaker lock doesn't conflict with the `FOR KEY SHARE` locks that foreign-key checks from `entries` take, so FK validation never queues behind a balance lock.
- **Cost:** locks are held for the rest of the transaction, which is about 4 round trips plus the commit fsync. On the durable benchmark, the commit fsync dominates hold time (see [Benchmarks](#benchmarks)). The fix would be a server-side function that runs the whole posting in one round trip.

**Non-negative balances.** For an account without overdraft, the debit side locks all of its shards and drains them in order (`planDebit`). The posting fails with `422 insufficient funds` if the shards together can't cover the amount. Every shard therefore stays at or above zero, and so does the account. Reconciliation independently counts any negative non-overdraft shard.

> I also tried ordering locks by `(shards DESC, account_id, shard)` so hot accounts are locked first. The idea was that a transfer shouldn't sit on a quiet row while it queues for a hot one. In an A/B test (3 × 20 s each, Postgres on tmpfs, 16-shard hot account) it made no measurable difference: 2,269–2,952 tps vs 2,386–2,838 tps. I reverted it, because simpler code wins a tie.

### Hot accounts: sub-account sharding

An account created with `"shards": N` gets N balance rows.

- **Credits** pick one random shard, so N concurrent payers of the same merchant usually lock different rows.
- **Debits** from a non-overdraft sharded account lock all N shards, since they must prove sufficient funds, and drain them in order. Debits from overdraft accounts (like `world`) lock one random shard, because there is nothing to prove.
- The balance read is `sum(balance)` over at most 64 rows.

**The tradeoff:** sharding speeds up inbound traffic, but a debit from a sharded account serializes with every credit to it. That suits the typical shape of a hot account (many payers, occasional payouts). An account with hot debits would need the second technique below.

Measured on the same machine with Postgres on tmpfs, a single hot account reaches **619 transfers/s**. Split into 16 shards, it reaches **3,161/s (5.1×)**, on par with traffic spread over 200 accounts.

The alternative I didn't build is **buffered aggregation**: credits to a hot account append to a pending table without locking the balance, and a background job folds them in. It scales further, but the account's balance becomes eventually consistent, which is fine for a merchant's incoming funds and wrong for anything that can be debited.

### Idempotency keys

Every `POST` requires an `Idempotency-Key` header. The key, a SHA-256 of `method + path + body`, the status code and the response body are stored **in the same transaction as the ledger write**:

1. Run the posting.
2. `INSERT INTO idempotency_keys … ON CONFLICT (key) DO NOTHING`.
3. If the insert happened, commit. Ledger rows and the idempotency record become visible together.
4. If it conflicted, another request with this key already committed. Roll back our own writes and return the stored response with `Idempotent-Replayed: true`. If the stored hash doesn't match, return `422` because the key was reused for a different request.

If two identical requests race, the second one's insert **waits** on the first one's uncommitted key and then sees the conflict. Exactly one executes, and the test fires 32 in parallel to check this. The first-time path needs no extra "does this key exist?" round trip. The tradeoff is that a replay re-does the work before discarding it. That's fine, because replays are rare.

**Business rejections (`404`, `422 insufficient funds`) are stored too**, following Stripe's model, so replaying a key replays the rejection. They're stored in a separate statement after rolling back, so a rejected request can never leave partial writes. **Unexpected errors (`5xx`) are not stored**, so a client can safely retry the same key. Keys expire after `IDEMPOTENCY_TTL` (24 h), handled by the worker.

### Transactional outbox → Redis Streams

`ledger.Post` inserts a `transaction.posted` event into `outbox` inside the posting's transaction. An event therefore exists **if and only if** the posting committed. The worker drains it:

```sql
DELETE FROM outbox WHERE id IN (SELECT id FROM outbox ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED)
RETURNING id, payload, created_at
```

It then pipelines `XADD`s to `ledger.events` and commits. If Redis fails, the transaction rolls back and the events stay put. If the worker dies after `XADD` but before `COMMIT`, those events are sent again. Delivery is therefore **at-least-once**, and consumers dedupe on the `outbox_id` field. `SKIP LOCKED` lets several relays run at once, at the cost of strict global ordering between them. With one relay, events arrive in outbox-id order. The stream is capped at about 1M entries (`MAXLEN ~`). Publish lag is exported as `kinesis_outbox_lag_seconds`.

I chose Redis Streams over NATS because it's one small container, has consumer groups, and is enough for a demo. The relay is ~100 lines, and swapping it for NATS JetStream would only change the publish call.

### Reconciliation

`ledger.Reconcile` runs four checks inside one `REPEATABLE READ READ ONLY` transaction, so it sees a single consistent snapshot even while transfers keep committing:

| Check | Must be |
|---|---|
| `sum(entries.amount)` | 0 |
| transactions whose entries don't sum to 0 | 0 |
| shards whose `balance` ≠ sum of their entries | 0 |
| non-overdraft shards with `balance < 0` | 0 |

The worker runs it every `RECON_INTERVAL`, logs `RECONCILIATION MISMATCH` at error level if a check fails, and sets `kinesis_reconciliation_ok` (alert on `== 0`). `GET /v1/reconcile` and `kinesis reconcile` run it on demand. A test corrupts one shard behind the ledger's back to confirm drift is detected. Cost: full scans. The benchmark database reconciled 300k transactions in under 0.5 s, and beyond tens of millions of rows you'd checkpoint (see next steps).

### Observability and operations

- **Logs:** `log/slog` JSON to stdout. Request logs are at debug level so load tests don't flood the output, 5xx responses are logged at error, and reconciliation results at info/error.
- **Metrics** (`/metrics` on both processes):
  - `kinesis_ledger_transactions_total{kind,result}`: TPS is `rate(...{result="ok"}[1m])`
  - `kinesis_http_request_duration_seconds{method,route,code}`: p50/p99 via `histogram_quantile`
  - `kinesis_lock_wait_seconds`: time spent acquiring shard row locks
  - `kinesis_idempotent_replays_total`, `kinesis_outbox_published_total`, `kinesis_outbox_lag_seconds`, `kinesis_reconciliation_ok`
- **Graceful shutdown:** on SIGTERM/SIGINT, the API stops accepting connections and drains in-flight requests (`SHUTDOWN_TIMEOUT`). The worker finishes its current batch; an interrupted batch simply rolls back.
- **Config:** environment variables only (see below). The dashboard's `/v1/demo/burst` endpoint only exists when `DEMO_ENABLED=true`.

## API reference

All bodies are JSON and amounts are integers in minor units. Errors look like `{"error": "..."}`.

| Method | Path | Body / query | Success |
|---|---|---|---|
| `POST` | `/v1/accounts` | `{"name": "alice", "allow_overdraft": false, "shards": 1}` | `201` account |
| `GET` | `/v1/accounts` | `?cursor=&limit=` (≤ 500) | `200 {"data": [...], "next_cursor": "..."}` |
| `GET` | `/v1/accounts/{id}` | | `200` account with `balance` |
| `GET` | `/v1/accounts/{id}/transactions` | `?cursor=&limit=` | `200` page, newest first |
| `POST` | `/v1/deposits` | `{"account_id": 2, "amount": 10000}` | `201` transaction |
| `POST` | `/v1/withdrawals` | `{"account_id": 2, "amount": 500}` | `201` transaction |
| `POST` | `/v1/transfers` | `{"from_account_id": 2, "to_account_id": 3, "amount": 2500}` | `201` transaction |
| `GET` | `/v1/transactions` | `?cursor=&limit=` | `200` page across all accounts |
| `GET` | `/v1/reconcile` | | `200` report |
| `GET` | `/v1/stats` | | `200 {"committed": n}` (used by the dashboard) |
| `POST` | `/v1/demo/burst` | `{"transfers": 2000, "concurrency": 32}` | `200` burst summary + reconcile (demo only) |
| `GET` | `/metrics`, `/healthz`, `/` | | Prometheus, health, dashboard |

**Every `POST` requires `Idempotency-Key`** (1–255 chars). Replays return the original status and body, plus `Idempotent-Replayed: true`.

Account:

```json
{"id": 2, "name": "alice", "allow_overdraft": false, "shards": 1, "balance": 7500, "created_at": "2026-09-27T17:57:36.2Z"}
```

Transaction:

```json
{"id": 41, "kind": "transfer", "debit_account_id": 2, "credit_account_id": 3, "amount": 2500, "created_at": "2026-09-27T17:57:37.1Z"}
```

Pagination: pass the previous page's `next_cursor` as `?cursor=`. An empty `next_cursor` means the last page. Cursors are keyset-based (`id < cursor`), so pages stay stable while new transactions arrive.

| Status | Meaning |
|---|---|
| `400` | Malformed JSON, unknown field, missing key, invalid amount/ids, same-account transfer, world account in a transfer |
| `404` | Account not found (stored under the idempotency key) |
| `422` | Insufficient funds (stored), or idempotency key reused with a different request |
| `500` | Unexpected error (not stored; retry with the same key) |

## Configuration

See [`.env.example`](.env.example).

| Variable | Default | |
|---|---|---|
| `DATABASE_URL` | `postgres://kinesis:kinesis@localhost:5432/kinesis?sslmode=disable` | |
| `DB_MAX_CONNS` | `32` | pgxpool size |
| `REDIS_URL` | `redis://localhost:6379/0` | worker only |
| `OUTBOX_STREAM` / `OUTBOX_BATCH` / `OUTBOX_INTERVAL` | `ledger.events` / `500` / `100ms` | |
| `HTTP_ADDR` | `:8080` | |
| `LOG_LEVEL` | `info` | `debug` logs every request |
| `SHUTDOWN_TIMEOUT` | `15s` | |
| `RECON_INTERVAL` | `30s` | worker |
| `IDEMPOTENCY_TTL` | `24h` | worker |
| `DEMO_ENABLED` | `false` | enables `/v1/demo/burst` |

## Testing

```bash
make test-short   # unit tests only, no Docker
make test         # everything, with -race; testcontainers starts Postgres 17 + Redis 7
```

- **Unit:** shard debit planning (spanning shards, skipping empty or negative shards, insufficient funds, overdraft).
- **Integration (`internal/ledger`):** deposit/transfer/withdraw lifecycle, overdraft, credits spreading across shards, debits spanning shards, keyset pagination, and a reconciler that catches deliberately corrupted balances.
- **Integration (`internal/api`):** idempotent replay, key reuse with a different body → 422, stored rejections, **32 concurrent requests with one key → exactly one transaction**.
- **Concurrency (`TestConcurrentTransfersPreserveInvariants`):** 5,000 transfers over HTTP from 64 goroutines across 30 accounts plus a 16-shard hot merchant that both receives and pays out, including opposite-direction pairs. It asserts: no unexpected statuses (so no deadlocks or serialization errors), no negative balances, money conserved exactly, and reconciliation OK. A typical run: 4,211 committed, 789 correctly rejected for insufficient funds, ~6 s.
- **Outbox:** a committed posting is published exactly once per batch, and a rolled-back posting never is.

CI (GitHub Actions) runs `golangci-lint` and `go test -race ./...` on pushes to `main` and on pull requests. Docker on the runner provides the containers.

## Benchmarks

**These numbers were measured, not estimated.** They come from `loadtest/bench.sh`: k6 with 64 VUs posting transfers back to back, each with a unique idempotency key, every response checked for `201`.

### Machine

| | |
|---|---|
| CPU | AMD Ryzen 7 7730U (8 cores / 16 threads), laptop |
| RAM | 15 GiB |
| Disk | Intel SSDPEKNU512GZH NVMe (QLC), btrfs root, **99% full** at test time |
| OS | CachyOS Linux, kernel 7.1.8 |
| Software | Docker 29.7.2, PostgreSQL 17.10 (`synchronous_commit=on`, `shared_buffers=512MB`), Redis 7, Go 1.27.1, k6 v2.3.0 |

Everything ran on this one laptop: k6, the API, Postgres, Redis and the worker. The laptop was also running a desktop session and unrelated containers, and the load average was 8–35 during the runs. **Treat these as a lower bound for a real deployment**, where the load generator and database would be on separate hosts with server-grade disks.

### Scenarios

- **uniform:** random transfers between 200 accounts.
- **hot-N:** every transfer goes from one of 200 random accounts to a single hot account with N shards.

Each scenario ran 20 s per round, 3 rounds, on a fresh database each round. The tables show the **median of the three rounds [min–max]**, taken per column. Lock wait is the mean from `kinesis_lock_wait_seconds`. Reconciliation passed after every scenario of every round.

#### Durable: Postgres data on the laptop disk (the `docker compose` default)

| Scenario | Transfers/s | p50 | p99 | Mean lock wait |
|---|---|---|---|---|
| uniform | **834** [727–860] | 49.6 ms [41.1–54.5] | 222 ms [218–222] | 19.1 ms |
| hot-1 | **147** [47–296] | 142 ms [136–213] | 2.23 s [0.93–5.38] | 240 ms |
| hot-16 | **736** [426–1,502] | 41.6 ms [24.3–77.1] | 491 ms [234–1,010] | 42.3 ms |
| hot-64 | **778** [373–1,285] | 50.6 ms [26.0–103] | 273 ms [159–380] | 19.6 ms |

#### Postgres on tmpfs (`loadtest/tmpfs.override.yml`, not durable)

| Scenario | Transfers/s | p50 | p99 | Mean lock wait |
|---|---|---|---|---|
| uniform | **3,555** [3,494–3,716] | 14.8 ms [14.0–15.1] | 50.4 ms [48.8–51.1] | 5.8 ms |
| hot-1 | **619** [564–641] | 80.0 ms [73.0–81.4] | 404 ms [379–499] | 72.6 ms |
| hot-16 | **3,161** [1,936–3,226] | 15.0 ms [14.8–23.1] | 75.3 ms [74.4–133] | 10.9 ms |
| hot-64 | **2,684** [2,623–2,932] | 19.5 ms [18.7–20.9] | 58.4 ms [53.8–68.6] | 8.1 ms |

### What the numbers say

- **The durable run is bounded by commit fsync on this disk, not by the ledger.** Sampling `pg_stat_activity` during a durable hot-16 run showed backends waiting on `LWLock:WALWrite` and `IO:WalSync`, plus row locks held by transactions that were themselves waiting to fsync. Moving the same database to tmpfs raises uniform throughput about 4× (834 → 3,555/s) and makes results repeatable. A QLC drive under a 99%-full btrfs gives erratic fsync latency, which is why the durable hot-account results vary 3–6× between rounds.
- **Sharding works.** With durability costs removed, one hot row caps the system at **619/s** with a 404 ms p99, because every transfer queues on that row. **16 shards give 3,161/s (5.1×)**, which matches the uniform workload. 64 shards add nothing on top of that: once shard contention is gone, the bottleneck moves to CPU, with k6, the API and Postgres sharing 16 threads.
- **The invariants held every time:** about 800k transfers across the six recorded rounds, with zero unbalanced transactions, zero shard/entry mismatches and zero negative balances.

Reproduce:

```bash
docker compose up --build -d && DURATION=20s ./loadtest/bench.sh
docker compose -f docker-compose.yml -f loadtest/tmpfs.override.yml up --build -d && DURATION=20s ./loadtest/bench.sh
```

## What I'd do next

- **One round trip per posting.** Move `Post` into a PL/pgSQL function (lock, check, write, outbox) so row locks aren't held across network hops. After fsync, this is the largest part of lock hold time.
- **Group commit tuning or `synchronous_commit` per class.** Keep full durability for money movement, and consider `commit_delay`/`commit_siblings` to batch fsyncs under load.
- **Incremental reconciliation.** Checkpoint per-shard sums at an entry id and only scan newer entries, which keeps reconciliation O(new data) and allows running it every few seconds.
- **Buffered aggregation for hot debit accounts**, and automatic re-sharding (currently `shards` is fixed at creation).
- **Multi-currency** with per-currency world accounts and FX postings, and holds/authorizations (two-phase transfers).
- **Partition `entries` and `transactions` by time**, and archive published outbox/idempotency rows in bulk (`DROP PARTITION` instead of `DELETE`).
- **AuthN/Z and rate limiting**, which aren't implemented at all. The API assumes a trusted network.
- **Consumer side:** a reference consumer group for `ledger.events` that dedupes on `outbox_id`, plus a DLQ.
- **Prometheus + Grafana in compose**, with an alert on `kinesis_reconciliation_ok == 0` and on outbox lag.
