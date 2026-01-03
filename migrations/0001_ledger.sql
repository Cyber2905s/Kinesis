-- Accounts are immutable after creation; balances live in account_shards.
CREATE TABLE accounts (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name            TEXT        NOT NULL CHECK (length(name) BETWEEN 1 AND 100),
    allow_overdraft BOOLEAN     NOT NULL DEFAULT false,
    shards          INT         NOT NULL DEFAULT 1 CHECK (shards BETWEEN 1 AND 64),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per (account, shard). A hot account spreads its balance over
-- several rows so concurrent credits lock different rows.
CREATE TABLE account_shards (
    account_id BIGINT NOT NULL REFERENCES accounts (id),
    shard      INT    NOT NULL,
    balance    BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (account_id, shard)
);

CREATE TABLE transactions (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kind           TEXT        NOT NULL CHECK (kind IN ('deposit', 'withdrawal', 'transfer')),
    debit_account  BIGINT      NOT NULL REFERENCES accounts (id),
    credit_account BIGINT      NOT NULL REFERENCES accounts (id),
    amount         BIGINT      NOT NULL CHECK (amount > 0),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (debit_account <> credit_account)
);
CREATE INDEX transactions_debit_idx ON transactions (debit_account, id);
CREATE INDEX transactions_credit_idx ON transactions (credit_account, id);

-- Signed amounts in minor units: negative = debit, positive = credit.
-- Every transaction's entries sum to zero, so all entries sum to zero.
CREATE TABLE entries (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tx_id      BIGINT NOT NULL REFERENCES transactions (id),
    account_id BIGINT NOT NULL,
    shard      INT    NOT NULL,
    amount     BIGINT NOT NULL CHECK (amount <> 0),
    FOREIGN KEY (account_id, shard) REFERENCES account_shards (account_id, shard)
);
CREATE INDEX entries_tx_idx ON entries (tx_id);

-- The external world: source of deposits, sink of withdrawals. It is the only
-- account allowed to go (deeply) negative; its balance mirrors total customer
-- funds. Sharded because every deposit and withdrawal touches it.
INSERT INTO accounts (name, allow_overdraft, shards) VALUES ('world', true, 16);
INSERT INTO account_shards (account_id, shard) SELECT 1, g FROM generate_series(0, 15) g;

CREATE TABLE idempotency_keys (
    key          TEXT        PRIMARY KEY,
    request_hash BYTEA       NOT NULL,
    status       INT         NOT NULL,
    response     JSONB       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idempotency_keys_created_idx ON idempotency_keys (created_at);

-- Transactional outbox: written in the same transaction as the ledger rows,
-- drained by the worker.
CREATE TABLE outbox (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    payload    JSONB       NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
