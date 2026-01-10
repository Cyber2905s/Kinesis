package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/jackc/pgx/v5"

	"github.com/Cyber2905s/kinesis/internal/ledger"
)

// errKeyReuse means an idempotency key came back with a different request.
var errKeyReuse = errors.New("idempotency key reused with a different request")

type result struct {
	status   int
	body     []byte
	replayed bool
}

// requestHash binds a key to one exact request.
func requestHash(r *http.Request, body []byte) []byte {
	h := sha256.New()
	h.Write([]byte(r.Method + " " + r.URL.Path + "\n"))
	h.Write(body)
	return h.Sum(nil)
}

// idempotent runs op and stores its response under key in the same
// transaction, so the ledger writes and the idempotency record commit
// together or not at all.
//
// The record is inserted after op with ON CONFLICT DO NOTHING. If another
// request holds the same key uncommitted, the insert waits for it; if that
// request commits, ours finds the conflict, rolls back its own writes and
// returns the stored response instead. Exactly one execution wins, with no
// extra round trip on the (common) first-time path.
//
// Business rejections (404, 422) are stored too, like Stripe: replaying the
// key replays the rejection. They are stored in their own statement after
// rolling back, so a failed op can never leave partial writes behind.
// Unexpected errors are not stored; the client may retry with the same key.
func (s *Server) idempotent(ctx context.Context, key string, hash []byte, op func(pgx.Tx) (int, any, error)) (result, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return result{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit

	status, v, opErr := op(tx)
	if opErr != nil {
		code, known := statusOf(opErr)
		if !known {
			return result{}, opErr
		}
		if err := tx.Rollback(ctx); err != nil {
			return result{}, err
		}
		return s.store(ctx, key, hash, code, map[string]string{"error": opErr.Error()}, s.pool)
	}
	res, err := s.store(ctx, key, hash, status, v, tx)
	if err != nil || res.replayed {
		return res, err
	}
	return res, tx.Commit(ctx)
}

// store inserts the idempotency record, or loads the existing one on conflict.
func (s *Server) store(ctx context.Context, key string, hash []byte, status int, v any, db ledger.DB) (result, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return result{}, err
	}
	var inserted bool
	err = db.QueryRow(ctx, `
		INSERT INTO idempotency_keys (key, request_hash, status, response) VALUES ($1, $2, $3, $4)
		ON CONFLICT (key) DO NOTHING RETURNING true`, key, hash, status, body).Scan(&inserted)
	if err == nil {
		return result{status: status, body: body}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return result{}, err
	}
	// Conflict: someone committed this key first. Under READ COMMITTED this
	// next statement sees their row.
	var stored result
	var storedHash []byte
	if err := db.QueryRow(ctx, `SELECT request_hash, status, response FROM idempotency_keys WHERE key = $1`, key).
		Scan(&storedHash, &stored.status, &stored.body); err != nil {
		return result{}, err
	}
	if !bytes.Equal(storedHash, hash) {
		return result{}, errKeyReuse
	}
	stored.replayed = true
	return stored, nil
}
