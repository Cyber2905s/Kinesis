package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/jackc/pgx/v5"

	"github.com/Cyber2905s/kinesis/internal/ledger"
	"github.com/Cyber2905s/kinesis/internal/metrics"
)

// write handles the shared shape of every mutating endpoint: require an
// Idempotency-Key, decode and validate the body, run op idempotently.
// It returns the result it wrote, or ok=false if it wrote an error of its own.
func (s *Server) write(w http.ResponseWriter, r *http.Request, req interface{ validate() error }, op func(pgx.Tx) (int, any, error)) (res result, ok bool) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" || len(key) > 255 {
		writeError(w, http.StatusBadRequest, "Idempotency-Key header is required (1-255 chars)")
		return res, false
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<10))
	if err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, "body too large")
		return res, false
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return res, false
	}
	if err := req.validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return res, false
	}
	res, err = s.idempotent(r.Context(), key, requestHash(r, body), op)
	switch {
	case errors.Is(err, errKeyReuse):
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return res, false
	case err != nil:
		s.fail(w, err)
		return res, false
	}
	if res.replayed {
		metrics.IdempotentReplays.Inc()
		w.Header().Set("Idempotent-Replayed", "true")
	}
	writeRaw(w, res.status, res.body)
	return res, true
}

type createAccountReq struct {
	Name           string `json:"name"`
	AllowOverdraft bool   `json:"allow_overdraft"`
	Shards         int32  `json:"shards"`
}

func (q *createAccountReq) validate() error {
	if q.Shards == 0 {
		q.Shards = 1
	}
	if n := len(q.Name); n < 1 || n > 100 {
		return errors.New("name must be 1-100 bytes")
	}
	if q.Shards < 1 || q.Shards > 64 {
		return errors.New("shards must be 1..64")
	}
	return nil
}

func (s *Server) createAccount(w http.ResponseWriter, r *http.Request) {
	var req createAccountReq
	s.write(w, r, &req, func(tx pgx.Tx) (int, any, error) {
		a, err := ledger.CreateAccount(r.Context(), tx, req.Name, req.AllowOverdraft, req.Shards)
		return http.StatusCreated, a, err
	})
}

type moveReq struct {
	AccountID int64 `json:"account_id"`
	From      int64 `json:"from_account_id"`
	To        int64 `json:"to_account_id"`
	Amount    int64 `json:"amount"`

	kind          ledger.Kind
	debit, credit int64
}

// validate resolves the debit/credit pair for the request's kind, rejecting
// fields that don't belong to it.
func (q *moveReq) validate() error {
	if q.Amount < 1 || q.Amount > ledger.MaxAmount {
		return fmt.Errorf("amount must be 1..%d (minor units)", ledger.MaxAmount)
	}
	switch {
	case q.kind == ledger.Transfer && q.AccountID == 0 && q.From > 0 && q.To > 0:
		q.debit, q.credit = q.From, q.To
	case q.kind == ledger.Deposit && q.AccountID > 0 && q.From == 0 && q.To == 0:
		q.debit, q.credit = ledger.WorldAccountID, q.AccountID
	case q.kind == ledger.Withdrawal && q.AccountID > 0 && q.From == 0 && q.To == 0:
		q.debit, q.credit = q.AccountID, ledger.WorldAccountID
	default:
		return errors.New("transfers take from_account_id and to_account_id; deposits and withdrawals take account_id")
	}
	if q.AccountID == ledger.WorldAccountID || q.From == ledger.WorldAccountID || q.To == ledger.WorldAccountID {
		return errors.New("the world account is only reachable via deposits and withdrawals")
	}
	if q.debit == q.credit {
		return errors.New("from_account_id and to_account_id must differ")
	}
	return nil
}

func (s *Server) deposit(w http.ResponseWriter, r *http.Request)  { s.move(w, r, ledger.Deposit) }
func (s *Server) withdraw(w http.ResponseWriter, r *http.Request) { s.move(w, r, ledger.Withdrawal) }
func (s *Server) transfer(w http.ResponseWriter, r *http.Request) { s.move(w, r, ledger.Transfer) }

func (s *Server) move(w http.ResponseWriter, r *http.Request, kind ledger.Kind) {
	req := moveReq{kind: kind}
	res, ok := s.write(w, r, &req, func(tx pgx.Tx) (int, any, error) {
		t, err := ledger.Post(r.Context(), tx, kind, req.debit, req.credit, req.Amount)
		return http.StatusCreated, t, err
	})
	if ok {
		s.count(kind, res)
	}
}

// count records a committed posting outcome. Replays are counted separately
// by write, so TPS reflects real ledger work only.
func (s *Server) count(kind ledger.Kind, res result) {
	if res.replayed {
		return
	}
	outcome := "ok"
	switch res.status {
	case http.StatusCreated:
		s.committed.Add(1)
	case http.StatusUnprocessableEntity:
		outcome = "insufficient_funds"
	case http.StatusNotFound:
		outcome = "not_found"
	}
	metrics.Transactions.WithLabelValues(string(kind), outcome).Inc()
}
