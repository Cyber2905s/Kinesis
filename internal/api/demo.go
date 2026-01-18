package api

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Cyber2905s/kinesis/internal/ledger"
)

type burstReq struct {
	Transfers   int `json:"transfers"`
	Concurrency int `json:"concurrency"`
}

type burstResult struct {
	Transfers int           `json:"transfers"`
	Committed int           `json:"committed"`
	Rejected  int           `json:"rejected"` // insufficient funds: expected, proves the guard works
	Errors    int           `json:"errors"`
	Duration  int64         `json:"duration_ms"`
	TPS       float64       `json:"tps"`
	P50       float64       `json:"p50_ms"`
	P99       float64       `json:"p99_ms"`
	Reconcile ledger.Report `json:"reconcile"`
}

// burst fires transfers between demo accounts through the same idempotent
// posting path the HTTP handlers use (minus HTTP), then reconciles.
// Roughly half of them hit a 16-shard "hot" merchant account.
func (s *Server) burst(w http.ResponseWriter, r *http.Request) {
	req := burstReq{Transfers: 2000, Concurrency: 32}
	if r.ContentLength != 0 {
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<10)).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON")
			return
		}
	}
	if req.Transfers < 1 || req.Transfers > 20000 || req.Concurrency < 1 || req.Concurrency > 128 {
		writeError(w, http.StatusBadRequest, "transfers must be 1..20000, concurrency 1..128")
		return
	}
	ctx := r.Context()
	users, merchant, err := s.demoAccounts(ctx)
	if err != nil {
		s.fail(w, err)
		return
	}

	var (
		mu    sync.Mutex
		out   = burstResult{Transfers: req.Transfers}
		lat   = make([]time.Duration, 0, req.Transfers)
		jobs  = make(chan int)
		wg    sync.WaitGroup
		start = time.Now()
	)
	for range req.Concurrency {
		wg.Go(func() {
			for i := range jobs {
				from, to := users[rand.IntN(len(users))], merchant
				switch n := rand.IntN(10); {
				case n < 1:
					from, to = merchant, users[rand.IntN(len(users))]
				case n < 5:
					for to = from; to == from; to = users[rand.IntN(len(users))] {
					}
				}
				amount := 1 + rand.Int64N(20_000)
				key := fmt.Sprintf("burst-%d-%d", start.UnixNano(), i)
				t0 := time.Now()
				hash := sha256.Sum256([]byte(key))
				res, err := s.idempotent(ctx, key, hash[:], func(tx pgx.Tx) (int, any, error) {
					t, err := ledger.Post(ctx, tx, ledger.Transfer, from, to, amount)
					return http.StatusCreated, t, err
				})
				d := time.Since(t0)
				if err == nil {
					s.count(ledger.Transfer, res)
				}
				mu.Lock()
				lat = append(lat, d)
				switch {
				case err != nil:
					out.Errors++
				case res.status == http.StatusCreated:
					out.Committed++
				default:
					out.Rejected++
				}
				mu.Unlock()
			}
		})
	}
	for i := range req.Transfers {
		jobs <- i
	}
	close(jobs)
	wg.Wait()

	elapsed := time.Since(start)
	slices.Sort(lat)
	ms := func(q float64) float64 { return float64(lat[int(q*float64(len(lat)-1))].Microseconds()) / 1000 }
	out.Duration, out.TPS = elapsed.Milliseconds(), float64(out.Committed)/elapsed.Seconds()
	out.P50, out.P99 = ms(0.50), ms(0.99)
	if out.Reconcile, err = ledger.Reconcile(ctx, s.pool); err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

// demoAccounts returns the demo users and the hot merchant, creating and
// funding them on first use. An advisory lock stops concurrent bursts from
// seeding twice.
func (s *Server) demoAccounts(ctx context.Context) (users []int64, merchant int64, err error) {
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(0x64656d6f)`); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `SELECT id, name FROM accounts WHERE name LIKE 'demo-%' ORDER BY id`)
		if err != nil {
			return err
		}
		type acct struct {
			ID   int64
			Name string
		}
		existing, err := pgx.CollectRows(rows, pgx.RowToStructByPos[acct])
		if err != nil {
			return err
		}
		for _, a := range existing {
			if a.Name == "demo-merchant" {
				merchant = a.ID
			} else {
				users = append(users, a.ID)
			}
		}
		if merchant != 0 {
			return nil
		}
		m, err := ledger.CreateAccount(ctx, tx, "demo-merchant", false, 16)
		if err != nil {
			return err
		}
		merchant = m.ID
		for i := range 20 {
			a, err := ledger.CreateAccount(ctx, tx, fmt.Sprintf("demo-user-%02d", i+1), false, 1)
			if err != nil {
				return err
			}
			if _, err := ledger.Post(ctx, tx, ledger.Deposit, ledger.WorldAccountID, a.ID, 1_000_000); err != nil {
				return err
			}
			users = append(users, a.ID)
		}
		return nil
	})
	return users, merchant, err
}
