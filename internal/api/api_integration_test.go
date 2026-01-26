package api_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Cyber2905s/kinesis/internal/api"
	"github.com/Cyber2905s/kinesis/internal/ledger"
	"github.com/Cyber2905s/kinesis/internal/testdb"
)

type client struct {
	t    *testing.T
	base string
	http *http.Client
}

func newClient(t *testing.T) *client {
	pool := testdb.Postgres(t)
	srv := httptest.NewServer(api.New(pool, slog.New(slog.DiscardHandler), true).Handler())
	t.Cleanup(srv.Close)
	tr := &http.Transport{MaxIdleConnsPerHost: 128}
	return &client{t: t, base: srv.URL, http: &http.Client{Transport: tr}}
}

var keySeq atomic.Int64

func newKey() string { return fmt.Sprintf("test-%d-%d", rand.Int64(), keySeq.Add(1)) }

// do sends a request and decodes a JSON object response into out (if non-nil).
func (c *client) do(method, path, key string, body any, out any) (int, http.Header) {
	c.t.Helper()
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, c.base+path, rd)
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		c.t.Error(err)
		return 0, nil
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			c.t.Errorf("%s %s: decode %q: %v", method, path, raw, err)
		}
	}
	return resp.StatusCode, resp.Header
}

func (c *client) account(name string, shards int, funds int64) int64 {
	c.t.Helper()
	var a ledger.Account
	if code, _ := c.do("POST", "/v1/accounts", newKey(), map[string]any{"name": name, "shards": shards}, &a); code != 201 {
		c.t.Fatalf("create account: %d", code)
	}
	if funds > 0 {
		if code, _ := c.do("POST", "/v1/deposits", newKey(), map[string]any{"account_id": a.ID, "amount": funds}, nil); code != 201 {
			c.t.Fatalf("deposit: %d", code)
		}
	}
	return a.ID
}

func (c *client) balance(id int64) int64 {
	c.t.Helper()
	var a ledger.Account
	if code, _ := c.do("GET", fmt.Sprintf("/v1/accounts/%d", id), "", nil, &a); code != 200 {
		c.t.Fatalf("get account %d: %d", id, code)
	}
	return a.Balance
}

func (c *client) assertReconciled() {
	c.t.Helper()
	var rep ledger.Report
	if code, _ := c.do("GET", "/v1/reconcile", "", nil, &rep); code != 200 || !rep.OK {
		c.t.Fatalf("reconcile: %d %+v", code, rep)
	}
}

func TestIdempotencyKeys(t *testing.T) {
	c := newClient(t)
	from, to := c.account("idem-from", 1, 1000), c.account("idem-to", 1, 0)
	body := map[string]any{"from_account_id": from, "to_account_id": to, "amount": 100}

	if code, _ := c.do("POST", "/v1/transfers", "", body, nil); code != 400 {
		t.Fatalf("missing key: got %d, want 400", code)
	}

	key := newKey()
	var first, second ledger.Txn
	code1, _ := c.do("POST", "/v1/transfers", key, body, &first)
	code2, hdr := c.do("POST", "/v1/transfers", key, body, &second)
	if code1 != 201 || code2 != 201 || first != second || hdr.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("replay: %d %+v / %d %+v replayed=%q", code1, first, code2, second, hdr.Get("Idempotent-Replayed"))
	}
	if got := c.balance(from); got != 900 {
		t.Fatalf("balance after replay = %d, want 900 (debited once)", got)
	}

	body["amount"] = 101
	if code, _ := c.do("POST", "/v1/transfers", key, body, nil); code != 422 {
		t.Fatalf("key reuse with different body: got %d, want 422", code)
	}

	// Business rejections are stored and replayed too.
	body["amount"] = 1_000_000
	rejectKey := newKey()
	for range 2 {
		if code, _ := c.do("POST", "/v1/transfers", rejectKey, body, nil); code != 422 {
			t.Fatalf("insufficient funds: got %d, want 422", code)
		}
	}
}

func TestConcurrentRequestsSameKey(t *testing.T) {
	c := newClient(t)
	from, to := c.account("race-from", 1, 1000), c.account("race-to", 1, 0)
	key := newKey()
	body := map[string]any{"from_account_id": from, "to_account_id": to, "amount": 10}
	var wg sync.WaitGroup
	var ids sync.Map
	for range 32 {
		wg.Go(func() {
			var txn ledger.Txn
			if code, _ := c.do("POST", "/v1/transfers", key, body, &txn); code != 201 {
				t.Errorf("got %d", code)
			}
			ids.Store(txn.ID, true)
		})
	}
	wg.Wait()
	n := 0
	ids.Range(func(_, _ any) bool { n++; return true })
	if n != 1 || c.balance(from) != 990 {
		t.Fatalf("32 concurrent requests with one key produced %d txns, balance %d", n, c.balance(from))
	}
}

// TestConcurrentTransfersPreserveInvariants fires thousands of parallel
// transfers through the full HTTP + idempotency + ledger path, including a
// hot sharded account that both receives and pays out, and in both
// directions between the same pairs (the classic deadlock shape).
func TestConcurrentTransfersPreserveInvariants(t *testing.T) {
	c := newClient(t)
	const (
		users     = 30
		funds     = 10_000
		transfers = 5000
		workers   = 64
	)
	ids := make([]int64, users)
	for i := range ids {
		ids[i] = c.account(fmt.Sprintf("user-%d", i), 1, funds)
	}
	merchant := c.account("hot-merchant", 16, 0)
	all := append(append([]int64{}, ids...), merchant)

	var ok, rejected atomic.Int64
	jobs := make(chan [2]int64)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for j := range jobs {
				body := map[string]any{"from_account_id": j[0], "to_account_id": j[1], "amount": 1 + rand.Int64N(500)}
				switch code, _ := c.do("POST", "/v1/transfers", newKey(), body, nil); code {
				case 201:
					ok.Add(1)
				case 422:
					rejected.Add(1)
				default:
					t.Errorf("transfer %v: unexpected status %d", j, code)
				}
			}
		})
	}
	for range transfers {
		var from, to int64
		switch r := rand.IntN(10); {
		case r < 4: // pay the hot merchant
			from, to = ids[rand.IntN(users)], merchant
		case r < 5: // merchant refunds (debits lock all 16 shards)
			from, to = merchant, ids[rand.IntN(users)]
		default:
			from, to = ids[rand.IntN(users)], ids[rand.IntN(users)]
		}
		if from == to {
			to = ids[(rand.IntN(users-1)+1+slices.Index(ids, from))%users]
		}
		jobs <- [2]int64{from, to}
	}
	close(jobs)
	wg.Wait()

	var total int64
	for _, id := range all {
		b := c.balance(id)
		if b < 0 {
			t.Errorf("account %d went negative: %d", id, b)
		}
		total += b
	}
	if total != users*funds {
		t.Fatalf("money not conserved: total %d, want %d", total, users*funds)
	}
	c.assertReconciled()
	t.Logf("%d committed, %d rejected for insufficient funds", ok.Load(), rejected.Load())
	if ok.Load() == 0 {
		t.Fatal("no transfer succeeded")
	}
}

func TestDemoBurst(t *testing.T) {
	c := newClient(t)
	var out struct {
		Committed, Rejected, Errors int
		Reconcile                   ledger.Report
	}
	for range 2 { // second run reuses the seeded accounts
		if code, _ := c.do("POST", "/v1/demo/burst", "", map[string]int{"transfers": 500, "concurrency": 16}, &out); code != 200 {
			t.Fatalf("burst: %d", code)
		}
		if out.Errors != 0 || out.Committed == 0 || out.Committed+out.Rejected != 500 || !out.Reconcile.OK {
			t.Fatalf("burst result %+v", out)
		}
	}
}
