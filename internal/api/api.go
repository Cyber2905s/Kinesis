// Package api serves the HTTP API and the dashboard.
package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Cyber2905s/kinesis/internal/ledger"
	"github.com/Cyber2905s/kinesis/internal/metrics"
)

type Server struct {
	pool *pgxpool.Pool
	log  *slog.Logger
	demo bool
	// committed counts postings committed by this process; the dashboard
	// derives live TPS from its deltas.
	committed atomic.Int64
}

func New(pool *pgxpool.Pool, log *slog.Logger, demo bool) *Server {
	return &Server{pool: pool, log: log, demo: demo}
}

func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Recoverer, s.observe)

	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := s.pool.Ping(r.Context()); err != nil {
			writeError(w, http.StatusServiceUnavailable, "database unavailable")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	r.Handle("/metrics", promhttp.Handler())

	r.Route("/v1", func(r chi.Router) {
		r.Get("/accounts", s.listAccounts)
		r.Get("/accounts/{id}", s.getAccount)
		r.Get("/accounts/{id}/transactions", s.listTxns)
		r.Get("/transactions", s.listTxns)
		r.Get("/reconcile", s.reconcile)
		r.Get("/stats", func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, http.StatusOK, map[string]int64{"committed": s.committed.Load()})
		})
	})
	return r
}

// observe records latency per route pattern and logs server errors.
func (s *Server) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r)
		route := chi.RouteContext(r.Context()).RoutePattern()
		if route == "" {
			route = "unmatched"
		}
		elapsed := time.Since(start)
		metrics.HTTPDuration.WithLabelValues(r.Method, route, strconv.Itoa(ww.Status())).Observe(elapsed.Seconds())
		lvl := slog.LevelDebug
		if ww.Status() >= 500 {
			lvl = slog.LevelError
		}
		s.log.Log(r.Context(), lvl, "http", "method", r.Method, "route", route, "status", ww.Status(), "duration_ms", elapsed.Milliseconds())
	})
}

func (s *Server) getAccount(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	a, err := ledger.GetAccount(r.Context(), s.pool, id)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, a)
}

func (s *Server) listAccounts(w http.ResponseWriter, r *http.Request) {
	after, limit, ok := page(w, r)
	if !ok {
		return
	}
	accts, err := ledger.ListAccounts(r.Context(), s.pool, after, limit)
	if err != nil {
		s.fail(w, err)
		return
	}
	var next string
	if len(accts) == limit {
		next = strconv.FormatInt(accts[len(accts)-1].ID, 10)
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": accts, "next_cursor": next})
}

// listTxns serves both /v1/transactions and /v1/accounts/{id}/transactions.
func (s *Server) listTxns(w http.ResponseWriter, r *http.Request) {
	var acct int64
	if chi.URLParam(r, "id") != "" {
		var ok bool
		if acct, ok = pathID(w, r); !ok {
			return
		}
		if _, err := ledger.GetAccount(r.Context(), s.pool, acct); err != nil {
			s.fail(w, err)
			return
		}
	}
	before, limit, ok := page(w, r)
	if !ok {
		return
	}
	txns, err := ledger.ListTxns(r.Context(), s.pool, acct, before, limit)
	if err != nil {
		s.fail(w, err)
		return
	}
	var next string
	if len(txns) == limit {
		next = strconv.FormatInt(txns[len(txns)-1].ID, 10)
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": txns, "next_cursor": next})
}

func (s *Server) reconcile(w http.ResponseWriter, r *http.Request) {
	rep, err := ledger.Reconcile(r.Context(), s.pool)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, rep)
}

func pathID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, "invalid account id")
		return 0, false
	}
	return id, true
}

// page parses ?cursor=&limit=. The cursor is the last ID of the previous page.
func page(w http.ResponseWriter, r *http.Request) (cursor int64, limit int, ok bool) {
	q := r.URL.Query()
	limit = 50
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 500 {
			writeError(w, http.StatusBadRequest, "limit must be 1..500")
			return 0, 0, false
		}
		limit = n
	}
	if v := q.Get("cursor"); v != "" {
		c, err := strconv.ParseInt(v, 10, 64)
		if err != nil || c < 0 {
			writeError(w, http.StatusBadRequest, "invalid cursor")
			return 0, 0, false
		}
		cursor = c
	}
	return cursor, limit, true
}

// statusOf maps ledger errors to HTTP statuses; ok is false for unexpected errors.
func statusOf(err error) (int, bool) {
	switch {
	case errors.Is(err, ledger.ErrNotFound):
		return http.StatusNotFound, true
	case errors.Is(err, ledger.ErrInsufficientFunds):
		return http.StatusUnprocessableEntity, true
	}
	return http.StatusInternalServerError, false
}

func (s *Server) fail(w http.ResponseWriter, err error) {
	code, known := statusOf(err)
	if !known {
		s.log.Error("request failed", "err", err)
		writeError(w, code, "internal error")
		return
	}
	writeError(w, code, err.Error())
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		code, b = http.StatusInternalServerError, []byte(`{"error":"encoding response"}`)
	}
	writeRaw(w, code, b)
}

func writeRaw(w http.ResponseWriter, code int, b []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(b)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
