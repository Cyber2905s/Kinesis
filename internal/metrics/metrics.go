// Package metrics holds the Prometheus collectors shared across the binary.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// TPS: rate(kinesis_ledger_transactions_total{result="ok"}[1m])
	Transactions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kinesis_ledger_transactions_total",
		Help: "Ledger postings by kind and result.",
	}, []string{"kind", "result"})

	// p50/p99: histogram_quantile(0.99, sum by (le, route) (rate(kinesis_http_request_duration_seconds_bucket[1m])))
	HTTPDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kinesis_http_request_duration_seconds",
		Help:    "HTTP request latency.",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 14), // 0.5ms .. ~4s
	}, []string{"method", "route", "code"})

	LockWait = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "kinesis_lock_wait_seconds",
		Help:    "Time spent acquiring account row locks (SELECT ... FOR NO KEY UPDATE).",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 16), // 0.1ms .. ~3s
	})

	IdempotentReplays = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kinesis_idempotent_replays_total",
		Help: "Write requests answered from a stored idempotency record.",
	})

	OutboxPublished = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kinesis_outbox_published_total",
		Help: "Ledger events published to the broker.",
	})

	OutboxLag = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "kinesis_outbox_lag_seconds",
		Help:    "Delay between commit of a ledger event and its publication.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
	})

	ReconOK = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kinesis_reconciliation_ok",
		Help: "1 if the last reconciliation found no discrepancies, else 0.",
	})
)
