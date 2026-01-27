#!/usr/bin/env bash
# Runs the three k6 scenarios used for the README numbers against a running
# stack (docker compose up) and prints lock-wait stats from /metrics per run.
set -euo pipefail
BASE_URL=${BASE_URL:-http://localhost:8080}
export BASE_URL VUS=${VUS:-64} DURATION=${DURATION:-30s}

lockwait() { curl -s "$BASE_URL/metrics" | awk '/^kinesis_lock_wait_seconds_(sum|count)/ {print $2}' | paste -sd' '; }

run() {
  echo "=== $1 (VUS=$VUS DURATION=$DURATION) load: $(cut -d' ' -f1-3 /proc/loadavg)"
  read -r s0 c0 <<<"$(lockwait)"
  env "${@:2}" k6 run -q loadtest/transfers.js 2>&1 | grep -E 'http_req_duration\.|http_reqs|checks_succeeded|iterations\.'
  read -r s1 c1 <<<"$(lockwait)"
  awk -v s0="$s0" -v s1="$s1" -v c0="$c0" -v c1="$c1" \
    'BEGIN { printf "    mean lock wait: %.2f ms over %d postings\n", 1000*(s1-s0)/(c1-c0), c1-c0 }'
  curl -s "$BASE_URL/v1/reconcile"; echo
}

run uniform       HOT_RATIO=0
run hot-1-shard   HOT_RATIO=1 HOT_SHARDS=1
run hot-16-shards HOT_RATIO=1 HOT_SHARDS=16
run hot-64-shards HOT_RATIO=1 HOT_SHARDS=64
