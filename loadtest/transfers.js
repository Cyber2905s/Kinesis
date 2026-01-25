// k6 load test: POST /v1/transfers with unique idempotency keys.
//
//   k6 run loadtest/transfers.js                      # uniform traffic
//   HOT_RATIO=1 HOT_SHARDS=1  k6 run loadtest/transfers.js   # every transfer credits one unsharded account
//   HOT_RATIO=1 HOT_SHARDS=16 k6 run loadtest/transfers.js   # same, account sharded 16 ways
//
// Env: BASE_URL, VUS (64), DURATION (30s), ACCOUNTS (200), HOT_RATIO (0), HOT_SHARDS (16)
import http from "k6/http";
import { check } from "k6";

const BASE = __ENV.BASE_URL || "http://localhost:8080";
const ACCOUNTS = +(__ENV.ACCOUNTS || 200);
const HOT_RATIO = +(__ENV.HOT_RATIO || 0);
const HOT_SHARDS = +(__ENV.HOT_SHARDS || 16);
const RUN = Date.now();

export const options = {
  scenarios: {
    transfers: { executor: "constant-vus", vus: +(__ENV.VUS || 64), duration: __ENV.DURATION || "30s" },
  },
  summaryTrendStats: ["avg", "med", "p(90)", "p(99)", "max"],
  thresholds: {
    "http_req_failed{scenario:transfers}": ["rate<0.01"],
    checks: ["rate>0.99"],
  },
};

function post(path, body, key) {
  return http.post(`${BASE}${path}`, JSON.stringify(body), {
    headers: { "Content-Type": "application/json", "Idempotency-Key": key },
  });
}

export function setup() {
  const create = (name, shards) => {
    const r = post("/v1/accounts", { name, shards }, `k6-${RUN}-acct-${name}`);
    if (r.status !== 201) throw new Error(`create ${name}: ${r.status} ${r.body}`);
    const id = r.json("id");
    // Fund generously: this measures throughput, not the NSF path.
    const d = post("/v1/deposits", { account_id: id, amount: 1e12 }, `k6-${RUN}-dep-${id}`);
    if (d.status !== 201) throw new Error(`deposit ${id}: ${d.status}`);
    return id;
  };
  const ids = [];
  for (let i = 0; i < ACCOUNTS; i++) ids.push(create(`k6-${RUN}-${i}`, 1));
  const hot = create(`k6-${RUN}-hot`, HOT_SHARDS);
  return { ids, hot };
}

export default function ({ ids, hot }) {
  const from = ids[Math.floor(Math.random() * ids.length)];
  let to;
  if (Math.random() < HOT_RATIO) {
    to = hot;
  } else {
    do { to = ids[Math.floor(Math.random() * ids.length)]; } while (to === from);
  }
  const r = post(
    "/v1/transfers",
    { from_account_id: from, to_account_id: to, amount: 1 + Math.floor(Math.random() * 10000) },
    `k6-${RUN}-${__VU}-${__ITER}`,
  );
  check(r, { "201 created": (res) => res.status === 201 });
}
