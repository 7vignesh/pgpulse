// k6 load test: 50 concurrent tenants against the PgPulse API.
//
//   SCENARIO=mixed (default) | ingest | analytics
//   BASE_URL   API base url (default http://localhost:3000)
//   TENANTS    number of tenants / virtual users (default 50)
//   DURATION   steady-state duration (default 2m)
//
// setup() creates TENANTS fresh tenants through the public API, so every VU is
// its own tenant with its own api key. That is what makes it "50 concurrent
// tenants": RLS scoping, per-key rate limiting and per-tenant index ranges are
// all exercised the way real customers would. Each VU runs an open loop with a
// fixed think time, so the offered load is roughly TENANTS / THINK_S req/s.
//
// Run with Docker (no local install): npm run bench:k6 (see benchmarks/README.md)

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Counter } from 'k6/metrics';

const BASE = __ENV.BASE_URL || 'http://localhost:3000';
const TENANTS = Number(__ENV.TENANTS || 50);
const DURATION = __ENV.DURATION || '2m';
const SCENARIO = __ENV.SCENARIO || 'mixed';
const THINK_S = Number(__ENV.THINK_S || 0.2);

const ENDPOINTS = ['/api/login', '/api/search', '/api/products', '/api/orders', '/api/cart', '/api/users/:id'];
const METHODS = ['GET', 'GET', 'GET', 'POST', 'PUT'];

const ingestLatency = new Trend('pgpulse_ingest_ms', true);
const analyticsLatency = new Trend('pgpulse_analytics_ms', true);
const rejected = new Counter('pgpulse_rejected');

// Thresholds are the pass/fail contract printed at the end of the run.
const thresholds = {
  http_req_failed: ['rate<0.01'],
};
if (SCENARIO !== 'analytics') {
  thresholds['pgpulse_ingest_ms'] = ['p(95)<250'];
}
if (SCENARIO !== 'ingest') {
  thresholds['pgpulse_analytics_ms'] = ['p(95)<500'];
}

export const options = {
  setupTimeout: '120s',
  scenarios: {
    tenants: {
      executor: 'constant-vus',
      vus: TENANTS,
      duration: DURATION,
      gracefulStop: '10s',
    },
  },
  thresholds,
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

export function setup() {
  const health = http.get(`${BASE}/health`);
  if (health.status !== 200 && health.status !== 503) {
    throw new Error(`API not reachable at ${BASE} (status ${health.status})`);
  }
  const run = Date.now();
  const tenants = [];
  for (let i = 0; i < TENANTS; i++) {
    const res = http.post(
      `${BASE}/v1/tenants`,
      JSON.stringify({ name: `k6-${run}-${i}`, plan: 'pro' }),
      { headers: { 'content-type': 'application/json' } },
    );
    if (res.status !== 201) throw new Error(`tenant create failed: ${res.status} ${res.body}`);
    tenants.push(res.json('api_key'));
  }
  // Give every tenant some history so analytics queries have rows to scan.
  for (const key of tenants) {
    const events = [];
    for (let j = 0; j < 500; j++) events.push(randomEvent());
    http.post(`${BASE}/v1/events/batch`, JSON.stringify({ events }), {
      headers: { 'content-type': 'application/json', 'x-api-key': key },
    });
  }
  return { tenants };
}

function pick(a) {
  return a[Math.floor(Math.random() * a.length)];
}

function randomEvent() {
  const r = Math.random();
  return {
    endpoint: pick(ENDPOINTS),
    method: pick(METHODS),
    status_code: r < 0.9 ? 200 : r < 0.97 ? 404 : 500,
    latency_ms: Math.floor(5 + Math.random() * Math.random() * 800),
    user_agent: 'k6-load-test',
  };
}

function ingest(key) {
  const batch = Math.random() < 0.5;
  const headers = { 'content-type': 'application/json', 'x-api-key': key };
  const res = batch
    ? http.post(
        `${BASE}/v1/events/batch`,
        JSON.stringify({ events: Array.from({ length: 50 }, randomEvent) }),
        { headers, tags: { op: 'ingest_batch' } },
      )
    : http.post(`${BASE}/v1/events`, JSON.stringify(randomEvent()), { headers, tags: { op: 'ingest_single' } });
  ingestLatency.add(res.timings.duration);
  if (res.status === 429 || res.status === 503) rejected.add(1);
  check(res, { 'ingest accepted': (r) => r.status === 201 });
}

const ANALYTICS = [
  '/v1/analytics/overview',
  '/v1/analytics/endpoints?limit=10',
  '/v1/analytics/timeseries?granularity=hour',
  '/v1/analytics/errors',
  '/v1/analytics/latency?endpoint=%2Fapi%2Fsearch',
];

function analytics(key) {
  const path = pick(ANALYTICS);
  const res = http.get(`${BASE}${path}`, {
    headers: { 'x-api-key': key },
    tags: { op: path.split('?')[0].replace('/v1/analytics/', 'analytics_') },
  });
  analyticsLatency.add(res.timings.duration);
  if (res.status === 429 || res.status === 503) rejected.add(1);
  check(res, { 'analytics ok': (r) => r.status === 200 });
}

export default function (data) {
  // VU ids start at 1; each VU is pinned to one tenant.
  const key = data.tenants[(__VU - 1) % data.tenants.length];
  if (SCENARIO === 'ingest') ingest(key);
  else if (SCENARIO === 'analytics') analytics(key);
  else if (Math.random() < 0.7) ingest(key);
  else analytics(key);
  sleep(THINK_S);
}
