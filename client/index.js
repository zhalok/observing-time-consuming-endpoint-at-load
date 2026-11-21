const http = require("node:http");
const fs = require("node:fs");
const path = require("node:path");

const URL_ = process.env.SERVER_URL || "http://localhost:8080/export-csv";
const TOTAL = Number(process.env.TOTAL_REQUESTS || 10000);
const MAX_ITERATIONS = Number(process.env.MAX_ITERATIONS || 10);
const TIMEOUT_MS = Number(process.env.TIMEOUT_MS || 60000);
const METRICS_FILE = process.env.METRICS_FILE || "/metrics/client.prom";
const SAMPLE_MS = Number(process.env.SAMPLE_MS || 1000);

const agent = new http.Agent({ keepAlive: true, maxSockets: Infinity });

let inflight = 0;
let peakInflight = 0;
let roundsCompleted = 0;

// Updated the moment each request completes. Cumulative across rounds
// (Prometheus counters must never decrease).
const totals = { sent: 0, succeeded: 0, failed: 0 };
const errorTotals = {};

function request() {
  inflight++;
  totals.sent++;
  peakInflight = Math.max(peakInflight, inflight);
  return doRequest().then(
    () => { totals.succeeded++; },
    (err) => {
      totals.failed++;
      const k = err.code || err.message;
      errorTotals[k] = (errorTotals[k] || 0) + 1;
      throw err;
    }
  ).finally(() => inflight--);
}

function doRequest() {
  return new Promise((resolve, reject) => {
    const req = http.get(URL_, { agent, timeout: TIMEOUT_MS }, (res) => {
      res.resume();
      res.on("end", () =>
        res.statusCode >= 200 && res.statusCode < 300
          ? resolve()
          : reject(new Error(`status ${res.statusCode}`))
      );
      res.on("error", reject);
    });
    req.on("timeout", () => req.destroy(new Error("timeout")));
    req.on("error", reject);
  });
}

// The file is an append-only log in Prometheus exposition syntax
// (`name{labels} value ts_ms`). Each snapshot adds one point per series; the
// reader groups lines by metric family before serving them.
function append(lines) {
  fs.mkdirSync(path.dirname(METRICS_FILE), { recursive: true });
  fs.appendFileSync(METRICS_FILE, lines.join("\n") + "\n");
}

// Snapshot of the live in-memory counters, taken on a timer.
function sample() {
  const ts = ` ${Date.now()}`;
  append([
    "# HELP client_requests_sent_total Requests sent by the client",
    "# TYPE client_requests_sent_total counter",
    `client_requests_sent_total ${totals.sent}${ts}`,
    "# HELP client_requests_succeeded_total Requests that succeeded",
    "# TYPE client_requests_succeeded_total counter",
    `client_requests_succeeded_total ${totals.succeeded}${ts}`,
    "# HELP client_requests_failed_total Requests that failed",
    "# TYPE client_requests_failed_total counter",
    `client_requests_failed_total ${totals.failed}${ts}`,
    "# HELP client_request_errors_total Failed requests by error",
    "# TYPE client_request_errors_total counter",
    ...Object.entries(errorTotals).map(
      ([k, v]) => `client_request_errors_total{error=${JSON.stringify(k)}} ${v}${ts}`
    ),
    "# HELP client_requests_inflight Requests currently in flight",
    "# TYPE client_requests_inflight gauge",
    `client_requests_inflight ${inflight}${ts}`,
    "# HELP client_rounds_completed Rounds completed so far",
    "# TYPE client_rounds_completed gauge",
    `client_rounds_completed ${roundsCompleted}${ts}`,
  ]);
}

function sampleRoundEnd(durationSec) {
  const ts = ` ${Date.now()}`;
  append([
    "# HELP client_round_duration_seconds Duration of a round",
    "# TYPE client_round_duration_seconds gauge",
    `client_round_duration_seconds ${durationSec}${ts}`,
    "# HELP client_round_peak_inflight Peak in-flight requests in a round",
    "# TYPE client_round_peak_inflight gauge",
    `client_round_peak_inflight ${peakInflight}${ts}`,
  ]);
}

let stopped = false;
process.on("SIGINT", () => { stopped = true; console.log("\nStopping after current batch..."); });
process.on("SIGTERM", () => { stopped = true; });

const REPORT_MS = Number(process.env.INFLIGHT_REPORT_MS || 1000);
setInterval(() => console.log(`  inflight=${inflight}`), REPORT_MS).unref();
setInterval(sample, SAMPLE_MS).unref();

async function main() {
  // Start each run with a fresh log: leftovers from older runs/formats (e.g.
  // samples without a timestamp) would make Prometheus reject newer points as
  // out of order.
  fs.mkdirSync(path.dirname(METRICS_FILE), { recursive: true });
  fs.writeFileSync(METRICS_FILE, "");
  sample(); // baseline point before any load
  for (let round = 1; !stopped && round <= MAX_ITERATIONS; round++) {
    const start = Date.now();
    peakInflight = 0;
    const results = await Promise.allSettled(Array.from({ length: TOTAL }, request));
    const durationSec = (Date.now() - start) / 1000;
    roundsCompleted = round;

    const ok = results.filter((r) => r.status === "fulfilled").length;
    const errors = {};
    for (const r of results) {
      if (r.status === "rejected") {
        const k = r.reason.code || r.reason.message;
        errors[k] = (errors[k] || 0) + 1;
      }
    }
    sample(); // exact final point for the round
    sampleRoundEnd(durationSec);

    console.log(
      `[round ${round}] total=${TOTAL} succeeded=${ok} failed=${TOTAL - ok} peakInflight=${peakInflight} time=${durationSec.toFixed(1)}s` +
        (TOTAL - ok ? ` errors=${JSON.stringify(errors)}` : "")
    );
  }
}

main().then(() => console.log("Done."));
