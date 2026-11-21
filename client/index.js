const http = require("node:http");
const fs = require("node:fs");
const path = require("node:path");

// Submit endpoint: creates a job and returns its id. Job completion/failure
// stats are not measured here: the server reads them from the database.
const URL_ = process.env.SERVER_URL || "http://localhost:8080/export-csv";
const TOTAL = Number(process.env.TOTAL_REQUESTS || 10000);
const MAX_ITERATIONS = Number(process.env.MAX_ITERATIONS || 10);
const TIMEOUT_MS = Number(process.env.TIMEOUT_MS || 60000);
// Pause between bursts (submission is fast, so rounds would otherwise run back to back).
const ROUND_INTERVAL_MS = Number(process.env.ROUND_INTERVAL_MS || 1000);
const METRICS_FILE = process.env.METRICS_FILE || "/metrics/client.prom";
const SAMPLE_MS = Number(process.env.SAMPLE_MS || 1000);

const agent = new http.Agent({ keepAlive: true, maxSockets: Infinity });

let inflight = 0;
let peakInflight = 0;
let roundsCompleted = 0;

// Cumulative across rounds (Prometheus counters must never decrease).
//   sent      = submission requests sent
//   succeeded = submission returned a job id (exported as client_submissions_succeeded_total)
//   failed    = submission failed, no job was created (exported as client_submissions_failed_total)
const totals = { sent: 0, succeeded: 0, failed: 0 };
const errorTotals = {};

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// GET a URL and resolve with the parsed JSON body (2xx only).
function getJSON(url) {
  inflight++;
  peakInflight = Math.max(peakInflight, inflight);
  return new Promise((resolve, reject) => {
    const req = http.get(url, { agent, timeout: TIMEOUT_MS }, (res) => {
      const chunks = [];
      res.on("data", (c) => chunks.push(c));
      res.on("end", () => {
        if (res.statusCode < 200 || res.statusCode >= 300) {
          const err = new Error(`status ${res.statusCode}`);
          err.body = Buffer.concat(chunks).toString().slice(0, 200); // what the server said
          return reject(err);
        }
        try {
          resolve(JSON.parse(Buffer.concat(chunks).toString()));
        } catch (e) {
          reject(new Error("bad json"));
        }
      });
      res.on("error", reject);
    });
    req.on("timeout", () => req.destroy(new Error("timeout")));
    req.on("error", reject);
  }).finally(() => inflight--);
}

// Metric label / grouping key for an error.
function errorKey(err) {
  return err.code || err.message;
}

function countError(err) {
  const k = errorKey(err);
  errorTotals[k] = (errorTotals[k] || 0) + 1;
}

// Human-readable detail for logs: code, message, syscall, address and any
// response body.
function describeError(err) {
  // Connection errors to "localhost" come wrapped in an AggregateError (one
  // inner error per address tried); the useful details are on the inner ones.
  if (Array.isArray(err.errors) && err.errors.length) {
    return err.errors.map(describeError).join(" | ");
  }
  const parts = [];
  if (err.code) parts.push(`code=${err.code}`);
  if (err.message) parts.push(`message="${err.message}"`);
  if (err.syscall) parts.push(`syscall=${err.syscall}`);
  if (err.address) parts.push(`address=${err.address}${err.port ? ":" + err.port : ""}`);
  if (err.body) parts.push(`body=${JSON.stringify(err.body)}`);
  return parts.join(" ");
}

// API call: create a job. Resolves with its id, rejects if the submission
// failed. Does not touch any counters; main() records the outcome.
async function submit() {
  const body = await getJSON(URL_);
  if (!body.job_id) throw new Error("no job_id");
  return body.job_id;
}

// The file is an append-only log in Prometheus exposition syntax
// (`name{labels} value ts_ms`). Each snapshot adds one point per series; the
// reader groups lines by metric family before serving them.
function append(lines) {
  fs.mkdirSync(path.dirname(METRICS_FILE), { recursive: true });
  fs.appendFileSync(METRICS_FILE, lines.join("\n") + "\n");
}

// Snapshot of the in-memory counters, taken on a timer.
function sample() {
  const ts = ` ${Date.now()}`;
  append([
    "# HELP client_requests_sent_total Job submissions sent by the client",
    "# TYPE client_requests_sent_total counter",
    `client_requests_sent_total ${totals.sent}${ts}`,
    "# HELP client_submissions_succeeded_total Job submissions that returned a job id (sync request succeeded)",
    "# TYPE client_submissions_succeeded_total counter",
    `client_submissions_succeeded_total ${totals.succeeded}${ts}`,
    "# HELP client_submissions_failed_total Job submissions that failed (sync request failed)",
    "# TYPE client_submissions_failed_total counter",
    `client_submissions_failed_total ${totals.failed}${ts}`,
    "# HELP client_request_errors_total Failed submissions by error",
    "# TYPE client_request_errors_total counter",
    ...Object.entries(errorTotals).map(
      ([k, v]) => `client_request_errors_total{error=${JSON.stringify(k)}} ${v}${ts}`
    ),
    "# HELP client_rounds_completed Rounds completed so far",
    "# TYPE client_rounds_completed gauge",
    `client_rounds_completed ${roundsCompleted}${ts}`,
  ]);
}

function sampleRoundEnd(durationSec) {
  const ts = ` ${Date.now()}`;
  append([
    "# HELP client_round_duration_seconds Duration of a round's submission burst",
    "# TYPE client_round_duration_seconds gauge",
    `client_round_duration_seconds ${durationSec}${ts}`,
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

    // Burst: submit TOTAL jobs at once and wait for all of them to settle.
    const submitted = await Promise.allSettled(Array.from({ length: TOTAL }, submit));

    // Walk through the results and update the stats. "fulfilled" is
    // Promise.allSettled's state: the submission returned a job id.
    totals.sent += TOTAL;
    let ok = 0;
    const roundErrors = new Map(); // error key -> { count, sample }
    for (const r of submitted) {
      if (r.status === "fulfilled") {
        totals.succeeded++;
        ok++;
      } else {
        totals.failed++;
        countError(r.reason);
        const key = errorKey(r.reason);
        const entry = roundErrors.get(key) || { count: 0, sample: r.reason };
        entry.count++;
        roundErrors.set(key, entry);
      }
    }

    const durationSec = (Date.now() - start) / 1000;
    roundsCompleted = round;
    sample(); // exact point for the round
    sampleRoundEnd(durationSec);

    console.log(
      `[round ${round}] submitted=${ok} failed=${TOTAL - ok} peakInflight=${peakInflight} time=${durationSec.toFixed(1)}s`
    );
    // Why they failed: one line per distinct error, with the count and the
    // details of the first occurrence.
    for (const [key, { count, sample }] of roundErrors) {
      console.log(`[round ${round}]   ${count}x ${key} (${describeError(sample)})`);
    }

    if (!stopped && round < MAX_ITERATIONS) await sleep(ROUND_INTERVAL_MS);
  }
}

main().then(() => console.log("Done."));
