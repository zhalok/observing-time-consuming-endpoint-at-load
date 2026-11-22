const http = require("node:http");

// Target endpoint, picked via SERVER_URL:
//   /process-csv-async -> creates a job and returns its id (202)
//   /process-csv-sync  -> does the work in the request and returns the total (200)
// Job completion/failure stats are not measured here: the server reads them
// from the database.
const URL_ = process.env.SERVER_URL || "http://localhost:8080/process-csv-async";
const TOTAL = Number(process.env.TOTAL_REQUESTS || 10000);
const MAX_ITERATIONS = Number(process.env.MAX_ITERATIONS || 10);
const TIMEOUT_MS = Number(process.env.TIMEOUT_MS || 60000);
// Pause between bursts (submission is fast, so rounds would otherwise run back to back).
const ROUND_INTERVAL_MS = Number(process.env.ROUND_INTERVAL_MS || 1000);

const agent = new http.Agent({ keepAlive: true, maxSockets: Infinity });

let inflight = 0;
let peakInflight = 0;

// Cumulative across rounds.
//   sent      = requests sent
//   succeeded = request succeeded
//   failed    = request failed
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

// API call: hit the endpoint. Resolves on success (async: a job id came back,
// sync: the total came back), rejects otherwise. Does not touch any counters;
// main() records the outcome.
async function submit() {
  totals.sent++; // counted when the request is sent, so the metric moves during the burst
  const body = await getJSON(URL_);
  if (!body.job_id && body.total_c_acctbal === undefined) throw new Error("unexpected response");
  return body;
}

let stopped = false;
process.on("SIGINT", () => { stopped = true; console.log("\nStopping after current batch..."); });
process.on("SIGTERM", () => { stopped = true; });

const REPORT_MS = Number(process.env.INFLIGHT_REPORT_MS || 1000);
setInterval(() => console.log(`  inflight=${inflight}`), REPORT_MS).unref();

async function main() {
  for (let round = 1; !stopped && round <= MAX_ITERATIONS; round++) {
    const start = Date.now();
    peakInflight = 0;

    // Burst: submit TOTAL jobs at once and wait for all of them to settle.
    const submitted = await Promise.allSettled(Array.from({ length: TOTAL }, submit));

    // Walk through the results and update the stats. "fulfilled" is
    // Promise.allSettled's state: the submission returned a job id.
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
