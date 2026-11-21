const http = require("node:http");
const fs = require("node:fs");

const PORT = Number(process.env.PORT || 9465);
const METRICS_FILE = process.env.METRICS_FILE || "/metrics/client.prom";
const CONTENT_TYPE = "text/plain; version=0.0.4";

// The file is an append-only log of exposition lines. The exposition format
// requires all lines of a metric family to be contiguous with one HELP/TYPE,
// so regroup them by family before serving. Samples keep their own timestamps.
function render(text) {
  const families = new Map(); // name -> { help, type, samples[] }
  const fam = (n) => {
    if (!families.has(n)) families.set(n, { help: null, type: null, samples: [] });
    return families.get(n);
  };

  const lines = text.split("\n");
  lines.pop(); // drop the trailing partial line (or "" if file ends with \n)

  for (const line of lines) {
    if (!line) continue;
    const meta = line.match(/^# (HELP|TYPE) (\S+) (.*)$/);
    if (meta) {
      const f = fam(meta[2]);
      if (meta[1] === "HELP" && !f.help) f.help = line;
      if (meta[1] === "TYPE" && !f.type) f.type = line;
      continue;
    }
    // Only serve samples that carry an explicit ms timestamp. A sample without
    // one would be stamped with scrape time and make Prometheus reject the
    // older timestamped points as out of order.
    if (!/ \d{13}$/.test(line)) continue;
    const name = line.match(/^[a-zA-Z_:][a-zA-Z0-9_:]*/);
    if (name) fam(name[0]).samples.push(line);
  }

  const out = [];
  for (const f of families.values()) {
    if (f.help) out.push(f.help);
    if (f.type) out.push(f.type);
    out.push(...f.samples);
  }
  return out.length ? out.join("\n") + "\n" : "";
}

http
  .createServer((req, res) => {
    if (req.url !== "/metrics") {
      res.writeHead(404).end("not found\n");
      return;
    }
    fs.readFile(METRICS_FILE, "utf8", (err, text) => {
      // Missing file = no round finished yet: serve an empty, valid exposition.
      res.writeHead(200, { "Content-Type": CONTENT_TYPE }).end(err ? "" : render(text));
    });
  })
  .listen(PORT, () => console.log(`serving ${METRICS_FILE} on :${PORT}/metrics`));
