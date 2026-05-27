const http = require("http");

// Lazy load logic to prevent startup crashes
let fetchAndIngestAllReports = null;
let loadError = null;

try {
  const logic = require("./fetchReports");
  fetchAndIngestAllReports = logic.fetchAndIngestAllReports;
} catch (err) {
  console.error("[cron] Failed to load fetchReports logic:", err.message);
  loadError = err.message;
}

const TRANSFORM_WORKER_URL = process.env.TRANSFORM_WORKER_URL ||
  "https://cynthiaos-transform-worker-production.up.railway.app";

let pipelineRunning = false;

function postPromotion() {
  return new Promise((resolve, reject) => {
    try {
      const url = new URL(`${TRANSFORM_WORKER_URL}/gold/run`);
      const options = {
        hostname: url.hostname,
        path: url.pathname,
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      };
      const req = http.request(options, (res) => {
        let data = '';
        res.on('data', (chunk) => data += chunk);
        res.on('end', () => {
          try { resolve(JSON.parse(data)); } catch (e) { resolve({ processed: false, error: 'JSON parse error' }); }
        });
      });
      req.on('error', (e) => reject(e));
      req.end();
    } catch (e) { reject(e); }
  });
}

async function drainGoldPromotion() {
  let promoted = 0;
  for (let i = 0; i < 100; i++) {
    try {
      const result = await postPromotion();
      if (!result.processed) break;
      promoted++;
    } catch (err) { break; }
  }
  return promoted;
}

async function runPipeline() {
  if (!fetchAndIngestAllReports) throw new Error("Logic not loaded: " + loadError);
  const startedAt = new Date().toISOString();
  console.log(`[cron] Pipeline started: ${startedAt}`);
  const fetchResults = await fetchAndIngestAllReports();
  const promoted = await drainGoldPromotion();
  console.log(`[cron] Pipeline done. Promoted: ${promoted}`);
  return { success: true, reports: fetchResults, promoted };
}

const PORT = parseInt(process.env.PORT ?? "3002", 10);
const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "application/json" });
  if (req.url === "/health") {
    res.end(JSON.stringify({ status: "ok", running: pipelineRunning, load_error: loadError, node: process.version }));
    return;
  }
  if (req.method === "POST" && req.url === "/run") {
    if (pipelineRunning) return res.end(JSON.stringify({ success: false, error: "Running" }));
    pipelineRunning = true;
    res.end(JSON.stringify({ success: true, message: "Started" }));
    runPipeline().catch(e => console.error(e)).finally(() => pipelineRunning = false);
    return;
  }
  res.end(JSON.stringify({ error: "not_found" }));
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`[cron] Server listening on ${PORT} (Node ${process.version})`);
  
  // Catch-up in 5s
  setTimeout(() => {
    const nyNow = new Date(new Date().toLocaleString("en-US", { timeZone: "America/New_York" }));
    if (nyNow.getHours() >= 6 && !pipelineRunning && fetchAndIngestAllReports) {
      console.log("[cron] Startup catch-up starting...");
      pipelineRunning = true;
      runPipeline().catch(e => console.error(e)).finally(() => pipelineRunning = false);
    }
  }, 5000);
});
