/**
 * CynthiaOS Daily Pipeline Cron Worker
 *
 * Runs every day at 6:00 AM Eastern (11:00 UTC) via Railway native cron.
 * Also exposes a POST /run HTTP endpoint for on-demand pipeline triggers
 * from the CynthiaOS "Sync Now" button.
 *
 * Executes the full pipeline in sequence:
 *
 *   1. Fetch all 29 AppFolio reports via the AppFolio API
 *   2. POST each report to the CynthiaOS ingestion endpoint (Bronze layer)
 *   3. Trigger Gold promotion on the Transform Worker (Silver → Gold)
 *      — loops until all pending Silver records are drained (not just once)
 *
 * Environment variables required:
 *   APPFOLIO_CLIENT_ID       — AppFolio Basic Auth client ID
 *   APPFOLIO_CLIENT_SECRET   — AppFolio Basic Auth client secret
 *   INGESTION_URL            — CynthiaOS ingestion worker base URL (optional, has default)
 *   TRANSFORM_WORKER_URL     — CynthiaOS transform worker base URL (optional, has default)
 */
const http = require("http");
const { fetchAndIngestAllReports } = require("./fetchReports");

const TRANSFORM_WORKER_URL = process.env.TRANSFORM_WORKER_URL ||
  "https://cynthiaos-transform-worker-production.up.railway.app";

const MAX_GOLD_ITERATIONS = 100;
let pipelineRunning = false;

async function runGoldPromotionOnce() {
  const res = await fetch(`${TRANSFORM_WORKER_URL}/gold/run`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
  });
  const body = await res.json();
  if (!res.ok) {
    throw new Error(`Gold promotion failed: HTTP ${res.status} — ${JSON.stringify(body)}`);
  }
  return body;
}

async function drainGoldPromotion() {
  console.log("[cron] Step 3: Draining Gold promotion queue...");
  let promoted = 0;
  let skipped  = 0;
  let iteration = 0;
  while (iteration < MAX_GOLD_ITERATIONS) {
    iteration++;
    const result = await runGoldPromotionOnce();
    console.log(`[cron]   Gold iteration ${iteration}: processed=${result.processed} report_type=${result.report_type ?? "n/a"}`);
    if (!result.processed) break;
    if (result.skipped) skipped++; else promoted++;
  }
  return { promoted, skipped, iterations: iteration };
}

async function runPipeline() {
  const startedAt = new Date().toISOString();
  console.log(`[cron] Pipeline started at ${startedAt}`);
  const fetchResults = await fetchAndIngestAllReports();
  const goldResult = await drainGoldPromotion();
  const finishedAt = new Date().toISOString();
  console.log(`[cron] Pipeline completed at ${finishedAt}`);
  return { started_at: startedAt, finished_at: finishedAt, reports: fetchResults, gold: goldResult };
}

const PORT = parseInt(process.env.PORT ?? "3002", 10);
const server = http.createServer(async (req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status: "ok", running: pipelineRunning }));
    return;
  }
  if (req.method === "POST" && req.url === "/run") {
    if (pipelineRunning) {
      res.writeHead(409, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ success: false, error: "Already running" }));
      return;
    }
    pipelineRunning = true;
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ success: true, message: "Started" }));
    runPipeline().catch(e => console.error(e)).finally(() => pipelineRunning = false);
    return;
  }
  res.writeHead(404).end();
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`[cron] Listening on ${PORT}`);
  
  // Daily scheduler: Check every 30 minutes if it's 6 AM ET
  setInterval(async () => {
    const nyNow = new Date(new Date().toLocaleString("en-US", { timeZone: "America/New_York" }));
    if (nyNow.getHours() === 6 && nyNow.getMinutes() < 30 && !pipelineRunning) {
      console.log("[cron] Scheduled run starting...");
      pipelineRunning = true;
      try { await runPipeline(); } catch (e) { console.error(e); } finally { pipelineRunning = false; }
    }
  }, 30 * 60 * 1000);

  // Catch-up: Run once on startup if it's past 6 AM ET
  const nyNow = new Date(new Date().toLocaleString("en-US", { timeZone: "America/New_York" }));
  if (nyNow.getHours() >= 6 && !pipelineRunning) {
    console.log("[cron] Startup catch-up starting...");
    pipelineRunning = true;
    runPipeline().catch(e => console.error(e)).finally(() => pipelineRunning = false);
  }
});
