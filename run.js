/**
 * CynthiaOS Daily Pipeline Cron Worker
 *
 * Runs as a persistent HTTP service on Railway.
 * - GET /         → 200 health check (Railway default health probe)
 * - GET /health   → 200 detailed health check
 * - POST /run     → on-demand pipeline trigger (used by Sync Now button)
 *
 * Fires the full pipeline automatically at 6:00 AM ET every day.
 * Uses a per-minute tick + date check so it's DST-safe and crash-resilient.
 *
 * Environment variables:
 *   APPFOLIO_CLIENT_ID       — AppFolio Basic Auth client ID
 *   APPFOLIO_CLIENT_SECRET   — AppFolio Basic Auth client secret
 *   TRANSFORM_WORKER_URL     — Transform worker base URL (optional)
 *   WORKER_SHARED_SECRET     — Shared secret for the transform worker (REQUIRED)
 *   PORT                     — HTTP port (set automatically by Railway)
 */
const http   = require("http");
const { fetchAndIngestAllReports } = require("./fetchReports");

const TRANSFORM_WORKER_URL = process.env.TRANSFORM_WORKER_URL ||
  "https://cynthiaos-transform-worker-production.up.railway.app";
const PORT = parseInt(process.env.PORT ?? "3002", 10);

let pipelineRunning = false;
let lastRunDate     = "";

// ── Gold promotion ────────────────────────────────────────────────────────────
async function runGoldPromotion() {
  for (let i = 0; i < 100; i++) {
    const res = await fetch(`${TRANSFORM_WORKER_URL}/gold/run`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        // Service-to-service credential for the transform worker's auth
        // boundary. The worker fails closed — if WORKER_SHARED_SECRET is not
        // set here the nightly Gold promotion will 503 and the dashboard goes
        // stale, so this var must exist on this service.
        "X-Worker-Key": process.env.WORKER_SHARED_SECRET ?? "",
      },
    }).then(r => r.json()).catch(() => ({ processed: false }));
    if (!res.processed) {
      console.log(`[cron] Gold queue drained after ${i + 1} iteration(s).`);
      break;
    }
  }
}

// ── Full pipeline ─────────────────────────────────────────────────────────────
async function runPipeline() {
  const startedAt = new Date().toISOString();
  console.log(`[cron] Pipeline started at ${startedAt}`);
  try {
    const fetchResults = await fetchAndIngestAllReports();
    console.log(`[cron] Ingestion: ${fetchResults.success.length} OK, ${fetchResults.failed.length} failed`);
    await runGoldPromotion();
    console.log(`[cron] Pipeline complete at ${new Date().toISOString()}`);
  } catch (err) {
    console.error(`[cron] Pipeline error:`, err.message);
  }
}

// ── Daily scheduler: fires at 6 AM ET, once per calendar day ─────────────────
async function tick() {
  if (pipelineRunning) return;
  const nyDate = new Date(new Date().toLocaleString("en-US", { timeZone: "America/New_York" }));
  const dateStr = nyDate.toISOString().slice(0, 10);
  const hour    = nyDate.getHours();
  const minute  = nyDate.getMinutes();

  // Run once per day only inside the intended 6:00-6:10 AM ET window.
  // The previous `hour >= 6` catch-up condition caused every post-6 AM
  // deploy/restart to immediately launch a long AppFolio pipeline, which made
  // the web service fail health checks and prevented on-demand /run usage.
  if (hour === 6 && minute <= 10 && lastRunDate !== dateStr) {
    lastRunDate     = dateStr;
    pipelineRunning = true;
    console.log(`[cron] Scheduled 6 AM ET run starting for ${dateStr}...`);
    runPipeline().finally(() => {
      pipelineRunning = false;
      console.log('[cron] Scheduled pipeline complete; service remains online for health checks and /run.');
    });
  }
}

setInterval(tick, 60_000);
tick();

const hoursUntil6am = () => {
  const ny = new Date(new Date().toLocaleString("en-US", { timeZone: "America/New_York" }));
  const next = new Date(ny); next.setHours(6, 0, 0, 0);
  if (ny >= next) next.setDate(next.getDate() + 1);
  return Math.round((next - ny) / 3_600_000 * 10) / 10;
};

// ── HTTP server ───────────────────────────────────────────────────────────────
const server = http.createServer(async (req, res) => {

  // Root — Railway default health check
  if (req.method === "GET" && req.url === "/") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status: "ok", service: "cynthiaos-cron-worker", pipeline_running: pipelineRunning }));
    return;
  }

  // Health check
  if (req.method === "GET" && req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status: "ok", pipeline_running: pipelineRunning, next_run_in_hours: hoursUntil6am() }));
    return;
  }

  // On-demand pipeline trigger (Sync Now button)
  if (req.method === "POST" && req.url === "/run") {
    if (pipelineRunning) {
      res.writeHead(409, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ success: false, error: "Pipeline already running" }));
      return;
    }
    const jobId = `sync_${Date.now()}`;
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({
      success: true,
      job_id: jobId,
      message: "Pipeline started. Data will be updated in approximately 5-10 minutes.",
    }));
    pipelineRunning = true;
    runPipeline().finally(() => { pipelineRunning = false; });
    return;
  }

  res.writeHead(404, { "Content-Type": "application/json" });
  res.end(JSON.stringify({ error: "not_found" }));
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`[cron] HTTP server listening on port ${PORT}`);
  console.log(`[cron] Next scheduled run in ~${hoursUntil6am()}h (6:00 AM ET)`);
});
