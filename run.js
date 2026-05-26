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

// Maximum Gold promotion iterations per cron run (well above the 29-report catalogue).
const MAX_GOLD_ITERATIONS = 100;

// Track whether a pipeline run is already in progress (prevent concurrent runs)
let pipelineRunning = false;

/**
 * Calls /gold/run once and returns the response body. Throws on HTTP error.
 */
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

/**
 * Loops /gold/run until the transform worker reports no more pending Silver
 * records ("processed: false") or until MAX_GOLD_ITERATIONS is reached.
 */
async function drainGoldPromotion() {
  console.log("[cron] Step 3: Draining Gold promotion queue...");
  let promoted = 0;
  let skipped  = 0;
  let iteration = 0;
  while (iteration < MAX_GOLD_ITERATIONS) {
    iteration++;
    const result = await runGoldPromotionOnce();
    console.log(`[cron]   Gold iteration ${iteration}: processed=${result.processed} report_type=${result.report_type ?? "n/a"} gold_rows=${result.gold_row_count ?? 0} skipped=${result.skipped ?? false}`);
    if (!result.processed) {
      console.log(`[cron] Gold queue drained after ${iteration} iteration(s). Promoted: ${promoted}, Skipped: ${skipped}`);
      break;
    }
    if (result.skipped) {
      skipped++;
    } else {
      promoted++;
    }
  }
  if (iteration >= MAX_GOLD_ITERATIONS) {
    console.warn(`[cron] ⚠️  Gold promotion hit MAX_GOLD_ITERATIONS (${MAX_GOLD_ITERATIONS}). There may still be pending Silver records.`);
  }
  return { promoted, skipped, iterations: iteration };
}

/**
 * Runs the full pipeline. Returns a result summary object.
 */
async function runPipeline() {
  const startedAt = new Date().toISOString();
  console.log(`\n${"=".repeat(60)}`);
  console.log(`[cron] CynthiaOS pipeline started at ${startedAt}`);
  console.log(`${"=".repeat(60)}\n`);

  // ── Step 1 + 2: Fetch all AppFolio reports and ingest into Bronze ─────────
  console.log("[cron] Step 1+2: Fetching and ingesting AppFolio reports...");
  const fetchResults = await fetchAndIngestAllReports();

  if (fetchResults.failed.length > 0) {
    console.warn(
      `[cron] ⚠️  ${fetchResults.failed.length} report(s) failed to fetch/ingest:`,
      fetchResults.failed.map(f => `${f.id} (${f.error})`).join(", ")
    );
  }

  // ── Step 3: Promote Silver → Gold (drain full queue) ─────────────────────
  const goldResult = await drainGoldPromotion();

  const finishedAt = new Date().toISOString();
  console.log(`\n${"=".repeat(60)}`);
  console.log(`[cron] Pipeline completed at ${finishedAt}`);
  console.log(`[cron] Reports:  ${fetchResults.success.length} succeeded, ${fetchResults.failed.length} failed`);
  console.log(`[cron] Gold:     ${goldResult.promoted} promoted, ${goldResult.skipped} skipped (${goldResult.iterations} iterations)`);
  console.log(`${"=".repeat(60)}\n`);

  return {
    started_at:  startedAt,
    finished_at: finishedAt,
    reports: {
      succeeded: fetchResults.success.length,
      failed:    fetchResults.failed.length,
      failures:  fetchResults.failed.map(f => ({ id: f.id, error: f.error })),
    },
    gold: goldResult,
  };
}

// ── HTTP server for on-demand triggers ───────────────────────────────────────
const PORT = parseInt(process.env.PORT ?? "3002", 10);

const server = http.createServer(async (req, res) => {
  // Health check
  if (req.method === "GET" && req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status: "ok", pipeline_running: pipelineRunning }));
    return;
  }

  // On-demand pipeline trigger
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
      message: "Pipeline started. Data will be updated in approximately 5–10 minutes.",
    }));

    // Run pipeline asynchronously
    pipelineRunning = true;
    runPipeline()
      .then(result => {
        console.log(`[cron] On-demand run ${jobId} completed:`, JSON.stringify(result));
      })
      .catch(err => {
        console.error(`[cron] On-demand run ${jobId} failed:`, err.message);
      })
      .finally(() => {
        pipelineRunning = false;
      });
    return;
  }

  res.writeHead(404, { "Content-Type": "application/json" });
  res.end(JSON.stringify({ error: "not_found" }));
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`[cron] HTTP server listening on port ${PORT}`);
});

// ── Internal daily scheduler — fires at 6:00 AM Eastern every day ───────────────
// Uses setTimeout (not Railway cronSchedule) so the process stays alive as a
// persistent HTTP service (enabling the Sync Now button) while still running
// the daily 6 AM ET ingestion automatically.
//
// RESTART RESILIENCE: On startup, checks whether today's run was already
// completed. If the service restarted after 6 AM and today's run was missed,
// it fires immediately rather than waiting until tomorrow.

const INGESTION_WORKER_URL = process.env.INGESTION_WORKER_URL ||
  'https://cynthiaos-ingestion-worker-production.up.railway.app';

/**
 * Returns the date string (YYYY-MM-DD) for today in America/New_York.
 */
function todayET() {
  return new Date().toLocaleDateString("en-CA", { timeZone: "America/New_York" }); // YYYY-MM-DD
}

/**
 * Returns ms until the next 6:00 AM ET.
 */
function msUntilNext6amET() {
  const now = new Date();
  const nyNow = new Date(now.toLocaleString("en-US", { timeZone: "America/New_York" }));
  const next6am = new Date(nyNow);
  next6am.setHours(6, 0, 0, 0);
  if (nyNow >= next6am) next6am.setDate(next6am.getDate() + 1);
  return next6am.getTime() - nyNow.getTime();
}

/**
 * Checks the DB to see if a bronze record was ingested today.
 * Returns true if today's run already completed.
 */
async function todayAlreadyRan() {
  const postgres = require('postgres');
  const DATABASE_URL = process.env.DATABASE_URL;
  if (!DATABASE_URL) return false;
  let sql;
  try {
    sql = postgres(DATABASE_URL, { ssl: 'require', max: 1 });
    const today = todayET();
    const rows = await sql`
      SELECT 1 FROM bronze_appfolio_reports
      WHERE report_type = 'rent_roll'
        AND report_date::text = ${today}
      LIMIT 1
    `;
    return rows.length > 0;
  } catch (err) {
    console.warn('[cron] todayAlreadyRan check failed:', err.message);
    return false; // assume not run — safer to re-run than to skip
  } finally {
    if (sql) await sql.end();
  }
}

function scheduleNextRun() {
  const ms = msUntilNext6amET();
  const hh = Math.floor(ms / 3_600_000);
  const mm = Math.floor((ms % 3_600_000) / 60_000);
  console.log(`[cron] Next scheduled run in ${hh}h ${mm}m (6:00 AM ET)`);
  setTimeout(async () => {
    if (!pipelineRunning) {
      console.log("[cron] Scheduled 6 AM ET run starting...");
      pipelineRunning = true;
      try {
        const result = await runPipeline();
        console.log("[cron] Scheduled run completed:", JSON.stringify(result));
      } catch (err) {
        console.error("[cron] Scheduled run failed:", err.message);
      } finally {
        pipelineRunning = false;
      }
    } else {
      console.warn("[cron] Scheduled run skipped — pipeline already running");
    }
    scheduleNextRun();
  }, ms);
}

// ── Startup catch-up check ────────────────────────────────────────────────────
// If the service restarted after 6 AM ET and today's run was missed, fire
// immediately. Otherwise just schedule the next 6 AM run as normal.
(async () => {
  const nyNow = new Date(new Date().toLocaleString("en-US", { timeZone: "America/New_York" }));
  const currentHourET = nyNow.getHours();

  if (currentHourET >= 6) {
    // It's past 6 AM ET — check if today's run already happened
    const alreadyRan = await todayAlreadyRan();
    if (!alreadyRan) {
      console.log(`[cron] Startup catch-up: today's run (${todayET()}) was missed — running now...`);
      pipelineRunning = true;
      try {
        const result = await runPipeline();
        console.log('[cron] Catch-up run completed:', JSON.stringify(result));
      } catch (err) {
        console.error('[cron] Catch-up run failed:', err.message);
      } finally {
        pipelineRunning = false;
      }
    } else {
      console.log(`[cron] Startup check: today's run (${todayET()}) already completed — no catch-up needed.`);
    }
  } else {
    console.log(`[cron] Startup check: it is before 6 AM ET — scheduling normally.`);
  }

  scheduleNextRun();
})();
