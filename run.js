/**
 * CynthiaOS Daily Pipeline Cron Worker
 *
 * Runs every day at 6:00 AM Eastern (11:00 UTC) via Railway native cron.
 * Executes the full pipeline in sequence:
 *
 *   1. Fetch all 29 AppFolio reports via the AppFolio API
 *   2. POST each report to the CynthiaOS ingestion endpoint (Bronze layer)
 *   3. Trigger Gold promotion on the Transform Worker (Silver → Gold)
 *
 * Environment variables required:
 *   APPFOLIO_CLIENT_ID       — AppFolio Basic Auth client ID
 *   APPFOLIO_CLIENT_SECRET   — AppFolio Basic Auth client secret
 *   INGESTION_URL            — CynthiaOS ingestion worker base URL (optional, has default)
 *   TRANSFORM_WORKER_URL     — CynthiaOS transform worker base URL (optional, has default)
 */

const { fetchAndIngestAllReports } = require("./fetchReports");

const TRANSFORM_WORKER_URL = process.env.TRANSFORM_WORKER_URL ||
  "https://cynthiaos-transform-worker-production.up.railway.app";

async function runGoldPromotion() {
  console.log("[cron] Step 3: Triggering Gold promotion...");
  const res = await fetch(`${TRANSFORM_WORKER_URL}/gold/run`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
  });

  const body = await res.json();
  if (!res.ok) {
    throw new Error(`Gold promotion failed: HTTP ${res.status} — ${JSON.stringify(body)}`);
  }

  console.log("[cron] Gold promotion result:", JSON.stringify(body));
  return body;
}

async function main() {
  const startedAt = new Date().toISOString();
  console.log(`\n${"=".repeat(60)}`);
  console.log(`[cron] CynthiaOS daily pipeline started at ${startedAt}`);
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

  // ── Step 3: Promote Silver → Gold ─────────────────────────────────────────
  await runGoldPromotion();

  const finishedAt = new Date().toISOString();
  console.log(`\n${"=".repeat(60)}`);
  console.log(`[cron] Pipeline completed at ${finishedAt}`);
  console.log(`[cron] Reports: ${fetchResults.success.length} succeeded, ${fetchResults.failed.length} failed`);
  console.log(`${"=".repeat(60)}\n`);

  // Exit non-zero if any reports failed, so Railway logs it as a failed run
  if (fetchResults.failed.length > 0 && fetchResults.success.length === 0) {
    process.exit(1);
  }

  process.exit(0);
}

main().catch(err => {
  console.error("[cron] Unhandled error:", err.message);
  process.exit(1);
});
