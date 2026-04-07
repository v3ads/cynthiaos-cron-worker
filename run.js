/**
 * CynthiaOS Daily Pipeline Cron Worker
 *
 * This script is executed by Railway's native cron scheduler at 6 AM Eastern
 * (11:00 UTC) every day. It triggers the Gold promotion pass on the Transform
 * Worker, ensuring any Silver records ingested since the last run are promoted
 * into the Gold layer.
 *
 * Railway cron schedule: 0 11 * * *  (11:00 UTC = 06:00 Eastern)
 */

const TRANSFORM_WORKER_URL =
  process.env.TRANSFORM_WORKER_URL ||
  "https://cynthiaos-transform-worker-production.up.railway.app";

async function runGoldPromotion() {
  const startedAt = new Date().toISOString();
  console.log(`[cynthiaos-cron-worker] Starting Gold promotion run at ${startedAt}`);

  try {
    const res = await fetch(`${TRANSFORM_WORKER_URL}/gold/run`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
    });

    const body = await res.json();
    const finishedAt = new Date().toISOString();

    if (!res.ok) {
      console.error(
        `[cynthiaos-cron-worker] Gold promotion FAILED — HTTP ${res.status}`,
        JSON.stringify(body)
      );
      process.exit(1);
    }

    console.log(
      `[cynthiaos-cron-worker] Gold promotion completed at ${finishedAt}`,
      JSON.stringify(body)
    );

    // "processed: false" with "No Silver records pending" is a valid success state
    process.exit(0);
  } catch (err) {
    console.error("[cynthiaos-cron-worker] Unexpected error:", err.message);
    process.exit(1);
  }
}

runGoldPromotion();
