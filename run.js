/**
 * CynthiaOS Cron Worker — Background Version
 * 
 * This version does NOT run an HTTP server. It runs as a persistent
 * background process that manages its own schedule. This bypasses
 * Railway 502/Health Check issues.
 */

const logic = require("./fetchReports");
const http = require("http");

const TRANSFORM_WORKER_URL = process.env.TRANSFORM_WORKER_URL ||
  "https://cynthiaos-transform-worker-production.up.railway.app";

async function postPromotion() {
  return new Promise((resolve) => {
    try {
      const url = new URL(`${TRANSFORM_WORKER_URL}/gold/run`);
      const req = http.request(url, { method: 'POST' }, (res) => {
        let data = '';
        res.on('data', (chunk) => data += chunk);
        res.on('end', () => {
          try { resolve(JSON.parse(data)); } catch (e) { resolve({ processed: false }); }
        });
      });
      req.on('error', () => resolve({ processed: false }));
      req.end();
    } catch (e) { resolve({ processed: false }); }
  });
}

async function runPipeline() {
  console.log(`[cron] [${new Date().toISOString()}] Starting pipeline...`);
  try {
    await logic.fetchAndIngestAllReports();
    console.log(`[cron] Ingestion complete. Starting gold promotion...`);
    for (let i = 0; i < 50; i++) {
      const res = await postPromotion();
      if (!res.processed) break;
    }
    console.log(`[cron] Pipeline complete.`);
  } catch (err) {
    console.error(`[cron] Pipeline failed:`, err.message);
  }
}

// Main Loop
console.log(`[cron] Worker started (Node ${process.version})`);

let lastRunDate = "";

async function tick() {
  const now = new Date();
  const nyTime = now.toLocaleString("en-US", { timeZone: "America/New_York" });
  const nyDate = new Date(nyTime);
  const dateStr = nyDate.toISOString().slice(0, 10);
  const hour = nyDate.getHours();

  // 1. Startup Catch-up or Daily Run (6 AM ET)
  if (hour >= 6 && lastRunDate !== dateStr) {
    console.log(`[cron] Triggering daily run for ${dateStr} (Hour: ${hour})`);
    lastRunDate = dateStr;
    await runPipeline();
  }
}

// Run tick every minute
setInterval(tick, 60000);

// Immediate first tick
tick();
