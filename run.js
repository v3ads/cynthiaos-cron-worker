const http = require("http");
const { fetchAndIngestAllReports } = require("./fetchReports");

const TRANSFORM_WORKER_URL = process.env.TRANSFORM_WORKER_URL ||
  "https://cynthiaos-transform-worker-production.up.railway.app";

let pipelineRunning = false;

/**
 * Simple HTTP POST helper using built-in 'http' to avoid 'fetch' issues
 */
function postPromotion() {
  return new Promise((resolve, reject) => {
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
        try {
          resolve(JSON.parse(data));
        } catch (e) {
          resolve({ processed: false, error: 'JSON parse error' });
        }
      });
    });
    req.on('error', (e) => reject(e));
    req.end();
  });
}

async function drainGoldPromotion() {
  console.log("[cron] Step 3: Draining Gold promotion queue...");
  let promoted = 0;
  let iteration = 0;
  while (iteration < 100) {
    iteration++;
    try {
      const result = await postPromotion();
      console.log(`[cron]   Gold iteration ${iteration}: processed=${result.processed}`);
      if (!result.processed) break;
      promoted++;
    } catch (err) {
      console.error(`[cron]   Gold iteration ${iteration} failed:`, err.message);
      break;
    }
  }
  return { promoted, iterations: iteration };
}

async function runPipeline() {
  const startedAt = new Date().toISOString();
  console.log(`[cron] Pipeline started at ${startedAt}`);
  try {
    const fetchResults = await fetchAndIngestAllReports();
    const goldResult = await drainGoldPromotion();
    const finishedAt = new Date().toISOString();
    console.log(`[cron] Pipeline completed at ${finishedAt}`);
    return { success: true, reports: fetchResults, gold: goldResult };
  } catch (err) {
    console.error(`[cron] Pipeline failed:`, err.message);
    return { success: false, error: err.message };
  }
}

const PORT = parseInt(process.env.PORT ?? "3002", 10);
const server = http.createServer((req, res) => {
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
  console.log(`[cron] HTTP server listening on port ${PORT}`);

  // Daily scheduler (every 30 mins)
  setInterval(() => {
    const nyNow = new Date(new Date().toLocaleString("en-US", { timeZone: "America/New_York" }));
    if (nyNow.getHours() === 6 && nyNow.getMinutes() < 30 && !pipelineRunning) {
      console.log("[cron] Scheduled run starting...");
      pipelineRunning = true;
      runPipeline().catch(e => console.error(e)).finally(() => pipelineRunning = false);
    }
  }, 30 * 60 * 1000);

  // Catch-up run (after 10s to ensure server is stable)
  const nyNow = new Date(new Date().toLocaleString("en-US", { timeZone: "America/New_York" }));
  if (nyNow.getHours() >= 6 && !pipelineRunning) {
    console.log("[cron] Scheduling startup catch-up in 10s...");
    setTimeout(() => {
      if (!pipelineRunning) {
        console.log("[cron] Startup catch-up starting...");
        pipelineRunning = true;
        runPipeline().catch(e => console.error(e)).finally(() => pipelineRunning = false);
      }
    }, 10000);
  }
});
