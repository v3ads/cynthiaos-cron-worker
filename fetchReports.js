/**
 * AppFolio Report Fetcher — with Rate Limit Compliance
 *
 * Fetches all 29 AppFolio reports and POSTs each to the CynthiaOS
 * ingestion endpoint. Implements:
 *
 *   - 1-second inter-request delay between every AppFolio API call
 *   - 202 Accepted polling (async report generation) with 5 s interval
 *   - 429 Too Many Requests: reads Retry-After header, waits, then retries
 *   - Exponential backoff with full jitter for transient 5xx errors
 *   - Per-report error isolation (one failure does not stop the rest)
 *
 * Rate limit strategy:
 *   AppFolio enforces per-second, per-minute, and per-hour limits on a
 *   per-customer-partner basis. Exact thresholds are not publicly documented.
 *   The 1 s inter-request floor keeps us well under any reasonable per-second
 *   limit. The 429 handler ensures we back off dynamically if we do hit a
 *   window limit.
 */

const APPFOLIO_BASE_URL      = "https://cynthiagardens.appfolio.com/api/v1/reports";
const APPFOLIO_CLIENT_ID     = process.env.APPFOLIO_CLIENT_ID;
const APPFOLIO_CLIENT_SECRET = process.env.APPFOLIO_CLIENT_SECRET;
// Use Railway private networking (bypasses edge proxy 413 limit)
// Falls back to public URL if INGESTION_URL is explicitly set
const INGESTION_URL          = process.env.INGESTION_URL ||
  "http://cynthiaos-ingestion-worker.railway.internal:3001";

// ── Tuning constants ──────────────────────────────────────────────────────────
const INTER_REQUEST_DELAY_MS = 1_000;   // 1 s between every AppFolio API call
const POLL_INTERVAL_MS       = 5_000;   // 5 s between 202 polls
const POLL_MAX_ATTEMPTS      = 36;      // max 3 minutes per async report
const FETCH_TIMEOUT_MS       = 30_000;  // 30 s per HTTP request
const MAX_RETRIES            = 5;       // max retries for 429 / 5xx
const BACKOFF_BASE_MS        = 2_000;   // base for exponential backoff (2 s)
const BACKOFF_MAX_MS         = 60_000;  // cap backoff at 60 s

// ── Helpers ───────────────────────────────────────────────────────────────────
function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

/** Full-jitter exponential backoff: random value in [0, min(cap, base * 2^attempt)] */
function backoffMs(attempt) {
  const ceiling = Math.min(BACKOFF_MAX_MS, BACKOFF_BASE_MS * Math.pow(2, attempt));
  return Math.floor(Math.random() * ceiling);
}

/** Parse Retry-After header — supports both integer seconds and HTTP-date formats */
function parseRetryAfter(header) {
  if (!header) return null;
  const seconds = parseInt(header, 10);
  if (!isNaN(seconds)) return seconds * 1000;
  const date = new Date(header);
  if (!isNaN(date.getTime())) return Math.max(0, date.getTime() - Date.now());
  return null;
}

// ── Date helpers ──────────────────────────────────────────────────────────────
function toISO(date) {
  return date.toISOString().slice(0, 10);
}

function getDateParams() {
  const today    = new Date();
  const jan1     = new Date(today.getFullYear(), 0, 1);
  const t12Start = new Date(today); t12Start.setFullYear(t12Start.getFullYear() - 1);
  const fwdEnd   = new Date(today); fwdEnd.setFullYear(fwdEnd.getFullYear() + 1);
  return {
    today:    toISO(today),
    jan1:     toISO(jan1),
    t12Start: toISO(t12Start),
    fwdEnd:   toISO(fwdEnd),
  };
}

// ── Report catalogue ──────────────────────────────────────────────────────────
function buildReportCatalogue(dates) {
  const { today, jan1, t12Start, fwdEnd } = dates;
  const ytd  = `from_date=${jan1}&to_date=${today}`;
  const t12  = `from_date=${t12Start}&to_date=${today}`;
  const fwd  = `from_date=${today}&to_date=${fwdEnd}`;
  const asof = `as_of_date=${today}`;

  return [
    // Static
    { id: "rent_roll",                url: `${APPFOLIO_BASE_URL}/rent_roll`,                          reportDate: today },
    { id: "rent_roll_itemized",       url: `${APPFOLIO_BASE_URL}/rent_roll_itemized`,                 reportDate: today },
    { id: "delinquency",              url: `${APPFOLIO_BASE_URL}/delinquency`,                        reportDate: today },
    { id: "aged_receivables_detail",  url: `${APPFOLIO_BASE_URL}/aged_receivables_detail`,            reportDate: today },
    { id: "unit_vacancy",             url: `${APPFOLIO_BASE_URL}/unit_vacancy`,                       reportDate: today },
    { id: "tenant_directory",         url: `${APPFOLIO_BASE_URL}/tenant_directory`,                   reportDate: today },
    { id: "owner_directory",          url: `${APPFOLIO_BASE_URL}/owner_directory`,                    reportDate: today },
    { id: "property_directory",       url: `${APPFOLIO_BASE_URL}/property_directory`,                 reportDate: today },
    { id: "vendor_directory",         url: `${APPFOLIO_BASE_URL}/vendor_directory`,                   reportDate: today },
    { id: "unit_directory",           url: `${APPFOLIO_BASE_URL}/unit_directory`,                     reportDate: today },
    // As-of-date
    { id: "balance_sheet",            url: `${APPFOLIO_BASE_URL}/balance_sheet?${asof}`,              reportDate: today },
    // YTD
    { id: "trial_balance",            url: `${APPFOLIO_BASE_URL}/trial_balance?${ytd}`,               reportDate: today },
    { id: "cash_flow",                url: `${APPFOLIO_BASE_URL}/cash_flow?${ytd}`,                   reportDate: today },
    { id: "income_statement",         url: `${APPFOLIO_BASE_URL}/income_statement?${ytd}`,            reportDate: today },
    { id: "general_ledger",           url: `${APPFOLIO_BASE_URL}/general_ledger?${ytd}`,              reportDate: today },
    { id: "check_register_detail",    url: `${APPFOLIO_BASE_URL}/check_register_detail?${ytd}`,       reportDate: today },
    { id: "deposit_register",         url: `${APPFOLIO_BASE_URL}/deposit_register?${ytd}`,            reportDate: today },
    { id: "charge_detail",            url: `${APPFOLIO_BASE_URL}/charge_detail?${ytd}`,               reportDate: today },
    { id: "receivables_activity",     url: `${APPFOLIO_BASE_URL}/receivables_activity?${ytd}`,        reportDate: today },
    { id: "lease_history",            url: `${APPFOLIO_BASE_URL}/lease_history?${ytd}`,               reportDate: today },
    { id: "unit_turn_detail",         url: `${APPFOLIO_BASE_URL}/unit_turn_detail?${ytd}`,            reportDate: today },
    { id: "renewal_summary",          url: `${APPFOLIO_BASE_URL}/renewal_summary?${ytd}`,             reportDate: today },
    { id: "prospect_source_tracking", url: `${APPFOLIO_BASE_URL}/prospect_source_tracking?${ytd}`,   reportDate: today },
    { id: "guest_cards",              url: `${APPFOLIO_BASE_URL}/guest_cards?${ytd}`,                 reportDate: today },
    { id: "rental_applications",      url: `${APPFOLIO_BASE_URL}/rental_applications?${ytd}`,         reportDate: today },
    { id: "work_order",               url: `${APPFOLIO_BASE_URL}/work_order?${ytd}`,                  reportDate: today },
    // T12
    { id: "twelve_month_cash_flow",        url: `${APPFOLIO_BASE_URL}/twelve_month_cash_flow?${t12}`,        reportDate: today },
    { id: "twelve_month_income_statement", url: `${APPFOLIO_BASE_URL}/twelve_month_income_statement?${t12}`, reportDate: today },
    // Forward-looking
    { id: "lease_expiration_detail",  url: `${APPFOLIO_BASE_URL}/lease_expiration_detail?${fwd}`,    reportDate: today },
  ];
}

// ── Core HTTP fetch with retry / backoff ──────────────────────────────────────
/**
 * Performs a single GET request to AppFolio with:
 *   - Timeout enforcement
 *   - 429 Retry-After handling
 *   - Exponential backoff with jitter for 5xx errors
 *   - Returns { status, headers, json } on success
 */
async function appfolioGet(url, authHeader, attempt = 0) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);

  let res;
  try {
    res = await fetch(url, {
      headers: { Authorization: authHeader, Accept: "application/json" },
      signal: controller.signal,
    });
  } finally {
    clearTimeout(timer);
  }

  // 429 Too Many Requests — respect Retry-After
  if (res.status === 429) {
    if (attempt >= MAX_RETRIES) {
      throw new Error(`Rate limited (429) after ${MAX_RETRIES} retries on ${url}`);
    }
    const retryAfterMs = parseRetryAfter(res.headers.get("retry-after")) ?? backoffMs(attempt);
    console.warn(`  [rate-limit] 429 received. Waiting ${Math.round(retryAfterMs / 1000)}s before retry ${attempt + 1}/${MAX_RETRIES}...`);
    await sleep(retryAfterMs);
    return appfolioGet(url, authHeader, attempt + 1);
  }

  // 5xx transient errors — exponential backoff with jitter
  if (res.status >= 500 && res.status < 600) {
    if (attempt >= MAX_RETRIES) {
      throw new Error(`Server error (${res.status}) after ${MAX_RETRIES} retries on ${url}`);
    }
    const wait = backoffMs(attempt);
    console.warn(`  [backoff] HTTP ${res.status}. Waiting ${Math.round(wait / 1000)}s before retry ${attempt + 1}/${MAX_RETRIES}...`);
    await sleep(wait);
    return appfolioGet(url, authHeader, attempt + 1);
  }

  return res;
}

// ── Fetch one AppFolio report (handles 202 async polling) ─────────────────────
async function fetchReport(report, authHeader) {
  let targetUrl  = report.url;
  let pollCount  = 0;

  while (pollCount < POLL_MAX_ATTEMPTS) {
    const res = await appfolioGet(targetUrl, authHeader);

    // 202 Accepted — report is being generated; poll the Location header
    if (res.status === 202) {
      const location = res.headers.get("location");
      if (location) targetUrl = location;
      pollCount++;
      console.log(`  [${report.id}] 202 Accepted — polling (${pollCount}/${POLL_MAX_ATTEMPTS})...`);
      await sleep(POLL_INTERVAL_MS);
      continue;
    }

    if (!res.ok) {
      throw new Error(`HTTP ${res.status} ${res.statusText} fetching ${report.id}`);
    }

    const json = await res.json();
    // AppFolio returns either { results: [...] } or a direct array
    const rows = Array.isArray(json) ? json : (json.results ?? json);
    return rows;
  }

  throw new Error(`${report.id} did not complete after ${POLL_MAX_ATTEMPTS} polls (${Math.round(POLL_MAX_ATTEMPTS * POLL_INTERVAL_MS / 60000)} min)`);
}

// ── POST one report to the CynthiaOS ingestion endpoint ──────────────────────
async function ingestReport(reportId, reportDate, rows) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);

  let res;
  try {
    res = await fetch(`${INGESTION_URL}/ingest/report`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        source:      "appfolio",
        report_type: reportId,
        report_date: reportDate,
        payload:     { results: rows },
      }),
      signal: controller.signal,
    });
  } finally {
    clearTimeout(timer);
  }

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Ingestion failed for ${reportId}: HTTP ${res.status} — ${text.slice(0, 200)}`);
  }

  return await res.json();
}

// ── Main export ───────────────────────────────────────────────────────────────
async function fetchAndIngestAllReports() {
  if (!APPFOLIO_CLIENT_ID || !APPFOLIO_CLIENT_SECRET) {
    throw new Error("APPFOLIO_CLIENT_ID and APPFOLIO_CLIENT_SECRET must be set");
  }

  const authHeader = "Basic " + Buffer.from(
    `${APPFOLIO_CLIENT_ID}:${APPFOLIO_CLIENT_SECRET}`
  ).toString("base64");

  const dates   = getDateParams();
  const reports = buildReportCatalogue(dates);
  const results = { success: [], failed: [] };

  console.log(`[fetchReports] Starting fetch of ${reports.length} AppFolio reports for ${dates.today}`);
  console.log(`[fetchReports] Inter-request delay: ${INTER_REQUEST_DELAY_MS}ms | Max retries: ${MAX_RETRIES} | 429 backoff: up to ${BACKOFF_MAX_MS / 1000}s`);

  for (let i = 0; i < reports.length; i++) {
    const report = reports[i];

    // Enforce inter-request delay before every request (except the very first)
    if (i > 0) {
      await sleep(INTER_REQUEST_DELAY_MS);
    }

    try {
      console.log(`  [${report.id}] (${i + 1}/${reports.length}) Fetching...`);
      const rows = await fetchReport(report, authHeader);
      const rowCount = Array.isArray(rows) ? rows.length : "?";
      console.log(`  [${report.id}] Fetched ${rowCount} rows. Ingesting...`);

      const ingested = await ingestReport(report.id, report.reportDate, rows);
      console.log(`  [${report.id}] ✅ Ingested — bronze_id=${ingested.bronze_report_id ?? "?"}`);
      results.success.push(report.id);
    } catch (err) {
      console.error(`  [${report.id}] ❌ FAILED: ${err.message}`);
      results.failed.push({ id: report.id, error: err.message });
    }
  }

  console.log(`\n[fetchReports] Done. ${results.success.length}/${reports.length} succeeded, ${results.failed.length} failed.`);
  if (results.failed.length > 0) {
    console.error("[fetchReports] Failed reports:", results.failed.map(f => `${f.id} (${f.error})`).join("; "));
  }

  return results;
}

module.exports = { fetchAndIngestAllReports };
