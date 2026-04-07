/**
 * AppFolio Report Fetcher
 *
 * Fetches all 29 AppFolio reports and POSTs each to the CynthiaOS
 * ingestion endpoint. Handles:
 *   - Static reports (no date params)
 *   - As-of-date reports (as_of_date = today)
 *   - YTD date-range reports (Jan 1 → today)
 *   - Trailing-12-month reports (365 days ago → today)
 *   - Forward-looking reports (today → 365 days from today)
 *   - Async 202 polling until the report is ready
 */

const APPFOLIO_BASE_URL = "https://cynthiagardens.appfolio.com/api/v1/reports";
const APPFOLIO_CLIENT_ID     = process.env.APPFOLIO_CLIENT_ID;
const APPFOLIO_CLIENT_SECRET = process.env.APPFOLIO_CLIENT_SECRET;
const INGESTION_URL = process.env.INGESTION_URL ||
  "https://cynthiaos-ingestion-worker-production-8068.up.railway.app";

const POLL_INTERVAL_MS = 5_000;   // 5 s between polls
const POLL_MAX_ATTEMPTS = 24;     // max 2 minutes per report
const FETCH_TIMEOUT_MS  = 30_000; // 30 s per HTTP request

// ── Date helpers ──────────────────────────────────────────────────────────────
function toISO(date) {
  return date.toISOString().slice(0, 10);
}

function getDateParams() {
  const today     = new Date();
  const jan1      = new Date(today.getFullYear(), 0, 1);
  const t12Start  = new Date(today); t12Start.setFullYear(t12Start.getFullYear() - 1);
  const fwdEnd    = new Date(today); fwdEnd.setFullYear(fwdEnd.getFullYear() + 1);

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
    // ── Static (no params) ────────────────────────────────────────────────
    { id: "rent_roll",               url: `${APPFOLIO_BASE_URL}/rent_roll`,               reportDate: today },
    { id: "rent_roll_itemized",      url: `${APPFOLIO_BASE_URL}/rent_roll_itemized`,      reportDate: today },
    { id: "delinquency",             url: `${APPFOLIO_BASE_URL}/delinquency`,             reportDate: today },
    { id: "aged_receivables_detail", url: `${APPFOLIO_BASE_URL}/aged_receivables_detail`, reportDate: today },
    { id: "unit_vacancy",            url: `${APPFOLIO_BASE_URL}/unit_vacancy`,            reportDate: today },
    { id: "tenant_directory",        url: `${APPFOLIO_BASE_URL}/tenant_directory`,        reportDate: today },
    { id: "owner_directory",         url: `${APPFOLIO_BASE_URL}/owner_directory`,         reportDate: today },
    { id: "property_directory",      url: `${APPFOLIO_BASE_URL}/property_directory`,      reportDate: today },
    { id: "vendor_directory",        url: `${APPFOLIO_BASE_URL}/vendor_directory`,        reportDate: today },
    { id: "unit_directory",          url: `${APPFOLIO_BASE_URL}/unit_directory`,          reportDate: today },

    // ── As-of-date ────────────────────────────────────────────────────────
    { id: "balance_sheet",           url: `${APPFOLIO_BASE_URL}/balance_sheet?${asof}`,  reportDate: today },

    // ── YTD date-range ────────────────────────────────────────────────────
    { id: "trial_balance",           url: `${APPFOLIO_BASE_URL}/trial_balance?${ytd}`,           reportDate: today },
    { id: "cash_flow",               url: `${APPFOLIO_BASE_URL}/cash_flow?${ytd}`,               reportDate: today },
    { id: "income_statement",        url: `${APPFOLIO_BASE_URL}/income_statement?${ytd}`,        reportDate: today },
    { id: "general_ledger",          url: `${APPFOLIO_BASE_URL}/general_ledger?${ytd}`,          reportDate: today },
    { id: "check_register_detail",   url: `${APPFOLIO_BASE_URL}/check_register_detail?${ytd}`,   reportDate: today },
    { id: "deposit_register",        url: `${APPFOLIO_BASE_URL}/deposit_register?${ytd}`,        reportDate: today },
    { id: "charge_detail",           url: `${APPFOLIO_BASE_URL}/charge_detail?${ytd}`,           reportDate: today },
    { id: "receivables_activity",    url: `${APPFOLIO_BASE_URL}/receivables_activity?${ytd}`,    reportDate: today },
    { id: "lease_history",           url: `${APPFOLIO_BASE_URL}/lease_history?${ytd}`,           reportDate: today },
    { id: "unit_turn_detail",        url: `${APPFOLIO_BASE_URL}/unit_turn_detail?${ytd}`,        reportDate: today },
    { id: "renewal_summary",         url: `${APPFOLIO_BASE_URL}/renewal_summary?${ytd}`,         reportDate: today },
    { id: "prospect_source_tracking",url: `${APPFOLIO_BASE_URL}/prospect_source_tracking?${ytd}`,reportDate: today },
    { id: "guest_cards",             url: `${APPFOLIO_BASE_URL}/guest_cards?${ytd}`,             reportDate: today },
    { id: "rental_applications",     url: `${APPFOLIO_BASE_URL}/rental_applications?${ytd}`,     reportDate: today },
    { id: "work_order",              url: `${APPFOLIO_BASE_URL}/work_order?${ytd}`,              reportDate: today },

    // ── Trailing 12 months ────────────────────────────────────────────────
    { id: "twelve_month_cash_flow",         url: `${APPFOLIO_BASE_URL}/twelve_month_cash_flow?${t12}`,         reportDate: today },
    { id: "twelve_month_income_statement",  url: `${APPFOLIO_BASE_URL}/twelve_month_income_statement?${t12}`,  reportDate: today },

    // ── Forward-looking ───────────────────────────────────────────────────
    { id: "lease_expiration_detail", url: `${APPFOLIO_BASE_URL}/lease_expiration_detail?${fwd}`, reportDate: today },
  ];
}

// ── Fetch one AppFolio report (with 202 polling) ──────────────────────────────
async function fetchReport(report) {
  const authHeader = "Basic " + Buffer.from(
    `${APPFOLIO_CLIENT_ID}:${APPFOLIO_CLIENT_SECRET}`
  ).toString("base64");

  let attempts = 0;
  let targetUrl = report.url;

  while (attempts < POLL_MAX_ATTEMPTS) {
    attempts++;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);

    let res;
    try {
      res = await fetch(targetUrl, {
        headers: { Authorization: authHeader, Accept: "application/json" },
        signal: controller.signal,
      });
    } finally {
      clearTimeout(timer);
    }

    // 202 Accepted — report is being generated asynchronously; poll the Location header
    if (res.status === 202) {
      const location = res.headers.get("location");
      if (location) targetUrl = location;
      console.log(`  [${report.id}] 202 Accepted — polling (attempt ${attempts})...`);
      await new Promise(r => setTimeout(r, POLL_INTERVAL_MS));
      continue;
    }

    if (!res.ok) {
      throw new Error(`HTTP ${res.status} ${res.statusText} for ${report.id}`);
    }

    const json = await res.json();
    // AppFolio returns either { results: [...] } or a direct array
    const rows = Array.isArray(json) ? json : (json.results ?? json);
    return rows;
  }

  throw new Error(`${report.id} did not complete after ${POLL_MAX_ATTEMPTS} polls`);
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

  const dates    = getDateParams();
  const reports  = buildReportCatalogue(dates);
  const results  = { success: [], failed: [] };

  console.log(`[fetchReports] Starting fetch of ${reports.length} AppFolio reports for date ${dates.today}`);

  for (const report of reports) {
    try {
      console.log(`  [${report.id}] Fetching...`);
      const rows = await fetchReport(report);
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

  console.log(`\n[fetchReports] Done. ${results.success.length} succeeded, ${results.failed.length} failed.`);
  if (results.failed.length > 0) {
    console.error("[fetchReports] Failed reports:", results.failed.map(f => f.id).join(", "));
  }

  return results;
}

module.exports = { fetchAndIngestAllReports };
