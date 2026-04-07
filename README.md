# cynthiaos-cron-worker

Daily pipeline cron worker for CynthiaOS. Deployed as a Railway service with a native cron schedule. Runs fully automatically — no manual intervention required.

## What it does

Runs every day at **6:00 AM Eastern (11:00 UTC)**. Executes the full CynthiaOS data pipeline in sequence:

1. **Fetch** — Calls all 29 AppFolio report endpoints with correct date parameters (static, as-of-date, YTD, T12, forward-looking).
2. **Ingest** — POSTs each report's data to the CynthiaOS Ingestion Worker (`POST /ingest/report`), writing it to the Bronze layer.
3. **Promote** — Calls `POST /gold/run` on the Transform Worker, promoting Silver records to the Gold layer so all API endpoints reflect the latest data.

## Schedule

```
0 11 * * *   →   11:00 UTC = 06:00 America/New_York
```

## Reports fetched (29 total)

| Category | Reports |
|---|---|
| Static (current state) | `rent_roll`, `rent_roll_itemized`, `delinquency`, `aged_receivables_detail`, `unit_vacancy`, `tenant_directory`, `owner_directory`, `property_directory`, `vendor_directory`, `unit_directory` |
| As-of-date | `balance_sheet` |
| YTD (Jan 1 → today) | `trial_balance`, `cash_flow`, `income_statement`, `general_ledger`, `check_register_detail`, `deposit_register`, `charge_detail`, `receivables_activity`, `lease_history`, `unit_turn_detail`, `renewal_summary`, `prospect_source_tracking`, `guest_cards`, `rental_applications`, `work_order` |
| Trailing 12 months | `twelve_month_cash_flow`, `twelve_month_income_statement` |
| Forward-looking | `lease_expiration_detail` |

## Environment variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `APPFOLIO_CLIENT_ID` | ✅ | — | AppFolio Basic Auth client ID |
| `APPFOLIO_CLIENT_SECRET` | ✅ | — | AppFolio Basic Auth client secret |
| `INGESTION_URL` | — | `https://cynthiaos-ingestion-worker-production-8068.up.railway.app` | CynthiaOS ingestion worker base URL |
| `TRANSFORM_WORKER_URL` | — | `https://cynthiaos-transform-worker-production.up.railway.app` | CynthiaOS transform worker base URL |

## Deployment

This service is deployed on Railway. The `railway.json` file configures the cron schedule natively — Railway spins up the container, runs `node run.js` to completion, and shuts it down.
