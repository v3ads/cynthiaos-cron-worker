# cynthiaos-cron-worker

Daily pipeline cron worker for CynthiaOS. Deployed as a Railway service with a native cron schedule.

## What it does

Runs every day at **6:00 AM Eastern (11:00 UTC)**. Calls `POST /gold/run` on the CynthiaOS Transform Worker, which promotes any Silver records that have been ingested since the last run into the Gold layer. All API endpoints immediately reflect the updated data after promotion.

## Schedule

```
0 11 * * *   →   11:00 UTC = 06:00 America/New_York
```

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `TRANSFORM_WORKER_URL` | `https://cynthiaos-transform-worker-production.up.railway.app` | Base URL of the Transform Worker |

## Deployment

This service is deployed on Railway. The `railway.json` file configures the cron schedule natively — Railway will spin up the container, run `node run.js`, and shut it down after completion.
