# dashboard

Phase 2, with the daily forecast it watches. A React app built into the same image and served by a Cloud Run service that scales to zero. It never queries BigQuery: it reads the daily JSON snapshots the evaluate step exports to Cloud Storage, one folder per day, so it can show any past day as it looked then.

Planned views: forecast fan chart against actuals, slot-by-slot error heatmap, coverage and bias over time, model health card, fallback level per run.

Local development: `npm run dev` against a snapshot copied from dev, or against fixtures.

Tests: Vitest, next to the components, against fixture snapshots. The dashboard owns its tests; it never depends on a live pipeline to test.
