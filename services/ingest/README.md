# Ingestion Service

The ingestion service pulls electricity measurements on a fixed schedule, writes them to TimescaleDB, exposes Prometheus metrics, and logs cycle outcomes.

## Responsibilities

- Fetch electricity measurements from a configured source.
- Fetches all available rows for the last hour using API pagination (limit=100).
- Normalize data into the `measurements` table schema.
- Batch insert rows with retries on transient DB errors.
- Expose metrics at `:8000/metrics`.
- Log each ingestion cycle (rows written, duration, errors).

## Configuration

Environment variables:

- `DB_HOST`
- `DB_PORT`
- `DB_NAME`
- `DB_USER`
- `DB_PASSWORD`
- `INGEST_INTERVAL_SECONDS`

## Data Model

Target table: `measurements`

Columns:
- `ts` (TIMESTAMPTZ)
- `source` (TEXT)
- `metric` (TEXT)
- `value` (DOUBLE PRECISION)
- `meta` (JSONB, optional)
- `inserted_at` (TIMESTAMPTZ, default now())

## Data Handling Notes

- During inserts, NaN/NA/NaT values are sanitized to SQL NULL for `value` and any nested `meta` fields.

## Prometheus Metrics

- `ingest_rows_written_total` (counter)
- `ingest_errors_total` (counter)
- `ingest_last_success_timestamp` (gauge)

## Logs

Each cycle should log:

- rows written
- duration (seconds)
- error count (if any)

## Runbook (Verification)

From repo root:

1. Build and start the stack:
   - `docker compose -f docker_compose.yml up -d --build`
2. Confirm containers are running:
   - `docker compose -f docker_compose.yml ps`
3. Verify metrics endpoint:
   - `curl http://localhost:8002/metrics`

If you are upgrading an existing database, add the new column before restarting ingestion:

- `ALTER TABLE measurements ADD COLUMN IF NOT EXISTS inserted_at TIMESTAMPTZ NOT NULL DEFAULT now();`

Grafana checks:

- Prometheus query: `ingest_rows_written_total`
- Loki query: `{container="ds_ingest"}`
- Postgres query:
  - `SELECT ts, metric, value FROM measurements ORDER BY ts DESC LIMIT 100;`
