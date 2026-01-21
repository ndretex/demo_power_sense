# Ingestion Service

The ingestion service pulls electricity measurements on a fixed schedule, writes them to ClickHouse, exposes Prometheus metrics, and logs cycle outcomes.

## Responsibilities

- Fetch electricity measurements from a configured source.
- Fetches all available rows for the last hour using API pagination (limit=100).
- Normalize data into the `measurements` table schema.
- Batch insert rows with retries on transient DB errors.
- Compute a per-row `version` hash to deduplicate identical values for the same `ukey` while keeping distinct versions.
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

Latest-version view: `measurements_latest` (materialized view)

Columns:
- `ts` (DateTime64(3, UTC))
- `source` (String)
- `metric` (String)
- `value` (Nullable(Float64))
- `ukey` (String JSON text: {perimetre, nature, metric, date, time})
- `version` (UInt64 hash of `value` + `ukey`)
- `inserted_at` (DateTime64(3, UTC), default now())

Latest-version view (`measurements_latest`) uses `inserted_at` to keep the most recently inserted row per `ukey`.

## Data Handling Notes

- During inserts, NaN/NA/NaT values are sanitized to SQL NULL for `value`.
- Identical rows (same `ukey` and `version`) are collapsed by ClickHouse.

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
   - `docker compose -f docker-compose.yml up -d --build`
2. Confirm containers are running:
   - `docker compose -f docker-compose.yml ps`
3. Verify metrics endpoint:
   - `curl http://localhost:8002/metrics`

If you are upgrading an existing ClickHouse database, align the schema before restarting ingestion:

- `ALTER TABLE measurements ADD COLUMN IF NOT EXISTS ukey String;`
- `ALTER TABLE measurements DROP COLUMN IF EXISTS meta;`
- `ALTER TABLE measurements MODIFY ORDER BY (ukey, version);`
- `ALTER TABLE measurements MODIFY ENGINE = ReplacingMergeTree(version);`
- `ALTER TABLE measurements_latest ADD COLUMN IF NOT EXISTS ukey String;`
- `ALTER TABLE measurements_latest DROP COLUMN IF EXISTS meta;`
- `ALTER TABLE measurements_latest MODIFY ORDER BY (ukey);`

Grafana checks:

- Prometheus query: `ingest_rows_written_total`
- Loki query: `{container="ds_ingest"}`
- ClickHouse query:
   - `SELECT ts, metric, value, ukey, version FROM measurements ORDER BY ts DESC LIMIT 100;`
