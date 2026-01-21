# Ingestion Pipeline

This folder contains the Prefect ingestion flow and tasks that load electricity data into ClickHouse.

## Step-by-step pipeline

1. **Check ClickHouse emptiness** (`DPS-check_db_empty`)
   - Queries the `measurements` table to determine if it is empty.
   - Logs the `db_empty` flag.

2. **Bootstrap historical data** (`DPS-bootstrap_history`, conditional)
   - Runs only if the table is empty.
   - Downloads a historical dataset, parses it, normalizes records, and returns rows for insert.
   - Logs completion once the bootstrap is done.

3. **Fetch recent API data** (`DPS-fetch_api_results`)
   - Builds a time-windowed API URL (last hour by default).
   - Pages through the API using `limit`/`offset` until all results are collected.
   - Returns a list of raw records.

4. **Normalize results** (`DPS-normalize_chunk`)
   - Converts API records into measurement rows.
   - Each numeric field becomes a measurement with timestamps and source metadata.

5. **Write rows to ClickHouse** (`DPS-write_rows`)
   - Inserts normalized rows into the `measurements` table.
   - Retries on transient database errors.
   - Deduplicates rows with the same `ukey` and `value` and assigns a sequential `version` per `ukey` when values change.

Bootstrap normalization and inserts run sequentially to avoid parallel fan-out.

6. **Flow summary logging** (`DPS-ingest_cycle`)
   - Aggregates rows written across chunks.
   - Logs total rows, duration, and error count for the cycle.

## Files

- `flow.py`: Prefect flow orchestration (`DPS-ingest_cycle`).
- `tasks.py`: Task implementations for fetch, normalize, and insert.
- `transform.py`: Record normalization helpers.
- `deployments.py`: Registers the scheduled deployment (every 5 minutes).
