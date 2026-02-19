# dbt Setup and Usage

## What dbt manages

- `measurements_latest` is now built from a dbt model (`fct_measurements_latest` with alias `measurements_latest`).
- `fct_ingestion_daily_coverage` reports daily ingestion coverage per `source` and `metric`, with `is_missing_day = true` for missing days.

## Run with Docker Compose

Build and run dbt models:

```bash
docker compose run --rm dbt run
```

Run tests:

```bash
docker compose run --rm dbt test
```

Build + test:

```bash
docker compose run --rm dbt build
```

## Migration from previous materialized view setup

If your ClickHouse instance already has the old materialized-view objects, drop them once before the first dbt run:

```sql
drop view if exists electricity.measurements_latest_mv;
drop table if exists electricity.measurements_latest;
```

## Typical queries

Latest rows by `ukey`:

```sql
select *
from electricity.measurements_latest
limit 100;
```

Missing ingestion days:

```sql
select *
from electricity.fct_ingestion_daily_coverage
where is_missing_day
order by data_day desc, source, metric;
```
