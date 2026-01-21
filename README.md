# Electricity Ingestion + Observability Foundation

## Purpose

Develop all phases of an on‑prem, open‑source electricity forecasting and anomaly detection project, from ingestion through production monitoring.

Current phase (Phase 1):

1. Streaming‑ish ingestion (poll every 5–15 min) of electricity load
2. Feature pipeline (lags, rolling stats, calendar)
3. Forecast model producing next 7 days (hourly or 30‑min)
4. Anomaly detection from forecast residuals + data quality checks
5. Production deployment (API + scheduled batch forecasts)
6. Monitoring: infra + data quality + drift + model performance + alerting

## Features

- Scheduled electricity data ingestion.
- Time‑series storage in ClickHouse.
- Prometheus metrics for ingestion health.
- Centralized logging with Loki + Promtail.
- Grafana exploration across metrics, logs, and time‑series data.
- NaN/NA/NaT values are sanitized to SQL NULL during ingestion inserts.
- Each measurement row stores insertion time in `inserted_at` (defaults to current time).
- Each measurement row includes a `ukey` JSON string and a `version` hash to keep distinct values while collapsing duplicates.
- A materialized view (`measurements_latest`) keeps the latest inserted version per `ukey`.

## Services (Docker Compose)

- **clickhouse**: ClickHouse backing store.
- **ingest**: Python ingestion service exposing Prometheus metrics on port 8000.
- **prometheus**: Scrapes ingest metrics.
- **loki** + **promtail**: Collect and index logs.
- **grafana**: Dashboards and data exploration.

## Repository Structure

- docker-compose.yml
- ch/init/001.database.sql
- ch/init/002.tables.sql
- ch/init/003.views.sql
- observability/
  - grafana/provisioning/datasources/datasources.yml
  - prometheus/prometheus.yml
  - loki/config.yml
  - promtail/config.yml
- services/ingest/
  - Dockerfile
  - requirements.txt
  - app/
    - main.py
    - config.py
    - ingest.py
    - db.py
    - metrics.py
    - logging.py

## Service Documentation

- See the ingestion service details in services/ingest/README.md.
