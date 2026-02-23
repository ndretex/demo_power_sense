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
- (Metrics disabled) Prometheus is kept for future phases.
- Centralized logging with Loki + Promtail.
- Grafana exploration across metrics, logs, and time‑series data.
- Database access is centralized behind the Data API service.
- Baseline z‑score anomaly detection on 15‑minute consommation data stored in a dedicated anomalies table.
- NaN/NA/NaT values are sanitized to SQL NULL during ingestion inserts.
- Each measurement row stores insertion time in `inserted_at` (defaults to current time).
- Each measurement row includes a `ukey` JSON string and a sequential `version` that increments when the value for a `ukey` changes (1, 2, 3...).
- The `measurements` table deduplicates on (`ukey`, `version`) and keeps the latest `inserted_at` during merges.
- dbt models build derived analytics relations, including `measurements_latest` and daily ingestion coverage checks.

## Services (Docker Compose)

- **demo_power_sense_clickhouse**: ClickHouse backing store.
- **demo_power_sense_prefect_worker**: Prefect worker running project flows (ingest now, future phases later).
- **demo_power_sense_api_data**: FastAPI service for all ClickHouse inserts and queries.
- **demo_power_sense_dbt**: Builds and tests derived models in ClickHouse.
- **demo_power_sense_prometheus**: Reserved for future metrics.
- **demo_power_sense_loki** + **demo_power_sense_promtail**: Collect and index logs.
- **demo_power_sense_grafana**: Dashboards and data exploration.

## Networking

- The `demo_power_sense_prefect_worker` service joins the external `microservices-network` to reach the shared Prefect API.
- The `demo_power_sense_api_data` service is isolated to the project default network to avoid cross-project traffic on `microservices-network`.
- Container names, service hostnames, and locally built image tags use the `demo_power_sense_*` / `demo_power_sense/*` convention to avoid collisions with other projects on shared Docker networks.

## Repository Structure

- docker-compose.yml
- ch/init/001.database.sql
- ch/init/002.tables.sql
- ch/init/003.views.sql
- dbt/
  - dbt_project.yml
  - profiles.yml
  - models/staging/
  - models/marts/
- observability/
  - grafana/provisioning/datasources/datasources.yml
  - prometheus/prometheus.yml
  - loki/config.yml
  - promtail/config.yml
- services/prefect_worker/
  - Dockerfile
  - requirements.txt
  - main.py
  - config.py
  - db.py
  - logging_config.py
  - anomaly_detection/
  - ingestion/
    - flow.py
    - tasks.py
    - transform.py
- services/api_data/
  - Dockerfile
  - requirements.txt
  - main.py
  - config.py
  - db.py
  - logging_config.py

## Service Documentation

- See the Prefect worker details in services/prefect_worker/README.md.
- See the Data API details in services/api_data/README.md.
- See dbt usage in docs/dbt_setup.md.
