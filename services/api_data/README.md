# Data API Service

This service exposes a FastAPI interface for inserting and fetching measurements in ClickHouse. It is the only supported entrypoint for database interactions.

## Responsibilities

- Insert measurements with versioning and de-duplication.
- Fetch measurements and latest snapshots.
- Provide simple health and count endpoints.

## Configuration

Environment variables:

- DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
- LOG_LEVEL

## Endpoints

- GET /health
- POST /measurements/ingest
- GET /measurements
- GET /measurements/latest
- GET /measurements/count
- POST /anomalies/ingest
- GET /anomalies

## Example Requests

- Insert rows
  - POST /measurements/ingest
  - Body: {"rows": [{"ts": "2024-01-01T00:00:00Z", "source": "France", "metric": "load", "value": 123.4, "perimetre": "France", "nature": "Nationale"}]}

- Fetch rows
  - GET /measurements?metric=load&limit=100

- Latest rows
  - GET /measurements/latest?limit=100

- Insert anomalies
  - POST /anomalies/ingest
  - Body: {"rows": [{"ts": "2024-01-01T00:00:00Z", "source": "France", "metric": "consommation", "value": 123.4, "zscore": 3.2, "mean": 100.0, "std": 7.3, "threshold": 3.0, "dow": 0, "hour": 0, "minute": 0}]}

- Fetch anomalies
  - GET /anomalies?metric=consommation&limit=100
