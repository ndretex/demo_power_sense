# Prefect Worker Service

This service runs a Prefect worker that executes project flows (ingest now, model training/deployment/prediction later). The worker joins the external microservices-network to reach the existing Prefect API.

## Responsibilities

- Run a Prefect worker connected to the existing Prefect API.
- Host ingestion flow code under ingestion/.

## Configuration

Environment variables:

- DB connection: DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
- Ingestion: API_URL, HISTORY_DATA_URL, INGEST_INTERVAL_SECONDS
- Prefect: PREFECT_API_URL, PREFECT_WORK_POOL_NAME

## Flow Layout

- ingestion (implemented)
- model_training (placeholder)
- model_deployment (placeholder)
- prediction (placeholder)

## Flow: ingest_cycle

Location: ingestion/flow.py

The ingest flow performs a single ingestion cycle (bootstrap history if empty, fetch, normalize, insert, logs). Schedule it in Prefect to run every 5–15 minutes.

At container startup, `init.sh` runs `ingestion/deployments.py` to register `ingest_cycle` on a 5‑minute interval schedule.

## Deployment Notes

Register the flow with your existing Prefect server (from the repo root or inside the container):

- Ensure the work pool exists (process type).
- Build and apply a deployment for ingestion/flow.py:ingest_cycle.
- Set a schedule in the Prefect UI or in the deployment YAML.

The worker in docker-compose will pick up the deployment runs from the configured pool.

## Observability

Logs are shipped via Promtail/Loki as before (container name is preserved for dashboards).
