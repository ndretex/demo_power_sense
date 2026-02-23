# Copilot/Codex/Opus Instructions

You are working in an on‑prem, open‑source electricity forecasting + anomaly detection project. The current phase is ingestion + observability.

## Always Read

- Read all README.md files for project and service context.

## Always Explain

- When suggesting or writing code, explain your reasoning in the chat window. 
- Keep the code self-explanatory with clear variable/function names and short comments.
- Add docstrings to all functions and classes.
- For long todo lists or complex refactoring, explain your approach and seek confirmation before proceeding.

## Primary Goals

- Keep the ingestion service simple, reliable, and observable.
- Route all ClickHouse access through the demo_power_sense_api_data service.
- Preserve the existing Docker Compose and observability configurations.
- Ensure Grafana can query ClickHouse, Prometheus, and Loki without changes to provisioning unless requested.

## Project Conventions

- The compose file is named `docker-compose.yml` (do not rename).
- Ingestion flows live in `services/prefect_worker/ingestion/`.
- Use environment variables for DB connectivity:
  - `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`
- The ingestion interval is controlled by `INGEST_INTERVAL_SECONDS`.
- Table name and columns are fixed: `measurements(ts, source, metric, value, ukey, version, inserted_at)`.
- Use pendulum package for datetime handling.

## Logging Expectations

- Log each ingestion cycle with rows written, duration, and errors.
- Avoid overly verbose logs; prefer structured or consistent formats.

## Do/Don’t

- **Do** keep changes minimal and focused on requirements.
- **Do** prefer batch inserts with retries for transient DB errors.
- **Do** keep code modular (`config.py`, `db.py`, `logging_config.py`).
- **Don’t** add new services or change ports without explicit request.
- **Don’t** modify observability configs unless asked.

## Testing

- Verify all services start without errors.

## Documentation and obsolete code cleanup

At the end of your work, seek confirmation that the changes meet requirements and work as intended. Then, update documentation:
- Update README.md when behavior, setup, or verification steps change.
- Update each service README.md for service-specific changes.
- Update .github/copilot-instructions.md for changes in notes, reminders, conventions or expectations.
- Finally, keep documentation clear and coherent.
- Check for obsolete code/files. Ask confirmation before deleting anything.

# Copilot Notes

## Things to Remember

- This is a multi‑phase project (ingestion → features → forecasting → anomaly detection → production deployment → monitoring). We are currently in Phase 1 (ingestion + observability).
- Ingestion flows currently do not expose Prometheus metrics (Prefect worker metrics disabled).
- all Prefect flows and tasks must have a prefix 'DPS-' in their names.
- ClickHouse runs as the `demo_power_sense_clickhouse` service in docker-compose.yml.
- Grafana has pre‑provisioned datasources (Prometheus, ClickHouse, Loki).
- Logs flow through Promtail to Loki (no extra sidecar).
- Ingestion flows live under services/prefect_worker/ingestion.
- Keep the schema fixed: `measurements(ts, source, metric, value, ukey, version, inserted_at)`.
- Versioning: `version` is a sequential integer per `ukey` that increments when `value` changes; duplicates with same `ukey` and `value` are skipped.
- Avoid changing observability configs unless explicitly requested.
- NaN/NA/NaT values are sanitized to SQL NULL during ingestion inserts.

## Known Good Checks

- Loki query: `{container="ds_ingest"}` (container name preserved for dashboards)
- ClickHouse query: `SELECT ts, metric, value, ukey, version FROM measurements ORDER BY ts DESC LIMIT 100;`
- Ports binding errors: check that ports are not already in use, if yes, change project port. DO NOT TRY TO STOP OTHER PROJECT SERVICES.
