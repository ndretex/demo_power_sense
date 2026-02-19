# Anomaly Detection (Baseline Z-Score)

This folder contains the early anomaly detection flow using a baseline z-score per day-of-week and 15-minute bucket for the `consommation` metric.

## How it works

1. Fetches historical measurements for the configured lookback window.
2. Builds baseline mean and standard deviation per (day_of_week, hour, minute).
3. Scores recent rows (last evaluation window) with z-score.
4. Writes anomaly rows to the `anomalies` table.

## Configuration

Environment variables (see `services/prefect_worker/config.py`):

- `ANOMALY_METRIC` (default: `consommation`)
- `ANOMALY_LOOKBACK_DAYS` (default: `84`)
- `ANOMALY_EVAL_HOURS` (default: `24`)
- `ANOMALY_Z_THRESHOLD` (default: `3.0`)
- `ANOMALY_MIN_SAMPLES` (default: `20`)
- `ANOMALY_RESULT_METRIC` (default: `consommation_anomaly_zscore`)
- `ANOMALY_NATURE` (default: `anomaly_detection`)

## Flow

- `DPS-anomaly_detection_cycle` in `flow.py`
