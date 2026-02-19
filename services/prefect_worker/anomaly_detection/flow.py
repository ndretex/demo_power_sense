from __future__ import annotations

import time
import pendulum
from typing import Dict

from prefect import flow, get_run_logger

from config import (
    ANOMALY_EVAL_HOURS,
    ANOMALY_LOOKBACK_DAYS,
    ANOMALY_METRIC,
)
from .tasks import (
    build_anomaly_baseline,
    fetch_anomaly_history,
    score_anomalies,
    write_anomalies,
)


@flow(name="DPS-anomaly_detection_cycle")
def anomaly_detection_cycle(metric: str = ANOMALY_METRIC) -> Dict[str, float]:
    """Run the baseline z-score anomaly detection cycle."""
    logger = get_run_logger()
    start = time.time()
    errors = 0
    written = 0

    now = pendulum.now("UTC")
    history_start = now.subtract(days=ANOMALY_LOOKBACK_DAYS)
    eval_start = now.subtract(hours=ANOMALY_EVAL_HOURS)

    try:
        rows = fetch_anomaly_history(history_start, now, metric=metric)
        baseline = build_anomaly_baseline(rows, eval_start)
        anomalies = score_anomalies(rows, eval_start, baseline)
        written = write_anomalies(anomalies)
    except Exception as exc:
        errors = 1
        logger.exception("anomaly_detection_cycle failed: %s", exc)
        raise
    finally:
        duration = time.time() - start
        logger.info(
            "anomaly_detection_cycle rows_written=%d duration=%.2fs errors=%d",
            written,
            duration,
            errors,
        )

    return {"rows_written": written, "errors": errors, "duration": duration}
