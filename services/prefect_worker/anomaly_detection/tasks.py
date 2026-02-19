from __future__ import annotations

import pendulum
from typing import List, Tuple

import pandas as pd
from prefect import get_run_logger, task

import db
from config import ANOMALY_METRIC, ANOMALY_MIN_SAMPLES, ANOMALY_Z_THRESHOLD


def _chunk_time_ranges(
    start: pendulum.DateTime, end: pendulum.DateTime, step_hours: int = 24
) -> List[Tuple[pendulum.DateTime, pendulum.DateTime]]:
    """Split a time range into chunked windows."""
    ranges: List[Tuple[pendulum.DateTime, pendulum.DateTime]] = []
    cursor = start
    step = pendulum.duration(hours=step_hours)
    while cursor < end:
        next_cursor = min(cursor + step, end)
        ranges.append((cursor, next_cursor))
        cursor = next_cursor
    return ranges


def _to_iso(ts) -> str:
    """Convert a datetime-like object to ISO-8601 string using pendulum."""
    # Accept datetime or pendulum.DateTime
    try:
        p = pendulum.instance(ts)
    except Exception:
        # fallback: convert via str()
        return str(ts)
    # ensure UTC
    if p.tzinfo is None:
        p = p.in_timezone("UTC")
    else:
        p = p.in_timezone("UTC")
    return p.to_iso8601_string()


@task(name="DPS-fetch_anomaly_history", retries=2, retry_delay_seconds=5)
def fetch_anomaly_history(
    start_ts: pendulum.DateTime,
    end_ts: pendulum.DateTime,
    metric: str = ANOMALY_METRIC,
    chunk_hours: int = 24,
) -> List[dict]:
    """Fetch measurement history in chunks for anomaly detection."""
    logger = get_run_logger()
    rows: List[dict] = []
    for window_start, window_end in _chunk_time_ranges(start_ts, end_ts, chunk_hours):
        logger.info(
            "fetch anomaly window start=%s end=%s",
            window_start.isoformat(),
            window_end.isoformat(),
        )
        chunk = db.fetch_measurements(
            start_ts=_to_iso(window_start),
            end_ts=_to_iso(window_end),
            metric=metric,
            order="asc",
        )
        logger.info("anomaly window rows=%d", len(chunk))
        rows.extend(chunk)
    return rows


@task(name="DPS-build_anomaly_baseline")
def build_anomaly_baseline(
    rows: List[dict],
    eval_start: pendulum.DateTime,
    min_samples: int = ANOMALY_MIN_SAMPLES,
) -> pd.DataFrame:
    """Build baseline mean/std per day-of-week and 15-minute bucket."""
    logger = get_run_logger()
    if not rows:
        logger.info("no rows supplied for baseline")
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    if df.empty:
        logger.info("no rows supplied for baseline")
        return pd.DataFrame()

    df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts", "value"])

    eval_ts = pd.Timestamp(eval_start)
    if eval_ts.tzinfo is None:
        eval_ts = eval_ts.tz_localize("UTC")
    else:
        eval_ts = eval_ts.tz_convert("UTC")
    df = df[df["ts"] < eval_ts]
    if df.empty:
        logger.info("no rows available before eval_start")
        return pd.DataFrame()

    df["dow"] = df["ts"].dt.dayofweek
    df["hour"] = df["ts"].dt.hour
    df["minute"] = df["ts"].dt.minute

    baseline = (
        df.groupby(["dow", "hour", "minute"])
        .agg(
            mean=("value", "mean"),
            std=("value", "std"),
            samples=("value", "count"),
        )
        .reset_index()
    )
    baseline = baseline[baseline["samples"] >= min_samples]
    baseline.loc[baseline["std"] == 0, "std"] = pd.NA

    logger.info("baseline buckets=%d", len(baseline))
    return baseline


@task(name="DPS-score_anomalies")
def score_anomalies(
    rows: List[dict],
    eval_start: pendulum.DateTime,
    baseline: pd.DataFrame,
    z_threshold: float = ANOMALY_Z_THRESHOLD,
) -> pd.DataFrame:
    """Score recent rows using the baseline and return anomalies."""
    logger = get_run_logger()
    if not rows or baseline.empty:
        logger.info("no rows or baseline available for scoring")
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    if df.empty:
        logger.info("no rows available for scoring")
        return pd.DataFrame()

    df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts", "value"])
    eval_ts = pd.Timestamp(eval_start)
    if eval_ts.tzinfo is None:
        eval_ts = eval_ts.tz_localize("UTC")
    else:
        eval_ts = eval_ts.tz_convert("UTC")
    df = df[df["ts"] >= eval_ts]
    if df.empty:
        logger.info("no rows within evaluation window")
        return pd.DataFrame()

    df["dow"] = df["ts"].dt.dayofweek
    df["hour"] = df["ts"].dt.hour
    df["minute"] = df["ts"].dt.minute

    merged = df.merge(baseline, on=["dow", "hour", "minute"], how="inner")
    if merged.empty:
        logger.info("no rows matched baseline buckets")
        return pd.DataFrame()

    merged["zscore"] = (merged["value"] - merged["mean"]) / merged["std"]
    merged["threshold"] = z_threshold
    anomalies = merged[merged["zscore"].abs() >= z_threshold].copy()

    logger.info(
        "scored rows=%d anomalies=%d threshold=%.2f",
        len(merged),
        len(anomalies),
        z_threshold,
    )
    return anomalies


@task(name="DPS-write_anomalies", retries=2, retry_delay_seconds=3)
def write_anomalies(anomalies: pd.DataFrame) -> int:
    """Write anomaly rows to the anomalies table."""
    logger = get_run_logger()
    if anomalies.empty:
        logger.info("no anomalies to write")
        return 0

    rows = []
    for _, rec in anomalies.iterrows():
        ts = rec.get("ts")
        if isinstance(ts, pd.Timestamp):
            ts = ts.to_pydatetime()
        if ts is None:
            continue

        source = rec.get("source")
        metric = rec.get("metric")
        value = rec.get("value")
        zscore = float(rec.get("zscore"))
        mean = float(rec.get("mean"))
        std = float(rec.get("std"))
        threshold = float(rec.get("threshold", ANOMALY_Z_THRESHOLD))
        dow = int(rec.get("dow"))
        hour = int(rec.get("hour"))
        minute = int(rec.get("minute"))
        rows.append(
            (
                ts,
                source,
                metric,
                value,
                zscore,
                mean,
                std,
                threshold,
                dow,
                hour,
                minute,
            )
        )

    if not rows:
        logger.info("no anomaly rows to write")
        return 0

    inserted = db.insert_anomalies(rows)
    logger.info("anomaly rows written=%d", inserted)
    return inserted
