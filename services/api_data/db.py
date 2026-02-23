import json
import math
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import clickhouse_connect
from clickhouse_connect.driver.exceptions import ClickHouseError

from config import DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER


def _is_nullish(value) -> bool:
    """Return True when the value should be stored as SQL NULL."""
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    try:
        import pandas as pd

        if value is pd.NA or value is pd.NaT:
            return True
    except Exception:
        pass
    return False


def _ensure_utc(ts: datetime) -> datetime:
    """Ensure timestamps are timezone-aware in UTC."""
    if getattr(ts, "tzinfo", None) is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def _format_ukey(ts: datetime, perimetre: str, nature, metric: str) -> str:
    """Build the deterministic ukey JSON string for a measurement."""
    ts = _ensure_utc(ts)
    date_str = ts.strftime("%Y%m%d")
    time_str = ts.strftime("%H:%M:%S")
    payload = {
        "perimetre": perimetre,
        "nature": nature,
        "metric": metric,
        "date": date_str,
        "time": time_str,
    }
    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )


def get_client():
    """Create a ClickHouse client with configured credentials."""
    return clickhouse_connect.get_client(
        host=DB_HOST,
        port=DB_PORT,
        username=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
    )


def _values_equal(left, right) -> bool:
    """Compare values while treating None as equal."""
    if left is None and right is None:
        return True
    return left == right


def _fetch_latest_state(
    client, ukeys: List[str], chunk_size: int = 500
) -> Dict[str, Tuple[Optional[float], Optional[int]]]:
    """Fetch latest value/version per ukey from the latest view."""
    if not ukeys:
        return {}
    state: Dict[str, Tuple[Optional[float], Optional[int]]] = {}
    for i in range(0, len(ukeys), chunk_size):
        batch = ukeys[i : i + chunk_size]
        result = client.query(
            "SELECT ukey, value, version FROM int_electricity__measurements_latest WHERE ukey IN %(ukeys)s",
            parameters={"ukeys": batch},
        )
        for ukey, value, version in result.result_rows:
            state[ukey] = (value, int(version) if version is not None else None)
    return state


def insert_measurements(
    rows: List[Tuple],
    retry_seconds: int = 1,
    max_retries: int = 5,
) -> int:
    """
    Batch insert rows into measurements.

    rows: list of (ts, source, metric, value, perimetre, nature)
    Returns number of rows inserted/upserted.
    """

    if not rows:
        return 0

    attempts = 0
    while True:
        try:
            client = get_client()

            prepared_rows: List[Tuple] = []
            staged: List[Tuple] = []
            for r in rows:
                ts, source, metric, value, perimetre, nature = r
                clean_value = None if _is_nullish(value) else value
                ukey_json = _format_ukey(ts, perimetre, nature, metric)
                staged.append((ts, source, metric, clean_value, ukey_json))

            ukeys = list({row[4] for row in staged})
            latest_state = _fetch_latest_state(client, ukeys)

            for ts, source, metric, clean_value, ukey_json in staged:
                last_value, last_version = latest_state.get(ukey_json, (None, None))
                if _values_equal(clean_value, last_value):
                    continue
                next_version = 1 if last_version is None else int(last_version) + 1
                prepared_rows.append(
                    (ts, source, metric, clean_value, ukey_json, next_version)
                )
                latest_state[ukey_json] = (clean_value, next_version)

            if not prepared_rows:
                return 0

            client.insert(
                "measurements",
                prepared_rows,
                column_names=["ts", "source", "metric", "value", "ukey", "version"],
            )
            return len(prepared_rows)
        except ClickHouseError:
            attempts += 1
            if attempts >= max_retries:
                raise
            time.sleep(retry_seconds * attempts)


def fetch_measurements(
    start_ts: Optional[datetime] = None,
    end_ts: Optional[datetime] = None,
    source: Optional[str] = None,
    metric: Optional[str] = None,
    ukey: Optional[str] = None,
    limit: int = 100,
    order: str = "desc",
) -> List[Dict[str, Optional[str]]]:
    """Fetch measurements with optional filters."""
    client = get_client()

    clauses: List[str] = []
    params: Dict[str, object] = {"limit": limit}

    if start_ts:
        clauses.append("ts >= %(start_ts)s")
        params["start_ts"] = _ensure_utc(start_ts)
    if end_ts:
        clauses.append("ts <= %(end_ts)s")
        params["end_ts"] = _ensure_utc(end_ts)
    if source:
        clauses.append("source = %(source)s")
        params["source"] = source
    if metric:
        clauses.append("metric = %(metric)s")
        params["metric"] = metric
    if ukey:
        clauses.append("ukey = %(ukey)s")
        params["ukey"] = ukey

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    order_sql = "DESC" if order.lower() == "desc" else "ASC"

    query = (
        "SELECT ts, source, metric, value, ukey, version, inserted_at "
        "FROM measurements "
        f"{where_sql} "
        f"ORDER BY ts {order_sql} "
        "LIMIT %(limit)s"
    )

    result = client.query(query, parameters=params)
    rows: List[Dict[str, Optional[str]]] = []
    for ts, source_val, metric_val, value, ukey_val, version, inserted_at in result.result_rows:
        rows.append(
            {
                "ts": _ensure_utc(ts).isoformat(),
                "source": source_val,
                "metric": metric_val,
                "value": value,
                "ukey": ukey_val,
                "version": int(version),
                "inserted_at": _ensure_utc(inserted_at).isoformat(),
            }
        )
    return rows


def fetch_latest_measurements(
    ukey: Optional[str] = None,
    limit: int = 100,
    order: str = "desc",
) -> List[Dict[str, Optional[str]]]:
    """Fetch latest measurements per ukey from the latest view."""
    client = get_client()

    clauses: List[str] = []
    params: Dict[str, object] = {"limit": limit}

    if ukey:
        clauses.append("ukey = %(ukey)s")
        params["ukey"] = ukey

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    order_sql = "DESC" if order.lower() == "desc" else "ASC"

    query = (
        "SELECT ts, source, metric, value, ukey, version, inserted_at "
        "FROM int_electricity__measurements_latest "
        f"{where_sql} "
        f"ORDER BY ts {order_sql} "
        "LIMIT %(limit)s"
    )

    result = client.query(query, parameters=params)
    rows: List[Dict[str, Optional[str]]] = []
    for ts, source_val, metric_val, value, ukey_val, version, inserted_at in result.result_rows:
        rows.append(
            {
                "ts": _ensure_utc(ts).isoformat(),
                "source": source_val,
                "metric": metric_val,
                "value": value,
                "ukey": ukey_val,
                "version": int(version),
                "inserted_at": _ensure_utc(inserted_at).isoformat(),
            }
        )
    return rows


def insert_anomalies(
    rows: List[Tuple],
    retry_seconds: int = 1,
    max_retries: int = 5,
) -> int:
    """
    Batch insert rows into anomalies.

    rows: list of (ts, source, metric, value, zscore, mean, std, threshold, dow, hour, minute)
    Returns number of rows inserted.
    """

    if not rows:
        return 0

    attempts = 0
    while True:
        try:
            client = get_client()
            prepared_rows: List[Tuple] = []
            for (
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
            ) in rows:
                clean_value = None if _is_nullish(value) else value
                prepared_rows.append(
                    (
                        _ensure_utc(ts),
                        source,
                        metric,
                        clean_value,
                        float(zscore),
                        float(mean),
                        float(std),
                        float(threshold),
                        int(dow),
                        int(hour),
                        int(minute),
                    )
                )

            client.insert(
                "anomalies",
                prepared_rows,
                column_names=[
                    "ts",
                    "source",
                    "metric",
                    "value",
                    "zscore",
                    "mean",
                    "std",
                    "threshold",
                    "dow",
                    "hour",
                    "minute",
                ],
            )
            return len(prepared_rows)
        except ClickHouseError:
            attempts += 1
            if attempts >= max_retries:
                raise
            time.sleep(retry_seconds * attempts)


def fetch_anomalies(
    start_ts: Optional[datetime] = None,
    end_ts: Optional[datetime] = None,
    source: Optional[str] = None,
    metric: Optional[str] = None,
    limit: int = 100,
    order: str = "desc",
) -> List[Dict[str, Optional[str]]]:
    """Fetch anomalies with optional filters."""
    client = get_client()

    clauses: List[str] = []
    params: Dict[str, object] = {"limit": limit}

    if start_ts:
        clauses.append("ts >= %(start_ts)s")
        params["start_ts"] = _ensure_utc(start_ts)
    if end_ts:
        clauses.append("ts <= %(end_ts)s")
        params["end_ts"] = _ensure_utc(end_ts)
    if source:
        clauses.append("source = %(source)s")
        params["source"] = source
    if metric:
        clauses.append("metric = %(metric)s")
        params["metric"] = metric

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    order_sql = "DESC" if order.lower() == "desc" else "ASC"

    query = (
        "SELECT ts, source, metric, value, zscore, mean, std, threshold, dow, hour, minute, inserted_at "
        "FROM anomalies "
        f"{where_sql} "
        f"ORDER BY ts {order_sql} "
        "LIMIT %(limit)s"
    )

    result = client.query(query, parameters=params)
    rows: List[Dict[str, Optional[str]]] = []
    for (
        ts,
        source_val,
        metric_val,
        value,
        zscore,
        mean,
        std,
        threshold,
        dow,
        hour,
        minute,
        inserted_at,
    ) in result.result_rows:
        rows.append(
            {
                "ts": _ensure_utc(ts).isoformat(),
                "source": source_val,
                "metric": metric_val,
                "value": value,
                "zscore": float(zscore),
                "mean": float(mean),
                "std": float(std),
                "threshold": float(threshold),
                "dow": int(dow),
                "hour": int(hour),
                "minute": int(minute),
                "inserted_at": _ensure_utc(inserted_at).isoformat(),
            }
        )
    return rows


def count_measurements() -> int:
    """Return the total number of measurements."""
    client = get_client()
    result = client.query("SELECT count() FROM measurements")
    return int(result.result_rows[0][0]) if result.result_rows else 0


def ping() -> bool:
    """Return True when ClickHouse is reachable."""
    try:
        client = get_client()
        result = client.query("SELECT 1")
        return bool(result.result_rows)
    except Exception:
        return False
