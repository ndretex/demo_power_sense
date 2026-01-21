import json
import math
import time
from datetime import timezone
from typing import Dict, List, Optional, Tuple

import clickhouse_connect
from clickhouse_connect.driver.exceptions import ClickHouseError

from config import DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER


def _is_nullish(value) -> bool:
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


def _format_ukey(ts, perimetre: str, nature, metric: str) -> str:
    if getattr(ts, "tzinfo", None) is not None:
        ts = ts.astimezone(timezone.utc)
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
    return clickhouse_connect.get_client(
        host=DB_HOST,
        port=DB_PORT,
        username=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
    )




def _values_equal(left, right) -> bool:
    if left is None and right is None:
        return True
    return left == right


def _fetch_latest_state(
    client, ukeys: List[str], chunk_size: int = 500
) -> Dict[str, Tuple[Optional[float], Optional[int]]]:
    if not ukeys:
        return {}
    state: Dict[str, Tuple[Optional[float], Optional[int]]] = {}
    for i in range(0, len(ukeys), chunk_size):
        batch = ukeys[i : i + chunk_size]
        result = client.query(
            "SELECT ukey, value, version FROM measurements_latest FINAL WHERE ukey IN %(ukeys)s",
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
