import time
from typing import List, Tuple

import requests

from config import DATA_API_URL


def _build_url(path: str) -> str:
    """Join a path to the base Data API URL."""
    base = DATA_API_URL.rstrip("/")
    return f"{base}/{path.lstrip('/')}"


def _is_nullish(value) -> bool:
    """Return True when the value should be stored as SQL NULL."""
    if value is None:
        return True
    try:
        import math

        if isinstance(value, float) and math.isnan(value):
            return True
    except Exception:
        pass
    try:
        import pandas as pd

        if value is pd.NA or value is pd.NaT:
            return True
    except Exception:
        pass
    return False


def _post_json(path: str, payload: dict, timeout: int = 30) -> requests.Response:
    """Send a JSON POST request to the Data API."""
    url = _build_url(path)
    return requests.post(url, json=payload, timeout=timeout)


def _get_json(path: str, timeout: int = 30) -> dict:
    """Send a GET request to the Data API and return JSON."""
    url = _build_url(path)
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def insert_measurements(
    rows: List[Tuple],
    retry_seconds: int = 1,
    max_retries: int = 5,
) -> int:
    """
    Batch insert rows into measurements via the Data API.

    rows: list of (ts, source, metric, value, perimetre, nature)
    Returns number of rows inserted/upserted.
    """

    if not rows:
        return 0

    payload = {
        "rows": [
            {
                "ts": ts.isoformat(),
                "source": source,
                "metric": metric,
                "value": None if _is_nullish(value) else value,
                "perimetre": perimetre,
                "nature": nature,
            }
            for ts, source, metric, value, perimetre, nature in rows
        ]
    }

    attempts = 0
    while True:
        try:
            resp = _post_json("measurements/ingest", payload, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            return int(data.get("inserted", 0))
        except requests.RequestException:
            attempts += 1
            if attempts >= max_retries:
                raise
            time.sleep(retry_seconds * attempts)


def count_measurements() -> int:
    """Return total measurement count from the Data API."""
    data = _get_json("measurements/count")
    return int(data.get("count", 0))


def is_empty() -> bool:
    """Return True when no measurements exist."""
    return count_measurements() == 0
