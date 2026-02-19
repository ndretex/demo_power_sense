import pendulum
from typing import Any, Dict, List, Tuple
from urllib.parse import urlencode, urlparse, urlunparse, parse_qs

import io
import urllib.request
import zipfile

import requests
from prefect import get_run_logger, task

from config import API_URL, DATA_API_URL, HISTORY_DATA_URL
from ingestion.transform import normalize_record, remap_metric_name


def _build_paged_url(base_url: str, limit: int, offset: int) -> str:
    parsed = urlparse(base_url)
    query = parse_qs(parsed.query)
    query["limit"] = [str(limit)]
    query["offset"] = [str(offset)]
    new_query = urlencode(query, doseq=True)
    return urlunparse(parsed._replace(query=new_query))


def _build_windowed_url(base_url: str, window_days: int = 1) -> str:
    parsed = urlparse(base_url)
    query = parse_qs(parsed.query)

    window_start = pendulum.now("UTC").subtract(days=window_days)
    window_iso = window_start.to_iso8601_string()
    query["where"] = [f"date_heure > '{window_iso}'"]

    new_query = urlencode(query, doseq=True)
    return urlunparse(parsed._replace(query=new_query))


def _build_data_api_url(path: str) -> str:
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


def _post_data_api(path: str, payload: dict, timeout: int = 30) -> requests.Response:
    """Send a JSON POST request to the Data API."""
    url = _build_data_api_url(path)
    return requests.post(url, json=payload, timeout=timeout)


def _get_data_api(path: str, timeout: int = 30) -> dict:
    """Send a GET request to the Data API and return JSON."""
    url = _build_data_api_url(path)
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
            resp = _post_data_api("measurements/ingest", payload, timeout=60)
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
    data = _get_data_api("measurements/count")
    return int(data.get("count", 0))


def is_empty() -> bool:
    """Return True when no measurements exist."""
    return count_measurements() == 0


@task(name="DPS-check_db_empty")
def check_db_empty() -> bool:
    """Return True when no measurements exist via the Data API."""
    logger = get_run_logger()
    try:
        empty = is_empty()
        logger.info("db_empty=%s", empty)
        return empty
    except Exception as exc:
        logger.exception("db empty check failed: %s", exc)
        raise


@task(name="DPS-bootstrap_history")
def bootstrap_history() -> List[Tuple]:
    logger = get_run_logger()
    try:
        # download temporary zip file, extract XLS, parse rows
        with urllib.request.urlopen(HISTORY_DATA_URL) as resp:
            zip_bytes = resp.read()

        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
            # find the first xls/xlsx file
            xls_filename = None
            for name in z.namelist():
                if name.lower().endswith((".xls", ".xlsx")):
                    xls_filename = name
                    break
            if not xls_filename:
                raise ValueError("No XLS/XLSX file found in history zip")

            content_bytes = z.read(xls_filename)
            ext = xls_filename.lower().rsplit(".", 1)[-1]
            if ext == "xls":
                engine = "xlrd"
            else:
                engine = "openpyxl"
            try:
                import pandas as pd

                df = pd.read_excel(
                    io.BytesIO(content_bytes), engine=engine, index_col=False, header=0
                )
            except Exception:
                import pandas as pd

                text = content_bytes.decode("latin-1")
                df = pd.read_csv(
                    io.StringIO(text), sep="\t", index_col=False, header=0, low_memory=False
                )

            df = df[df["Nature"].notnull()]
            new_columns = [remap_metric_name(col) for col in df.columns]
            df.columns = new_columns
            if "date" in df.columns and "heure" in df.columns:
                df["date_heure"] = df.apply(
                    lambda row: f"{row['date']}T{row['heure']}:00+00:00", axis=1
                )

        import pandas as pd

        cutoff_date = pendulum.now("UTC").subtract(days=1).date()
        df = df[pd.to_datetime(df["date_heure"]).dt.date <= cutoff_date]

        logger.info("bootstrap history rows=%d", len(df))

        rows: List[Tuple] = []
        for _, rec in df.iterrows():
            clean = {k: v for k, v in rec.items() if v is not None}
            rows.extend(normalize_record(clean))

        logger.info("bootstrap normalized rows=%d", len(rows))
        return rows
    except Exception as exc:
        logger.exception("history bootstrap failed: %s", exc)
        raise


@task(name="DPS-fetch_api_results", retries=2, retry_delay_seconds=5)
def fetch_api_results(api_url: str = API_URL, page_limit: int = 100) -> List[Dict[str, Any]]:
    logger = get_run_logger()
    if not api_url:
        raise ValueError("API_URL is empty; cannot fetch data")

    all_results: List[Dict[str, Any]] = []
    offset = 0

    base_url = _build_windowed_url(api_url)
    logger.info("fetch window url=%s", base_url)

    while True:
        page_url = _build_paged_url(base_url, limit=page_limit, offset=offset)
        logger.info("fetch page offset=%d limit=%d", offset, page_limit)
        resp = requests.get(page_url, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        results = payload.get("results") or []
        logger.info("page offset=%d count=%d total=%d", offset, len(results), len(all_results) + len(results))
        if not results:
            break

        all_results.extend(results)
        if len(results) < page_limit:
            break
        offset += page_limit

    return all_results


# @task(name="DPS-split_results")
def split_results(results: List[Dict[str, Any]], chunk_size: int = 500) -> List[List[Dict[str, Any]]]:
    if chunk_size <= 0:
        return [results]
    return [results[i : i + chunk_size] for i in range(0, len(results), chunk_size)]


# @task(name="DPS-split_rows")
def split_rows(rows: List[Tuple], chunk_size: int = 5000) -> List[List[Tuple]]:
    if chunk_size <= 0:
        return [rows]
    return [rows[i : i + chunk_size] for i in range(0, len(rows), chunk_size)]


# @task(name="DPS-normalize_chunk")
def normalize_chunk(results: List[Dict[str, Any]]) -> List[Tuple]:
    rows: List[Tuple] = []
    for rec in results:
        rows.extend(normalize_record(rec))
    return rows


# @task(name="DPS-remap_metric_name")
def remap_metric_name_task(original_name: str) -> str:
    return remap_metric_name(original_name)


# @task(name="DPS-normalize_record")
def normalize_record_task(rec: Dict[str, Any]) -> List[Tuple]:
    return normalize_record(rec)


# @task(name="DPS-flatten_rows")
def flatten_rows(row_lists: List[List[Tuple]]) -> List[Tuple]:
    rows: List[Tuple] = []
    for chunk in row_lists:
        rows.extend(chunk)
    return rows


# @task(name="DPS-write_rows", retries=2, retry_delay_seconds=3)
def write_rows(rows: List[Tuple]) -> int:
    if not rows:
        return 0
    n = insert_measurements(rows)
    return n
