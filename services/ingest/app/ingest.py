import json
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple
from urllib.parse import urlencode, urlparse, urlunparse, parse_qs

import requests

from . import db, metrics
from .config import API_URL, INGEST_INTERVAL_SECONDS
from .transform import normalize_record


logger = logging.getLogger("ingest")


def _build_paged_url(base_url: str, limit: int, offset: int) -> str:
    parsed = urlparse(base_url)
    query = parse_qs(parsed.query)
    query["limit"] = [str(limit)]
    query["offset"] = [str(offset)]
    new_query = urlencode(query, doseq=True)
    return urlunparse(parsed._replace(query=new_query))


def _build_windowed_url(base_url: str, window_seconds: int = 3600) -> str:
    parsed = urlparse(base_url)
    query = parse_qs(parsed.query)

    window_start = datetime.now(timezone.utc) - timedelta(seconds=window_seconds)
    window_iso = window_start.isoformat(timespec="seconds")
    query["where"] = [f"date_heure >= '{window_iso}'"]

    new_query = urlencode(query, doseq=True)
    return urlunparse(parsed._replace(query=new_query))


def _fetch_all_results(api_url: str, page_limit: int = 100) -> List[Dict[str, Any]]:
    all_results: List[Dict[str, Any]] = []
    offset = 0

    base_url = _build_windowed_url(api_url)
    print(f"Using time window URL: {base_url}")

    while True:
        page_url = _build_paged_url(base_url, limit=page_limit, offset=offset)
        print(f"Fetching page offset={offset} limit={page_limit} url={page_url}")
        resp = requests.get(page_url, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        results = payload.get("results") or []
        print(f"Fetched page offset={offset} count={len(results)} total={len(all_results) + len(results)}")
        if not results:
            break

        all_results.extend(results)
        if len(results) < page_limit:
            break
        offset += page_limit

    return all_results


def fetch_and_store_once(api_url: str = API_URL) -> Tuple[int, int]:
    """Fetch data from API, normalize and store. Returns (rows_written, errors)
    """
    if not api_url:
        logger.error("No API_URL configured; skipping fetch")
        return 0, 1

    try:
        results = _fetch_all_results(api_url)
        print(f"Fetched {len(results)} results from API")
    except Exception as e:
        logger.exception("Error fetching API: %s", e)
        metrics.ingest_errors_total.inc()
        return 0, 1
    all_rows: List[Tuple] = []
    for rec in results:
        rows = normalize_record(rec)
        all_rows.extend(rows)

    try:
        n = db.insert_measurements(all_rows)
        metrics.ingest_rows_written_total.inc(n)
        metrics.ingest_last_success_timestamp.set(time.time())
        logger.info("cycle rows=%d duration=NA errors=0", n)
        return n, 0
    except Exception:
        logger.exception("Error writing to DB")
        metrics.ingest_errors_total.inc()
        return 0, 1


def run_loop(interval_seconds: int = INGEST_INTERVAL_SECONDS, api_url: str = API_URL) -> None:
    while True:
        start = time.time()
        rows, errors = fetch_and_store_once(api_url)
        duration = time.time() - start
        logger.info("ingest_cycle rows=%d duration=%.2fs errors=%d", rows, duration, errors)
        time.sleep(interval_seconds)
