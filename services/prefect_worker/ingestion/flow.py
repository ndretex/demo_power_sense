import time
from typing import Dict

from prefect import flow, get_run_logger

from .tasks import (
    bootstrap_history,
    check_db_empty,
    fetch_api_results,
    normalize_chunk,
    write_rows,
)
from config import API_URL


@flow(name="DPS-ingest_cycle")
def ingest_cycle(api_url: str = API_URL) -> Dict[str, float]:
    logger = get_run_logger()
    start = time.time()
    written = 0
    errors = 0

    try:
        if check_db_empty():
            bootstrap_rows = bootstrap_history()
            bootstrap_counts = []
            bootstrap_counts.append(write_rows(bootstrap_rows))
            logger.info("bootstrap_inserted rows=%d", sum(bootstrap_counts))
        results = fetch_api_results(api_url)
        rows = normalize_chunk(results)
        written = write_rows(rows)
    except Exception as exc:
        errors = 1
        logger.exception("ingest_cycle failed: %s", exc)
        raise
    finally:
        duration = time.time() - start
        logger.info("ingest_cycle rows=%d duration=%.2fs errors=%d", written, duration, errors)

    return {"rows": written, "errors": errors, "duration": duration}
