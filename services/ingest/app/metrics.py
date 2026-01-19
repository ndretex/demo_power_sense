from prometheus_client import Counter, Gauge, start_http_server
from .config import METRICS_PORT

# Prometheus metrics expected by repo conventions
ingest_rows_written_total = Counter(
    "ingest_rows_written_total", "Total rows written by the ingest service"
)
ingest_errors_total = Counter(
    "ingest_errors_total", "Total ingestion errors"
)
ingest_last_success_timestamp = Gauge(
    "ingest_last_success_timestamp", "Unix timestamp of last successful ingestion"
)


def start_metrics_server(port: int = METRICS_PORT) -> None:
    start_http_server(port)
