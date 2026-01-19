import os
from typing import Optional


def get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    return os.getenv(name, default)


DB_HOST = get_env("DB_HOST", "postgres")
DB_PORT = int(get_env("DB_PORT", "5432"))
DB_NAME = get_env("DB_NAME", "postgres")
DB_USER = get_env("DB_USER", "postgres")
DB_PASSWORD = get_env("DB_PASSWORD", "postgres")

API_URL = get_env(
    "API_URL",
    "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-national-tr/records?where=date_heure%3E%3Dnow()%20-%201%20hour&order_by=date_heure%20DESC",
)
HISTORY_DATA_URL = get_env(
    "HISTORY_DATA_URL",
    "https://www.data.gouv.fr/api/1/datasets/r/1ae6c731-991f-4441-9663-adc99005fac5"
)
INGEST_INTERVAL_SECONDS = int(get_env("INGEST_INTERVAL_SECONDS", "60"))
METRICS_PORT = int(get_env("METRICS_PORT", "8000"))

# optional source tag used when storing rows
DEFAULT_SOURCE = get_env("DEFAULT_SOURCE", "France")
