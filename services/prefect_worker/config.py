import os
from typing import Optional


def get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    return os.getenv(name, default)


DB_HOST = get_env("DB_HOST", "clickhouse")
DB_PORT = int(get_env("DB_PORT", "8123"))
DB_NAME = get_env("DB_NAME", "electricity")
DB_USER = get_env("DB_USER", "electricity")
DB_PASSWORD = get_env("DB_PASSWORD", "electricity")


# INGESTION PHASE CONFIGURATION
API_URL = get_env(
    "API_URL",
    "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-national-tr/records?where=date_heure%3E%3Dnow()%20-%201%20day&order_by=date_heure%20DESC",
)
HISTORY_DATA_URL = get_env(
    "HISTORY_DATA_URL",
    "https://www.data.gouv.fr/api/1/datasets/r/1ae6c731-991f-4441-9663-adc99005fac5"
)
INGEST_INTERVAL_SECONDS = int(get_env("INGEST_INTERVAL_SECONDS", "60"))

# optional source tag used when storing rows
DEFAULT_SOURCE = get_env("SOURCE_NAME", get_env("DEFAULT_SOURCE", "France"))



# Prefect worker configuration
PREFECT_API_URL = get_env("PREFECT_API_URL")
PREFECT_WORK_POOL_NAME = get_env("PREFECT_WORK_POOL_NAME", "default")
