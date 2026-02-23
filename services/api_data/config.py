import os
from typing import Optional


def get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    """Return an environment variable value with optional default."""
    return os.getenv(name, default)


DB_HOST = get_env("DB_HOST", "demo_power_sense_clickhouse")
DB_PORT = int(get_env("DB_PORT", "8123"))
DB_NAME = get_env("DB_NAME", "electricity")
DB_USER = get_env("DB_USER", "electricity")
DB_PASSWORD = get_env("DB_PASSWORD", "electricity")

LOG_LEVEL = get_env("LOG_LEVEL", "INFO")
