import logging
import sys


def configure_logging(level: str = None) -> None:
    level = level or "INFO"
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    fmt = "%(asctime)s %(levelname)s %(name)s %(message)s"
    handler.setFormatter(logging.Formatter(fmt))
    root = logging.getLogger()
    root.handlers = []
    root.addHandler(handler)
    root.setLevel(numeric_level)
