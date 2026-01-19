import threading
import logging
import sys
from pathlib import Path

# Ensure parent dir is on sys.path so `import app.*` works when running
# the script directly from `/app/app/main.py` inside the container.
parent = str(Path(__file__).resolve().parent.parent)
if parent not in sys.path:
    sys.path.insert(0, parent)

# support running as a module or as a script: try relative imports first,
# fall back to absolute imports when executed directly
try:
    from .logging_config import configure_logging
    from .metrics import start_metrics_server
    from .ingest import run_loop
    from .db import bootstrap_history
    from .config import METRICS_PORT, INGEST_INTERVAL_SECONDS
except Exception:
    # When executed as a script inside the container the package root
    # may be `app`, so try importing from `app` as a fallback.
    from app.logging_config import configure_logging
    from app.metrics import start_metrics_server
    from app.ingest import run_loop
    from app.db import bootstrap_history
    from app.config import METRICS_PORT, INGEST_INTERVAL_SECONDS


def main() -> None:
    configure_logging()
    logger = logging.getLogger("main")

    # start prometheus metrics endpoint
    start_metrics_server(METRICS_PORT)
    logger.info("metrics_server listening on :%s", METRICS_PORT)

    # bootstrap history data
    if bootstrap_history():
        logger.info("history data bootstrap complete")
    
    # start ingestion loop in background thread
    t = threading.Thread(target=run_loop, args=(INGEST_INTERVAL_SECONDS,), daemon=True)
    t.start()

    # keep main thread alive
    try:
        t.join()
    except KeyboardInterrupt:
        logger.info("shutdown requested")


if __name__ == "__main__":
    main()
