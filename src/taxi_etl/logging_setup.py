"""Pipeline logging to console and logs/."""

import logging
from datetime import datetime, timezone
from pathlib import Path


def setup_logging(*, logs_dir: Path, run_id: str) -> logging.Logger:
    logs_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_path = logs_dir / f"pipeline_{stamp}_{run_id[:8]}.log"

    logger = logging.getLogger("taxi_etl")
    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    logger.propagate = False

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    stream = logging.StreamHandler()
    stream.setFormatter(fmt)
    logger.addHandler(stream)

    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    logger.info("log file: %s", log_path)
    return logger
