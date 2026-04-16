"""Centralized logging configuration moved under infrastructure.

Uses JSON format in production (QUANTMATE_LOG_FORMAT=json) and plain text
in development.
"""

import logging
from typing import Optional

from app.infrastructure.config import get_runtime_str


def configure_logging(level: int = logging.INFO) -> None:
    log_format = get_runtime_str(
        env_keys="QUANTMATE_LOG_FORMAT",
        db_key="logging.format",
        default="text",
    )

    if log_format == "json":
        from app.infrastructure.logging.json_formatter import configure_json_logging

        configure_json_logging(level)
        return

    fmt = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    try:
        logging.basicConfig(level=level, format=fmt, datefmt=datefmt, force=True)
    except TypeError:
        root = logging.getLogger()
        for h in list(root.handlers):
            root.removeHandler(h)
        logging.basicConfig(level=level, format=fmt, datefmt=datefmt)

    for name in ("uvicorn.error", "uvicorn.access"):
        log = logging.getLogger(name)
        for h in list(log.handlers):
            try:
                h.setFormatter(logging.Formatter(fmt, datefmt=datefmt))
            except Exception:
                pass


def get_logger(name: Optional[str] = None) -> logging.Logger:
    return logging.getLogger(name if name else __name__)
