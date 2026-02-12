"""Centralized logging configuration moved under infrastructure.

Copied from `app.api.logging_setup` and re-exported via
`app.infrastructure.logging` to keep imports stable.
"""
from typing import Optional
import logging


def configure_logging(level: int = logging.INFO) -> None:
    fmt = '%(asctime)s %(levelname)s [%(name)s] %(message)s'
    datefmt = '%Y-%m-%d %H:%M:%S'
    try:
        logging.basicConfig(level=level, format=fmt, datefmt=datefmt, force=True)
    except TypeError:
        root = logging.getLogger()
        for h in list(root.handlers):
            root.removeHandler(h)
        logging.basicConfig(level=level, format=fmt, datefmt=datefmt)

    for name in ('uvicorn.error', 'uvicorn.access'):
        log = logging.getLogger(name)
        for h in list(log.handlers):
            try:
                h.setFormatter(logging.Formatter(fmt, datefmt=datefmt))
            except Exception:
                pass


def get_logger(name: Optional[str] = None) -> logging.Logger:
    return logging.getLogger(name if name else __name__)
