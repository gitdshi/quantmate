"""Centralized logging configuration for TraderMate.

Call `configure_logging()` early in each process entrypoint to ensure
all logs include date and time in a consistent format.
"""
from typing import Optional
import logging


def configure_logging(level: int = logging.INFO) -> None:
    """Configure root logging with timestamped formatter.

    Uses a consistent format and applies it to commonly-used servers
    such as uvicorn so their logs also include the timestamp.
    """
    fmt = '%(asctime)s %(levelname)s [%(name)s] %(message)s'
    datefmt = '%Y-%m-%d %H:%M:%S'
    # Python 3.8+ supports force to replace existing handlers
    try:
        logging.basicConfig(level=level, format=fmt, datefmt=datefmt, force=True)
    except TypeError:
        # Older Python without `force` support: remove handlers first
        root = logging.getLogger()
        for h in list(root.handlers):
            root.removeHandler(h)
        logging.basicConfig(level=level, format=fmt, datefmt=datefmt)

    # Ensure uvicorn loggers use the same formatter if present.
    for name in ('uvicorn.error', 'uvicorn.access'):
        log = logging.getLogger(name)
        for h in list(log.handlers):
            try:
                h.setFormatter(logging.Formatter(fmt, datefmt=datefmt))
            except Exception:
                # Best-effort; don't fail startup for logging issues
                pass


def get_logger(name: Optional[str] = None) -> logging.Logger:
    return logging.getLogger(name if name else __name__)
