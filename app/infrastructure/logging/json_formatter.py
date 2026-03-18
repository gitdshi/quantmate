"""Structured JSON logging formatter for production use.

Usage:
    from app.infrastructure.logging.json_formatter import configure_json_logging
    configure_json_logging()  # Call at application startup for JSON log output
"""

from __future__ import annotations

import json
import logging
import traceback
from datetime import datetime, timezone


class JSONFormatter(logging.Formatter):
    """Format log records as single-line JSON for structured log aggregation."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        if record.exc_info and record.exc_info[1]:
            log_entry["exception"] = {
                "type": record.exc_info[0].__name__ if record.exc_info[0] else "Unknown",
                "message": str(record.exc_info[1]),
                "traceback": traceback.format_exception(*record.exc_info),
            }

        # Include extra fields if they are set
        for key in ("request_id", "user_id", "method", "path", "status_code", "duration_ms"):
            val = getattr(record, key, None)
            if val is not None:
                log_entry[key] = val

        return json.dumps(log_entry, default=str, ensure_ascii=False)


def configure_json_logging(level: int = logging.INFO) -> None:
    """Replace default formatters with JSON formatter."""
    formatter = JSONFormatter()
    root = logging.getLogger()
    root.setLevel(level)

    # Replace all existing handlers' formatters
    if root.handlers:
        for handler in root.handlers:
            handler.setFormatter(formatter)
    else:
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        root.addHandler(handler)

    # Also format uvicorn loggers
    for name in ("uvicorn.error", "uvicorn.access"):
        log = logging.getLogger(name)
        for h in log.handlers:
            h.setFormatter(formatter)
