"""Unit tests for app.infrastructure.logging.json_formatter."""

from __future__ import annotations

import json
import logging

import pytest

import app.infrastructure.logging.json_formatter as _mod


class TestJSONFormatter:
    def test_format_basic(self):
        fmt = _mod.JSONFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="test.py",
            lineno=1, msg="hello world", args=(), exc_info=None,
        )
        output = fmt.format(record)
        parsed = json.loads(output)
        assert parsed["level"] == "INFO"
        assert parsed["message"] == "hello world"
        assert "timestamp" in parsed

    def test_format_with_exception(self):
        fmt = _mod.JSONFormatter()
        try:
            raise ValueError("boom")
        except ValueError:
            import sys
            exc_info = sys.exc_info()
        record = logging.LogRecord(
            name="test", level=logging.ERROR, pathname="test.py",
            lineno=1, msg="error occurred", args=(), exc_info=exc_info,
        )
        output = fmt.format(record)
        parsed = json.loads(output)
        assert parsed["level"] == "ERROR"
        assert "exception" in parsed
        exc = parsed["exception"]
        if isinstance(exc, dict):
            assert "boom" in exc.get("message", "")
        else:
            assert "boom" in exc

    def test_format_with_extras(self):
        fmt = _mod.JSONFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="test.py",
            lineno=1, msg="request", args=(), exc_info=None,
        )
        record.request_id = "abc-123"
        record.user_id = 42
        record.method = "GET"
        record.path = "/api/health"
        record.status_code = 200
        record.duration_ms = 15.3
        output = fmt.format(record)
        parsed = json.loads(output)
        assert parsed["request_id"] == "abc-123"
        assert parsed["user_id"] == 42

    def test_format_warning_level(self):
        fmt = _mod.JSONFormatter()
        record = logging.LogRecord(
            name="test", level=logging.WARNING, pathname="test.py",
            lineno=1, msg="warning msg", args=(), exc_info=None,
        )
        output = fmt.format(record)
        parsed = json.loads(output)
        assert parsed["level"] == "WARNING"


class TestConfigureJsonLogging:
    def test_configure(self):
        _mod.configure_json_logging(level=logging.DEBUG)
        root = logging.getLogger()
        # Should have at least one handler with JSONFormatter
        has_json = any(
            isinstance(h.formatter, _mod.JSONFormatter) for h in root.handlers
        )
        assert has_json
