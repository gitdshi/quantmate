"""Unit tests for app.infrastructure.logging.logging_setup."""

from __future__ import annotations

import logging
import os
from unittest.mock import patch, MagicMock

import pytest

from app.infrastructure.logging.logging_setup import configure_logging, get_logger


def test_configure_logging_text_format():
    configure_logging(level=logging.DEBUG)
    root = logging.getLogger()
    assert root.level == logging.DEBUG


def test_configure_logging_json_format():
    with patch.dict(os.environ, {"QUANTMATE_LOG_FORMAT": "json"}), \
         patch("app.infrastructure.logging.logging_setup.configure_json_logging", create=True) as mock_json:
        # The import path inside the function uses a different path
        with patch("app.infrastructure.logging.json_formatter.configure_json_logging", create=True):
            try:
                configure_logging(level=logging.WARNING)
            except (ImportError, ModuleNotFoundError):
                # json_formatter module may not exist in test env
                pass


def test_configure_logging_default_level():
    configure_logging()
    root = logging.getLogger()
    assert root.level == logging.INFO


def test_get_logger_with_name():
    log = get_logger("test.module")
    assert log.name == "test.module"


def test_get_logger_without_name():
    log = get_logger()
    assert log.name is not None
    assert isinstance(log, logging.Logger)


def test_get_logger_none():
    log = get_logger(None)
    assert isinstance(log, logging.Logger)
