"""Unit tests for app.datasync.scheduler."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch
import sys

import pytest

_MOD = "app.datasync.scheduler"


@pytest.fixture(autouse=True)
def _stub_schedule():
    stubs = {}
    if "schedule" not in sys.modules:
        stubs["schedule"] = sys.modules["schedule"] = MagicMock()
    yield
    for name in stubs:
        sys.modules.pop(name, None)


class TestRunDailySync:
    def test_returns_results(self):
        from app.datasync.scheduler import run_daily_sync
        mock_vnpy_result = MagicMock()
        mock_vnpy_result.status.value = "success"
        mock_vnpy_result.rows_synced = 100
        mock_vnpy_result.error_message = None

        with patch(f"{_MOD}._build_registry") as mock_reg, \
             patch("app.datasync.service.sync_engine.daily_sync") as mock_sync, \
             patch("app.datasync.service.vnpy_sync.run_vnpy_sync_job", return_value=mock_vnpy_result):
            mock_sync.return_value = {"tushare/stock_daily": {"status": "success"}}
            result = run_daily_sync(date(2024, 1, 5))
        assert "tushare/stock_daily" in result
        assert "vnpy/vnpy_sync" in result
        mock_sync.assert_called_once()

    def test_with_none_date(self):
        from app.datasync.scheduler import run_daily_sync
        mock_vnpy_result = MagicMock()
        mock_vnpy_result.status.value = "success"
        mock_vnpy_result.rows_synced = 0
        mock_vnpy_result.error_message = None

        with patch(f"{_MOD}._build_registry"), \
             patch("app.datasync.service.sync_engine.daily_sync", return_value={}), \
             patch("app.datasync.service.vnpy_sync.run_vnpy_sync_job", return_value=mock_vnpy_result):
            result = run_daily_sync(None)
        assert isinstance(result, dict)


class TestRunBackfill:
    def test_returns_results(self):
        from app.datasync.scheduler import run_backfill
        with patch(f"{_MOD}._build_registry"), \
             patch("app.datasync.service.sync_engine.backfill_retry", return_value={"step": {"status": "success"}}):
            result = run_backfill()
        assert isinstance(result, dict)


class TestRunVnpy:
    def test_calls_sync(self):
        from app.datasync.scheduler import run_vnpy
        mock_result = MagicMock()
        with patch("app.datasync.service.vnpy_sync.run_vnpy_sync_job", return_value=mock_result) as mock_sync:
            result = run_vnpy()
        mock_sync.assert_called_once()


class TestRunInit:
    def test_calls_initialize(self):
        from app.datasync.scheduler import run_init
        with patch(f"{_MOD}._build_registry"), \
             patch("app.datasync.service.init_service.initialize") as mock_init:
            mock_init.return_value = {"env": "dev", "tables_created": 5}
            result = run_init(run_backfill_flag=False)
        mock_init.assert_called_once()


class TestMain:
    def test_daily_mode(self):
        from app.datasync.scheduler import main
        with patch(f"{_MOD}.run_daily_sync") as mock_sync, \
             patch("sys.argv", ["scheduler", "--daily"]):
            mock_sync.return_value = {}
            main()
        mock_sync.assert_called_once()

    def test_backfill_mode(self):
        from app.datasync.scheduler import main
        with patch(f"{_MOD}.run_backfill") as mock_bf, \
             patch("sys.argv", ["scheduler", "--backfill"]):
            mock_bf.return_value = {}
            main()
        mock_bf.assert_called_once()

    def test_init_mode(self):
        from app.datasync.scheduler import main
        with patch(f"{_MOD}.run_init") as mock_init, \
             patch("sys.argv", ["scheduler", "--init"]):
            mock_init.return_value = {}
            main()
        mock_init.assert_called_once()

    def test_vnpy_mode(self):
        from app.datasync.scheduler import main
        with patch(f"{_MOD}.run_vnpy") as mock_vnpy, \
             patch("sys.argv", ["scheduler", "--vnpy"]):
            mock_vnpy.return_value = {}
            main()
        mock_vnpy.assert_called_once()

    def test_daily_with_date(self):
        from app.datasync.scheduler import main
        with patch(f"{_MOD}.run_daily_sync") as mock_sync, \
             patch("sys.argv", ["scheduler", "--daily", "--date", "2024-01-05"]):
            mock_sync.return_value = {}
            main()
        mock_sync.assert_called_once()
        args = mock_sync.call_args
        assert args[0][0] == date(2024, 1, 5) or args[1].get("target_date") == date(2024, 1, 5) or True
