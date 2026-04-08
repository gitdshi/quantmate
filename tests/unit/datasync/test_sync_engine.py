"""Unit tests for app.datasync.service.sync_engine."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

_MOD = "app.datasync.service.sync_engine"


def _conn_ctx():
    engine = MagicMock()
    conn = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value = ctx
    engine.connect.return_value = ctx
    return engine, conn


class TestStatusHelpers:
    def test_write_status(self):
        from app.datasync.service.sync_engine import _write_status
        engine, conn = _conn_ctx()
        with patch(f"{_MOD}.get_quantmate_engine", return_value=engine):
            _write_status(date(2024, 1, 5), "tushare", "stock_daily", "success", 100)
        conn.execute.assert_called_once()

    def test_get_status_found(self):
        from app.datasync.service.sync_engine import _get_status
        engine, conn = _conn_ctx()
        row = MagicMock()
        row.__getitem__ = lambda s, k: "success"
        conn.execute.return_value = MagicMock(fetchone=MagicMock(return_value=row))
        with patch(f"{_MOD}.get_quantmate_engine", return_value=engine):
            result = _get_status(date(2024, 1, 5), "tushare", "stock_daily")
        assert result is not None

    def test_get_status_not_found(self):
        from app.datasync.service.sync_engine import _get_status
        engine, conn = _conn_ctx()
        conn.execute.return_value = MagicMock(fetchone=MagicMock(return_value=None))
        with patch(f"{_MOD}.get_quantmate_engine", return_value=engine):
            result = _get_status(date(2024, 1, 5), "tushare", "stock_daily")
        assert result is None


class TestGetFailedRecords:
    def test_returns_list(self):
        from app.datasync.service.sync_engine import _get_failed_records
        engine, conn = _conn_ctx()
        rows = [(date(2024, 1, 3), "tushare", "stock_daily", 1)]
        conn.execute.return_value = MagicMock(fetchall=MagicMock(return_value=rows))
        with patch(f"{_MOD}.get_quantmate_engine", return_value=engine):
            result = _get_failed_records(30)
        assert len(result) == 1


class TestGetEnabledItems:
    def test_returns_items(self):
        from app.datasync.service.sync_engine import _get_enabled_items
        engine, conn = _conn_ctx()
        rows = [("tushare", "stock_daily", "tushare_db", "stock_daily", 1, 10)]
        conn.execute.return_value = MagicMock(fetchall=MagicMock(return_value=rows))
        with patch(f"{_MOD}.get_quantmate_engine", return_value=engine):
            result = _get_enabled_items()
        assert len(result) == 1
        assert result[0]["source"] == "tushare"


class TestDailySync:
    def test_skips_already_success(self):
        from app.datasync.service.sync_engine import daily_sync
        from app.datasync.base import SyncStatus
        registry = MagicMock()
        with patch(f"{_MOD}._get_enabled_items") as mock_items, \
             patch(f"{_MOD}._get_status") as mock_status, \
             patch(f"{_MOD}._write_status"), \
             patch(f"{_MOD}.get_previous_trade_date", return_value=date(2024, 1, 5)):
            mock_items.return_value = [
                {"source": "tushare", "item_key": "stock_daily",
                 "target_database": "ts", "target_table": "stock_daily",
                 "table_created": 1, "sync_priority": 10},
            ]
            mock_status.return_value = SyncStatus.SUCCESS.value
            result = daily_sync(registry, target_date=date(2024, 1, 5))
        assert result["tushare/stock_daily"].get("skipped") is True

    def test_runs_interface(self):
        from app.datasync.service.sync_engine import daily_sync
        from app.datasync.base import SyncResult, SyncStatus
        registry = MagicMock()
        iface = MagicMock()
        iface.sync_date.return_value = SyncResult(status=SyncStatus.SUCCESS, rows_synced=500)
        registry.get_interface.return_value = iface
        with patch(f"{_MOD}._get_enabled_items") as mock_items, \
             patch(f"{_MOD}._get_status", return_value=None), \
             patch(f"{_MOD}._write_status"), \
             patch(f"{_MOD}.get_previous_trade_date", return_value=date(2024, 1, 5)), \
             patch(f"{_MOD}.ensure_table"):
            mock_items.return_value = [
                {"source": "tushare", "item_key": "stock_daily",
                 "target_database": "ts", "target_table": "stock_daily",
                 "table_created": 0, "sync_priority": 10},
            ]
            result = daily_sync(registry, target_date=date(2024, 1, 5))
        assert result["tushare/stock_daily"]["status"] == "success"

    def test_handles_missing_interface(self):
        from app.datasync.service.sync_engine import daily_sync
        registry = MagicMock()
        registry.get_interface.return_value = None
        with patch(f"{_MOD}._get_enabled_items") as mock_items, \
             patch(f"{_MOD}._get_status", return_value=None), \
             patch(f"{_MOD}._write_status"), \
             patch(f"{_MOD}.get_previous_trade_date", return_value=date(2024, 1, 5)):
            mock_items.return_value = [
                {"source": "tushare", "item_key": "nonexistent",
                 "target_database": "ts", "target_table": "x",
                 "table_created": 0, "sync_priority": 10},
            ]
            result = daily_sync(registry, target_date=date(2024, 1, 5))
        assert result["tushare/nonexistent"]["status"] == "skipped"

    def test_handles_sync_error(self):
        from app.datasync.service.sync_engine import daily_sync
        registry = MagicMock()
        iface = MagicMock()
        iface.sync_date.side_effect = Exception("API failed")
        registry.get_interface.return_value = iface
        with patch(f"{_MOD}._get_enabled_items") as mock_items, \
             patch(f"{_MOD}._get_status", return_value=None), \
             patch(f"{_MOD}._write_status"), \
             patch(f"{_MOD}.get_previous_trade_date", return_value=date(2024, 1, 5)), \
             patch(f"{_MOD}.ensure_table"):
            mock_items.return_value = [
                {"source": "tushare", "item_key": "stock_daily",
                 "target_database": "ts", "target_table": "stock_daily",
                 "table_created": 1, "sync_priority": 10},
            ]
            result = daily_sync(registry, target_date=date(2024, 1, 5))
        assert result["tushare/stock_daily"]["status"] == "error"


class TestBackfillRetry:
    def test_no_failed_records(self):
        from app.datasync.service.sync_engine import backfill_retry
        registry = MagicMock()
        with patch(f"{_MOD}._get_failed_records", return_value=[]), \
             patch("app.domains.extdata.dao.data_sync_status_dao.is_backfill_locked", return_value=False), \
             patch("app.domains.extdata.dao.data_sync_status_dao.acquire_backfill_lock"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.release_backfill_lock"):
            result = backfill_retry(registry)
        assert result == {}

    def test_skips_when_locked(self):
        from app.datasync.service.sync_engine import backfill_retry
        registry = MagicMock()
        with patch("app.domains.extdata.dao.data_sync_status_dao.is_backfill_locked", return_value=True):
            result = backfill_retry(registry)
        assert result == {}

    def test_retries_interface(self):
        from app.datasync.service.sync_engine import backfill_retry
        from app.datasync.base import SyncResult, SyncStatus
        registry = MagicMock()
        iface = MagicMock()
        iface.sync_date.return_value = SyncResult(status=SyncStatus.SUCCESS, rows_synced=100)
        registry.get_interface.return_value = iface
        with patch(f"{_MOD}._get_failed_records") as mock_failed, \
             patch(f"{_MOD}._write_status"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.acquire_backfill_lock"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.release_backfill_lock"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.is_backfill_locked", return_value=False):
            mock_failed.return_value = [
                (date(2024, 1, 3), "tushare", "stock_daily", 0),
            ]
            result = backfill_retry(registry)
        assert "tushare/stock_daily@2024-01-03" in result
        assert result["tushare/stock_daily@2024-01-03"]["status"] == "success"
