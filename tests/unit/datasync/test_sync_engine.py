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

    def test_includes_stale_running_records(self):
        from app.datasync.service.sync_engine import _get_failed_records
        engine, conn = _conn_ctx()
        rows = [(date(2024, 1, 3), "tushare", "stock_daily", 1)]
        conn.execute.return_value = MagicMock(fetchall=MagicMock(return_value=rows))
        with patch.dict("os.environ", {"SYNC_STATUS_RUNNING_STALE_HOURS": "8"}), \
             patch(f"{_MOD}.get_quantmate_engine", return_value=engine):
            _get_failed_records(30)

        sql = conn.execute.call_args.args[0].text
        params = conn.execute.call_args.args[1]
        assert "status = 'running'" in sql
        assert "TIMESTAMPDIFF" in sql
        assert params["stale_seconds"] == 8 * 3600


class TestBackfillHelpers:
    def test_groups_backfill_records_by_date(self):
        from app.datasync.service.sync_engine import _group_backfill_records_by_date

        grouped = _group_backfill_records_by_date(
            [
                (date(2024, 1, 3), "tushare", "stock_daily", 0),
                (date(2024, 1, 3), "akshare", "index_daily", 0),
                (date(2024, 1, 4), "tushare", "stock_weekly", 1),
            ]
        )

        assert len(grouped) == 2
        assert grouped[0][0] == date(2024, 1, 3)
        assert len(grouped[0][1]) == 2

    def test_builds_backfill_log_context_with_symbols(self):
        from app.datasync.base import SyncResult, SyncStatus
        from app.datasync.service.sync_engine import _build_backfill_log_context

        result = SyncResult(
            SyncStatus.PARTIAL,
            12,
            "Failed: 510300",
            details={"symbols": ["159919", "510300"], "failed_symbols": ["510300"]},
        )
        context = _build_backfill_log_context(date(2024, 1, 5), "akshare", "fund_etf_daily", result)

        assert context["date"] == "2024-01-05"
        assert context["interface"] == "akshare/fund_etf_daily"
        assert context["symbols"] == "159919,510300"
        assert context["failed_symbols"] == "510300"

    def test_backfill_uses_independent_source_limit(self):
        import app.datasync.service.sync_engine as sync_engine

        class FakeSemaphore:
            def __init__(self, value):
                self.value = value

            def acquire(self):
                return True

            def release(self):
                return True

        with patch.dict(
            "os.environ",
            {
                "TUSHARE_CONCURRENCY": "3",
                "BACKFILL_TUSHARE_CONCURRENCY": "1",
            },
        ), patch(f"{_MOD}.Semaphore", FakeSemaphore), \
            patch.object(sync_engine, "_source_semaphores", {}), \
            patch.object(sync_engine, "_backfill_source_semaphores", {}):
            sync_sem = sync_engine._get_source_semaphore("tushare")
            backfill_sem = sync_engine._get_backfill_source_semaphore("tushare")

        assert sync_sem is not backfill_sem
        assert sync_sem.value == 3
        assert backfill_sem.value == 1


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
            result = backfill_retry(registry, max_workers=2)
        assert "tushare/stock_daily@2024-01-03" in result
        assert result["tushare/stock_daily@2024-01-03"]["status"] == "success"

    def test_uses_configured_worker_count(self):
        from concurrent.futures import Future

        from app.datasync.base import SyncResult, SyncStatus
        from app.datasync.service.sync_engine import backfill_retry

        registry = MagicMock()
        iface = MagicMock()
        iface.sync_date.return_value = SyncResult(status=SyncStatus.SUCCESS, rows_synced=3)
        registry.get_interface.return_value = iface

        class FakeExecutor:
            def __init__(self, max_workers, thread_name_prefix=None):
                self.max_workers = max_workers
                self.thread_name_prefix = thread_name_prefix

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def submit(self, fn, *args, **kwargs):
                future = Future()
                try:
                    future.set_result(fn(*args, **kwargs))
                except Exception as exc:
                    future.set_exception(exc)
                return future

        created = []

        def _make_executor(max_workers, thread_name_prefix=None):
            created.append((max_workers, thread_name_prefix))
            return FakeExecutor(max_workers, thread_name_prefix)

        with patch.dict("os.environ", {"BACKFILL_WORKERS": "10"}), \
             patch(f"{_MOD}.ThreadPoolExecutor", side_effect=_make_executor), \
             patch(f"{_MOD}._get_failed_records", return_value=[(date(2024, 1, 3), "tushare", "stock_daily", 0)]), \
             patch(f"{_MOD}._write_status"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.acquire_backfill_lock"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.release_backfill_lock"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.is_backfill_locked", return_value=False):
            backfill_retry(registry)

        assert created == [(10, "datasync-backfill")]

    def test_skips_non_historical_interface(self):
        from app.datasync.service.sync_engine import backfill_retry
        registry = MagicMock()
        iface = MagicMock()
        iface.supports_backfill.return_value = False
        registry.get_interface.return_value = iface

        with patch(f"{_MOD}._get_failed_records", return_value=[(date(2024, 1, 3), "akshare", "stock_zh_index_spot", 0)]), \
             patch(f"{_MOD}._write_status") as mock_write, \
             patch("app.domains.extdata.dao.data_sync_status_dao.acquire_backfill_lock"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.release_backfill_lock"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.is_backfill_locked", return_value=False):
            result = backfill_retry(registry)

        iface.sync_date.assert_not_called()
        mock_write.assert_called_once()
        assert result["akshare/stock_zh_index_spot@2024-01-03"]["skipped"] is True
