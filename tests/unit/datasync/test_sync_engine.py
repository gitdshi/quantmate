"""Unit tests for app.datasync.service.sync_engine."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from unittest.mock import MagicMock, patch


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
        conn.execute.return_value = MagicMock(fetchone=MagicMock(return_value=("success", 12)))
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
            result = _get_failed_records(date(2023, 12, 4), date(2024, 1, 3))
        assert len(result) == 1

    def test_defaults_to_all_dates_newest_first(self):
        from app.datasync.service.sync_engine import _get_failed_records

        engine, conn = _conn_ctx()
        conn.execute.return_value = MagicMock(fetchall=MagicMock(return_value=[]))

        with patch(f"{_MOD}.get_quantmate_engine", return_value=engine):
            _get_failed_records()

        sql = conn.execute.call_args.args[0].text
        params = conn.execute.call_args.args[1]
        assert "sync_date <= :e" in sql
        assert "sync_date >= :s" not in sql
        assert "ORDER BY sync_date DESC" in sql
        assert "s" not in params

    def test_includes_stale_running_records(self):
        from app.datasync.service.sync_engine import _get_failed_records
        engine, conn = _conn_ctx()
        rows = [(date(2024, 1, 3), "tushare", "stock_daily", 1)]
        conn.execute.return_value = MagicMock(fetchall=MagicMock(return_value=rows))
        with patch.dict("os.environ", {"SYNC_STATUS_RUNNING_STALE_HOURS": "8"}), \
             patch(f"{_MOD}.get_quantmate_engine", return_value=engine):
            _get_failed_records(date(2023, 12, 4), date(2024, 1, 3))

        sql = conn.execute.call_args.args[0].text
        params = conn.execute.call_args.args[1]
        assert "status = 'running'" in sql
        assert "TIMESTAMPDIFF" in sql
        assert params["stale_seconds"] == 8 * 3600

    def test_includes_zero_row_success_records(self):
        from app.datasync.service.sync_engine import _get_failed_records

        engine, conn = _conn_ctx()
        conn.execute.return_value = MagicMock(fetchall=MagicMock(return_value=[]))

        with patch(f"{_MOD}.get_quantmate_engine", return_value=engine):
            _get_failed_records(date(2023, 12, 4), date(2024, 1, 3))

        sql = conn.execute.call_args.args[0].text
        assert "status = 'success'" in sql
        assert "COALESCE(rows_synced, 0) = 0" in sql


class TestBackfillRetryFiltering:
    def test_detects_quota_cooldown_records(self):
        from app.datasync.service.sync_engine import _is_quota_cooldown_record

        updated_at = datetime(2024, 1, 3, 12, 0, 0)
        record = (
            date(2024, 1, 3),
            "tushare",
            "us_daily",
            0,
            "抱歉，您每天最多访问该接口5次，权限的具体详情访问：https://tushare.pro/document/1?doc_id=108。",
            "pending",
            0,
            updated_at,
        )

        assert _is_quota_cooldown_record(record, now=updated_at + timedelta(hours=1)) is True
        assert _is_quota_cooldown_record(record, now=updated_at + timedelta(hours=6)) is False

    def test_filters_quota_cooldown_records(self):
        from app.datasync.service.sync_engine import _filter_backfill_retry_records

        updated_at = datetime(2024, 1, 3, 12, 0, 0)
        records = [
            (
                date(2024, 1, 3),
                "tushare",
                "us_daily",
                0,
                "抱歉，您每天最多访问该接口5次，权限的具体详情访问：https://tushare.pro/document/1?doc_id=108。",
                "pending",
                0,
                updated_at,
            ),
            (
                date(2024, 1, 3),
                "tushare",
                "stock_daily",
                0,
                "some other error",
                "pending",
                0,
                updated_at,
            ),
        ]

        filtered = _filter_backfill_retry_records(records, now=updated_at + timedelta(minutes=30))

        assert filtered == [records[1]]

    def test_keeps_only_one_quota_retry_per_interface_per_pass(self):
        from app.datasync.service.sync_engine import _filter_backfill_retry_records

        updated_at = datetime(2024, 1, 3, 12, 0, 0)
        records = [
            (
                date(2024, 1, 4),
                "tushare",
                "report_rc",
                0,
                "抱歉，您每小时最多访问该接口10次，权限的具体详情访问：https://tushare.pro/document/1?doc_id=108。",
                "pending",
                0,
                updated_at,
            ),
            (
                date(2024, 1, 3),
                "tushare",
                "report_rc",
                0,
                "抱歉，您每小时最多访问该接口10次，权限的具体详情访问：https://tushare.pro/document/1?doc_id=108。",
                "pending",
                0,
                updated_at,
            ),
            (
                date(2024, 1, 4),
                "tushare",
                "stock_daily",
                0,
                "some other error",
                "pending",
                0,
                updated_at,
            ),
        ]

        filtered = _filter_backfill_retry_records(records, now=updated_at + timedelta(hours=2))

        assert filtered == [records[0], records[2]]


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

    def test_backfill_defaults_akshare_to_serial(self):
        from app.datasync.service.sync_engine import _get_backfill_source_concurrency_limit

        with patch(f"{_MOD}.get_runtime_config", side_effect=lambda **kwargs: kwargs["default"]):
            limit = _get_backfill_source_concurrency_limit("akshare")

        assert limit == 1

    def test_groups_contiguous_trade_dates(self):
        from app.datasync.service.sync_engine import _group_contiguous_trade_dates

        with patch(f"{_MOD}.get_trade_calendar", return_value=[
            date(2024, 1, 3),
            date(2024, 1, 4),
            date(2024, 1, 5),
            date(2024, 1, 8),
        ]):
            grouped = _group_contiguous_trade_dates([
                date(2024, 1, 3),
                date(2024, 1, 4),
                date(2024, 1, 8),
            ])

        assert grouped == [
            [date(2024, 1, 3), date(2024, 1, 4)],
            [date(2024, 1, 8)],
        ]

    def test_reopens_known_terminal_trade_cal_error(self):
        from app.datasync.service.sync_engine import _effective_retry_count

        record = (
            date(2024, 1, 3),
            "tushare",
            "trade_cal",
            3,
            "(1054, \"Unknown column 'updated_at' in 'field list'\")",
        )

        assert _effective_retry_count(record) == 0

    def test_keeps_other_terminal_errors_closed(self):
        from app.datasync.service.sync_engine import _effective_retry_count

        record = (
            date(2024, 1, 3),
            "tushare",
            "stock_daily",
            3,
            "quota exceeded",
        )

        assert _effective_retry_count(record) is None


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

    def test_backfill_keys_follow_enabled_items(self):
        from app.datasync.service.sync_engine import _get_enabled_backfill_keys

        with patch(f"{_MOD}._get_enabled_items", return_value=[
            {"source": "tushare", "item_key": "stock_daily"},
            {"source": "akshare", "item_key": "index_daily"},
        ]):
            result = _get_enabled_backfill_keys()

        assert result == {
            ("tushare", "stock_daily"),
            ("akshare", "index_daily"),
        }


class TestDailySync:
    def test_uses_latest_completed_trade_date_by_default(self):
        from app.datasync.service.sync_engine import _get_latest_completed_trade_date

        with patch(f"{_MOD}.get_trade_calendar", return_value=[
            date(2024, 1, 4),
            date(2024, 1, 5),
            date(2024, 1, 8),
        ]):
            result = _get_latest_completed_trade_date(today=date(2024, 1, 8))

        assert result == date(2024, 1, 5)

    def test_uses_latest_trade_date_on_non_trading_day(self):
        from app.datasync.service.sync_engine import _get_latest_completed_trade_date

        with patch(f"{_MOD}.get_trade_calendar", return_value=[
            date(2024, 1, 4),
            date(2024, 1, 5),
        ]):
            result = _get_latest_completed_trade_date(today=date(2024, 1, 6))

        assert result == date(2024, 1, 5)

    def test_skips_already_success(self):
        from app.datasync.service.sync_engine import daily_sync
        from app.datasync.base import SyncStatus
        registry = MagicMock()
        with patch(f"{_MOD}._get_enabled_items") as mock_items, \
             patch(f"{_MOD}._get_status_snapshot") as mock_status, \
             patch(f"{_MOD}._write_status"):
            mock_items.return_value = [
                {"source": "tushare", "item_key": "stock_daily",
                 "target_database": "ts", "target_table": "stock_daily",
                 "table_created": 1, "sync_priority": 10},
            ]
            mock_status.return_value = (SyncStatus.SUCCESS.value, 10)
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
               patch(f"{_MOD}._get_status_snapshot", return_value=(None, 0)), \
             patch(f"{_MOD}._write_status"), \
             patch(f"{_MOD}.ensure_table") as mock_ensure:
            mock_items.return_value = [
                {"source": "tushare", "item_key": "stock_daily",
                 "target_database": "ts", "target_table": "stock_daily",
                 "table_created": 1, "sync_priority": 10},
            ]
            result = daily_sync(registry, target_date=date(2024, 1, 5))
        assert result["tushare/stock_daily"]["status"] == "success"
        mock_ensure.assert_called_once()

    def test_skips_runtime_unsupported_interface(self):
        from app.datasync.service.sync_engine import daily_sync

        registry = MagicMock()
        iface = MagicMock()
        iface.supports_scheduled_sync.return_value = False
        registry.get_interface.return_value = iface

        with patch(f"{_MOD}._get_enabled_items", return_value=[
            {"source": "tushare", "item_key": "fina_indicator",
             "target_database": "ts", "target_table": "fina_indicator",
             "table_created": 1, "sync_priority": 10},
        ]), \
            patch(f"{_MOD}._write_status") as mock_write:
            result = daily_sync(registry, target_date=date(2024, 1, 5))

        assert result["tushare/fina_indicator"]["skipped"] is True
        assert result["tushare/fina_indicator"]["status"] == "success"
        mock_write.assert_called_once()

    def test_handles_missing_interface(self):
        from app.datasync.service.sync_engine import daily_sync
        registry = MagicMock()
        registry.get_interface.return_value = None
        with patch(f"{_MOD}._get_enabled_items") as mock_items, \
             patch(f"{_MOD}._get_status_snapshot", return_value=(None, 0)), \
             patch(f"{_MOD}._write_status"):
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
             patch(f"{_MOD}._get_status_snapshot", return_value=(None, 0)), \
             patch(f"{_MOD}._write_status"), \
             patch(f"{_MOD}.ensure_table"):
            mock_items.return_value = [
                {"source": "tushare", "item_key": "stock_daily",
                 "target_database": "ts", "target_table": "stock_daily",
                 "table_created": 1, "sync_priority": 10},
            ]
            result = daily_sync(registry, target_date=date(2024, 1, 5))
        assert result["tushare/stock_daily"]["status"] == "error"

    def test_reopens_zero_row_success_for_strict_trading_day_interface(self):
        from app.datasync.service.sync_engine import daily_sync
        from app.datasync.base import SyncResult, SyncStatus

        registry = MagicMock()
        iface = MagicMock()
        iface.requires_nonempty_trading_day_data.return_value = True
        iface.sync_date.return_value = SyncResult(status=SyncStatus.SUCCESS, rows_synced=5)
        registry.get_interface.return_value = iface

        with patch(f"{_MOD}._get_enabled_items", return_value=[
            {"source": "tushare", "item_key": "stock_daily",
             "target_database": "ts", "target_table": "stock_daily",
             "table_created": 1, "sync_priority": 10},
        ]), \
            patch(f"{_MOD}._get_status_snapshot", return_value=(SyncStatus.SUCCESS.value, 0)), \
            patch(f"{_MOD}._write_status"), \
            patch(f"{_MOD}.ensure_table"), \
            patch(f"{_MOD}.get_trade_calendar", return_value=[date(2024, 1, 5)]):
            result = daily_sync(registry, target_date=date(2024, 1, 5))

        iface.sync_date.assert_called_once_with(date(2024, 1, 5))
        assert result["tushare/stock_daily"]["status"] == "success"

    def test_converts_zero_row_success_to_pending_for_strict_trading_day_interface(self):
        from app.datasync.service.sync_engine import _normalize_zero_row_success
        from app.datasync.base import SyncResult, SyncStatus

        iface = MagicMock()
        iface.requires_nonempty_trading_day_data.return_value = True

        with patch(f"{_MOD}.get_trade_calendar", return_value=[date(2024, 1, 5)]):
            result = _normalize_zero_row_success(
                iface,
                date(2024, 1, 5),
                "tushare",
                "stock_daily",
                SyncResult(SyncStatus.SUCCESS, 0, "No trading data"),
            )

        assert result.status == SyncStatus.PENDING
        assert result.rows_synced == 0


class TestBackfillRetry:
    def test_no_failed_records(self):
        from app.datasync.service.sync_engine import backfill_retry

        registry = MagicMock()
        with patch(f"{_MOD}._get_enabled_backfill_keys", return_value=set()), \
             patch(f"{_MOD}._get_failed_records", return_value=[]) as mock_failed, \
             patch("app.domains.extdata.dao.data_sync_status_dao.is_backfill_locked", return_value=False), \
             patch("app.domains.extdata.dao.data_sync_status_dao.acquire_backfill_lock"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.release_backfill_lock"), \
             patch(f"{_MOD}._resolve_backfill_window", return_value=(date(2025, 4, 17), date(2026, 4, 16))):
            result = backfill_retry(registry)

        assert result == {}
        mock_failed.assert_called_once_with(date(2025, 4, 17), date(2026, 4, 16))

    def test_uses_env_coverage_window_for_default_backfill_window(self):
        from app.datasync.service.sync_engine import _resolve_backfill_window

        with patch(
            "app.datasync.service.init_service.get_coverage_window",
            return_value={"start_date": date(2025, 4, 17), "end_date": date(2026, 4, 16)},
        ):
            result = _resolve_backfill_window()

        assert result == (date(2025, 4, 17), date(2026, 4, 16))

    def test_skips_when_locked(self):
        from app.datasync.service.sync_engine import backfill_retry

        registry = MagicMock()
        with patch("app.domains.extdata.dao.data_sync_status_dao.is_backfill_locked", return_value=True):
            result = backfill_retry(registry)

        assert result == {}

    def test_retries_interface(self):
        from app.datasync.base import SyncResult, SyncStatus
        from app.datasync.service.sync_engine import backfill_retry

        registry = MagicMock()
        iface = MagicMock()
        iface.sync_date.return_value = SyncResult(status=SyncStatus.SUCCESS, rows_synced=100)
        registry.get_interface.return_value = iface

        with patch(f"{_MOD}._get_enabled_backfill_keys", return_value={("tushare", "stock_daily")}), \
             patch(f"{_MOD}._get_failed_records") as mock_failed, \
             patch(f"{_MOD}._write_status"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.acquire_backfill_lock"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.release_backfill_lock"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.is_backfill_locked", return_value=False):
            mock_failed.return_value = [(date(2024, 1, 3), "tushare", "stock_daily", 0)]
            result = backfill_retry(registry, max_workers=2)

        assert "tushare/stock_daily@2024-01-03" in result
        assert result["tushare/stock_daily@2024-01-03"]["status"] == "success"

    def test_skips_runtime_unsupported_backfill_interface(self):
        from app.datasync.service.sync_engine import backfill_retry

        registry = MagicMock()
        iface = MagicMock()
        iface.supports_scheduled_sync.return_value = False
        registry.get_interface.return_value = iface

        with patch(f"{_MOD}._get_enabled_backfill_keys", return_value={("tushare", "fina_indicator")}), \
             patch(f"{_MOD}._get_failed_records", return_value=[(date(2024, 1, 3), "tushare", "fina_indicator", 0)]), \
             patch(f"{_MOD}._write_status") as mock_write, \
             patch("app.domains.extdata.dao.data_sync_status_dao.acquire_backfill_lock"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.release_backfill_lock"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.is_backfill_locked", return_value=False):
            result = backfill_retry(registry, max_workers=1)

        assert result["tushare/fina_indicator@2024-01-03"]["skipped"] is True
        assert result["tushare/fina_indicator@2024-01-03"]["status"] == "success"
        mock_write.assert_called_once()

    def test_quota_pause_does_not_consume_retry_budget(self):
        from app.datasync.base import SyncResult, SyncStatus
        from app.datasync.service.sync_engine import backfill_retry

        registry = MagicMock()
        iface = MagicMock()
        iface.sync_date.return_value = SyncResult(
            status=SyncStatus.PENDING,
            rows_synced=0,
            error_message="daily quota",
            details={"quota_exceeded": True, "quota_scope": "day"},
        )
        registry.get_interface.return_value = iface

        with patch(f"{_MOD}._get_enabled_backfill_keys", return_value={("tushare", "stock_daily")}), \
             patch(f"{_MOD}._get_failed_records", return_value=[(date(2024, 1, 3), "tushare", "stock_daily", 2)]), \
             patch(f"{_MOD}._write_status") as mock_write, \
             patch("app.domains.extdata.dao.data_sync_status_dao.acquire_backfill_lock"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.release_backfill_lock"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.is_backfill_locked", return_value=False):
            result = backfill_retry(registry, max_workers=1)

        assert result["tushare/stock_daily@2024-01-03"]["status"] == "pending"
        assert mock_write.call_args_list[0].kwargs["retry_count"] == 3
        assert mock_write.call_args_list[-1].kwargs["retry_count"] == 2

    def test_skips_quota_cooled_records_before_task_submission(self):
        from app.datasync.service.sync_engine import backfill_retry

        registry = MagicMock()

        quota_updated_at = datetime.now() - timedelta(hours=1)
        cooled_record = (
            date(2024, 1, 3),
            "tushare",
            "us_daily",
            0,
            "抱歉，您每天最多访问该接口5次，权限的具体详情访问：https://tushare.pro/document/1?doc_id=108。",
            "pending",
            0,
            quota_updated_at,
        )

        with patch(f"{_MOD}._get_enabled_backfill_keys", return_value={("tushare", "us_daily")}), \
             patch(f"{_MOD}._get_failed_records", return_value=[cooled_record]), \
             patch(f"{_MOD}._write_status") as mock_write, \
             patch("app.domains.extdata.dao.data_sync_status_dao.acquire_backfill_lock"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.release_backfill_lock"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.is_backfill_locked", return_value=False):
            result = backfill_retry(registry, max_workers=1)

        assert result == {}
        mock_write.assert_not_called()
        registry.get_interface.assert_not_called()

    def test_submits_only_one_quota_retry_task_per_interface(self):
        from app.datasync.base import SyncResult, SyncStatus
        from app.datasync.service.sync_engine import backfill_retry

        registry = MagicMock()
        iface = MagicMock()
        iface.sync_date.return_value = SyncResult(status=SyncStatus.SUCCESS, rows_synced=1)
        registry.get_interface.return_value = iface

        updated_at = datetime.now() - timedelta(hours=2)
        records = [
            (
                date(2024, 1, 4),
                "tushare",
                "report_rc",
                0,
                "抱歉，您每小时最多访问该接口10次，权限的具体详情访问：https://tushare.pro/document/1?doc_id=108。",
                "pending",
                0,
                updated_at,
            ),
            (
                date(2024, 1, 3),
                "tushare",
                "report_rc",
                0,
                "抱歉，您每小时最多访问该接口10次，权限的具体详情访问：https://tushare.pro/document/1?doc_id=108。",
                "pending",
                0,
                updated_at,
            ),
        ]

        with patch(f"{_MOD}._get_enabled_backfill_keys", return_value={("tushare", "report_rc")}), \
             patch(f"{_MOD}._get_failed_records", return_value=records), \
             patch(f"{_MOD}._write_status"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.acquire_backfill_lock"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.release_backfill_lock"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.is_backfill_locked", return_value=False):
            result = backfill_retry(registry, max_workers=1)

        assert registry.get_interface.call_count == 1
        iface.sync_date.assert_called_once_with(date(2024, 1, 4))
        assert "tushare/report_rc@2024-01-04" in result
        assert "tushare/report_rc@2024-01-03" not in result

    def test_defers_remaining_interface_tasks_after_quota_pause_in_same_pass(self):
        from app.datasync.base import SyncResult, SyncStatus
        from app.datasync.service.sync_engine import backfill_retry

        registry = MagicMock()
        quota_iface = MagicMock()
        quota_iface.sync_date.return_value = SyncResult(
            status=SyncStatus.PENDING,
            rows_synced=0,
            error_message="hour quota",
            details={"quota_exceeded": True, "quota_scope": "hour", "quota_retry_after": "360"},
        )
        other_iface = MagicMock()
        other_iface.sync_date.return_value = SyncResult(status=SyncStatus.SUCCESS, rows_synced=5)

        def _get_interface(source, item_key):
            if (source, item_key) == ("tushare", "report_rc"):
                return quota_iface
            if (source, item_key) == ("tushare", "stock_daily"):
                return other_iface
            return None

        registry.get_interface.side_effect = _get_interface

        records = [
            (date(2024, 1, 4), "tushare", "report_rc", 0),
            (date(2024, 1, 4), "tushare", "stock_daily", 0),
            (date(2024, 1, 3), "tushare", "report_rc", 0),
        ]

        with patch(f"{_MOD}._get_enabled_backfill_keys", return_value={
            ("tushare", "report_rc"),
            ("tushare", "stock_daily"),
        }), \
             patch(f"{_MOD}._get_failed_records", return_value=records), \
             patch(f"{_MOD}._write_status"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.acquire_backfill_lock"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.release_backfill_lock"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.is_backfill_locked", return_value=False):
            result = backfill_retry(registry, max_workers=2)

        quota_iface.sync_date.assert_called_once_with(date(2024, 1, 4))
        other_iface.sync_date.assert_called_once_with(date(2024, 1, 4))
        assert result["tushare/report_rc@2024-01-04"]["status"] == "pending"
        assert result["tushare/stock_daily@2024-01-04"]["status"] == "success"
        assert "tushare/report_rc@2024-01-03" not in result

    def test_reopens_historical_zero_row_success_for_strict_interface(self):
        from app.datasync.base import SyncResult, SyncStatus
        from app.datasync.service.sync_engine import backfill_retry

        registry = MagicMock()
        iface = MagicMock()
        iface.requires_nonempty_trading_day_data.return_value = True
        iface.sync_date.return_value = SyncResult(status=SyncStatus.SUCCESS, rows_synced=8)
        registry.get_interface.return_value = iface

        with patch(f"{_MOD}._get_enabled_backfill_keys", return_value={("tushare", "stock_daily")}), \
             patch(f"{_MOD}._get_failed_records", return_value=[
                 (date(2024, 1, 3), "tushare", "stock_daily", 0, None, "success", 0),
             ]), \
             patch(f"{_MOD}.get_trade_calendar", return_value=[date(2024, 1, 3)]), \
             patch(f"{_MOD}._write_status"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.acquire_backfill_lock"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.release_backfill_lock"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.is_backfill_locked", return_value=False):
            result = backfill_retry(registry, max_workers=2)

        iface.sync_date.assert_called_once_with(date(2024, 1, 3))
        assert result["tushare/stock_daily@2024-01-03"]["status"] == "success"

    def test_submits_newest_dates_first(self):
        from concurrent.futures import Future

        from app.datasync.base import SyncResult, SyncStatus
        from app.datasync.service.sync_engine import backfill_retry

        registry = MagicMock()
        iface = MagicMock()
        iface.sync_date.return_value = SyncResult(status=SyncStatus.SUCCESS, rows_synced=1)
        registry.get_interface.return_value = iface

        submitted_dates = []

        class FakeExecutor:
            def __init__(self, max_workers, thread_name_prefix=None):
                self.max_workers = max_workers
                self.thread_name_prefix = thread_name_prefix

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def submit(self, fn, task, *args, **kwargs):
                submitted_dates.append(task.start_date)
                future = Future()
                try:
                    future.set_result(fn(task, *args, **kwargs))
                except Exception as exc:
                    future.set_exception(exc)
                return future

        with patch(f"{_MOD}._get_enabled_backfill_keys", return_value={("tushare", "stock_daily")}), \
             patch(f"{_MOD}.ThreadPoolExecutor", FakeExecutor), \
             patch(f"{_MOD}._get_failed_records", return_value=[
                 (date(2024, 1, 3), "tushare", "stock_daily", 0),
                 (date(2024, 1, 4), "tushare", "stock_daily", 0),
             ]), \
             patch(f"{_MOD}._write_status"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.acquire_backfill_lock"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.release_backfill_lock"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.is_backfill_locked", return_value=False):
            backfill_retry(registry, max_workers=2)

        assert submitted_dates == [date(2024, 1, 4), date(2024, 1, 3)]

    def test_uses_range_backfill_for_range_interfaces(self):
        from app.datasync.base import SyncResult, SyncStatus
        from app.datasync.service.sync_engine import backfill_retry

        registry = MagicMock()
        iface = MagicMock()
        iface.backfill_mode.return_value = "range"
        iface.sync_range.return_value = SyncResult(status=SyncStatus.SUCCESS, rows_synced=10)
        iface.get_backfill_rows_by_date.return_value = {
            date(2024, 1, 3): 4,
            date(2024, 1, 4): 6,
        }
        registry.get_interface.return_value = iface

        with patch(f"{_MOD}._get_enabled_backfill_keys", return_value={("tushare", "dividend")}), \
             patch(f"{_MOD}._get_failed_records", return_value=[
                 (date(2024, 1, 3), "tushare", "dividend", 0),
                 (date(2024, 1, 4), "tushare", "dividend", 1),
             ]), \
             patch(f"{_MOD}.get_trade_calendar", return_value=[date(2024, 1, 3), date(2024, 1, 4)]), \
             patch(f"{_MOD}._write_status") as mock_write, \
             patch("app.domains.extdata.dao.data_sync_status_dao.acquire_backfill_lock"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.release_backfill_lock"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.is_backfill_locked", return_value=False):
            result = backfill_retry(registry, max_workers=2)

        iface.sync_range.assert_called_once_with(date(2024, 1, 3), date(2024, 1, 4))
        iface.sync_date.assert_not_called()
        assert result["tushare/dividend@2024-01-03"]["rows"] == 4
        assert result["tushare/dividend@2024-01-04"]["rows"] == 6
        assert result["tushare/dividend@2024-01-03"]["backfill_mode"] == "range"
        assert mock_write.call_count == 4

    def test_keeps_date_backfill_for_marketwide_interfaces(self):
        from app.datasync.base import SyncResult, SyncStatus
        from app.datasync.service.sync_engine import backfill_retry

        registry = MagicMock()
        iface = MagicMock()
        iface.backfill_mode.return_value = "date"
        iface.sync_date.return_value = SyncResult(status=SyncStatus.SUCCESS, rows_synced=5)
        registry.get_interface.return_value = iface

        with patch(f"{_MOD}._get_enabled_backfill_keys", return_value={("tushare", "stock_daily")}), \
             patch(f"{_MOD}._get_failed_records", return_value=[
                 (date(2024, 1, 3), "tushare", "stock_daily", 0),
                 (date(2024, 1, 4), "tushare", "stock_daily", 0),
             ]), \
             patch(f"{_MOD}._write_status"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.acquire_backfill_lock"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.release_backfill_lock"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.is_backfill_locked", return_value=False):
            result = backfill_retry(registry, max_workers=2)

        assert iface.sync_date.call_count == 2
        iface.sync_range.assert_not_called()
        assert result["tushare/stock_daily@2024-01-03"]["backfill_mode"] == "date"
        assert result["tushare/stock_daily@2024-01-04"]["backfill_mode"] == "date"

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
             patch(f"{_MOD}._get_enabled_backfill_keys", return_value={("tushare", "stock_daily")}), \
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

        with patch(f"{_MOD}._get_enabled_backfill_keys", return_value={("akshare", "stock_zh_index_spot")}), \
             patch(f"{_MOD}._get_failed_records", return_value=[(date(2024, 1, 3), "akshare", "stock_zh_index_spot", 0)]), \
             patch(f"{_MOD}._write_status") as mock_write, \
             patch("app.domains.extdata.dao.data_sync_status_dao.acquire_backfill_lock"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.release_backfill_lock"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.is_backfill_locked", return_value=False):
            result = backfill_retry(registry)

        iface.sync_date.assert_not_called()
        mock_write.assert_called_once()
        assert result["akshare/stock_zh_index_spot@2024-01-03"]["skipped"] is True

    def test_filters_out_non_enabled_records_before_backfill(self):
        from app.datasync.base import SyncResult, SyncStatus
        from app.datasync.service.sync_engine import backfill_retry

        registry = MagicMock()
        iface = MagicMock()
        iface.sync_date.return_value = SyncResult(status=SyncStatus.SUCCESS, rows_synced=5)
        registry.get_interface.return_value = iface

        with patch(f"{_MOD}._get_enabled_backfill_keys", return_value={("tushare", "stock_daily")}), \
             patch(f"{_MOD}._get_failed_records", return_value=[
                 (date(2024, 1, 3), "vnpy", "vnpy_sync", 0),
                 (date(2024, 1, 3), "tushare", "stock_daily", 0),
             ]), \
             patch(f"{_MOD}._write_status"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.acquire_backfill_lock"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.release_backfill_lock"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.is_backfill_locked", return_value=False):
            result = backfill_retry(registry, max_workers=2)

        registry.get_interface.assert_called_once_with("tushare", "stock_daily")
        assert "vnpy/vnpy_sync@2024-01-03" not in result
        assert result["tushare/stock_daily@2024-01-03"]["status"] == "success"

    def test_retries_recoverable_terminal_trade_cal_rows(self):
        from app.datasync.base import SyncResult, SyncStatus
        from app.datasync.service.sync_engine import backfill_retry

        registry = MagicMock()
        iface = MagicMock()
        iface.backfill_mode.return_value = "date"
        iface.sync_date.return_value = SyncResult(status=SyncStatus.SUCCESS, rows_synced=1)
        registry.get_interface.return_value = iface

        with patch(f"{_MOD}._get_enabled_backfill_keys", return_value={("tushare", "trade_cal")}), \
             patch(f"{_MOD}._get_failed_records", return_value=[
                 (
                     date(2024, 1, 3),
                     "tushare",
                     "trade_cal",
                     3,
                     "(1054, \"Unknown column 'updated_at' in 'field list'\")",
                 ),
             ]), \
             patch(f"{_MOD}._write_status") as mock_write, \
             patch("app.domains.extdata.dao.data_sync_status_dao.acquire_backfill_lock"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.release_backfill_lock"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.is_backfill_locked", return_value=False):
            result = backfill_retry(registry, max_workers=2)

        assert result["tushare/trade_cal@2024-01-03"]["status"] == "success"
        iface.sync_date.assert_called_once_with(date(2024, 1, 3))
        assert mock_write.call_args_list[0].kwargs["retry_count"] == 1
