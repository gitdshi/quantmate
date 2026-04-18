"""Unit tests for sync_init_service, datasync_tasks, parallel sync engine,
thread-safe rate limiting, and new DAO / settings routes."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, call, patch

import pytest

# ---------------------------------------------------------------------------
# Module path constants
# ---------------------------------------------------------------------------
_INIT_MOD = "app.datasync.service.sync_init_service"
_ENGINE_MOD = "app.datasync.service.sync_engine"
_TASK_MOD = "app.worker.service.datasync_tasks"
_INGEST_MOD = "app.datasync.service.tushare_ingest"
_DAO_CONN = "app.domains.market.dao.data_source_item_dao.connection"
_ROUTES_MOD = "app.api.routes.settings"


def _conn_ctx(rows=None, fetchone_val=None):
    """Helper to mock engine.begin()/connect() context manager."""
    engine = MagicMock()
    conn = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value = ctx
    engine.connect.return_value = ctx
    if rows is not None:
        conn.execute.return_value = MagicMock(fetchall=MagicMock(return_value=rows))
    if fetchone_val is not None:
        conn.execute.return_value = MagicMock(fetchone=MagicMock(return_value=fetchone_val))
    return engine, conn


# ===========================================================================
# sync_init_service tests
# ===========================================================================

class TestAlreadyInitialized:
    def test_returns_true_when_row_exists(self):
        from app.datasync.service.sync_init_service import _already_initialized
        engine, conn = _conn_ctx(fetchone_val=(1,))
        with patch(f"{_INIT_MOD}.get_quantmate_engine", return_value=engine):
            assert _already_initialized("tushare", "stock_daily") is True

    def test_returns_false_when_no_row(self):
        from app.datasync.service.sync_init_service import _already_initialized
        engine = MagicMock()
        conn = MagicMock()
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=conn)
        ctx.__exit__ = MagicMock(return_value=False)
        engine.connect.return_value = ctx
        conn.execute.return_value = MagicMock(fetchone=MagicMock(return_value=None))
        with patch(f"{_INIT_MOD}.get_quantmate_engine", return_value=engine):
            assert _already_initialized("tushare", "stock_daily") is False


class TestRecordInit:
    def test_records_init(self):
        from app.datasync.service.sync_init_service import _record_init
        engine, conn = _conn_ctx()
        with patch(f"{_INIT_MOD}.get_quantmate_engine", return_value=engine):
            _record_init("tushare", "stock_daily", date(2020, 1, 1), date(2024, 12, 31))
        conn.execute.assert_called_once()


class TestInitializeSyncStatus:
    def test_default_window_uses_env_coverage_floor(self):
        from app.datasync.service.sync_init_service import _resolve_default_sync_window

        with patch("app.datasync.service.init_service.get_coverage_window", return_value={"start_date": date(2025, 4, 16)}):
            start_date, end_date = _resolve_default_sync_window(date(2026, 4, 16))

        assert start_date == date(2025, 4, 16)
        assert end_date == date(2026, 4, 16)

    def test_skips_when_already_initialized(self):
        from app.datasync.service.sync_init_service import initialize_sync_status
        with patch(f"{_INIT_MOD}.ensure_sync_status_init_table"), \
             patch(f"{_INIT_MOD}._already_initialized", return_value=True):
            result = initialize_sync_status("tushare", "stock_daily", reconcile_missing=False)
        assert result == 0

    def test_seeds_pending_rows(self):
        from app.datasync.service.sync_init_service import initialize_sync_status
        trade_days = [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)]
        engine, conn = _conn_ctx()

        with patch(f"{_INIT_MOD}.ensure_sync_status_init_table"), \
             patch(f"{_ENGINE_MOD}.get_trade_calendar", return_value=trade_days), \
             patch(f"{_INIT_MOD}.get_quantmate_engine", return_value=engine), \
             patch(f"{_INIT_MOD}._record_init") as mock_record:
            result = initialize_sync_status(
                "tushare", "stock_daily",
                start_date=date(2024, 1, 1), end_date=date(2024, 1, 5),
            )

        assert result == 3
        assert conn.execute.called
        mock_record.assert_called_once_with("tushare", "stock_daily", date(2024, 1, 1), date(2024, 1, 5))

    def test_no_trade_days(self):
        from app.datasync.service.sync_init_service import initialize_sync_status
        with patch(f"{_INIT_MOD}.ensure_sync_status_init_table"), \
             patch(f"{_INIT_MOD}._get_initialized_bounds", return_value=None), \
             patch(f"{_INIT_MOD}._record_init"), \
             patch(f"{_ENGINE_MOD}.get_trade_calendar", return_value=[]):
            result = initialize_sync_status("tushare", "stock_daily")
        assert result == 0

    def test_uses_default_dates(self):
        from app.datasync.service.sync_init_service import initialize_sync_status
        trade_days = [date(2024, 6, 1)]
        engine, conn = _conn_ctx()

        with patch(f"{_INIT_MOD}.ensure_sync_status_init_table"), \
             patch(f"{_INIT_MOD}._resolve_default_sync_window", return_value=(date(2024, 5, 1), date(2024, 6, 1))), \
             patch(f"{_ENGINE_MOD}.get_trade_calendar", return_value=trade_days) as mock_cal, \
             patch(f"{_INIT_MOD}.get_quantmate_engine", return_value=engine), \
             patch(f"{_INIT_MOD}._record_init"):
            initialize_sync_status("tushare", "stock_daily")

        args = mock_cal.call_args[0]
        assert args[0] == date(2024, 5, 1)


class TestReconcileEnabledSyncStatus:
    def test_non_historical_interface_only_initializes_target_day(self):
        from app.datasync.service.sync_init_service import reconcile_enabled_sync_status

        engine, conn = _conn_ctx(rows=[("akshare", "stock_zh_index_spot")])
        registry = MagicMock()
        iface = MagicMock()
        iface.supports_backfill.return_value = False
        registry.get_interface.return_value = iface

        with patch(f"{_INIT_MOD}.ensure_sync_status_init_table"), \
             patch(f"{_INIT_MOD}.get_quantmate_engine", return_value=engine), \
             patch(f"{_INIT_MOD}.initialize_sync_status", return_value=1) as mock_init:
            result = reconcile_enabled_sync_status(
                registry,
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 5),
            )

        assert result["pending_records"] == 1
        mock_init.assert_called_once_with(
            "akshare",
            "stock_zh_index_spot",
            start_date=date(2024, 1, 5),
            end_date=date(2024, 1, 5),
            reconcile_missing=True,
        )

    def test_skips_enabled_item_when_token_points_do_not_support_it(self):
        from app.datasync.service.sync_init_service import reconcile_enabled_sync_status

        engine, conn = _conn_ctx(rows=[("tushare", "bak_daily", "bak_daily", 5000, None)])
        registry = MagicMock()
        registry.get_interface.return_value = MagicMock()

        with patch(f"{_INIT_MOD}.ensure_sync_status_init_table"), \
             patch(f"{_INIT_MOD}.get_quantmate_engine", return_value=engine), \
             patch("app.datasync.capabilities.load_source_config_map", return_value={"tushare": {"config_json": {"token_points": 2000}}}), \
             patch(f"{_INIT_MOD}.initialize_sync_status") as mock_init:
            result = reconcile_enabled_sync_status(
                registry,
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 5),
            )

        assert result["pending_records"] == 0
        assert result["items_reconciled"] == 0
        assert result["skipped_unsupported"] == [{"source": "tushare", "item_key": "bak_daily"}]
        mock_init.assert_not_called()


# ===========================================================================
# Parallel sync engine tests
# ===========================================================================

class TestSourceSemaphore:
    def test_tushare_gets_semaphore(self):
        from app.datasync.service.sync_engine import _get_source_semaphore, _source_semaphores
        _source_semaphores.clear()
        sem = _get_source_semaphore("tushare")
        assert sem is not None

    def test_unknown_source_returns_none(self):
        from app.datasync.service.sync_engine import _get_source_semaphore
        sem = _get_source_semaphore("unknown_source_xyz")
        assert sem is None


class TestSyncOneItem:
    def test_no_interface(self):
        from app.datasync.service.sync_engine import _sync_one_item
        registry = MagicMock()
        registry.get_interface.return_value = None
        item = {"source": "tushare", "item_key": "missing", "table_created": 1,
                "target_database": "ts", "target_table": "x", "sync_priority": 10}
        with patch(f"{_ENGINE_MOD}._get_source_semaphore", return_value=None):
            label, res = _sync_one_item(registry, item, date(2024, 1, 5), 1, 1)
        assert res["status"] == "skipped"

    def test_already_synced(self):
        from app.datasync.service.sync_engine import _sync_one_item
        from app.datasync.base import SyncStatus
        registry = MagicMock()
        registry.get_interface.return_value = MagicMock()
        item = {"source": "tushare", "item_key": "stock_daily", "table_created": 1,
                "target_database": "ts", "target_table": "stock_daily", "sync_priority": 10}
        with patch(f"{_ENGINE_MOD}._get_source_semaphore", return_value=None), \
             patch(f"{_ENGINE_MOD}._get_status_snapshot", return_value=(SyncStatus.SUCCESS.value, 1)):
            label, res = _sync_one_item(registry, item, date(2024, 1, 5), 1, 1)
        assert res.get("skipped") is True

    def test_successful_sync(self):
        from app.datasync.service.sync_engine import _sync_one_item
        from app.datasync.base import SyncResult, SyncStatus
        registry = MagicMock()
        iface = MagicMock()
        iface.sync_date.return_value = SyncResult(status=SyncStatus.SUCCESS, rows_synced=500)
        registry.get_interface.return_value = iface
        item = {"source": "tushare", "item_key": "stock_daily", "table_created": 1,
                "target_database": "ts", "target_table": "stock_daily", "sync_priority": 10}
        with patch(f"{_ENGINE_MOD}._get_source_semaphore", return_value=None), \
             patch(f"{_ENGINE_MOD}._get_status_snapshot", return_value=(None, 0)), \
             patch(f"{_ENGINE_MOD}._write_status"), \
             patch(f"{_ENGINE_MOD}.ensure_table"):
            label, res = _sync_one_item(registry, item, date(2024, 1, 5), 1, 1)
        assert res["status"] == "success"
        assert res["rows"] == 500

    def test_sync_exception(self):
        from app.datasync.service.sync_engine import _sync_one_item
        registry = MagicMock()
        iface = MagicMock()
        iface.sync_date.side_effect = Exception("API boom")
        registry.get_interface.return_value = iface
        item = {"source": "tushare", "item_key": "stock_daily", "table_created": 1,
                "target_database": "ts", "target_table": "stock_daily", "sync_priority": 10}
        with patch(f"{_ENGINE_MOD}._get_source_semaphore", return_value=None), \
             patch(f"{_ENGINE_MOD}._get_status_snapshot", return_value=(None, 0)), \
             patch(f"{_ENGINE_MOD}._write_status"), \
             patch(f"{_ENGINE_MOD}.ensure_table"):
            label, res = _sync_one_item(registry, item, date(2024, 1, 5), 1, 1)
        assert res["status"] == "error"

    def test_ddl_failure(self):
        from app.datasync.service.sync_engine import _sync_one_item
        registry = MagicMock()
        iface = MagicMock()
        registry.get_interface.return_value = iface
        item = {"source": "tushare", "item_key": "stock_daily", "table_created": 0,
                "target_database": "ts", "target_table": "stock_daily", "sync_priority": 10}
        with patch(f"{_ENGINE_MOD}._get_source_semaphore", return_value=None), \
             patch(f"{_ENGINE_MOD}._get_status_snapshot", return_value=(None, 0)), \
             patch(f"{_ENGINE_MOD}._write_status"), \
             patch(f"{_ENGINE_MOD}.ensure_table", side_effect=Exception("DDL boom")):
            label, res = _sync_one_item(registry, item, date(2024, 1, 5), 1, 1)
        assert res["status"] == "error"
        assert "DDL" in res["error"]


class TestDailySyncParallel:
    def test_parallel_sync_runs(self):
        from app.datasync.service.sync_engine import daily_sync
        from app.datasync.base import SyncResult, SyncStatus
        registry = MagicMock()
        iface = MagicMock()
        iface.sync_date.return_value = SyncResult(status=SyncStatus.SUCCESS, rows_synced=100)
        registry.get_interface.return_value = iface

        items = [
            {"source": "tushare", "item_key": "stock_daily",
             "target_database": "ts", "target_table": "stock_daily",
             "table_created": 1, "sync_priority": 10},
            {"source": "tushare", "item_key": "adj_factor",
             "target_database": "ts", "target_table": "adj_factor",
             "table_created": 1, "sync_priority": 30},
        ]

        with patch(f"{_ENGINE_MOD}._get_enabled_items", return_value=items), \
               patch(f"{_ENGINE_MOD}._get_status_snapshot", return_value=(None, 0)), \
               patch(f"{_ENGINE_MOD}._write_status"), \
               patch(f"{_ENGINE_MOD}.ensure_table"), \
             patch(f"{_ENGINE_MOD}.get_previous_trade_date", return_value=date(2024, 1, 5)):
            result = daily_sync(registry, target_date=date(2024, 1, 5), max_workers=2)

        assert len(result) == 2
        assert result["tushare/stock_daily"]["status"] == "success"
        assert result["tushare/adj_factor"]["status"] == "success"

    def test_daily_sync_max_workers_param(self):
        """Ensure max_workers parameter is respected."""
        from app.datasync.service.sync_engine import daily_sync
        registry = MagicMock()
        with patch(f"{_ENGINE_MOD}._get_enabled_items", return_value=[]), \
             patch(f"{_ENGINE_MOD}.get_previous_trade_date", return_value=date(2024, 1, 5)):
            result = daily_sync(registry, target_date=date(2024, 1, 5), max_workers=1)
        assert result == {}


# ===========================================================================
# call_pro local pacing state tests
# ===========================================================================

class TestCallProThreadSafe:
    def test_does_not_initialize_local_pacing_state(self):
        from app.datasync.service.tushare_ingest import call_pro

        for attr in ("_lock", "_last_call"):
            if hasattr(call_pro, attr):
                delattr(call_pro, attr)

        mock_pro = MagicMock()
        mock_pro.test_api.return_value = MagicMock()
        with patch(f"{_INGEST_MOD}.pro", mock_pro):
            call_pro("test_api", max_retries=1)

        assert not hasattr(call_pro, "_lock")
        assert not hasattr(call_pro, "_last_call")


# ===========================================================================
# datasync_tasks tests
# ===========================================================================

class TestRunBackfillTask:
    def test_no_interface(self):
        from app.worker.service.datasync_tasks import run_backfill_task
        registry = MagicMock()
        registry.get_interface.return_value = None

        with patch("rq.get_current_job", return_value=None), \
               patch("app.datasync.registry.build_default_registry", return_value=registry):
            result = run_backfill_task("tushare", "nonexistent")

        assert result["status"] == "skipped"

    def test_no_pending_dates(self):
        from app.worker.service.datasync_tasks import run_backfill_task
        registry = MagicMock()
        iface = MagicMock()
        registry.get_interface.return_value = iface
        engine, conn = _conn_ctx()

        # First call: fetch item row (table_created=1)
        item_row = MagicMock()
        item_row.__getitem__ = lambda s, k: {0: "tushare", 1: "stock_daily", 2: 1}[k]
        # Second call: no pending
        no_rows = MagicMock(fetchall=MagicMock(return_value=[]))
        exhausted_row = MagicMock(fetchone=MagicMock(return_value=(0,)))

        conn.execute.side_effect = [
            MagicMock(fetchone=MagicMock(return_value=item_row)),
            no_rows,
            exhausted_row,
        ]

        with patch("rq.get_current_job", return_value=None), \
             patch("app.datasync.registry.build_default_registry", return_value=registry), \
               patch("app.datasync.table_manager.ensure_table", return_value=True), \
             patch("app.infrastructure.db.connections.get_quantmate_engine", return_value=engine):
            result = run_backfill_task("tushare", "stock_daily")

        assert result["status"] == "complete"
        assert result["synced"] == 0
        assert result["exhausted"] == 0

    def test_reports_failed_when_only_exhausted_records_remain(self):
        from app.worker.service.datasync_tasks import run_backfill_task
        registry = MagicMock()
        iface = MagicMock()
        registry.get_interface.return_value = iface
        engine, conn = _conn_ctx()

        item_row = MagicMock()
        item_row.__getitem__ = lambda s, k: {0: "tushare", 1: "stock_daily", 2: 1}[k]
        no_rows = MagicMock(fetchall=MagicMock(return_value=[]))
        exhausted_row = MagicMock(fetchone=MagicMock(return_value=(2,)))

        conn.execute.side_effect = [
            MagicMock(fetchone=MagicMock(return_value=item_row)),
            no_rows,
            exhausted_row,
        ]

        with patch("rq.get_current_job", return_value=None), \
             patch("app.datasync.registry.build_default_registry", return_value=registry), \
               patch("app.datasync.table_manager.ensure_table", return_value=True), \
             patch("app.infrastructure.db.connections.get_quantmate_engine", return_value=engine):
            result = run_backfill_task("tushare", "stock_daily")

        assert result["status"] == "failed"
        assert result["exhausted"] == 2

    def test_uses_backfill_source_semaphore(self):
        from app.datasync.base import SyncResult, SyncStatus
        from app.worker.service.datasync_tasks import run_backfill_task

        registry = MagicMock()
        iface = MagicMock()
        iface.sync_date.return_value = SyncResult(SyncStatus.SUCCESS, 12)
        registry.get_interface.return_value = iface
        engine, conn = _conn_ctx()
        sem = MagicMock()

        item_row = MagicMock()
        item_row.__getitem__ = lambda s, k: {0: "tushare", 1: "stock_daily", 2: 1}[k]
        pending_rows = MagicMock(fetchall=MagicMock(return_value=[(date(2024, 1, 3), "pending", 0)]))
        remaining_row = MagicMock(fetchone=MagicMock(return_value=(0,)))
        exhausted_row = MagicMock(fetchone=MagicMock(return_value=(0,)))

        conn.execute.side_effect = [
            MagicMock(fetchone=MagicMock(return_value=item_row)),
            pending_rows,
            remaining_row,
            exhausted_row,
        ]

        with patch("rq.get_current_job", return_value=None), \
             patch("app.datasync.registry.build_default_registry", return_value=registry), \
               patch("app.datasync.table_manager.ensure_table", return_value=True), \
             patch("app.infrastructure.db.connections.get_quantmate_engine", return_value=engine), \
             patch("app.datasync.service.sync_engine._get_backfill_source_semaphore", return_value=sem), \
             patch("app.datasync.service.sync_engine._write_status"):
            result = run_backfill_task("tushare", "stock_daily")

        sem.acquire.assert_called_once()
        sem.release.assert_called_once()
        assert result["synced"] == 1

    def test_pauses_batch_on_quota_without_reenqueue(self):
        from app.datasync.base import SyncResult, SyncStatus
        from app.worker.service.datasync_tasks import run_backfill_task

        registry = MagicMock()
        iface = MagicMock()
        iface.sync_date.return_value = SyncResult(
            SyncStatus.PENDING,
            0,
            "daily quota",
            details={"quota_exceeded": True, "quota_scope": "day"},
        )
        registry.get_interface.return_value = iface
        engine, conn = _conn_ctx()

        item_row = MagicMock()
        item_row.__getitem__ = lambda s, k: {0: "tushare", 1: "stock_daily", 2: 1}[k]
        pending_rows = MagicMock(fetchall=MagicMock(return_value=[
            (date(2024, 1, 3), "pending", 0),
            (date(2024, 1, 4), "pending", 0),
        ]))
        remaining_row = MagicMock(fetchone=MagicMock(return_value=(2,)))
        exhausted_row = MagicMock(fetchone=MagicMock(return_value=(0,)))

        def _execute_side_effect(statement, *args, **kwargs):
            sql = str(statement)
            if "FROM data_source_items" in sql:
                return MagicMock(fetchone=MagicMock(return_value=item_row))
            if "ORDER BY sync_date ASC LIMIT" in sql:
                return pending_rows
            if "retry_count < :max_retries" in sql:
                return remaining_row
            if "retry_count >= :max_retries" in sql:
                return exhausted_row
            return MagicMock()

        conn.execute.side_effect = _execute_side_effect

        with patch("rq.get_current_job", return_value=None), \
             patch("app.datasync.registry.build_default_registry", return_value=registry), \
             patch("app.datasync.table_manager.ensure_table", return_value=True), \
             patch("app.infrastructure.db.connections.get_quantmate_engine", return_value=engine), \
               patch("app.datasync.service.sync_engine._get_backfill_source_semaphore", return_value=None), \
             patch("app.datasync.service.sync_engine._write_status") as mock_write, \
             patch("app.worker.service.config.get_queue") as mock_get_queue:
            result = run_backfill_task("tushare", "stock_daily")

        assert result["status"] == "partial"
        assert result["paused"] is True
        assert result["remaining"] == 2
        assert mock_write.call_args_list[0].kwargs["retry_count"] == 1
        assert mock_write.call_args_list[-1].kwargs["retry_count"] == 0
        mock_get_queue.assert_not_called()


def _dao_conn_ctx():
    """Helper to mock the connection("quantmate") context manager used by the DAO."""
    conn = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)
    return ctx, conn


# ===========================================================================
# DAO new methods tests
# ===========================================================================

class TestDataSourceItemDaoCategories:
    def test_list_with_categories(self):
        from app.domains.market.dao.data_source_item_dao import DataSourceItemDao
        ctx, conn = _dao_conn_ctx()
        mapping_data = {
            "source": "tushare", "item_key": "stock_daily", "display_name": "日线行情",
            "enabled": 1, "target_table": "stock_daily", "category": "股票数据",
            "sub_category": "行情数据", "api_name": "daily", "permission_points": 120,
            "rate_limit_note": "500条/分钟", "requires_permission": "0", "sync_priority": 20,
        }
        mock_row = MagicMock()
        mock_row._mapping = mapping_data
        conn.execute.return_value = MagicMock(fetchall=MagicMock(return_value=[mock_row]))

        with patch(_DAO_CONN, return_value=ctx):
            dao = DataSourceItemDao()
            items = dao.list_with_categories(source="tushare")

        assert len(items) == 1
        assert items[0]["category"] == "股票数据"

    def test_batch_update_by_permission(self):
        from app.domains.market.dao.data_source_item_dao import DataSourceItemDao
        ctx, conn = _dao_conn_ctx()
        conn.execute.return_value = MagicMock(rowcount=5)

        with patch(_DAO_CONN, return_value=ctx):
            dao = DataSourceItemDao()
            count = dao.batch_update_by_permission("tushare", 120, True)

        assert count == 5
        conn.execute.assert_called_once()
        sql = str(conn.execute.call_args.args[0])
        assert "NOT IN ('1', 'true', 'yes', 'paid')" in sql

    def test_get_distinct_permissions(self):
        from app.domains.market.dao.data_source_item_dao import DataSourceItemDao
        ctx, conn = _dao_conn_ctx()
        row1 = MagicMock()
        row1.__getitem__ = lambda s, k: 120
        row2 = MagicMock()
        row2.__getitem__ = lambda s, k: 2000
        conn.execute.return_value = MagicMock(fetchall=MagicMock(return_value=[row1, row2]))

        with patch(_DAO_CONN, return_value=ctx):
            dao = DataSourceItemDao()
            perms = dao.get_distinct_permissions("tushare")

        assert perms == [120, 2000]
        sql = str(conn.execute.call_args.args[0])
        assert "NOT IN ('1', 'true', 'yes', 'paid')" in sql


# ===========================================================================
# Settings routes tests (batch-by-permission, permissions endpoint)
# ===========================================================================

class TestSettingsRoutes:
    """White-box tests for the new route helper _trigger_sync_init."""

    @pytest.mark.anyio
    async def test_update_datasource_item_rejects_unsupported_enable(self):
        from app.api.exception_handlers import APIError
        from app.api.routes.settings import DataSourceItemUpdate, update_datasource_item

        mock_dao = MagicMock()
        mock_dao.get_by_key.return_value = {"enabled": 0}

        with patch("app.domains.market.dao.data_source_item_dao.DataSourceItemDao", return_value=mock_dao), \
             patch(f"{_ROUTES_MOD}._get_sync_support_map", return_value=set()):
            with pytest.raises(APIError) as excinfo:
                await update_datasource_item(
                    item_key="stock_company",
                    body=DataSourceItemUpdate(enabled=True),
                    source="tushare",
                    current_user=MagicMock(),
                )

        assert excinfo.value.status_code == 400

    @pytest.mark.anyio
    async def test_batch_update_only_triggers_new_supported_enables(self):
        from app.api.routes.settings import DataSourceBatchUpdate, batch_update_datasource_items

        mock_dao = MagicMock()
        mock_dao.get_by_key.side_effect = [
            {"enabled": 0},
            {"enabled": 1},
            {"enabled": 0},
            {"enabled": 1},
        ]
        mock_dao.batch_update.return_value = 2

        body = DataSourceBatchUpdate(
            items=[
                {"source": "tushare", "item_key": "stock_daily", "enabled": True},
                {"source": "tushare", "item_key": "adj_factor", "enabled": True},
                {"source": "tushare", "item_key": "stock_company", "enabled": True},
                {"source": "tushare", "item_key": "stock_basic", "enabled": False},
            ]
        )

        with patch("app.domains.market.dao.data_source_item_dao.DataSourceItemDao", return_value=mock_dao), \
             patch(
                 f"{_ROUTES_MOD}._get_sync_support_map",
                 return_value={("tushare", "stock_daily"), ("tushare", "adj_factor"), ("tushare", "stock_basic")},
             ), \
             patch(f"{_ROUTES_MOD}._ensure_table_for_item") as mock_ensure, \
             patch(f"{_ROUTES_MOD}._trigger_sync_init") as mock_trigger:
            result = await batch_update_datasource_items(body=body, current_user=MagicMock())

        assert result["updated"] == 2
        assert result["skipped_unsupported"] == [{"source": "tushare", "item_key": "stock_company"}]
        mock_dao.batch_update.assert_called_once_with(
            [
                {"source": "tushare", "item_key": "stock_daily", "enabled": True},
                {"source": "tushare", "item_key": "stock_basic", "enabled": False},
            ]
        )
        mock_ensure.assert_called_once_with("tushare", "stock_daily")
        mock_trigger.assert_called_once_with("tushare", "stock_daily")

    @pytest.mark.anyio
    async def test_batch_update_by_permission_filters_paid_and_unsupported_items(self):
        from app.api.routes.settings import DataSourceBatchByPermission, batch_update_by_permission

        mock_dao = MagicMock()
        mock_dao.batch_update.return_value = 1

        items = [
            {
                "source": "tushare",
                "item_key": "stock_daily",
                "permission_points": 120,
                "enabled": 0,
                "requires_permission": "0",
                "sync_supported": True,
            },
            {
                "source": "tushare",
                "item_key": "stock_company",
                "permission_points": 120,
                "enabled": 0,
                "requires_permission": "0",
                "sync_supported": False,
            },
            {
                "source": "tushare",
                "item_key": "rt_daily",
                "permission_points": 0,
                "enabled": 0,
                "requires_permission": "1",
                "sync_supported": True,
            },
            {
                "source": "tushare",
                "item_key": "stock_basic",
                "permission_points": 120,
                "enabled": 1,
                "requires_permission": "0",
                "sync_supported": True,
            },
        ]

        with patch("app.domains.market.dao.data_source_item_dao.DataSourceItemDao", return_value=mock_dao), \
             patch(f"{_ROUTES_MOD}._list_items_with_sync_support", return_value=items), \
             patch(f"{_ROUTES_MOD}._ensure_table_for_item") as mock_ensure, \
             patch(f"{_ROUTES_MOD}._trigger_sync_init") as mock_trigger:
            result = await batch_update_by_permission(
                body=DataSourceBatchByPermission(permission_points=120, enabled=True),
                source="tushare",
                current_user=MagicMock(),
            )

        assert result["updated"] == 1
        assert result["skipped_unsupported"] == [{"source": "tushare", "item_key": "stock_company"}]
        mock_dao.batch_update.assert_called_once_with(
            [{"source": "tushare", "item_key": "stock_daily", "enabled": True}]
        )
        mock_ensure.assert_called_once_with("tushare", "stock_daily")
        mock_trigger.assert_called_once_with("tushare", "stock_daily")

    @pytest.mark.anyio
    async def test_list_permission_levels_excludes_paid_and_unsupported(self):
        from app.api.routes.settings import list_permission_levels

        items = [
            {"permission_points": 2000, "requires_permission": "0", "sync_supported": True},
            {"permission_points": 120, "requires_permission": "0", "sync_supported": True},
            {"permission_points": 5000, "requires_permission": "0", "sync_supported": False},
            {"permission_points": 0, "requires_permission": "1", "sync_supported": True},
        ]

        with patch(f"{_ROUTES_MOD}._list_items_with_sync_support", return_value=items):
            result = await list_permission_levels(source="tushare", current_user=MagicMock())

        assert result == {"data": [120, 2000]}

    def test_get_sync_support_map_uses_tushare_token_points(self):
        from app.api.routes.settings import _get_sync_support_map

        registry = MagicMock()
        registry.get_interface.side_effect = lambda source, item_key: object() if item_key in {"stock_daily", "bak_daily", "stock_company"} else None
        mock_dao = MagicMock()
        mock_dao.list_all.return_value = [
            {"source": "tushare", "item_key": "stock_daily", "permission_points": 120, "api_name": "daily"},
            {"source": "tushare", "item_key": "stock_company", "permission_points": 120, "api_name": "stock_company"},
            {"source": "tushare", "item_key": "bak_daily", "permission_points": 5000, "api_name": "bak_daily"},
        ]

        with patch("app.datasync.registry.build_default_registry", return_value=registry), \
             patch("app.domains.market.dao.data_source_item_dao.DataSourceItemDao", return_value=mock_dao), \
             patch("app.datasync.capabilities.load_source_config_map", return_value={"tushare": {"config_json": {"token_points": 2000}}}):
            support_map = _get_sync_support_map("tushare")

        assert support_map == {("tushare", "stock_daily"), ("tushare", "stock_company")}

    def test_ensure_table_for_item_does_not_trust_table_created_flag(self):
        from app.api.routes.settings import _ensure_table_for_item

        mock_dao = MagicMock()
        mock_dao.get_by_key.return_value = {
            "source": "tushare",
            "item_key": "stock_daily",
            "table_created": 1,
            "target_database": "tushare",
            "target_table": "stock_daily",
        }
        registry = MagicMock()
        iface = MagicMock()
        iface.get_ddl.return_value = "CREATE TABLE stock_daily (...)"
        registry.get_interface.return_value = iface

        with patch("app.domains.market.dao.data_source_item_dao.DataSourceItemDao", return_value=mock_dao), \
             patch("app.datasync.registry.build_default_registry", return_value=registry), \
             patch("app.datasync.table_manager.ensure_table") as mock_ensure:
            _ensure_table_for_item("tushare", "stock_daily")

        mock_ensure.assert_called_once_with("tushare", "stock_daily", "CREATE TABLE stock_daily (...)")

    def test_trigger_sync_init_success(self):
        from app.api.routes.settings import _trigger_sync_init
        mock_queue = MagicMock()
        mock_job = MagicMock()
        mock_job.id = "job-123"
        mock_queue.enqueue.return_value = mock_job

        with patch("app.datasync.registry.build_default_registry", return_value=MagicMock()) as mock_registry, \
             patch("app.datasync.service.sync_init_service.reconcile_enabled_sync_status") as mock_init, \
             patch("app.worker.service.config.get_queue", return_value=mock_queue):
            result = _trigger_sync_init("tushare", "stock_daily")

        mock_init.assert_called_once_with(mock_registry.return_value, source="tushare", item_key="stock_daily")
        assert result == "job-123"

    def test_trigger_sync_init_failure(self):
        from app.api.routes.settings import _trigger_sync_init

        with patch("app.datasync.registry.build_default_registry", return_value=MagicMock()), \
             patch("app.datasync.service.sync_init_service.reconcile_enabled_sync_status", side_effect=Exception("DB error")):
            result = _trigger_sync_init("tushare", "stock_daily")

        assert result is None
