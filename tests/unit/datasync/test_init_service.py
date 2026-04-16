"""Unit tests for app.datasync.service.init_service."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

_MOD = "app.datasync.service.init_service"


def _engine_ctx():
    engine = MagicMock()
    conn = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value = ctx
    engine.connect.return_value = ctx
    return engine, conn


class TestGetEnv:
    def test_default_dev(self):
        from app.datasync.service.init_service import _get_env
        with patch.dict("os.environ", {}, clear=True):
            assert _get_env() == "dev"

    def test_from_app_env(self):
        from app.datasync.service.init_service import _get_env
        with patch.dict("os.environ", {"APP_ENV": "staging"}):
            assert _get_env() == "staging"

    def test_from_environment(self):
        from app.datasync.service.init_service import _get_env
        with patch.dict("os.environ", {"ENVIRONMENT": "prod"}, clear=True):
            assert _get_env() == "prod"


class TestLookbackDays:
    def test_dev(self):
        from app.datasync.service.init_service import _lookback_days
        with patch(f"{_MOD}._get_env", return_value="dev"):
            assert _lookback_days() == 365

    def test_staging(self):
        from app.datasync.service.init_service import _lookback_days
        with patch(f"{_MOD}._get_env", return_value="staging"):
            assert _lookback_days() == 10 * 365

    def test_prod(self):
        from app.datasync.service.init_service import _lookback_days
        with patch(f"{_MOD}._get_env", return_value="prod"):
            assert _lookback_days() == 20 * 365

    def test_env_specific_override(self):
        from app.datasync.service.init_service import _lookback_days
        with patch.dict("os.environ", {"DATASYNC_INIT_LOOKBACK_STAGING_DAYS": "1200"}, clear=True):
            assert _lookback_days("staging") == 1200

    def test_generic_override(self):
        from app.datasync.service.init_service import _lookback_days
        with patch.dict("os.environ", {"DATASYNC_INIT_LOOKBACK_DAYS": "900"}, clear=True):
            assert _lookback_days("prod") == 900

    def test_unknown_fallback(self):
        from app.datasync.service.init_service import _lookback_days
        with patch(f"{_MOD}._get_env", return_value="unknown"):
            assert _lookback_days() == 365


class TestCoverageWindow:
    def test_builds_window(self):
        from app.datasync.service.init_service import get_coverage_window

        target_end = date(2026, 4, 15)
        with patch(f"{_MOD}._get_env", return_value="prod"), \
             patch(f"{_MOD}._lookback_days", return_value=7300):
            result = get_coverage_window(target_end)

        assert result["env"] == "prod"
        assert result["lookback_days"] == 7300
        assert result["end_date"] == target_end
        assert result["start_date"] == date(2006, 4, 20)


class TestInitializationState:
    def test_detects_incomplete_init(self):
        from app.datasync.service.init_service import get_initialization_state

        engine, conn = _engine_ctx()
        conn.execute.side_effect = [MagicMock(fetchone=MagicMock(return_value=None))]

        with patch(f"{_MOD}.get_quantmate_engine", return_value=engine), \
             patch("app.datasync.cli.init_market_data.ensure_init_progress_table"), \
             patch(f"{_MOD}.ensure_sync_status_init_table"), \
             patch(f"{_MOD}._get_sync_status_coverage_state", return_value={
                 "window_start": date(2025, 4, 15),
                 "window_end": date(2026, 4, 15),
                 "trade_days_in_window": 244,
                 "enabled_sync_items": 15,
                 "missing_items": [{"source": "tushare", "item_key": "trade_cal"}],
                 "incomplete_items": [],
                 "unsupported_items": [],
             }):
            state = get_initialization_state()

        assert state["bootstrap_completed"] is False
        assert state["sync_status_initialized"] is False
        assert state["needs_initialization"] is True
        assert state["sync_status_missing_items"] == [{"source": "tushare", "item_key": "trade_cal"}]

    def test_detects_completed_init(self):
        from app.datasync.service.init_service import get_initialization_state

        engine, conn = _engine_ctx()
        conn.execute.side_effect = [MagicMock(fetchone=MagicMock(return_value=(1,)))]

        with patch(f"{_MOD}.get_quantmate_engine", return_value=engine), \
             patch("app.datasync.cli.init_market_data.ensure_init_progress_table"), \
             patch(f"{_MOD}.ensure_sync_status_init_table"), \
             patch(f"{_MOD}._get_sync_status_coverage_state", return_value={
                 "window_start": date(2025, 4, 15),
                 "window_end": date(2026, 4, 15),
                 "trade_days_in_window": 244,
                 "enabled_sync_items": 15,
                 "missing_items": [],
                 "incomplete_items": [],
                 "unsupported_items": [],
             }):
            state = get_initialization_state()

        assert state["bootstrap_completed"] is True
        assert state["sync_status_initialized"] is True
        assert state["needs_initialization"] is False
        assert state["enabled_sync_items"] == 15


class TestSyncStatusCoverageState:
    def test_detects_missing_enabled_item_coverage(self):
        from app.datasync.service.init_service import _get_sync_status_coverage_state

        engine, conn = _engine_ctx()
        conn.execute.side_effect = [
            MagicMock(fetchall=MagicMock(return_value=[("tushare", "trade_cal")])),
            MagicMock(fetchall=MagicMock(return_value=[])),
            MagicMock(fetchall=MagicMock(return_value=[])),
            MagicMock(fetchall=MagicMock(return_value=[])),
        ]

        registry = MagicMock()
        iface = MagicMock()
        iface.supports_backfill.return_value = True
        registry.get_interface.return_value = iface

        with patch(f"{_MOD}.get_quantmate_engine", return_value=engine), \
             patch("app.datasync.registry.build_default_registry", return_value=registry), \
             patch(
                 f"{_MOD}.get_coverage_window",
                 return_value={
                     "env": "dev",
                     "lookback_days": 365,
                     "start_date": date(2025, 4, 15),
                     "end_date": date(2026, 4, 15),
                 },
             ), \
             patch(
                 "app.domains.extdata.dao.data_sync_status_dao.get_cached_trade_dates",
                 return_value=[date(2026, 4, 15)],
             ):
            state = _get_sync_status_coverage_state()

        assert state["enabled_sync_items"] == 1
        assert state["missing_items"] == [{"source": "tushare", "item_key": "trade_cal"}]
        assert state["incomplete_items"] == []

    def test_detects_latest_only_item_missing_latest_status(self):
        from app.datasync.service.init_service import _get_sync_status_coverage_state

        engine, conn = _engine_ctx()
        conn.execute.side_effect = [
            MagicMock(fetchall=MagicMock(return_value=[("tushare", "stock_basic")])),
            MagicMock(fetchall=MagicMock(return_value=[("tushare", "stock_basic", date(2025, 4, 15), date(2026, 4, 14))])),
            MagicMock(fetchall=MagicMock(return_value=[])),
            MagicMock(fetchall=MagicMock(return_value=[])),
        ]

        registry = MagicMock()
        iface = MagicMock()
        iface.supports_backfill.return_value = False
        registry.get_interface.return_value = iface

        with patch(f"{_MOD}.get_quantmate_engine", return_value=engine), \
             patch("app.datasync.registry.build_default_registry", return_value=registry), \
             patch(
                 f"{_MOD}.get_coverage_window",
                 return_value={
                     "env": "dev",
                     "lookback_days": 365,
                     "start_date": date(2025, 4, 15),
                     "end_date": date(2026, 4, 15),
                 },
             ), \
             patch(
                 "app.domains.extdata.dao.data_sync_status_dao.get_cached_trade_dates",
                 return_value=[date(2026, 4, 15)],
             ):
            state = _get_sync_status_coverage_state()

        assert state["missing_items"] == []
        assert state["incomplete_items"] == [
            {
                "source": "tushare",
                "item_key": "stock_basic",
                "initialized_from": "2025-04-15",
                "initialized_to": "2026-04-14",
                "expected_rows": 1,
                "actual_rows": 0,
            }
        ]


class TestInitialize:
    def test_returns_dict(self):
        from app.datasync.service.init_service import initialize
        registry = MagicMock()
        registry.all_sources.return_value = []
        registry.all_interfaces.return_value = []

        engine, conn = _engine_ctx()
        conn.execute.return_value = MagicMock(
            fetchall=MagicMock(return_value=[]),
        )
        with patch(f"{_MOD}.get_quantmate_engine", return_value=engine), \
               patch(f"{_MOD}.ensure_tables"), \
               patch(f"{_MOD}.ensure_backfill_lock_table"), \
               patch(f"{_MOD}.ensure_sync_status_init_table"), \
               patch(f"{_MOD}.get_coverage_window", return_value={"env": "dev", "lookback_days": 365, "start_date": date(2025, 4, 15), "end_date": date(2026, 4, 15)}), \
               patch(f"{_MOD}._sync_registry_state", return_value={"items_normalized": 1, "tables_created": 2}), \
               patch(f"{_MOD}._ensure_trade_calendar_window", return_value=([date(2026, 4, 15)], True)), \
               patch(f"{_MOD}._reconcile_pending_records", return_value={"pending_records": 0, "items_reconciled": 0, "skipped_unsupported": []}):
            result = initialize(registry, run_backfill=False)
        assert "env" in result
        assert result["items_normalized"] == 1
        assert "tables_created" in result
        assert result["trade_calendar_refreshed"] is True

    def test_with_backfill(self):
        from app.datasync.service.init_service import initialize
        registry = MagicMock()
        registry.all_sources.return_value = []
        registry.all_interfaces.return_value = []

        engine, conn = _engine_ctx()
        conn.execute.return_value = MagicMock(
            fetchall=MagicMock(return_value=[]),
        )
        with patch(f"{_MOD}.get_quantmate_engine", return_value=engine), \
               patch(f"{_MOD}.ensure_tables"), \
               patch(f"{_MOD}.ensure_backfill_lock_table"), \
               patch(f"{_MOD}.ensure_sync_status_init_table"), \
             patch(f"{_MOD}.get_coverage_window", return_value={"env": "dev", "lookback_days": 365, "start_date": date(2025, 4, 15), "end_date": date(2026, 4, 15)}), \
             patch(f"{_MOD}._sync_registry_state", return_value={"items_normalized": 0, "tables_created": 0}), \
             patch(f"{_MOD}._ensure_trade_calendar_window", return_value=([date(2026, 4, 15)], False)), \
               patch(f"{_MOD}._reconcile_pending_records", return_value={"pending_records": 0, "items_reconciled": 0, "skipped_unsupported": []}), \
             patch("app.datasync.service.sync_engine.backfill_retry", return_value={"step": {"status": "success"}}):
            result = initialize(registry, run_backfill=True)
        assert "backfill" in result


class TestRuntimeReconcile:
    def test_reconciles_runtime_state(self):
        from app.datasync.service.init_service import reconcile_runtime_state

        registry = MagicMock()
        engine, conn = _engine_ctx()
        conn.execute.return_value = MagicMock(fetchall=MagicMock(return_value=[]))

        coverage_window = {
            "env": "staging",
            "lookback_days": 3650,
            "start_date": date(2016, 4, 17),
            "end_date": date(2026, 4, 15),
        }

        with patch(f"{_MOD}.get_quantmate_engine", return_value=engine), \
             patch(f"{_MOD}.ensure_tables"), \
             patch(f"{_MOD}.ensure_backfill_lock_table"), \
             patch(f"{_MOD}.ensure_sync_status_init_table"), \
             patch(f"{_MOD}.get_coverage_window", return_value=coverage_window), \
             patch(f"{_MOD}._sync_registry_state", return_value={"items_normalized": 0, "tables_created": 3}), \
             patch(f"{_MOD}._ensure_trade_calendar_window", return_value=([date(2026, 4, 15)], True)), \
             patch(f"{_MOD}._reconcile_pending_records", return_value={"pending_records": 4, "items_reconciled": 3, "skipped_unsupported": []}):
            result = reconcile_runtime_state(registry)

        assert result["env"] == "staging"
        assert result["tables_created"] == 3
        assert result["pending_records"] == 4
        assert result["trade_calendar_days"] == 1
        assert result["trade_calendar_refreshed"] is True


class TestSeedConfigs:
    def test_seeds(self):
        from app.datasync.service.init_service import _seed_configs
        engine, conn = _engine_ctx()
        registry = MagicMock()
        src = MagicMock()
        src.source_key = "tushare"
        src.display_name = "Tushare"
        src.requires_token = True
        registry.all_sources.return_value = [src]
        _seed_configs(engine, registry)
        conn.execute.assert_called()


class TestSeedItems:
    def test_seeds(self):
        from app.datasync.service.init_service import _seed_items
        engine, conn = _engine_ctx()
        registry = MagicMock()
        iface = MagicMock()
        iface.info.source_key = "tushare"
        iface.info.interface_key = "stock_daily"
        iface.info.display_name = "Stock Daily"
        iface.info.target_database = "tushare"
        iface.info.target_table = "stock_daily"
        iface.info.sync_priority = 10
        iface.info.enabled_by_default = True
        iface.info.description = "Daily OHLCV"
        iface.info.requires_permission = ""
        registry.all_interfaces.return_value = [iface]
        _seed_items(engine, registry)
        conn.execute.assert_called()


class TestNormalizeItemTargets:
    def test_normalizes_mismatched_target_database(self):
        from app.datasync.service.init_service import _normalize_item_targets

        engine, conn = _engine_ctx()
        execute_result = MagicMock()
        execute_result.rowcount = 2
        conn.execute.return_value = execute_result

        normalized = _normalize_item_targets(engine)

        assert normalized == 2
        sql = conn.execute.call_args.args[0].text
        assert "SET target_database = source" in sql
        assert "WHERE target_database <> source" in sql


class TestEnsureTables:
    def test_creates_tables(self):
        from app.datasync.service.init_service import _ensure_tables
        engine, conn = _engine_ctx()
        rows = [("tushare", "stock_daily", "ts_db", "stock_daily")]
        conn.execute.return_value = MagicMock(fetchall=MagicMock(return_value=rows))

        registry = MagicMock()
        iface = MagicMock()
        iface.get_ddl.return_value = "CREATE TABLE..."
        registry.get_interface.return_value = iface

        with patch(f"{_MOD}.ensure_table", return_value=True) as mock_ensure:
            created = _ensure_tables(engine, registry)
        assert created == 1
        mock_ensure.assert_called_once()


class TestGeneratePendingRecords:
    def test_generates_records(self):
        from app.datasync.service.init_service import _generate_pending_records
        engine, conn = _engine_ctx()
        registry = MagicMock()
        with patch(f"{_MOD}._reconcile_pending_records", return_value={"pending_records": 5, "items_reconciled": 1, "skipped_unsupported": []}):
            count = _generate_pending_records(engine, registry)
        assert count == 5
