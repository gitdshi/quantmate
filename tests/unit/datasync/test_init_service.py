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
            assert _lookback_days() == 5 * 365

    def test_prod(self):
        from app.datasync.service.init_service import _lookback_days
        with patch(f"{_MOD}._get_env", return_value="prod"):
            assert _lookback_days() == 30 * 365

    def test_unknown_fallback(self):
        from app.datasync.service.init_service import _lookback_days
        with patch(f"{_MOD}._get_env", return_value="unknown"):
            assert _lookback_days() == 365


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
               patch(f"{_MOD}._reconcile_pending_records", return_value={"pending_records": 0, "items_reconciled": 0, "skipped_unsupported": []}):
            result = initialize(registry, run_backfill=False)
        assert "env" in result
        assert "tables_created" in result

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
               patch(f"{_MOD}._reconcile_pending_records", return_value={"pending_records": 0, "items_reconciled": 0, "skipped_unsupported": []}), \
             patch("app.datasync.service.sync_engine.backfill_retry", return_value={"step": {"status": "success"}}):
            result = initialize(registry, run_backfill=True)
        assert "backfill" in result


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
