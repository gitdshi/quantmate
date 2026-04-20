"""Unit tests for app.datasync.service.init_service."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch


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


class TestWindowRules:
    def test_dev_window_years(self):
        from app.datasync.service.init_service import _get_env_window_years

        with patch(f"{_MOD}._get_env", return_value="dev"):
            assert _get_env_window_years() == 1

    def test_staging_window_years(self):
        from app.datasync.service.init_service import _get_env_window_years

        with patch(f"{_MOD}._get_env", return_value="staging"):
            assert _get_env_window_years() == 10

    def test_prod_window_years(self):
        from app.datasync.service.init_service import _get_env_window_years

        with patch(f"{_MOD}._get_env", return_value="prod"):
            assert _get_env_window_years() == 20

    def test_unknown_env_falls_back_to_dev_window(self):
        from app.datasync.service.init_service import _get_env_window_years

        with patch(f"{_MOD}._get_env", return_value="unknown"):
            assert _get_env_window_years() == 1

    def test_configured_sync_start_date_uses_today_when_blank(self):
        from app.datasync.service.init_service import _get_configured_sync_start_date

        with patch(f"{_MOD}.get_runtime_str", return_value=""):
            assert _get_configured_sync_start_date(date(2026, 4, 17)) is None

    def test_configured_sync_start_date_uses_explicit_date(self):
        from app.datasync.service.init_service import _get_configured_sync_start_date

        with patch(f"{_MOD}.get_runtime_str", return_value="2010-01-01"):
            assert _get_configured_sync_start_date(date(2026, 4, 17)) == date(2010, 1, 1)

    def test_configured_sync_start_date_ignores_invalid_date(self):
        from app.datasync.service.init_service import _get_configured_sync_start_date

        with patch(f"{_MOD}.get_runtime_str", return_value="not-a-date"):
            assert _get_configured_sync_start_date(date(2026, 4, 17)) is None


class TestCoverageWindow:
    def test_builds_window(self):
        from app.datasync.service.init_service import get_coverage_window

        target_end = date(2026, 4, 15)
        with patch(f"{_MOD}._get_env", return_value="prod"), \
             patch(f"{_MOD}._get_env_window_years", return_value=20), \
             patch(f"{_MOD}._get_configured_sync_start_date", return_value=date(2026, 4, 15)), \
             patch(f"{_MOD}._get_env_floor_start_date", return_value=date(2006, 4, 20)):
            result = get_coverage_window(target_end)

        assert result["env"] == "prod"
        assert result["window_years"] == 20
        assert result["configured_start_date"] == date(2026, 4, 15)
        assert result["env_floor_start_date"] == date(2006, 4, 20)
        assert result["end_date"] == target_end
        assert result["start_date"] == date(2026, 4, 15)

    def test_uses_env_floor_when_sync_start_date_is_blank(self):
        from app.datasync.service.init_service import get_coverage_window

        target_end = date(2026, 4, 15)
        with patch(f"{_MOD}._get_env", return_value="staging"), \
             patch(f"{_MOD}._get_env_window_years", return_value=10), \
             patch(f"{_MOD}._get_configured_sync_start_date", return_value=None), \
             patch(f"{_MOD}._get_env_floor_start_date", return_value=date(2016, 4, 17)):
            result = get_coverage_window(target_end)

        assert result["env"] == "staging"
        assert result["window_years"] == 10
        assert result["configured_start_date"] is None
        assert result["env_floor_start_date"] == date(2016, 4, 17)
        assert result["start_date"] == date(2016, 4, 17)
        assert result["end_date"] == target_end


class TestDataSourceItemMetadataCompatibility:
    def test_falls_back_to_legacy_enabled_item_query_on_missing_columns(self):
        from app.datasync.service.init_service import _fetch_enabled_item_metadata_rows

        conn = MagicMock()
        conn.execute.side_effect = [
            Exception("(1054, \"Unknown column 'dsi.api_name' in 'field list'\")"),
            MagicMock(fetchall=MagicMock(return_value=[("tushare", "stock_daily", "stock_daily", 0, "0")])),
        ]

        rows = _fetch_enabled_item_metadata_rows(conn)

        assert rows == [("tushare", "stock_daily", "stock_daily", 0, "0")]

    def test_falls_back_to_legacy_tushare_query_on_missing_columns(self):
        from app.datasync.service.init_service import _fetch_tushare_item_metadata_rows

        conn = MagicMock()
        conn.execute.side_effect = [
            Exception("(1054, \"Unknown column 'api_name' in 'field list'\")"),
            MagicMock(fetchall=MagicMock(return_value=[("tushare", "stock_daily", 1, "stock_daily", 0, "0")])),
        ]

        rows = _fetch_tushare_item_metadata_rows(conn)

        assert rows == [("tushare", "stock_daily", 1, "stock_daily", 0, "0")]


class TestReconcileBounds:
    def test_get_source_initialized_bounds_uses_enabled_items_only(self):
        from app.datasync.service.sync_init_service import _get_source_initialized_bounds

        engine, conn = _engine_ctx()
        conn.execute.return_value = MagicMock(fetchone=MagicMock(return_value=(date(2024, 1, 1), date(2024, 1, 31))))

        with patch("app.datasync.service.sync_init_service.get_quantmate_engine", return_value=engine):
            result = _get_source_initialized_bounds("tushare")

        assert result == (date(2024, 1, 1), date(2024, 1, 31))

    def test_reconcile_enabled_sync_status_inherits_source_window_for_new_item(self):
        from app.datasync.service.sync_init_service import reconcile_enabled_sync_status

        engine, conn = _engine_ctx()
        conn.execute.return_value = MagicMock(
            fetchall=MagicMock(return_value=[("tushare", "daily", "daily", 0, None)]),
        )

        registry = MagicMock()
        iface = MagicMock()
        iface.supports_backfill.return_value = True
        registry.get_interface.return_value = iface

        with patch("app.datasync.service.sync_init_service.get_quantmate_engine", return_value=engine), \
             patch("app.datasync.capabilities.load_source_config_map", return_value={}), \
             patch("app.datasync.capabilities.is_item_sync_supported", return_value=True), \
             patch("app.datasync.service.sync_init_service._get_initialized_bounds", return_value=None), \
             patch(
                 "app.datasync.service.sync_init_service._get_source_initialized_bounds",
                 return_value=(date(2024, 1, 1), date(2024, 1, 31)),
             ), \
             patch("app.datasync.service.sync_init_service.initialize_sync_status", return_value=23) as init_mock:
            result = reconcile_enabled_sync_status(registry, source="tushare", item_key="daily")

        init_mock.assert_called_once_with(
            "tushare",
            "daily",
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31),
            reconcile_missing=True,
            use_trade_calendar=True,
        )
        assert result["pending_records"] == 23
        assert result["item_results"] == [
            {
                "source": "tushare",
                "item_key": "daily",
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
                "pending_records": 23,
                "supports_backfill": True,
                "inherited_bounds": True,
            }
        ]


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
                     "window_years": 1,
                     "configured_start_date": date(2026, 4, 15),
                     "env_floor_start_date": date(2025, 4, 15),
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

    def test_marks_item_unsupported_when_token_points_are_insufficient(self):
        from app.datasync.service.init_service import _get_sync_status_coverage_state

        engine, conn = _engine_ctx()
        conn.execute.side_effect = [
            MagicMock(fetchall=MagicMock(return_value=[("tushare", "bak_daily", "bak_daily", 5000, None)])),
            MagicMock(fetchall=MagicMock(return_value=[])),
            MagicMock(fetchall=MagicMock(return_value=[])),
            MagicMock(fetchall=MagicMock(return_value=[])),
        ]

        registry = MagicMock()
        registry.get_interface.return_value = MagicMock()

        with patch(f"{_MOD}.get_quantmate_engine", return_value=engine), \
             patch("app.datasync.registry.build_default_registry", return_value=registry), \
             patch("app.datasync.capabilities.load_source_config_map", return_value={"tushare": {"config_json": {"token_points": 2000}}}), \
             patch(
                 f"{_MOD}.get_coverage_window",
                 return_value={
                     "env": "dev",
                     "window_years": 1,
                     "configured_start_date": date(2026, 4, 15),
                     "env_floor_start_date": date(2025, 4, 15),
                     "start_date": date(2025, 4, 15),
                     "end_date": date(2026, 4, 15),
                 },
             ), \
             patch(
                 "app.domains.extdata.dao.data_sync_status_dao.get_cached_trade_dates",
                 return_value=[date(2026, 4, 15)],
             ):
            state = _get_sync_status_coverage_state()

        assert state["enabled_sync_items"] == 0
        assert state["missing_items"] == []
        assert state["unsupported_items"] == [{"source": "tushare", "item_key": "bak_daily"}]

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
                     "window_years": 1,
                     "configured_start_date": date(2026, 4, 15),
                     "env_floor_start_date": date(2025, 4, 15),
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
               patch(f"{_MOD}.get_coverage_window", return_value={"env": "dev", "window_years": 1, "configured_start_date": date(2026, 4, 15), "env_floor_start_date": date(2025, 4, 15), "start_date": date(2025, 4, 15), "end_date": date(2026, 4, 15)}), \
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
                         patch(f"{_MOD}.get_coverage_window", return_value={"env": "dev", "window_years": 1, "configured_start_date": date(2026, 4, 15), "env_floor_start_date": date(2025, 4, 15), "start_date": date(2025, 4, 15), "end_date": date(2026, 4, 15)}), \
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
            "window_years": 10,
            "configured_start_date": date(2026, 4, 15),
            "env_floor_start_date": date(2016, 4, 17),
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


class TestBootstrapItemEnablement:
    def test_updates_tushare_enabled_flags_from_capability_config(self):
        from app.datasync.service.init_service import _sync_bootstrap_item_enablement

        engine, conn = _engine_ctx()
        conn.execute.side_effect = [
            MagicMock(fetchone=MagicMock(return_value=None)),
            MagicMock(
                fetchall=MagicMock(
                    return_value=[
                        ("tushare", "stock_daily", 0, "daily", 120, None),
                        ("tushare", "bak_daily", 1, "bak_daily", 5000, None),
                        ("tushare", "rt_daily", 1, "rt_daily", 0, "1"),
                    ]
                )
            ),
            MagicMock(),
        ]

        registry = MagicMock()
        registry.get_interface.side_effect = lambda source, item_key: object() if source == "tushare" else None

        with patch.dict("os.environ", {}, clear=True), \
             patch(
                 "app.datasync.capabilities.load_source_config_map",
                 return_value={"tushare": {"config_json": {"token_points": 2000}}},
             ):
            updated = _sync_bootstrap_item_enablement(engine, registry)

        assert updated == 3
        update_sql = conn.execute.call_args_list[-1].args[0].text
        update_params = conn.execute.call_args_list[-1].args[1]

        assert "SET enabled = :enabled" in update_sql
        assert {tuple(sorted(item.items())) for item in update_params} == {
            tuple(sorted({"source": "tushare", "item_key": "stock_daily", "enabled": 1}.items())),
            tuple(sorted({"source": "tushare", "item_key": "bak_daily", "enabled": 0}.items())),
            tuple(sorted({"source": "tushare", "item_key": "rt_daily", "enabled": 0}.items())),
        }

    def test_skips_after_bootstrap_completion(self):
        from app.datasync.service.init_service import _sync_bootstrap_item_enablement

        engine, conn = _engine_ctx()
        conn.execute.return_value = MagicMock(fetchone=MagicMock(return_value=(1,)))

        updated = _sync_bootstrap_item_enablement(engine, MagicMock())

        assert updated == 0
        assert conn.execute.call_count == 1


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
    def test_creates_bootstrap_tushare_tables_and_all_non_tushare_tables(self):
        from app.datasync.service.init_service import _ensure_tables
        engine, _ = _engine_ctx()

        registry = MagicMock()
        first = MagicMock()
        first.info.source_key = "tushare"
        first.info.target_database = "tushare"
        first.info.target_table = "stock_daily"
        first.get_ddl.return_value = "CREATE TABLE stock_daily (...)"

        duplicate = MagicMock()
        duplicate.info.source_key = "tushare"
        duplicate.info.target_database = "tushare"
        duplicate.info.target_table = "stock_daily"
        duplicate.get_ddl.return_value = "CREATE TABLE stock_daily (...)"

        second = MagicMock()
        second.info.source_key = "tushare"
        second.info.target_database = "tushare"
        second.info.target_table = "daily_basic"
        second.get_ddl.return_value = "CREATE TABLE daily_basic (...)"

        third = MagicMock()
        third.info.source_key = "akshare"
        third.info.target_database = "akshare"
        third.info.target_table = "index_daily"
        third.get_ddl.return_value = "CREATE TABLE index_daily (...)"

        registry.all_interfaces.return_value = [first, duplicate, second, third]

        with patch(f"{_MOD}.ensure_table", return_value=True) as mock_ensure:
            created = _ensure_tables(engine, registry)
        assert created == 2
        assert mock_ensure.call_count == 2
        mock_ensure.assert_any_call("tushare", "stock_daily", "CREATE TABLE stock_daily (...)")
        mock_ensure.assert_any_call("akshare", "index_daily", "CREATE TABLE index_daily (...)")


class TestSyncRegistryState:
    def test_syncs_bootstrap_enablement_before_table_creation(self):
        from app.datasync.service.init_service import _sync_registry_state

        engine = MagicMock()
        registry = MagicMock()

        with patch(f"{_MOD}._seed_configs"), \
             patch(f"{_MOD}._seed_items"), \
             patch(f"{_MOD}._sync_bootstrap_item_enablement", return_value=4), \
             patch(f"{_MOD}._normalize_item_targets", return_value=1), \
             patch(f"{_MOD}._ensure_tables", return_value=2):
            result = _sync_registry_state(engine, registry)

        assert result == {
            "bootstrap_item_enablement_updates": 4,
            "items_normalized": 1,
            "tables_created": 2,
        }


class TestGeneratePendingRecords:
    def test_generates_records(self):
        from app.datasync.service.init_service import _generate_pending_records
        engine, conn = _engine_ctx()
        registry = MagicMock()
        with patch(f"{_MOD}._reconcile_pending_records", return_value={"pending_records": 5, "items_reconciled": 1, "skipped_unsupported": []}):
            count = _generate_pending_records(engine, registry)
        assert count == 5
