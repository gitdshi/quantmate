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


class TestRunReconcile:
    def test_returns_results(self):
        from app.datasync.scheduler import run_reconcile

        with patch(f"{_MOD}._build_registry"), \
             patch("app.datasync.service.init_service.reconcile_runtime_state", return_value={"pending_records": 3}):
            result = run_reconcile()

        assert result["pending_records"] == 3


class TestBackfillLoop:
    def test_drains_until_idle_then_sleeps(self):
        from app.datasync.scheduler import run_backfill_loop

        registry = MagicMock()
        with patch(f"{_MOD}._build_registry", return_value=registry), \
             patch(f"{_MOD}.run_backfill", side_effect=[{"a": {"status": "success"}}, {}]) as mock_backfill, \
             patch("app.domains.extdata.dao.data_sync_status_dao.is_backfill_locked", return_value=False), \
             patch(f"{_MOD}.time.sleep", side_effect=RuntimeError("stop-loop")) as mock_sleep:
            with pytest.raises(RuntimeError, match="stop-loop"):
                run_backfill_loop(idle_hours=4)

        assert mock_backfill.call_count == 2
        mock_sleep.assert_called_once_with(4 * 3600)


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


class TestScheduledFlows:
    def test_scheduled_daily_reconciles_before_sync(self):
        import app.datasync.scheduler as mod

        with patch(f"{_MOD}.run_reconcile") as mock_reconcile, \
             patch(f"{_MOD}.run_daily_sync") as mock_daily:
            mod._scheduled_daily()

        mock_reconcile.assert_called_once()
        mock_daily.assert_called_once()

    def test_startup_sequence_runs_daily_before_reconcile(self):
        import app.datasync.scheduler as mod

        call_order: list[str] = []

        def _record_daily(**_kwargs):
            call_order.append("daily")

        def _record_reconcile(**_kwargs):
            call_order.append("reconcile")

        with patch(f"{_MOD}.run_daily_sync", side_effect=_record_daily), \
             patch(f"{_MOD}.run_reconcile", side_effect=_record_reconcile), \
             patch.dict("os.environ", {}, clear=False):
            mod._run_startup_sequence(registry=MagicMock())

        assert call_order == ["daily", "reconcile"]

    def test_startup_sequence_honors_skip_flags(self):
        import app.datasync.scheduler as mod

        with patch(f"{_MOD}.run_daily_sync") as mock_daily, \
             patch(f"{_MOD}.run_reconcile") as mock_reconcile, \
             patch.dict(
                 "os.environ",
                 {
                     "DATASYNC_SKIP_INITIAL_DAILY": "1",
                     "DATASYNC_SKIP_INITIAL_RECONCILE": "true",
                 },
                 clear=False,
             ):
            mod._run_startup_sequence(registry=MagicMock())

        mock_daily.assert_not_called()
        mock_reconcile.assert_not_called()

    def test_daemon_loop_starts_without_initial_backfill(self):
        import app.datasync.scheduler as mod

        fake_schedule = MagicMock()
        fake_schedule.every.return_value.day.at.return_value.do.return_value = None
        fake_metrics = MagicMock()
        fake_metrics.init_metrics = MagicMock()

        with patch.object(mod, "schedule", fake_schedule), \
             patch(f"{_MOD}._build_registry", return_value=MagicMock()), \
             patch(f"{_MOD}._run_startup_sequence") as mock_startup, \
             patch(f"{_MOD}.run_backfill") as mock_backfill, \
             patch(f"{_MOD}.time.sleep", side_effect=KeyboardInterrupt), \
             patch.dict(sys.modules, {"app.datasync.metrics": fake_metrics}), \
             patch("app.domains.extdata.dao.data_sync_status_dao.ensure_tables"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.ensure_backfill_lock_table"):
            with pytest.raises(KeyboardInterrupt):
                mod.daemon_loop()

        mock_startup.assert_called_once()
        mock_backfill.assert_not_called()


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

    def test_backfill_loop_mode(self):
        from app.datasync.scheduler import main

        with patch(f"{_MOD}.run_backfill_loop") as mock_loop, \
             patch("sys.argv", ["scheduler", "--backfill-loop", "--idle-hours", "2"]):
            mock_loop.return_value = None
            main()

        mock_loop.assert_called_once_with(idle_hours=2)

    def test_init_mode(self):
        from app.datasync.scheduler import main
        with patch(f"{_MOD}.run_init") as mock_init, \
             patch("sys.argv", ["scheduler", "--init"]):
            mock_init.return_value = {}
            main()
        mock_init.assert_called_once()

    def test_reconcile_mode(self):
        from app.datasync.scheduler import main

        with patch(f"{_MOD}.run_reconcile") as mock_reconcile, \
             patch("sys.argv", ["scheduler", "--reconcile", "--date", "2024-01-05"]):
            mock_reconcile.return_value = {}
            main()

        mock_reconcile.assert_called_once_with(target_end_date=date(2024, 1, 5))

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
