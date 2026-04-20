"""Coverage batch for data_sync_daemon additional functions and scheduler.

Targets ~180 miss across:
  - data_sync_daemon.py  148 miss (untested functions)
  - scheduler.py  32 miss
"""
from __future__ import annotations

from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


# Module shorthand
_M = "app.datasync.service.data_sync_daemon"


# ═══════════════════════════════════════════════════════════════════════
# data_sync_daemon.py — untested functions
# ═══════════════════════════════════════════════════════════════════════

@pytest.fixture(autouse=True)
def _daemon_fixtures(monkeypatch):
    """Stub out all DB / audit / status helpers used by daemon functions."""
    import app.datasync.service.data_sync_daemon as mod

    monkeypatch.setattr(mod, "write_step_status", lambda *a, **kw: None)
    monkeypatch.setattr(mod, "get_step_status", lambda *a, **kw: None)
    monkeypatch.setattr(mod, "ensure_tables", lambda: None)

    # stub legacy sync-log helpers
    monkeypatch.setattr(mod, "write_sync_log", lambda *a, **kw: None)
    monkeypatch.setattr(mod, "get_last_success_date", lambda *a, **kw: None)

    # stub DB-hitting helpers used inside steps
    monkeypatch.setattr(mod, "get_all_ts_codes", lambda: ["000001.SZ", "000002.SZ"])
    monkeypatch.setattr(mod, "get_stock_basic_count", lambda: 5000)
    monkeypatch.setattr(mod, "get_adj_factor_count_for_date", lambda d: 5000)
    monkeypatch.setattr(mod, "get_cached_trade_dates", lambda *a, **kw: [])
    monkeypatch.setattr(mod, "upsert_trade_dates", lambda *a, **kw: None)
    monkeypatch.setattr(mod, "truncate_trade_cal", lambda: None)
    monkeypatch.setattr(mod, "get_failed_steps", lambda *a, **kw: [])
    monkeypatch.setattr(mod, "is_backfill_locked", lambda: False)
    monkeypatch.setattr(mod, "acquire_backfill_lock", lambda: True)
    monkeypatch.setattr(mod, "release_backfill_lock", lambda: None)


class TestTradeCalendar:
    """get_trade_calendar, get_previous_trade_date, refresh_trade_calendar"""

    def test_get_trade_calendar_weekday_fallback(self):
        """When cache empty and akshare unavailable, fall back to weekdays."""
        from app.datasync.service.data_sync_daemon import get_trade_calendar
        dates = get_trade_calendar(date(2024, 1, 1), date(2024, 1, 7))
        # Should return weekdays (Jan 1 Mon–5 Fri = 5 days)
        assert len(dates) >= 1
        assert all(isinstance(d, date) for d in dates)

    @patch(f"{_M}.ak")
    def test_get_trade_calendar_from_akshare(self, mock_ak):
        """When AkShare available, fetch from API and cache."""
        import app.datasync.service.data_sync_daemon as mod
        mod.AKSHARE_AVAILABLE = True
        mock_ak.tool_trade_date_hist_sina.return_value = pd.DataFrame({
            "trade_date": [datetime(2024, 1, 2), datetime(2024, 1, 3), datetime(2024, 1, 4)],
        })
        dates = mod.get_trade_calendar(date(2024, 1, 1), date(2024, 1, 5))
        assert len(dates) >= 1

    @patch(f"{_M}.get_trade_calendar")
    def test_get_previous_trade_date(self, cal):
        from app.datasync.service.data_sync_daemon import get_previous_trade_date
        cal.return_value = [date(2024, 6, 3), date(2024, 6, 4), date(2024, 6, 5)]
        d = get_previous_trade_date(offset=1)
        assert d == date(2024, 6, 5)

    @patch(f"{_M}.get_trade_calendar")
    def test_get_previous_trade_date_short_list(self, cal):
        from app.datasync.service.data_sync_daemon import get_previous_trade_date
        cal.return_value = [date(2024, 6, 3)]
        d = get_previous_trade_date(offset=5)
        assert d == date(2024, 6, 3)

    @patch(f"{_M}.ak")
    def test_refresh_trade_calendar(self, mock_ak):
        import app.datasync.service.data_sync_daemon as mod
        mod.AKSHARE_AVAILABLE = True
        mock_ak.tool_trade_date_hist_sina.return_value = pd.DataFrame({
            "trade_date": [datetime(2024, 1, 2), datetime(2024, 1, 3)],
        })
        mod.refresh_trade_calendar()
        mock_ak.tool_trade_date_hist_sina.assert_called_once()


class TestSyncSteps:
    """run_*_step functions."""

    @patch(f"{_M}.ak_ingest_index_daily")
    def test_run_akshare_index_step_success(self, ingest):
        from app.datasync.service.data_sync_daemon import run_akshare_index_step, SyncStatus
        ingest.return_value = None  # success, no exception
        status, rows, err = run_akshare_index_step(date(2024, 1, 15))
        assert status == SyncStatus.SUCCESS
        assert rows >= 0

    @patch(f"{_M}.ak_ingest_index_daily")
    def test_run_akshare_index_step_all_fail(self, ingest):
        from app.datasync.service.data_sync_daemon import run_akshare_index_step, SyncStatus
        ingest.side_effect = RuntimeError("API fail")
        status, rows, err = run_akshare_index_step(date(2024, 1, 15))
        assert status == SyncStatus.ERROR

    @patch(f"{_M}.ingest_stock_basic")
    def test_run_tushare_stock_basic_step_ok(self, ingest):
        from app.datasync.service.data_sync_daemon import run_tushare_stock_basic_step, SyncStatus
        ingest.return_value = None
        status, rows, err = run_tushare_stock_basic_step(date(2024, 1, 15))
        assert status == SyncStatus.SUCCESS
        assert rows == 5000  # from fixture stub

    @patch(f"{_M}.ingest_stock_basic")
    def test_run_tushare_stock_basic_step_fail(self, ingest):
        from app.datasync.service.data_sync_daemon import run_tushare_stock_basic_step, SyncStatus
        ingest.side_effect = RuntimeError("fail")
        status, rows, err = run_tushare_stock_basic_step(date(2024, 1, 15))
        assert status == SyncStatus.ERROR

    @patch(f"{_M}.upsert_daily")
    @patch(f"{_M}.call_pro")
    def test_run_tushare_stock_daily_step_ok(self, pro, upsert):
        from app.datasync.service.data_sync_daemon import run_tushare_stock_daily_step, SyncStatus
        pro.return_value = pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20240115"]})
        upsert.return_value = 1
        status, rows, err = run_tushare_stock_daily_step(date(2024, 1, 15))
        assert status == SyncStatus.SUCCESS

    @patch(f"{_M}.call_pro")
    def test_run_tushare_stock_daily_step_fail(self, pro):
        from app.datasync.service.data_sync_daemon import run_tushare_stock_daily_step, SyncStatus
        pro.side_effect = RuntimeError("fail")
        status, rows, err = run_tushare_stock_daily_step(date(2024, 1, 15))
        assert status == SyncStatus.ERROR

    @patch(f"{_M}.call_pro")
    def test_run_tushare_stock_daily_step_empty(self, pro):
        from app.datasync.service.data_sync_daemon import run_tushare_stock_daily_step, SyncStatus
        pro.return_value = pd.DataFrame()
        status, rows, err = run_tushare_stock_daily_step(date(2024, 1, 15))
        assert status == SyncStatus.SUCCESS
        assert rows == 0

    @patch(f"{_M}.ingest_adj_factor")
    def test_run_tushare_adj_factor_step_ok(self, ingest):
        from app.datasync.service.data_sync_daemon import run_tushare_adj_factor_step, SyncStatus
        ingest.return_value = None
        status, rows, err = run_tushare_adj_factor_step(date(2024, 1, 15))
        assert status == SyncStatus.SUCCESS

    @patch(f"{_M}.ingest_adj_factor")
    def test_run_tushare_adj_factor_step_fail(self, ingest):
        from app.datasync.service.data_sync_daemon import run_tushare_adj_factor_step, SyncStatus
        ingest.side_effect = RuntimeError("fail")
        status, rows, err = run_tushare_adj_factor_step(date(2024, 1, 15))
        assert status == SyncStatus.ERROR

    @patch(f"{_M}.ingest_dividend")
    def test_run_tushare_dividend_step_ok(self, ingest):
        from app.datasync.service.data_sync_daemon import run_tushare_dividend_step, SyncStatus
        ingest.return_value = None
        status, rows, err = run_tushare_dividend_step(date(2024, 1, 15))
        assert status == SyncStatus.SUCCESS

    @patch(f"{_M}.upsert_dividend_df")
    @patch(f"{_M}.call_pro")
    def test_run_tushare_dividend_step_batch(self, pro, upsert):
        from app.datasync.service.data_sync_daemon import run_tushare_dividend_step, SyncStatus
        pro.return_value = pd.DataFrame({"ts_code": ["000001.SZ"]})
        upsert.return_value = 1
        status, rows, err = run_tushare_dividend_step(date(2024, 1, 15), use_batch=True)
        assert status == SyncStatus.SUCCESS

    @patch(f"{_M}.ingest_top10_holders")
    def test_run_tushare_top10_holders_step_ok(self, ingest):
        from app.datasync.service.data_sync_daemon import run_tushare_top10_holders_step, SyncStatus
        ingest.return_value = None
        status, rows, err = run_tushare_top10_holders_step(date(2024, 1, 15))
        assert status == SyncStatus.SUCCESS

    @patch(f"{_M}.sync_date_to_vnpy")
    def test_run_vnpy_sync_step_ok(self, sync):
        from app.datasync.service.data_sync_daemon import run_vnpy_sync_step, SyncStatus
        sync.return_value = (10, 100)
        status, rows, err = run_vnpy_sync_step(date(2024, 1, 15))
        assert status == SyncStatus.SUCCESS

    @patch(f"{_M}.sync_date_to_vnpy")
    def test_run_vnpy_sync_step_fail(self, sync):
        from app.datasync.service.data_sync_daemon import run_vnpy_sync_step, SyncStatus
        sync.side_effect = RuntimeError("fail")
        status, rows, err = run_vnpy_sync_step(date(2024, 1, 15))
        assert status == SyncStatus.ERROR

    @patch(f"{_M}.ingest_index_daily")
    def test_run_tushare_index_daily_step_ok(self, ingest):
        from app.datasync.service.data_sync_daemon import run_tushare_index_daily_step, SyncStatus
        ingest.return_value = 5
        status, rows, err = run_tushare_index_daily_step(date(2024, 1, 15))
        assert status == SyncStatus.SUCCESS

    @patch(f"{_M}.ingest_index_daily")
    def test_run_tushare_index_daily_step_error(self, ingest):
        from app.datasync.service.data_sync_daemon import run_tushare_index_daily_step, SyncStatus
        ingest.side_effect = RuntimeError("fail")
        status, rows, err = run_tushare_index_daily_step(date(2024, 1, 15))
        assert status == SyncStatus.ERROR

    @patch(f"{_M}.ingest_weekly")
    def test_run_tushare_stock_weekly_step_ok(self, ingest):
        from app.datasync.service.data_sync_daemon import run_tushare_stock_weekly_step, SyncStatus
        ingest.return_value = 10
        status, rows, err = run_tushare_stock_weekly_step(date(2024, 1, 15))
        assert status == SyncStatus.SUCCESS

    @patch(f"{_M}.ingest_monthly")
    def test_run_tushare_stock_monthly_step_ok(self, ingest):
        from app.datasync.service.data_sync_daemon import run_tushare_stock_monthly_step, SyncStatus
        ingest.return_value = 10
        status, rows, err = run_tushare_stock_monthly_step(date(2024, 1, 15))
        assert status == SyncStatus.SUCCESS

    @patch(f"{_M}.ingest_index_weekly")
    def test_run_tushare_index_weekly_step_ok(self, ingest):
        from app.datasync.service.data_sync_daemon import run_tushare_index_weekly_step, SyncStatus
        ingest.return_value = 5
        status, rows, err = run_tushare_index_weekly_step(date(2024, 1, 15))
        assert status == SyncStatus.SUCCESS


class TestDailyIngest:
    """daily_ingest orchestration."""

    @patch(f"{_M}.run_vnpy_sync_step")
    @patch(f"{_M}.run_tushare_index_weekly_step")
    @patch(f"{_M}.run_tushare_index_daily_step")
    @patch(f"{_M}.run_tushare_stock_monthly_step")
    @patch(f"{_M}.run_tushare_stock_weekly_step")
    @patch(f"{_M}.run_tushare_top10_holders_step")
    @patch(f"{_M}.run_tushare_dividend_step")
    @patch(f"{_M}.run_tushare_adj_factor_step")
    @patch(f"{_M}.run_tushare_stock_daily_step")
    @patch(f"{_M}.run_tushare_stock_basic_step")
    @patch(f"{_M}.run_akshare_index_step")
    def test_daily_ingest_success(self, ak_step, basic, daily, adj, div,
                                   top10, weekly, monthly, idx_d, idx_w, vnpy):
        from app.datasync.service.data_sync_daemon import daily_ingest, SyncStatus
        for fn in [ak_step, basic, daily, adj, div, top10, weekly, monthly, idx_d, idx_w, vnpy]:
            fn.return_value = (SyncStatus.SUCCESS, 10, None)
        r = daily_ingest(date(2024, 1, 15))
        assert isinstance(r, dict)

    @patch(f"{_M}.run_vnpy_sync_step")
    @patch(f"{_M}.run_tushare_index_weekly_step")
    @patch(f"{_M}.run_tushare_index_daily_step")
    @patch(f"{_M}.run_tushare_stock_monthly_step")
    @patch(f"{_M}.run_tushare_stock_weekly_step")
    @patch(f"{_M}.run_tushare_top10_holders_step")
    @patch(f"{_M}.run_tushare_dividend_step")
    @patch(f"{_M}.run_tushare_adj_factor_step")
    @patch(f"{_M}.run_tushare_stock_daily_step")
    @patch(f"{_M}.run_tushare_stock_basic_step")
    @patch(f"{_M}.run_akshare_index_step")
    def test_daily_ingest_with_error(self, ak_step, basic, daily, adj, div,
                                      top10, weekly, monthly, idx_d, idx_w, vnpy):
        from app.datasync.service.data_sync_daemon import daily_ingest, SyncStatus
        ak_step.return_value = (SyncStatus.ERROR, 0, "API error")
        for fn in [basic, daily, adj, div, top10, weekly, monthly, idx_d, idx_w, vnpy]:
            fn.return_value = (SyncStatus.SUCCESS, 10, None)
        r = daily_ingest(date(2024, 1, 15), continue_on_error=True)
        assert isinstance(r, dict)


class TestLegacySync:
    """sync_daily_for_date, run_sync_for_date, get_trade_days."""

    @patch(f"{_M}.ingest_daily")
    def test_sync_daily_for_date(self, ingest):
        from app.datasync.service.data_sync_daemon import sync_daily_for_date
        ingest.return_value = None
        sync_daily_for_date(date(2024, 1, 15))
        assert ingest.call_count == 2  # 2 ts_codes from fixture

    @patch(f"{_M}.sync_daily_for_date")
    def test_run_sync_for_date_daily(self, sync_fn):
        from app.datasync.service.data_sync_daemon import run_sync_for_date
        run_sync_for_date(date(2024, 1, 15), allowed_endpoints=["daily"])
        sync_fn.assert_called_once()

    @patch(f"{_M}.ingest_repo")
    def test_run_sync_for_date_repo(self, repo_fn):
        import app.datasync.service.data_sync_daemon as mod
        mod.DRY_RUN = False
        from app.datasync.service.data_sync_daemon import run_sync_for_date
        run_sync_for_date(date(2024, 1, 15), allowed_endpoints=["repo"])
        repo_fn.assert_called_once()

    @patch(f"{_M}.call_pro")
    def test_get_trade_days(self, pro):
        from app.datasync.service.data_sync_daemon import get_trade_days
        pro.return_value = pd.DataFrame({
            "cal_date": ["20240102", "20240103"],
            "is_open": [1, 1]
        })
        r = get_trade_days(date(2024, 1, 1), date(2024, 1, 5))
        assert len(r) == 2

    @patch(f"{_M}.call_pro")
    def test_get_trade_days_fallback(self, pro):
        from app.datasync.service.data_sync_daemon import get_trade_days
        pro.side_effect = RuntimeError("no api")
        r = get_trade_days(date(2024, 1, 1), date(2024, 1, 5))
        assert len(r) >= 1  # weekday fallback


class TestMissingDataBackfill:
    """missing_data_backfill, group_dates_by_month."""

    def test_group_dates_by_month(self):
        from app.datasync.service.data_sync_daemon import group_dates_by_month
        dates = [date(2024, 1, 5), date(2024, 1, 10), date(2024, 2, 3)]
        groups = group_dates_by_month(dates)
        assert len(groups) >= 1

    def test_group_dates_by_month_empty(self):
        from app.datasync.service.data_sync_daemon import group_dates_by_month
        assert group_dates_by_month([]) == []

    def test_missing_data_backfill_no_failures(self):
        """With no failed steps, backfill should do nothing."""
        from app.datasync.service.data_sync_daemon import missing_data_backfill
        missing_data_backfill(lookback_days=7)  # get_failed_steps returns []


class TestDaemonMainFunctions:
    """run_daily_job, run_backfill_job, status enum, DataSyncDaemon stub."""

    @patch(f"{_M}.daily_ingest")
    @patch(f"{_M}.get_previous_trade_date")
    def test_run_daily_job(self, prev, ingest):
        from app.datasync.service.data_sync_daemon import run_daily_job
        prev.return_value = date(2024, 6, 5)
        ingest.return_value = {}
        run_daily_job()
        ingest.assert_called_once()

    @patch(f"{_M}.missing_data_backfill")
    def test_run_backfill_job(self, backfill):
        from app.datasync.service.data_sync_daemon import run_backfill_job
        run_backfill_job()
        backfill.assert_called_once()

    def test_sync_step_enum(self):
        from app.datasync.service.data_sync_daemon import SyncStep, SyncStatus
        assert SyncStep.AKSHARE_INDEX.value == "akshare_index"
        assert SyncStatus.SUCCESS.value == "success"
        assert SyncStatus.ERROR.value == "error"

    def test_find_missing_trade_dates_stub(self):
        from app.datasync.service.data_sync_daemon import DataSyncDaemon
        missing = DataSyncDaemon.find_missing_trade_dates(lookback_days=7)
        assert isinstance(missing, list)
        assert missing == []  # stub always empty

    @patch(f"{_M}.bulk_upsert_status")
    @patch(f"{_M}.get_vnpy_counts")
    @patch(f"{_M}.get_suspend_counts")
    @patch(f"{_M}.get_suspend_d_counts")
    @patch(f"{_M}.get_moneyflow_counts")
    @patch(f"{_M}.get_bak_daily_counts")
    @patch(f"{_M}.get_stock_monthly_counts")
    @patch(f"{_M}.get_stock_weekly_counts")
    @patch(f"{_M}.get_adj_factor_counts")
    @patch(f"{_M}.get_stock_daily_counts")
    def test_initialize_sync_status_table(self, daily_c, adj_c, weekly_c, monthly_c, bak_c, moneyflow_c, suspend_d_c, suspend_c, vnpy_c, bulk):
        from app.datasync.service.data_sync_daemon import initialize_sync_status_table
        daily_c.return_value = {}
        adj_c.return_value = {}
        weekly_c.return_value = {}
        monthly_c.return_value = {}
        bak_c.return_value = {}
        moneyflow_c.return_value = {}
        suspend_d_c.return_value = {}
        suspend_c.return_value = {}
        vnpy_c.return_value = {}
        initialize_sync_status_table(lookback_years=1)
        bulk.assert_called()


# ═══════════════════════════════════════════════════════════════════════
# datasync/scheduler.py
# ═══════════════════════════════════════════════════════════════════════

class TestScheduler:

    @patch("app.datasync.service.vnpy_sync.run_vnpy_sync_job")
    @patch("app.datasync.service.sync_engine.daily_sync")
    @patch("app.datasync.registry.build_default_registry")
    def test_run_daily_sync(self, build, daily, vnpy):
        from app.datasync.scheduler import run_daily_sync
        from app.datasync.base import SyncResult, SyncStatus as BaseSyncStatus
        build.return_value = MagicMock()
        daily.return_value = {}
        vnpy.return_value = SyncResult(status=BaseSyncStatus.SUCCESS, rows_synced=10, error_message=None)
        r = run_daily_sync(date(2024, 1, 15))
        assert isinstance(r, dict)

    @patch("app.datasync.service.sync_engine.backfill_retry")
    @patch("app.datasync.registry.build_default_registry")
    def test_run_backfill(self, build, backfill):
        from app.datasync.scheduler import run_backfill
        build.return_value = MagicMock()
        backfill.return_value = {}
        r = run_backfill()
        backfill.assert_called_once()

    @patch("app.datasync.service.vnpy_sync.run_vnpy_sync_job")
    def test_run_vnpy(self, vnpy):
        from app.datasync.scheduler import run_vnpy
        from app.datasync.base import SyncResult, SyncStatus as BaseSyncStatus
        vnpy.return_value = SyncResult(status=BaseSyncStatus.SUCCESS, rows_synced=5, error_message=None)
        r = run_vnpy()
        vnpy.assert_called_once()

    @patch("app.datasync.service.init_service.initialize")
    @patch("app.datasync.registry.build_default_registry")
    def test_run_init(self, build, init_fn):
        from app.datasync.scheduler import run_init
        build.return_value = MagicMock()
        run_init(run_backfill_flag=False)
        init_fn.assert_called_once()

    def test_build_registry(self):
        from app.datasync.scheduler import _build_registry
        from app.datasync.registry import DataSourceRegistry
        reg = _build_registry()
        assert isinstance(reg, DataSourceRegistry)
