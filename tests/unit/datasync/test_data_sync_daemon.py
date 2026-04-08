"""Unit tests for app.datasync.service.data_sync_daemon — helper / utility functions."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date, timedelta
from unittest.mock import DEFAULT, MagicMock, patch
import sys

import pytest

_MOD = "app.datasync.service.data_sync_daemon"


@pytest.fixture(autouse=True)
def _mock_heavy_deps():
    """Stub out heavy module-level imports so the daemon module can be loaded."""
    stubs = {}
    for mod_name in [
        "akshare",
        "schedule",
    ]:
        if mod_name not in sys.modules:
            stubs[mod_name] = sys.modules[mod_name] = MagicMock()
    yield
    for mod_name in stubs:
        sys.modules.pop(mod_name, None)


# ── SyncStep / SyncStatus enums ─────────────────────────────────

class TestEnums:
    def test_sync_step_values(self):
        from app.datasync.service.data_sync_daemon import SyncStep
        assert SyncStep.AKSHARE_INDEX.value == "akshare_index"
        assert SyncStep.TUSHARE_STOCK_DAILY.value == "tushare_stock_daily"

    def test_sync_status_values(self):
        from app.datasync.service.data_sync_daemon import SyncStatus
        assert SyncStatus.PENDING.value == "pending"
        assert SyncStatus.SUCCESS.value == "success"
        assert SyncStatus.ERROR.value == "error"
        assert SyncStatus.RUNNING.value == "running"


# ── group_dates_by_month ─────────────────────────────────────────

class TestGroupDatesByMonth:
    def test_empty(self):
        from app.datasync.service.data_sync_daemon import group_dates_by_month
        assert group_dates_by_month([]) == []

    def test_single_date(self):
        from app.datasync.service.data_sync_daemon import group_dates_by_month
        d = date(2024, 1, 15)
        result = group_dates_by_month([d])
        assert result == [(d, d)]

    def test_same_month(self):
        from app.datasync.service.data_sync_daemon import group_dates_by_month
        dates = [date(2024, 1, 5), date(2024, 1, 10), date(2024, 1, 20)]
        result = group_dates_by_month(dates)
        assert len(result) == 1
        assert result[0] == (date(2024, 1, 5), date(2024, 1, 20))

    def test_different_months(self):
        from app.datasync.service.data_sync_daemon import group_dates_by_month
        dates = [date(2024, 1, 5), date(2024, 3, 10), date(2024, 5, 20)]
        result = group_dates_by_month(dates)
        assert len(result) == 3

    def test_contiguous_months(self):
        from app.datasync.service.data_sync_daemon import group_dates_by_month
        dates = [date(2024, 1, 25), date(2024, 2, 5)]
        result = group_dates_by_month(dates)
        # Within 31 days → single group
        assert len(result) == 1


# ── get_previous_trade_date ──────────────────────────────────────

class TestGetPreviousTradeDate:
    def test_with_trade_days(self):
        from app.datasync.service.data_sync_daemon import get_previous_trade_date
        with patch(f"{_MOD}.get_trade_calendar") as mock_cal:
            mock_cal.return_value = [date(2024, 1, 3), date(2024, 1, 4), date(2024, 1, 5)]
            result = get_previous_trade_date(1)
        assert result == date(2024, 1, 5)

    def test_offset_2(self):
        from app.datasync.service.data_sync_daemon import get_previous_trade_date
        with patch(f"{_MOD}.get_trade_calendar") as mock_cal:
            mock_cal.return_value = [date(2024, 1, 3), date(2024, 1, 4), date(2024, 1, 5)]
            result = get_previous_trade_date(2)
        assert result == date(2024, 1, 4)

    def test_no_trade_days(self):
        from app.datasync.service.data_sync_daemon import get_previous_trade_date
        with patch(f"{_MOD}.get_trade_calendar") as mock_cal:
            mock_cal.return_value = []
            result = get_previous_trade_date(1)
        assert result == date.today() - timedelta(days=1)

    def test_offset_exceeds_length(self):
        from app.datasync.service.data_sync_daemon import get_previous_trade_date
        with patch(f"{_MOD}.get_trade_calendar") as mock_cal:
            mock_cal.return_value = [date(2024, 1, 3)]
            result = get_previous_trade_date(5)
        assert result == date(2024, 1, 3)


# ── write_sync_log ───────────────────────────────────────────────

class TestWriteSyncLog:
    def test_dry_run_skips(self):
        from app.datasync.service.data_sync_daemon import write_sync_log
        with patch(f"{_MOD}.DRY_RUN", True), \
             patch(f"{_MOD}.dao_write_tushare_stock_sync_log") as mock_write:
            write_sync_log(date(2024, 1, 1), "daily", "success", 100)
        mock_write.assert_not_called()

    def test_writes(self):
        from app.datasync.service.data_sync_daemon import write_sync_log
        with patch(f"{_MOD}.DRY_RUN", False), \
             patch(f"{_MOD}.dao_write_tushare_stock_sync_log") as mock_write:
            write_sync_log(date(2024, 1, 1), "daily", "success", 100, None)
        mock_write.assert_called_once_with(date(2024, 1, 1), "daily", "success", 100, None)


# ── get_last_success_date ────────────────────────────────────────

class TestGetLastSuccessDate:
    def test_returns(self):
        from app.datasync.service.data_sync_daemon import get_last_success_date
        with patch(f"{_MOD}.dao_get_last_success_tushare_sync_date") as mock_fn:
            mock_fn.return_value = date(2024, 1, 1)
            result = get_last_success_date("daily")
        assert result == date(2024, 1, 1)


# ── REQUIRED_ENDPOINTS ───────────────────────────────────────────

class TestRequiredEndpoints:
    def test_has_required(self):
        from app.datasync.service.data_sync_daemon import REQUIRED_ENDPOINTS
        assert "tushare_stock_daily" in REQUIRED_ENDPOINTS
        assert "akshare_index" in REQUIRED_ENDPOINTS
        assert len(REQUIRED_ENDPOINTS) >= 6


# ── get_trade_calendar ───────────────────────────────────────────

class TestGetTradeCalendar:
    def test_from_cache(self):
        from app.datasync.service.data_sync_daemon import get_trade_calendar
        with patch(f"{_MOD}.get_cached_trade_dates") as mock_cache:
            mock_cache.return_value = [date(2024, 1, 3), date(2024, 1, 4)]
            result = get_trade_calendar(date(2024, 1, 1), date(2024, 1, 5))
        assert len(result) == 2

    def test_empty_cache_weekday_fallback(self):
        from app.datasync.service.data_sync_daemon import get_trade_calendar
        with patch(f"{_MOD}.get_cached_trade_dates") as mock_cache:
            mock_cache.return_value = []
            with patch(f"{_MOD}.AKSHARE_AVAILABLE", False):
                result = get_trade_calendar(date(2024, 1, 1), date(2024, 1, 7))
        # Should include weekdays only (Mon-Fri)
        for d in result:
            assert d.weekday() < 5


# ── daily_ingest ─────────────────────────────────────────────────

class TestDailyIngest:
    def test_uses_previous_trade_date_when_none(self):
        from app.datasync.service.data_sync_daemon import daily_ingest
        with patch(f"{_MOD}.get_previous_trade_date", return_value=date(2024, 1, 5)), \
             patch(f"{_MOD}.get_step_status", return_value="success"), \
             patch(f"{_MOD}.write_step_status"), \
             patch(f"{_MOD}.run_akshare_index_step"), \
             patch(f"{_MOD}.run_tushare_stock_basic_step"), \
             patch(f"{_MOD}.run_tushare_stock_daily_step"), \
             patch(f"{_MOD}.run_tushare_adj_factor_step"), \
             patch(f"{_MOD}.run_tushare_dividend_step"), \
             patch(f"{_MOD}.run_tushare_top10_holders_step"), \
             patch(f"{_MOD}.run_vnpy_sync_step"), \
             patch(f"{_MOD}.run_tushare_stock_weekly_step"), \
             patch(f"{_MOD}.run_tushare_stock_monthly_step"), \
             patch(f"{_MOD}.run_tushare_index_daily_step"), \
             patch(f"{_MOD}.run_tushare_index_weekly_step"):
            results = daily_ingest(None)
        # All steps should be skipped (status=success)
        for key in results:
            assert results[key].get("skipped", False) is True or results[key]["status"] in ("success",)


# ── Step functions ───────────────────────────────────────────────

@contextmanager
def _patch_all_steps():
    """Context manager to patch all DAO and ingest calls used by step functions.

    Uses DEFAULT so that ``patch.multiple`` *creates* the mocks internally and
    returns them in the context-dict, allowing tests to do ``mocks["xxx"]``.
    """
    with patch.multiple(
        _MOD,
        ingest_stock_basic=DEFAULT,
        get_stock_basic_count=DEFAULT,
        call_pro=DEFAULT,
        upsert_daily=DEFAULT,
        ingest_adj_factor=DEFAULT,
        get_adj_factor_count_for_date=DEFAULT,
        get_all_ts_codes=DEFAULT,
        ingest_dividend=DEFAULT,
        ingest_top10_holders=DEFAULT,
        sync_date_to_vnpy=DEFAULT,
        ingest_weekly=DEFAULT,
        ingest_monthly=DEFAULT,
        ingest_index_daily=DEFAULT,
        ingest_index_weekly=DEFAULT,
        ak_ingest_index_daily=DEFAULT,
        upsert_dividend_df=DEFAULT,
    ) as mocks, \
        patch(f"{_MOD}.INDEX_MAPPING", {"000001": "上证指数", "399001": "深证成指"}), \
        patch(f"{_MOD}.DRY_RUN", False):
        # Set sensible return-value defaults
        mocks["get_stock_basic_count"].return_value = 5000
        mocks["call_pro"].return_value = None
        mocks["get_adj_factor_count_for_date"].return_value = 4999
        mocks["get_all_ts_codes"].return_value = ["000001.SZ", "000002.SZ"]
        mocks["sync_date_to_vnpy"].return_value = (10, 500)
        mocks["ingest_weekly"].return_value = 100
        mocks["ingest_monthly"].return_value = 80
        mocks["ingest_index_daily"].return_value = 5
        mocks["ingest_index_weekly"].return_value = 5
        mocks["ak_ingest_index_daily"].return_value = 5
        yield mocks


class TestRunAkshareIndexStep:
    def test_success(self):
        from app.datasync.service.data_sync_daemon import run_akshare_index_step, SyncStatus
        with _patch_all_steps():
            status, rows, err = run_akshare_index_step(date(2024, 1, 15))
            assert status == SyncStatus.SUCCESS
            assert rows > 0
            assert err is None

    def test_partial_failure(self):
        from app.datasync.service.data_sync_daemon import run_akshare_index_step, SyncStatus
        with _patch_all_steps() as mocks:
            mocks["ak_ingest_index_daily"].side_effect = [5, Exception("timeout")]
            status, rows, err = run_akshare_index_step(date(2024, 1, 15))
            assert status in (SyncStatus.PARTIAL, SyncStatus.SUCCESS)

    def test_all_fail(self):
        from app.datasync.service.data_sync_daemon import run_akshare_index_step, SyncStatus
        with _patch_all_steps() as mocks:
            mocks["ak_ingest_index_daily"].side_effect = Exception("fail")
            status, rows, err = run_akshare_index_step(date(2024, 1, 15))
            assert status == SyncStatus.ERROR


class TestRunTushareStockBasicStep:
    def test_success(self):
        from app.datasync.service.data_sync_daemon import run_tushare_stock_basic_step, SyncStatus
        with _patch_all_steps():
            status, rows, err = run_tushare_stock_basic_step(date(2024, 1, 15))
            assert status == SyncStatus.SUCCESS
            assert rows == 5000
            assert err is None

    def test_error(self):
        from app.datasync.service.data_sync_daemon import run_tushare_stock_basic_step, SyncStatus
        with _patch_all_steps() as mocks:
            mocks["ingest_stock_basic"].side_effect = Exception("fail")
            status, rows, err = run_tushare_stock_basic_step(date(2024, 1, 15))
            assert status == SyncStatus.ERROR
            assert err is not None


class TestRunTushareStockDailyStep:
    def test_no_data(self):
        from app.datasync.service.data_sync_daemon import run_tushare_stock_daily_step, SyncStatus
        with _patch_all_steps() as mocks:
            mocks["call_pro"].return_value = None
            status, rows, err = run_tushare_stock_daily_step(date(2024, 1, 15))
            assert status == SyncStatus.SUCCESS
            assert rows == 0

    def test_with_data(self):
        import pandas as pd
        from app.datasync.service.data_sync_daemon import run_tushare_stock_daily_step, SyncStatus
        df = pd.DataFrame({"ts_code": ["000001.SZ"], "close": [10.0]})
        with _patch_all_steps() as mocks:
            mocks["call_pro"].return_value = df
            status, rows, err = run_tushare_stock_daily_step(date(2024, 1, 15))
            assert status == SyncStatus.SUCCESS

    def test_error(self):
        from app.datasync.service.data_sync_daemon import run_tushare_stock_daily_step, SyncStatus
        with _patch_all_steps() as mocks:
            mocks["call_pro"].side_effect = Exception("api error")
            status, rows, err = run_tushare_stock_daily_step(date(2024, 1, 15))
            assert status == SyncStatus.ERROR


class TestRunTushareAdjFactorStep:
    def test_success(self):
        from app.datasync.service.data_sync_daemon import run_tushare_adj_factor_step, SyncStatus
        with _patch_all_steps():
            status, rows, err = run_tushare_adj_factor_step(date(2024, 1, 15))
            assert status == SyncStatus.SUCCESS
            assert rows == 4999

    def test_error(self):
        from app.datasync.service.data_sync_daemon import run_tushare_adj_factor_step, SyncStatus
        with _patch_all_steps() as mocks:
            mocks["ingest_adj_factor"].side_effect = Exception("fail")
            status, rows, err = run_tushare_adj_factor_step(date(2024, 1, 15))
            assert status == SyncStatus.ERROR


class TestRunTushareDividendStep:
    def test_batch_success(self):
        import pandas as pd
        from app.datasync.service.data_sync_daemon import run_tushare_dividend_step, SyncStatus
        df = pd.DataFrame({"ts_code": ["000001.SZ"], "div_proc": [1.0]})
        with _patch_all_steps() as mocks:
            mocks["call_pro"].return_value = df
            status, rows, err = run_tushare_dividend_step(date(2024, 1, 15), use_batch=True)
            assert status == SyncStatus.SUCCESS

    def test_batch_empty(self):
        import pandas as pd
        from app.datasync.service.data_sync_daemon import run_tushare_dividend_step, SyncStatus
        with _patch_all_steps() as mocks:
            mocks["call_pro"].return_value = pd.DataFrame()
            status, rows, err = run_tushare_dividend_step(date(2024, 1, 15), use_batch=True)
            assert status == SyncStatus.SUCCESS
            assert rows == 0

    def test_daily_mode_success(self):
        from app.datasync.service.data_sync_daemon import run_tushare_dividend_step, SyncStatus
        with _patch_all_steps():
            status, rows, err = run_tushare_dividend_step(date(2024, 1, 15), use_batch=False)
            assert status == SyncStatus.SUCCESS

    def test_batch_permission_error(self):
        from app.datasync.service.data_sync_daemon import run_tushare_dividend_step, SyncStatus
        with _patch_all_steps() as mocks:
            mocks["call_pro"].side_effect = Exception("没有接口访问权限")
            status, rows, err = run_tushare_dividend_step(date(2024, 1, 15), use_batch=True)
            assert status == SyncStatus.PARTIAL


class TestRunTushareTop10HoldersStep:
    def test_success(self):
        from app.datasync.service.data_sync_daemon import run_tushare_top10_holders_step, SyncStatus
        with _patch_all_steps():
            status, rows, err = run_tushare_top10_holders_step(date(2024, 1, 15))
            assert status == SyncStatus.SUCCESS

    def test_all_fail(self):
        from app.datasync.service.data_sync_daemon import run_tushare_top10_holders_step, SyncStatus
        with _patch_all_steps() as mocks:
            mocks["ingest_top10_holders"].side_effect = Exception("timeout")
            status, rows, err = run_tushare_top10_holders_step(date(2024, 1, 15))
            assert status == SyncStatus.PARTIAL


class TestRunVnpySyncStep:
    def test_success(self):
        from app.datasync.service.data_sync_daemon import run_vnpy_sync_step, SyncStatus
        with _patch_all_steps():
            status, rows, err = run_vnpy_sync_step(date(2024, 1, 15))
            assert status == SyncStatus.SUCCESS
            assert rows == 500

    def test_no_symbols(self):
        from app.datasync.service.data_sync_daemon import run_vnpy_sync_step, SyncStatus
        with _patch_all_steps() as mocks:
            mocks["sync_date_to_vnpy"].return_value = (0, 0)
            status, rows, err = run_vnpy_sync_step(date(2024, 1, 15))
            assert status == SyncStatus.PARTIAL

    def test_error(self):
        from app.datasync.service.data_sync_daemon import run_vnpy_sync_step, SyncStatus
        with _patch_all_steps() as mocks:
            mocks["sync_date_to_vnpy"].side_effect = Exception("vnpy err")
            status, rows, err = run_vnpy_sync_step(date(2024, 1, 15))
            assert status == SyncStatus.ERROR


class TestRunTushareStockWeeklyStep:
    def test_success(self):
        from app.datasync.service.data_sync_daemon import run_tushare_stock_weekly_step, SyncStatus
        with _patch_all_steps():
            status, rows, err = run_tushare_stock_weekly_step(date(2024, 1, 15))
            assert status == SyncStatus.SUCCESS
            assert rows == 100

    def test_error(self):
        from app.datasync.service.data_sync_daemon import run_tushare_stock_weekly_step, SyncStatus
        with _patch_all_steps() as mocks:
            mocks["ingest_weekly"].side_effect = Exception("err")
            status, rows, err = run_tushare_stock_weekly_step(date(2024, 1, 15))
            assert status == SyncStatus.ERROR


class TestRunTushareStockMonthlyStep:
    def test_success(self):
        from app.datasync.service.data_sync_daemon import run_tushare_stock_monthly_step, SyncStatus
        with _patch_all_steps():
            status, rows, err = run_tushare_stock_monthly_step(date(2024, 1, 15))
            assert status == SyncStatus.SUCCESS
            assert rows == 80

    def test_error(self):
        from app.datasync.service.data_sync_daemon import run_tushare_stock_monthly_step, SyncStatus
        with _patch_all_steps() as mocks:
            mocks["ingest_monthly"].side_effect = Exception("err")
            status, rows, err = run_tushare_stock_monthly_step(date(2024, 1, 15))
            assert status == SyncStatus.ERROR


class TestRunTushareIndexDailyStep:
    def test_success(self):
        from app.datasync.service.data_sync_daemon import run_tushare_index_daily_step, SyncStatus
        with _patch_all_steps():
            status, rows, err = run_tushare_index_daily_step(date(2024, 1, 15))
            assert status == SyncStatus.SUCCESS

    def test_partial_failure(self):
        from app.datasync.service.data_sync_daemon import run_tushare_index_daily_step, SyncStatus
        call_count = 0
        def _side_effect(**kw):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("timeout")
            return 5
        with _patch_all_steps() as mocks:
            mocks["ingest_index_daily"].side_effect = _side_effect
            status, rows, err = run_tushare_index_daily_step(date(2024, 1, 15))
            assert status in (SyncStatus.PARTIAL, SyncStatus.SUCCESS)

    def test_all_fail(self):
        from app.datasync.service.data_sync_daemon import run_tushare_index_daily_step, SyncStatus
        with _patch_all_steps() as mocks:
            mocks["ingest_index_daily"].side_effect = Exception("err")
            status, rows, err = run_tushare_index_daily_step(date(2024, 1, 15))
            assert status == SyncStatus.ERROR


class TestRunTushareIndexWeeklyStep:
    def test_success(self):
        from app.datasync.service.data_sync_daemon import run_tushare_index_weekly_step, SyncStatus
        with _patch_all_steps():
            status, rows, err = run_tushare_index_weekly_step(date(2024, 1, 15))
            assert status == SyncStatus.SUCCESS

    def test_all_fail(self):
        from app.datasync.service.data_sync_daemon import run_tushare_index_weekly_step, SyncStatus
        with _patch_all_steps() as mocks:
            mocks["ingest_index_weekly"].side_effect = Exception("err")
            status, rows, err = run_tushare_index_weekly_step(date(2024, 1, 15))
            assert status == SyncStatus.ERROR


# ── daily_ingest with running steps ──────────────────────────────

class TestDailyIngestRunning:
    def _run_ingest(self, target=date(2024, 1, 15)):
        from app.datasync.service.data_sync_daemon import daily_ingest, SyncStatus
        # Simulate all steps not yet done
        with patch(f"{_MOD}.get_previous_trade_date", return_value=target), \
             patch(f"{_MOD}.get_step_status", return_value="pending"), \
             patch(f"{_MOD}.write_step_status"), \
             patch(f"{_MOD}.run_akshare_index_step", return_value=(SyncStatus.SUCCESS, 10, None)), \
             patch(f"{_MOD}.run_tushare_stock_basic_step", return_value=(SyncStatus.SUCCESS, 5000, None)), \
             patch(f"{_MOD}.run_tushare_stock_daily_step", return_value=(SyncStatus.SUCCESS, 200, None)), \
             patch(f"{_MOD}.run_tushare_adj_factor_step", return_value=(SyncStatus.SUCCESS, 4999, None)), \
             patch(f"{_MOD}.run_tushare_dividend_step", return_value=(SyncStatus.SUCCESS, 50, None)), \
             patch(f"{_MOD}.run_tushare_top10_holders_step", return_value=(SyncStatus.SUCCESS, 30, None)), \
             patch(f"{_MOD}.run_vnpy_sync_step", return_value=(SyncStatus.SUCCESS, 500, None)), \
             patch(f"{_MOD}.run_tushare_stock_weekly_step", return_value=(SyncStatus.SUCCESS, 100, None)), \
             patch(f"{_MOD}.run_tushare_stock_monthly_step", return_value=(SyncStatus.SUCCESS, 80, None)), \
             patch(f"{_MOD}.run_tushare_index_daily_step", return_value=(SyncStatus.SUCCESS, 25, None)), \
             patch(f"{_MOD}.run_tushare_index_weekly_step", return_value=(SyncStatus.SUCCESS, 25, None)):
            return daily_ingest(target)

    def test_all_steps_run(self):
        results = self._run_ingest()
        assert isinstance(results, dict)
        assert len(results) >= 10

    def test_with_error_continue(self):
        from app.datasync.service.data_sync_daemon import daily_ingest, SyncStatus
        with patch(f"{_MOD}.get_previous_trade_date", return_value=date(2024, 1, 15)), \
             patch(f"{_MOD}.get_step_status", return_value="pending"), \
             patch(f"{_MOD}.write_step_status"), \
             patch(f"{_MOD}.run_akshare_index_step", return_value=(SyncStatus.ERROR, 0, "fail")), \
             patch(f"{_MOD}.run_tushare_stock_basic_step", return_value=(SyncStatus.SUCCESS, 5000, None)), \
             patch(f"{_MOD}.run_tushare_stock_daily_step", return_value=(SyncStatus.SUCCESS, 200, None)), \
             patch(f"{_MOD}.run_tushare_adj_factor_step", return_value=(SyncStatus.SUCCESS, 4999, None)), \
             patch(f"{_MOD}.run_tushare_dividend_step", return_value=(SyncStatus.SUCCESS, 50, None)), \
             patch(f"{_MOD}.run_tushare_top10_holders_step", return_value=(SyncStatus.SUCCESS, 30, None)), \
             patch(f"{_MOD}.run_vnpy_sync_step", return_value=(SyncStatus.SUCCESS, 500, None)), \
             patch(f"{_MOD}.run_tushare_stock_weekly_step", return_value=(SyncStatus.SUCCESS, 100, None)), \
             patch(f"{_MOD}.run_tushare_stock_monthly_step", return_value=(SyncStatus.SUCCESS, 80, None)), \
             patch(f"{_MOD}.run_tushare_index_daily_step", return_value=(SyncStatus.SUCCESS, 25, None)), \
             patch(f"{_MOD}.run_tushare_index_weekly_step", return_value=(SyncStatus.SUCCESS, 25, None)):
            results = daily_ingest(date(2024, 1, 15), continue_on_error=True)
            assert isinstance(results, dict)


# ── sync_daily_for_date ──────────────────────────────────────────

class TestSyncDailyForDate:
    def test_runs_per_code(self):
        from app.datasync.service.data_sync_daemon import sync_daily_for_date
        with patch(f"{_MOD}.get_all_ts_codes", return_value=["000001.SZ", "000002.SZ"]), \
             patch(f"{_MOD}.ingest_daily") as mock_ingest, \
             patch(f"{_MOD}.write_sync_log"), \
             patch(f"{_MOD}.time.sleep"):
            sync_daily_for_date(date(2024, 1, 15))
            assert mock_ingest.call_count == 2

    def test_handles_failures(self):
        from app.datasync.service.data_sync_daemon import sync_daily_for_date
        with patch(f"{_MOD}.get_all_ts_codes", return_value=["000001.SZ"]), \
             patch(f"{_MOD}.ingest_daily", side_effect=Exception("err")), \
             patch(f"{_MOD}.write_sync_log") as mock_log, \
             patch(f"{_MOD}.time.sleep"):
            sync_daily_for_date(date(2024, 1, 15))
            mock_log.assert_called_once()


# ── run_sync_for_date ────────────────────────────────────────────

class TestRunSyncForDate:
    def test_daily_endpoint(self):
        from app.datasync.service.data_sync_daemon import run_sync_for_date
        with patch(f"{_MOD}.sync_daily_for_date") as mock_sync, \
             patch(f"{_MOD}.ingest_all_other_data"), \
             patch(f"{_MOD}.ingest_daily_basic"), \
             patch(f"{_MOD}.ingest_repo"):
            run_sync_for_date(date(2024, 1, 15), ["daily"])
            mock_sync.assert_called_once()

    def test_repo_endpoint(self):
        from app.datasync.service.data_sync_daemon import run_sync_for_date
        with patch(f"{_MOD}.sync_daily_for_date"), \
             patch(f"{_MOD}.ingest_all_other_data"), \
             patch(f"{_MOD}.ingest_daily_basic"), \
             patch(f"{_MOD}.ingest_repo") as mock_repo, \
             patch(f"{_MOD}.write_sync_log"), \
             patch(f"{_MOD}.DRY_RUN", False):
            run_sync_for_date(date(2024, 1, 15), ["repo"])
            mock_repo.assert_called_once()


# ── missing_data_backfill ────────────────────────────────────────

class TestMissingDataBackfill:
    def test_locked_skips(self):
        from app.datasync.service.data_sync_daemon import missing_data_backfill
        with patch(f"{_MOD}.is_backfill_locked", return_value=True):
            missing_data_backfill()  # Should return early

    def test_no_failed_steps(self):
        from app.datasync.service.data_sync_daemon import missing_data_backfill
        with patch(f"{_MOD}.is_backfill_locked", return_value=False), \
             patch(f"{_MOD}.acquire_backfill_lock"), \
             patch(f"{_MOD}.release_backfill_lock"), \
             patch(f"{_MOD}.get_failed_steps", return_value=[]):
            missing_data_backfill()

    def test_with_failed_steps(self):
        from app.datasync.service.data_sync_daemon import missing_data_backfill, SyncStep
        failed = [
            {"sync_date": date(2024, 1, 10), "step_name": SyncStep.TUSHARE_STOCK_DAILY.value},
        ]
        with patch(f"{_MOD}.is_backfill_locked", return_value=False), \
             patch(f"{_MOD}.acquire_backfill_lock"), \
             patch(f"{_MOD}.release_backfill_lock"), \
             patch(f"{_MOD}.get_failed_steps", return_value=failed), \
             patch(f"{_MOD}.daily_ingest") as mock_ingest:
            missing_data_backfill()
            mock_ingest.assert_called()


# ── refresh_trade_calendar ───────────────────────────────────────

class TestRefreshTradeCalendar:
    def test_akshare_available(self):
        import pandas as pd
        from app.datasync.service.data_sync_daemon import refresh_trade_calendar
        df = pd.DataFrame({"trade_date": [pd.Timestamp("2024-01-03")]})
        with patch(f"{_MOD}.AKSHARE_AVAILABLE", True), \
             patch(f"{_MOD}.ak") as mock_ak, \
             patch(f"{_MOD}.truncate_trade_cal"), \
             patch(f"{_MOD}.upsert_trade_dates") as mock_upsert:
            mock_ak.tool_trade_date_hist_sina.return_value = df
            refresh_trade_calendar()
            mock_upsert.assert_called_once()

    def test_akshare_not_available(self):
        from app.datasync.service.data_sync_daemon import refresh_trade_calendar
        with patch(f"{_MOD}.AKSHARE_AVAILABLE", False):
            refresh_trade_calendar()  # should skip


# ── get_trade_days ───────────────────────────────────────────────

class TestGetTradeDays:
    def test_from_tushare(self):
        import pandas as pd
        from app.datasync.service.data_sync_daemon import get_trade_days
        df = pd.DataFrame({
            "cal_date": ["20240103", "20240104", "20240105"],
            "is_open": [1, 1, 0],
        })
        with patch(f"{_MOD}.call_pro", return_value=df):
            result = get_trade_days(date(2024, 1, 1), date(2024, 1, 7))
            # Code does str(pd.to_datetime(d).date()) → "2024-01-03"
            assert "2024-01-03" in result
            assert "2024-01-05" not in result

    def test_fallback_on_error(self):
        from app.datasync.service.data_sync_daemon import get_trade_days
        with patch(f"{_MOD}.call_pro", side_effect=Exception("fail")):
            result = get_trade_days(date(2024, 1, 1), date(2024, 1, 7))
            # Fallback to weekdays
            assert isinstance(result, list)


# ── initialize_sync_status_table ─────────────────────────────────

class TestInitializeSyncStatusTable:
    def test_basic(self):
        from app.datasync.service.data_sync_daemon import initialize_sync_status_table
        with patch(f"{_MOD}.get_trade_calendar", return_value=[date(2024, 1, 3)]), \
             patch(f"{_MOD}.get_stock_daily_counts", return_value={"20240103": 100}), \
             patch(f"{_MOD}.get_adj_factor_counts", return_value={"20240103": 99}), \
             patch(f"{_MOD}.get_vnpy_counts", return_value={"20240103": 50}), \
             patch(f"{_MOD}.bulk_upsert_status") as mock_bulk:
            initialize_sync_status_table(lookback_years=1)
            mock_bulk.assert_called_once()


# ── run_daily_job / run_backfill_job ─────────────────────────────

class TestJobs:
    def test_run_daily_job(self):
        from app.datasync.service.data_sync_daemon import run_daily_job
        with patch(f"{_MOD}.daily_ingest") as mock_fn:
            run_daily_job()
            mock_fn.assert_called_once()

    def test_run_daily_job_error(self):
        from app.datasync.service.data_sync_daemon import run_daily_job
        with patch(f"{_MOD}.daily_ingest", side_effect=Exception("fail")):
            run_daily_job()  # should not raise

    def test_run_backfill_job(self):
        from app.datasync.service.data_sync_daemon import run_backfill_job
        with patch(f"{_MOD}.missing_data_backfill") as mock_fn:
            run_backfill_job()
            mock_fn.assert_called_once()

    def test_run_backfill_job_error(self):
        from app.datasync.service.data_sync_daemon import run_backfill_job
        with patch(f"{_MOD}.missing_data_backfill", side_effect=Exception("fail")):
            run_backfill_job()  # should not raise


# ── DataSyncDaemon class ─────────────────────────────────────────

class TestDataSyncDaemonClass:
    def test_find_missing_trade_dates(self):
        from app.datasync.service.data_sync_daemon import DataSyncDaemon
        result = DataSyncDaemon.find_missing_trade_dates()
        assert result == []

    def test_find_missing_trade_dates_with_lookback(self):
        from app.datasync.service.data_sync_daemon import DataSyncDaemon
        result = DataSyncDaemon.find_missing_trade_dates(lookback_days=30)
        assert result == []
