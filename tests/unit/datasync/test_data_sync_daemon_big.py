"""Tests for uncovered functions in app.datasync.service.data_sync_daemon.

Covers: run_tushare_adj_factor_step, run_tushare_dividend_step,
run_tushare_top10_holders_step, run_vnpy_sync_step,
run_tushare_stock_weekly_step, run_tushare_stock_monthly_step,
run_tushare_index_daily_step, run_tushare_index_weekly_step,
daily_ingest, missing_data_backfill, group_dates_by_month,
initialize_sync_status_table, run_daily_job, run_backfill_job,
DataSyncDaemon.
"""

from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import MagicMock, patch, call
import pytest

_MOD = "app.datasync.service.data_sync_daemon"


@pytest.fixture(autouse=True)
def _mock_dao(monkeypatch):
    """Prevent real DB calls from DAO functions."""
    import app.datasync.service.data_sync_daemon as mod
    monkeypatch.setattr(mod, "write_step_status", lambda *a, **kw: None)
    monkeypatch.setattr(mod, "get_step_status", lambda *a, **kw: None)
    monkeypatch.setattr(mod, "ensure_tables", lambda: None)


# ═══ run_tushare_adj_factor_step ═══════════════════════════════════════

class TestRunTushareAdjFactorStep:
    @patch(f"{_MOD}.get_adj_factor_count_for_date", return_value=100)
    @patch(f"{_MOD}.ingest_adj_factor")
    def test_success(self, mock_ingest, mock_count):
        from app.datasync.service.data_sync_daemon import run_tushare_adj_factor_step, SyncStatus
        status, rows, err = run_tushare_adj_factor_step(date(2024, 3, 15))
        assert status == SyncStatus.SUCCESS
        assert rows == 100
        assert err is None

    @patch(f"{_MOD}.ingest_adj_factor", side_effect=RuntimeError("fail"))
    def test_error(self, mock_ingest):
        from app.datasync.service.data_sync_daemon import run_tushare_adj_factor_step, SyncStatus
        status, rows, err = run_tushare_adj_factor_step(date(2024, 3, 15))
        assert status == SyncStatus.ERROR
        assert "fail" in err


# ═══ run_tushare_dividend_step ═════════════════════════════════════════

class TestRunTushareDividendStep:
    @patch(f"{_MOD}.get_all_ts_codes", return_value=["000001.SZ", "000002.SZ"])
    @patch(f"{_MOD}.ingest_dividend")
    def test_daily_mode(self, mock_ingest, mock_codes):
        from app.datasync.service.data_sync_daemon import run_tushare_dividend_step, SyncStatus
        status, rows, err = run_tushare_dividend_step(date(2024, 3, 15), use_batch=False)
        assert status == SyncStatus.SUCCESS
        assert rows == 2

    @patch(f"{_MOD}.upsert_dividend_df", return_value=10)
    @patch(f"{_MOD}.call_pro")
    def test_batch_mode(self, mock_call, mock_upsert):
        import pandas as pd
        from app.datasync.service.data_sync_daemon import run_tushare_dividend_step, SyncStatus
        mock_call.return_value = pd.DataFrame({"ts_code": ["000001.SZ"]})
        status, rows, err = run_tushare_dividend_step(date(2024, 3, 15), use_batch=True)
        assert status == SyncStatus.SUCCESS
        assert rows == 10

    @patch(f"{_MOD}.call_pro", return_value=None)
    def test_batch_empty(self, mock_call):
        from app.datasync.service.data_sync_daemon import run_tushare_dividend_step, SyncStatus
        status, rows, err = run_tushare_dividend_step(date(2024, 3, 15), use_batch=True)
        assert status == SyncStatus.SUCCESS
        assert rows == 0

    @patch(f"{_MOD}.call_pro", side_effect=RuntimeError("没有接口访问权限"))
    def test_batch_permission_denied(self, mock_call):
        from app.datasync.service.data_sync_daemon import run_tushare_dividend_step, SyncStatus
        status, rows, err = run_tushare_dividend_step(date(2024, 3, 15), use_batch=True)
        assert status == SyncStatus.PARTIAL
        assert "Permission denied" in err

    @patch(f"{_MOD}.call_pro", side_effect=RuntimeError("other error"))
    def test_batch_error(self, mock_call):
        from app.datasync.service.data_sync_daemon import run_tushare_dividend_step, SyncStatus
        status, rows, err = run_tushare_dividend_step(date(2024, 3, 15), use_batch=True)
        assert status == SyncStatus.ERROR

    @patch(f"{_MOD}.get_all_ts_codes", return_value=["000001.SZ"])
    @patch(f"{_MOD}.ingest_dividend", side_effect=RuntimeError("err"))
    def test_daily_all_fail(self, mock_ingest, mock_codes):
        from app.datasync.service.data_sync_daemon import run_tushare_dividend_step, SyncStatus
        status, rows, err = run_tushare_dividend_step(date(2024, 3, 15), use_batch=False)
        assert status == SyncStatus.PARTIAL


# ═══ run_tushare_top10_holders_step ════════════════════════════════════

class TestRunTushareTop10HoldersStep:
    @patch(f"{_MOD}.get_all_ts_codes", return_value=["000001.SZ", "000002.SZ"])
    @patch(f"{_MOD}.ingest_top10_holders")
    def test_success(self, mock_ingest, mock_codes):
        from app.datasync.service.data_sync_daemon import run_tushare_top10_holders_step, SyncStatus
        status, rows, err = run_tushare_top10_holders_step(date(2024, 3, 15))
        assert status == SyncStatus.SUCCESS
        assert rows == 2

    @patch(f"{_MOD}.get_all_ts_codes", return_value=["000001.SZ"])
    @patch(f"{_MOD}.ingest_top10_holders", side_effect=RuntimeError("err"))
    def test_all_fail(self, mock_ingest, mock_codes):
        from app.datasync.service.data_sync_daemon import run_tushare_top10_holders_step, SyncStatus
        status, rows, err = run_tushare_top10_holders_step(date(2024, 3, 15))
        assert status == SyncStatus.PARTIAL


# ═══ run_vnpy_sync_step ═══════════════════════════════════════════════

class TestRunVnpySyncStep:
    @patch(f"{_MOD}.sync_date_to_vnpy", return_value=(10, 500))
    def test_success(self, mock_sync):
        from app.datasync.service.data_sync_daemon import run_vnpy_sync_step, SyncStatus
        status, rows, err = run_vnpy_sync_step(date(2024, 3, 15))
        assert status == SyncStatus.SUCCESS
        assert rows == 500

    @patch(f"{_MOD}.sync_date_to_vnpy", return_value=(0, 0))
    def test_no_symbols(self, mock_sync):
        from app.datasync.service.data_sync_daemon import run_vnpy_sync_step, SyncStatus
        status, rows, err = run_vnpy_sync_step(date(2024, 3, 15))
        assert status == SyncStatus.PARTIAL

    @patch(f"{_MOD}.sync_date_to_vnpy", side_effect=RuntimeError("err"))
    def test_error(self, mock_sync):
        from app.datasync.service.data_sync_daemon import run_vnpy_sync_step, SyncStatus
        status, rows, err = run_vnpy_sync_step(date(2024, 3, 15))
        assert status == SyncStatus.ERROR


# ═══ run_tushare_stock_weekly_step ═════════════════════════════════════

class TestRunTushareStockWeeklyStep:
    @patch(f"{_MOD}.ingest_weekly", return_value=50)
    def test_success(self, mock_ingest):
        from app.datasync.service.data_sync_daemon import run_tushare_stock_weekly_step, SyncStatus
        status, rows, err = run_tushare_stock_weekly_step(date(2024, 3, 15))
        assert status == SyncStatus.SUCCESS
        assert rows == 50

    @patch(f"{_MOD}.ingest_weekly", side_effect=RuntimeError("err"))
    def test_error(self, mock_ingest):
        from app.datasync.service.data_sync_daemon import run_tushare_stock_weekly_step, SyncStatus
        status, rows, err = run_tushare_stock_weekly_step(date(2024, 3, 15))
        assert status == SyncStatus.ERROR


# ═══ run_tushare_stock_monthly_step ════════════════════════════════════

class TestRunTushareStockMonthlyStep:
    @patch(f"{_MOD}.ingest_monthly", return_value=30)
    def test_success(self, mock_ingest):
        from app.datasync.service.data_sync_daemon import run_tushare_stock_monthly_step, SyncStatus
        status, rows, err = run_tushare_stock_monthly_step(date(2024, 3, 15))
        assert status == SyncStatus.SUCCESS
        assert rows == 30

    @patch(f"{_MOD}.ingest_monthly", side_effect=RuntimeError("err"))
    def test_error(self, mock_ingest):
        from app.datasync.service.data_sync_daemon import run_tushare_stock_monthly_step, SyncStatus
        status, rows, err = run_tushare_stock_monthly_step(date(2024, 3, 15))
        assert status == SyncStatus.ERROR


# ═══ run_tushare_index_daily_step ══════════════════════════════════════

class TestRunTushareIndexDailyStep:
    @patch(f"{_MOD}.ingest_index_daily", return_value=5)
    def test_success(self, mock_ingest):
        from app.datasync.service.data_sync_daemon import run_tushare_index_daily_step, SyncStatus
        status, rows, err = run_tushare_index_daily_step(date(2024, 3, 15))
        assert status == SyncStatus.SUCCESS
        assert rows == 25  # 5 per code * 5 codes

    @patch(f"{_MOD}.ingest_index_daily", side_effect=RuntimeError("err"))
    def test_all_fail(self, mock_ingest):
        from app.datasync.service.data_sync_daemon import run_tushare_index_daily_step, SyncStatus
        status, rows, err = run_tushare_index_daily_step(date(2024, 3, 15))
        assert status == SyncStatus.ERROR

    @patch(f"{_MOD}.ingest_index_daily")
    def test_partial(self, mock_ingest):
        from app.datasync.service.data_sync_daemon import run_tushare_index_daily_step, SyncStatus
        # First succeeds, rest fail
        mock_ingest.side_effect = [5, RuntimeError("err"), RuntimeError("err"), RuntimeError("err"), RuntimeError("err")]
        status, rows, err = run_tushare_index_daily_step(date(2024, 3, 15))
        assert status == SyncStatus.PARTIAL


# ═══ run_tushare_index_weekly_step ═════════════════════════════════════

class TestRunTushareIndexWeeklyStep:
    @patch(f"{_MOD}.ingest_index_weekly", return_value=5)
    def test_success(self, mock_ingest):
        from app.datasync.service.data_sync_daemon import run_tushare_index_weekly_step, SyncStatus
        status, rows, err = run_tushare_index_weekly_step(date(2024, 3, 15))
        assert status == SyncStatus.SUCCESS

    @patch(f"{_MOD}.ingest_index_weekly", side_effect=RuntimeError("err"))
    def test_all_fail(self, mock_ingest):
        from app.datasync.service.data_sync_daemon import run_tushare_index_weekly_step, SyncStatus
        status, rows, err = run_tushare_index_weekly_step(date(2024, 3, 15))
        assert status == SyncStatus.ERROR


# ═══ group_dates_by_month ══════════════════════════════════════════════

class TestGroupDatesByMonth:
    def test_empty(self):
        from app.datasync.service.data_sync_daemon import group_dates_by_month
        assert group_dates_by_month([]) == []

    def test_single(self):
        from app.datasync.service.data_sync_daemon import group_dates_by_month
        result = group_dates_by_month([date(2024, 3, 15)])
        assert result == [(date(2024, 3, 15), date(2024, 3, 15))]

    def test_same_month(self):
        from app.datasync.service.data_sync_daemon import group_dates_by_month
        dates = [date(2024, 3, 1), date(2024, 3, 15), date(2024, 3, 31)]
        result = group_dates_by_month(dates)
        assert len(result) == 1
        assert result[0] == (date(2024, 3, 1), date(2024, 3, 31))

    def test_different_months(self):
        from app.datasync.service.data_sync_daemon import group_dates_by_month
        dates = [date(2024, 1, 15), date(2024, 5, 15)]
        result = group_dates_by_month(dates)
        assert len(result) == 2


# ═══ daily_ingest ══════════════════════════════════════════════════════

class TestDailyIngest:
    @patch(f"{_MOD}.run_vnpy_sync_step")
    @patch(f"{_MOD}.run_tushare_index_weekly_step")
    @patch(f"{_MOD}.run_tushare_index_daily_step")
    @patch(f"{_MOD}.run_tushare_stock_monthly_step")
    @patch(f"{_MOD}.run_tushare_stock_weekly_step")
    @patch(f"{_MOD}.run_tushare_top10_holders_step")
    @patch(f"{_MOD}.run_tushare_dividend_step")
    @patch(f"{_MOD}.run_tushare_adj_factor_step")
    @patch(f"{_MOD}.run_tushare_stock_daily_step")
    @patch(f"{_MOD}.run_tushare_stock_basic_step")
    @patch(f"{_MOD}.run_akshare_index_step")
    def test_all_steps_success(self, mock_ak, mock_basic, mock_daily, mock_adj,
                                mock_div, mock_h, mock_w, mock_m, mock_id, mock_iw, mock_vnpy):
        from app.datasync.service.data_sync_daemon import daily_ingest, SyncStatus
        for m in [mock_ak, mock_basic, mock_daily, mock_adj, mock_div, mock_h,
                  mock_w, mock_m, mock_id, mock_iw, mock_vnpy]:
            m.return_value = (SyncStatus.SUCCESS, 10, None)

        result = daily_ingest(target_date=date(2024, 3, 15))
        assert "akshare_index" in result
        assert "vnpy_sync" in result
        assert result["akshare_index"]["status"] == "success"

    @patch(f"{_MOD}.get_previous_trade_date", return_value=date(2024, 3, 15))
    @patch(f"{_MOD}.run_vnpy_sync_step")
    @patch(f"{_MOD}.run_tushare_index_weekly_step")
    @patch(f"{_MOD}.run_tushare_index_daily_step")
    @patch(f"{_MOD}.run_tushare_stock_monthly_step")
    @patch(f"{_MOD}.run_tushare_stock_weekly_step")
    @patch(f"{_MOD}.run_tushare_top10_holders_step")
    @patch(f"{_MOD}.run_tushare_dividend_step")
    @patch(f"{_MOD}.run_tushare_adj_factor_step")
    @patch(f"{_MOD}.run_tushare_stock_daily_step")
    @patch(f"{_MOD}.run_tushare_stock_basic_step")
    @patch(f"{_MOD}.run_akshare_index_step")
    def test_no_target_date(self, mock_ak, mock_basic, mock_daily, mock_adj,
                             mock_div, mock_h, mock_w, mock_m, mock_id, mock_iw, mock_vnpy, mock_prev):
        from app.datasync.service.data_sync_daemon import daily_ingest, SyncStatus
        for m in [mock_ak, mock_basic, mock_daily, mock_adj, mock_div, mock_h,
                  mock_w, mock_m, mock_id, mock_iw, mock_vnpy]:
            m.return_value = (SyncStatus.SUCCESS, 0, None)
        result = daily_ingest()
        mock_prev.assert_called_once()

    @patch(f"{_MOD}.run_akshare_index_step", side_effect=RuntimeError("boom"))
    def test_stop_on_error(self, mock_ak):
        from app.datasync.service.data_sync_daemon import daily_ingest
        result = daily_ingest(target_date=date(2024, 3, 15), continue_on_error=False)
        assert result["akshare_index"]["status"] == "error"
        assert len(result) == 1  # stopped after first step

    def test_skips_already_synced(self, monkeypatch):
        """When get_step_status returns success, the step is skipped."""
        import app.datasync.service.data_sync_daemon as mod
        # All steps return success status
        monkeypatch.setattr(mod, "get_step_status", lambda d, s: {"status": "success", "rows_processed": 5})
        result = mod.daily_ingest(target_date=date(2024, 3, 15))
        assert result["akshare_index"]["skipped"] is True
        assert result["vnpy_sync"]["skipped"] is True


# ═══ missing_data_backfill ═════════════════════════════════════════════

class TestMissingDataBackfill:
    @patch(f"{_MOD}.release_backfill_lock")
    @patch(f"{_MOD}.get_failed_steps", return_value=[])
    @patch(f"{_MOD}.acquire_backfill_lock")
    @patch(f"{_MOD}.is_backfill_locked", return_value=False)
    def test_no_failed_steps(self, mock_locked, mock_acquire, mock_failed, mock_release):
        from app.datasync.service.data_sync_daemon import missing_data_backfill
        missing_data_backfill(lookback_days=30)
        mock_acquire.assert_called_once()
        mock_release.assert_called_once()

    @patch(f"{_MOD}.is_backfill_locked", return_value=True)
    def test_already_locked(self, mock_locked):
        from app.datasync.service.data_sync_daemon import missing_data_backfill
        missing_data_backfill(lookback_days=30)
        # Should return immediately

    @patch(f"{_MOD}.acquire_backfill_lock", side_effect=RuntimeError("lock err"))
    @patch(f"{_MOD}.is_backfill_locked", return_value=False)
    def test_lock_acquire_fails(self, mock_locked, mock_acquire):
        from app.datasync.service.data_sync_daemon import missing_data_backfill
        missing_data_backfill(lookback_days=30)

    @patch(f"{_MOD}.release_backfill_lock")
    @patch(f"{_MOD}.ingest_dividend_by_date_range")
    @patch(f"{_MOD}.get_failed_steps")
    @patch(f"{_MOD}.acquire_backfill_lock")
    @patch(f"{_MOD}.is_backfill_locked", return_value=False)
    def test_dividend_backfill(self, mock_locked, mock_acquire, mock_failed, mock_ingest, mock_release):
        from app.datasync.service.data_sync_daemon import missing_data_backfill
        mock_failed.return_value = [
            (date(2024, 3, 1), "tushare_dividend"),
            (date(2024, 3, 15), "tushare_dividend"),
        ]
        missing_data_backfill(lookback_days=30)
        mock_ingest.assert_called_once()

    @patch(f"{_MOD}.release_backfill_lock")
    @patch(f"{_MOD}.ingest_top10_holders_by_date_range")
    @patch(f"{_MOD}.get_failed_steps")
    @patch(f"{_MOD}.acquire_backfill_lock")
    @patch(f"{_MOD}.is_backfill_locked", return_value=False)
    def test_top10_holders_backfill(self, mock_locked, mock_acquire, mock_failed, mock_ingest, mock_release):
        from app.datasync.service.data_sync_daemon import missing_data_backfill
        mock_failed.return_value = [
            (date(2024, 3, 1), "tushare_top10_holders"),
        ]
        missing_data_backfill(lookback_days=30)
        mock_ingest.assert_called_once()

    @patch(f"{_MOD}.release_backfill_lock")
    @patch(f"{_MOD}.ingest_adj_factor_by_date_range")
    @patch(f"{_MOD}.get_failed_steps")
    @patch(f"{_MOD}.acquire_backfill_lock")
    @patch(f"{_MOD}.is_backfill_locked", return_value=False)
    def test_adj_factor_backfill(self, mock_locked, mock_acquire, mock_failed, mock_ingest, mock_release):
        from app.datasync.service.data_sync_daemon import missing_data_backfill
        mock_failed.return_value = [
            (date(2024, 3, 1), "tushare_adj_factor"),
        ]
        missing_data_backfill(lookback_days=30)
        mock_ingest.assert_called_once()

    @patch(f"{_MOD}.release_backfill_lock")
    @patch(f"{_MOD}.daily_ingest")
    @patch(f"{_MOD}.get_failed_steps")
    @patch(f"{_MOD}.acquire_backfill_lock")
    @patch(f"{_MOD}.is_backfill_locked", return_value=False)
    def test_other_step_retries_daily(self, mock_locked, mock_acquire, mock_failed, mock_daily, mock_release):
        from app.datasync.service.data_sync_daemon import missing_data_backfill
        mock_failed.return_value = [
            (date(2024, 3, 1), "akshare_index"),
        ]
        missing_data_backfill(lookback_days=30)
        mock_daily.assert_called_once()


# ═══ initialize_sync_status_table ══════════════════════════════════════

class TestInitializeSyncStatusTable:
    @patch(f"{_MOD}.bulk_upsert_status", return_value=100)
    @patch(f"{_MOD}.get_vnpy_counts", return_value={})
    @patch(f"{_MOD}.get_suspend_counts", return_value={})
    @patch(f"{_MOD}.get_suspend_d_counts", return_value={})
    @patch(f"{_MOD}.get_moneyflow_counts", return_value={})
    @patch(f"{_MOD}.get_bak_daily_counts", return_value={})
    @patch(f"{_MOD}.get_stock_monthly_counts", return_value={})
    @patch(f"{_MOD}.get_stock_weekly_counts", return_value={})
    @patch(f"{_MOD}.get_adj_factor_counts", return_value={})
    @patch(f"{_MOD}.get_stock_daily_counts", return_value={})
    @patch(f"{_MOD}.get_trade_calendar", return_value=[date(2024, 3, 15)])
    def test_success(self, mock_cal, mock_dc, mock_ac, mock_wc, mock_mc, mock_bc, mock_mfc, mock_sdc, mock_sc, mock_vc, mock_bulk):
        from app.datasync.service.data_sync_daemon import initialize_sync_status_table
        initialize_sync_status_table(lookback_years=1)
        mock_bulk.assert_called_once()
        rows = mock_bulk.call_args[0][0]
        # 1 date includes daily, bak_daily, moneyflow, suspend_d, suspend, adj_factor, weekly, monthly, vnpy, akshare, dividend, top10_holders
        assert len(rows) == 12

    @patch(f"{_MOD}.get_trade_calendar", return_value=[])
    def test_no_trade_days(self, mock_cal):
        from app.datasync.service.data_sync_daemon import initialize_sync_status_table
        initialize_sync_status_table(lookback_years=1)


# ═══ run_daily_job / run_backfill_job ══════════════════════════════════

class TestSchedulerJobs:
    @patch(f"{_MOD}.daily_ingest")
    def test_run_daily_job(self, mock_ingest):
        from app.datasync.service.data_sync_daemon import run_daily_job
        run_daily_job()
        mock_ingest.assert_called_once()

    @patch(f"{_MOD}.daily_ingest", side_effect=RuntimeError("err"))
    def test_run_daily_job_error(self, mock_ingest):
        from app.datasync.service.data_sync_daemon import run_daily_job
        run_daily_job()  # should not raise

    @patch(f"{_MOD}.missing_data_backfill")
    def test_run_backfill_job(self, mock_backfill):
        from app.datasync.service.data_sync_daemon import run_backfill_job
        run_backfill_job()
        mock_backfill.assert_called_once()

    @patch(f"{_MOD}.missing_data_backfill", side_effect=RuntimeError("err"))
    def test_run_backfill_job_error(self, mock_backfill):
        from app.datasync.service.data_sync_daemon import run_backfill_job
        run_backfill_job()  # should not raise


# ═══ sync_daily_for_date ══════════════════════════════════════════════

class TestSyncDailyForDate:
    @patch(f"{_MOD}.write_sync_log")
    @patch(f"{_MOD}.ingest_daily")
    @patch(f"{_MOD}.get_all_ts_codes", return_value=["000001.SZ"])
    def test_success(self, mock_codes, mock_ingest, mock_log):
        from app.datasync.service.data_sync_daemon import sync_daily_for_date
        sync_daily_for_date(date(2024, 3, 15))
        mock_ingest.assert_called_once()

    @patch(f"{_MOD}.write_sync_log")
    @patch(f"{_MOD}.ingest_daily", side_effect=RuntimeError("err"))
    @patch(f"{_MOD}.get_all_ts_codes", return_value=["000001.SZ"])
    def test_with_failures(self, mock_codes, mock_ingest, mock_log):
        from app.datasync.service.data_sync_daemon import sync_daily_for_date
        sync_daily_for_date(date(2024, 3, 15))
        # Should complete despite failure


# ═══ DataSyncDaemon ═══════════════════════════════════════════════════

class TestDataSyncDaemon:
    def test_find_missing_trade_dates(self):
        from app.datasync.service.data_sync_daemon import DataSyncDaemon
        result = DataSyncDaemon.find_missing_trade_dates(lookback_days=30)
        assert result == []
