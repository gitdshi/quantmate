"""Unit tests for init_market_data checkpoint/resume logic.

Target: P1-DSYNC-CODE-001 - Init 断点续跑 + 自适应限流实现
"""

import os
import pytest
from unittest.mock import Mock, patch
from datetime import date, datetime

# Set required environment variables before importing
os.environ.setdefault('MYSQL_URL', 'mysql+pymysql://test:test@localhost:3306')
os.environ.setdefault('SECRET_KEY', 'test-secret-key-for-tests')
os.environ.setdefault('MYSQL_PASSWORD', 'test-password')
os.environ.setdefault('TUSHARE_DATABASE_URL', 'mysql+pymysql://test:test@localhost:3306/testdb')
os.environ.setdefault('TUSHARE_TOKEN', 'test_token_abc123')

# Import module under test using importlib to avoid import errors with missing DB
import importlib

imd = importlib.import_module("app.datasync.cli.init_market_data")


class TestPhaseRank:
    """Test phase_rank ordering matches PHASES list."""

    def test_phase_order(self):
        expected_order = [
            'schema', 'stock_basic', 'stock_company', 'new_share', 'daily', 'weekly', 'monthly', 'indexes', 'adj_factor',
            'dividend', 'top10_holders', 'bak_daily', 'moneyflow', 'suspend_d', 'suspend',
            'fina_indicator', 'income', 'balancesheet', 'cashflow', 'vnpy', 'sync_status', 'finished'
        ]
        for i, phase in enumerate(expected_order):
            assert imd.phase_rank(phase) == i

    def test_unknown_phase(self):
        assert imd.phase_rank('unknown') == -1


class TestShouldRunPhase:
    """Test should_run_phase determines correct execution flow."""

    def test_no_progress_resume_true(self):
        """When resume=True but no previous progress, should run all phases."""
        assert imd.should_run_phase(None, 'stock_basic', resume=True) is True
        assert imd.should_run_phase(None, 'daily', resume=True) is True

    def test_resume_false_always_runs(self):
        """When resume=False, should always run regardless of progress."""
        progress = {'phase': 'stock_basic', 'status': 'completed'}
        assert imd.should_run_phase(progress, 'stock_basic', resume=False) is True
        assert imd.should_run_phase(progress, 'daily', resume=False) is True

    def test_completed_phase_skipped(self):
        """A phase with status='completed' should not run again."""
        progress = {'phase': 'stock_basic', 'status': 'completed'}
        assert imd.should_run_phase(progress, 'stock_basic', resume=True) is False

    def test_incomplete_phase_continues(self):
        """If phase was incomplete (error or running), should run again."""
        progress = {'phase': 'stock_basic', 'status': 'error'}
        assert imd.should_run_phase(progress, 'stock_basic', resume=True) is True
        progress = {'phase': 'stock_basic', 'status': 'running'}
        assert imd.should_run_phase(progress, 'stock_basic', resume=True) is True
        progress = {'phase': 'stock_basic', 'status': 'paused'}
        assert imd.should_run_phase(progress, 'stock_basic', resume=True) is True

    def test_subsequent_phases_run(self):
        """Phases after saved phase should run."""
        progress = {'phase': 'stock_basic', 'status': 'completed'}
        assert imd.should_run_phase(progress, 'daily', resume=True) is True
        assert imd.should_run_phase(progress, 'indexes', resume=True) is True

    def test_prior_phases_skipped(self):
        """Phases before saved phase should NOT run (can't go backwards)."""
        progress = {'phase': 'daily', 'status': 'completed'}
        assert imd.should_run_phase(progress, 'stock_basic', resume=True) is False

    def test_future_phases_from_middle(self):
        """From a middle phase, later phases should run."""
        progress = {'phase': 'daily', 'status': 'completed'}
        assert imd.should_run_phase(progress, 'indexes', resume=True) is True
        assert imd.should_run_phase(progress, 'adj_factor', resume=True) is True

    def test_finished_phase_blocks_all(self):
        """If finished, no phases should run."""
        progress = {'phase': 'finished', 'status': 'completed'}
        assert imd.should_run_phase(progress, 'stock_basic', resume=True) is False
        assert imd.should_run_phase(progress, 'daily', resume=True) is False


class TestSelectPeriodEndTradeDates:
    def test_weekly_selects_last_trade_date_in_week(self):
        trade_dates = [
            '2025-04-28',
            '2025-04-29',
            '2025-04-30',
            '2025-05-06',
            '2025-05-07',
            '2025-05-09',
        ]
        assert imd.select_period_end_trade_dates(trade_dates, 'weekly') == ['2025-04-30', '2025-05-09']

    def test_monthly_selects_last_trade_date_in_month(self):
        trade_dates = [
            '2025-04-28',
            '2025-04-30',
            '2025-05-06',
            '2025-05-29',
            '2025-05-30',
        ]
        assert imd.select_period_end_trade_dates(trade_dates, 'monthly') == ['2025-04-30', '2025-05-30']

    def test_rejects_unknown_period(self):
        with pytest.raises(ValueError, match='Unsupported period'):
            imd.select_period_end_trade_dates(['2025-04-30'], 'quarterly')


class TestMainQuotaPause:
    def test_main_marks_progress_paused_on_quota_exhaustion(self):
        argv = ['init_market_data.py', '--skip-schema', '--skip-vnpy']
        parsed_args = imd.argparse.Namespace(
            start_date='2025-04-15',
            skip_schema=True,
            skip_aux=False,
            skip_vnpy=True,
            stock_statuses='L',
            batch_size=100,
            sleep_between=0.02,
            daily_start_date='2025-04-15',
            daily_lookback_days=None,
            resume=True,
            reset_progress=False,
        )

        with patch('sys.argv', argv), \
             patch.object(imd.argparse.ArgumentParser, 'parse_args', return_value=parsed_args), \
             patch.object(imd, 'ensure_init_progress_table'), \
             patch.object(imd, 'load_progress', side_effect=[None, {'phase': 'bak_daily', 'cursor_date': '2025-06-20'}]), \
             patch.object(imd, 'get_tushare_row_count', return_value=1), \
             patch.object(imd, 'get_loaded_trade_dates', return_value=['2025-06-19', '2025-06-20']), \
             patch.object(imd, 'should_run_phase', side_effect=lambda progress, phase, resume: phase == 'bak_daily'), \
             patch.object(imd, 'ingest_bak_daily_by_trade_dates', side_effect=imd.TushareQuotaExceededError('bak_daily', 'daily quota', scope='day')), \
             patch.object(imd, 'save_progress') as save_progress, \
             patch.object(imd, 'print_summary'):
            result = imd.main()

        assert result == 0
        assert save_progress.call_args_list[0].args[:2] == ('bak_daily', 'running')
        assert save_progress.call_args_list[-1].args[:2] == ('bak_daily', 'paused')
        assert save_progress.call_args_list[-1].kwargs['cursor_date'] == '2025-06-20'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
