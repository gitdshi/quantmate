"""Unit tests for init_market_data checkpoint/resume logic.

Target: P1-DSYNC-CODE-001 - Init 断点续跑 + 自适应限流实现
"""

import os
import pytest
from unittest.mock import Mock, call, patch
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


class TestEnsureSourceItemTables:
    def test_ensure_source_item_table_uses_registry_ddl(self):
        dao = Mock()
        dao.get_by_key.return_value = {'target_database': 'tushare', 'target_table': 'stock_basic'}
        registry = Mock()
        iface = Mock()
        iface.info.target_database = 'tushare'
        iface.info.target_table = 'stock_basic'
        iface.get_ddl.return_value = 'CREATE TABLE stock_basic (...)'
        registry.get_interface.return_value = iface

        with patch('app.domains.market.dao.data_source_item_dao.DataSourceItemDao', return_value=dao), \
             patch('app.datasync.registry.build_default_registry', return_value=registry), \
             patch('app.datasync.table_manager.ensure_table') as mock_ensure:
            imd.ensure_source_item_table('tushare', 'stock_basic')

        mock_ensure.assert_called_once_with('tushare', 'stock_basic', 'CREATE TABLE stock_basic (...)')

    def test_ensure_source_item_tables_iterates_all_items(self):
        with patch.object(imd, 'ensure_source_item_table') as mock_ensure:
            imd.ensure_source_item_tables('tushare', 'stock_daily', 'stock_basic')

        assert mock_ensure.call_args_list == [
            call('tushare', 'stock_daily'),
            call('tushare', 'stock_basic'),
        ]


class TestInitSyncStatusBootstrap:
    def test_bootstrap_uses_daily_and_aux_windows(self):
        with patch('app.domains.extdata.dao.data_sync_status_dao.ensure_tables') as mock_ensure_tables, \
             patch('app.datasync.service.sync_init_service.initialize_sync_status') as mock_initialize, \
             patch.object(imd, '_ensure_pending_sync_status_rows') as mock_pending:
            imd.bootstrap_init_sync_status('2025-01-01', '2025-04-15', '2026-04-17', skip_aux=True)

        mock_ensure_tables.assert_called_once()
        assert call('tushare', 'stock_basic', start_date=date(2026, 4, 17), end_date=date(2026, 4, 17), reconcile_missing=True) in mock_initialize.call_args_list
        assert call('tushare', 'stock_daily', start_date=date(2025, 4, 15), end_date=date(2026, 4, 17), reconcile_missing=True) in mock_initialize.call_args_list
        assert call('akshare', 'index_daily', start_date=date(2025, 4, 15), end_date=date(2026, 4, 17), reconcile_missing=True) in mock_initialize.call_args_list
        assert all(args.args[:2] != ('tushare', 'dividend') for args in mock_initialize.call_args_list)
        mock_pending.assert_any_call('tushare', 'stock_basic', [date(2026, 4, 17)])
        mock_pending.assert_any_call('tushare', 'stock_company', [date(2026, 4, 17)])


class TestPhaseProgressCallback:
    def test_marks_running_for_cursor_date(self):
        with patch.object(imd, 'save_progress') as mock_save, \
             patch('app.datasync.service.sync_engine._write_status') as mock_write:
            callback = imd.build_phase_progress_callback('daily', source='tushare', item_key='stock_daily')
            callback(cursor_date='2025-04-17')

        mock_save.assert_called_once_with('daily', 'running', cursor_ts_code=None, cursor_date='2025-04-17')
        mock_write.assert_called_once_with(date(2025, 4, 17), 'tushare', 'stock_daily', 'running')


class TestFinalizeInitPhaseSyncStatus:
    def test_finalizes_latest_only_phase(self):
        registry = Mock()
        iface = Mock()
        iface.info.source_key = 'tushare'
        iface.info.interface_key = 'stock_basic'
        registry.get_interface.return_value = iface

        with patch.object(imd, '_get_status_registry', return_value=registry), \
             patch.object(imd, '_get_table_row_count', return_value=42), \
             patch('app.datasync.service.sync_engine._normalize_zero_row_success', side_effect=lambda iface, sync_date, source, item_key, result: result), \
             patch('app.datasync.service.sync_engine._write_status') as mock_write:
            imd.finalize_init_phase_sync_status('stock_basic', '2025-01-01', '2025-04-15', '2026-04-17')

        mock_write.assert_called_once_with(date(2026, 4, 17), 'tushare', 'stock_basic', 'success', 42, None)

    def test_finalizes_trade_date_phase_across_trade_calendar(self):
        registry = Mock()
        iface = Mock()
        iface.info.source_key = 'tushare'
        iface.info.interface_key = 'stock_daily'
        registry.get_interface.return_value = iface
        trade_days = [date(2025, 4, 15), date(2025, 4, 16)]

        with patch.object(imd, '_get_status_registry', return_value=registry), \
             patch.object(imd, '_get_table_counts_by_date', return_value={date(2025, 4, 15): 10, date(2025, 4, 16): 12}), \
             patch.object(imd, '_ensure_pending_sync_status_rows') as mock_pending, \
             patch('app.datasync.service.sync_engine.get_trade_calendar', return_value=trade_days), \
             patch('app.datasync.service.sync_engine._normalize_zero_row_success', side_effect=lambda iface, sync_date, source, item_key, result: result), \
             patch('app.datasync.service.sync_engine._write_status') as mock_write:
            imd.finalize_init_phase_sync_status('daily', '2025-01-01', '2025-04-15', '2025-04-16')

        mock_pending.assert_called_once_with('tushare', 'stock_daily', trade_days)
        assert mock_write.call_args_list == [
            call(date(2025, 4, 15), 'tushare', 'stock_daily', 'success', 10, None),
            call(date(2025, 4, 16), 'tushare', 'stock_daily', 'success', 12, None),
        ]


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
                         patch.object(imd, 'bootstrap_init_sync_status'), \
             patch.object(imd, 'ensure_init_progress_table'), \
                         patch.object(imd, 'ensure_source_item_tables'), \
             patch.object(imd, 'load_progress', side_effect=[None, {'phase': 'bak_daily', 'cursor_date': '2025-06-20'}]), \
             patch.object(imd, 'get_tushare_row_count', return_value=1), \
             patch.object(imd, 'get_loaded_trade_dates', return_value=['2025-06-19', '2025-06-20']), \
             patch.object(imd, 'should_run_phase', side_effect=lambda progress, phase, resume: phase == 'bak_daily'), \
             patch.object(imd, 'ingest_bak_daily_by_trade_dates', side_effect=imd.TushareQuotaExceededError('bak_daily', 'daily quota', scope='day')), \
                         patch.object(imd, 'mark_init_phase_status_from_exception') as mock_mark_status, \
             patch.object(imd, 'save_progress') as save_progress, \
             patch.object(imd, 'print_summary'):
            result = imd.main()

        assert result == 0
        assert save_progress.call_args_list[0].args[:2] == ('bak_daily', 'running')
        assert save_progress.call_args_list[-1].args[:2] == ('bak_daily', 'paused')
        assert save_progress.call_args_list[-1].kwargs['cursor_date'] == '2025-06-20'
        mock_mark_status.assert_called_once()


class TestAkshareWindowAlignment:
    def test_indexes_phase_uses_same_daily_start_as_tushare(self):
        argv = ['init_market_data.py', '--skip-schema', '--skip-aux', '--skip-vnpy']
        parsed_args = imd.argparse.Namespace(
            start_date='2010-01-01',
            skip_schema=True,
            skip_aux=True,
            skip_vnpy=True,
            stock_statuses='L',
            batch_size=100,
            sleep_between=0.02,
            daily_start_date='2024-01-15',
            daily_lookback_days=None,
            resume=True,
            reset_progress=False,
        )

        with patch('sys.argv', argv), \
             patch.object(imd.argparse.ArgumentParser, 'parse_args', return_value=parsed_args), \
             patch.object(imd, 'bootstrap_init_sync_status'), \
             patch.object(imd, 'ensure_init_progress_table'), \
             patch.object(imd, 'ensure_source_item_tables'), \
             patch.object(imd, 'load_progress', return_value=None), \
             patch.object(imd, 'should_run_phase', side_effect=lambda progress, phase, resume: phase == 'indexes'), \
             patch.object(imd, 'ingest_all_indexes') as ingest_all_indexes, \
             patch.object(imd, 'finalize_init_phase_sync_status') as mock_finalize, \
             patch.object(imd, 'save_progress'), \
             patch.object(imd, 'print_summary'):
            result = imd.main()

        assert result == 0
        ingest_all_indexes.assert_called_once_with(start_date='2024-01-15')
        mock_finalize.assert_called_once_with('indexes', '2010-01-01', '2024-01-15', imd.date.today().isoformat())


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
