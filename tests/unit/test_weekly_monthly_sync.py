"""Tests for Issue #10: Weekly/Monthly/Index Data Sync.

These tests mock the database engine at the infrastructure level to avoid
requiring actual MySQL connections during testing.
"""
import sys
import os
import pytest
from unittest.mock import patch, MagicMock
from datetime import date

import pandas as pd
import numpy as np

# Pre-mock akshare before any imports that depend on it
if 'akshare' not in sys.modules:
    sys.modules['akshare'] = MagicMock()


# ──────────────────────────────────────────────
# Mock infrastructure before importing datasync modules
# ──────────────────────────────────────────────

@pytest.fixture(autouse=True)
def mock_db_engines(monkeypatch):
    """Mock all DB engine factories to prevent real connections."""
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

    monkeypatch.setattr("app.infrastructure.db.connections.get_tushare_engine", lambda: mock_engine)
    monkeypatch.setattr("app.infrastructure.db.connections.get_quantmate_engine", lambda: mock_engine)
    monkeypatch.setattr("app.infrastructure.db.connections.get_vnpy_engine", lambda: mock_engine)
    monkeypatch.setattr("app.infrastructure.db.connections.get_akshare_engine", lambda: mock_engine)

    return mock_engine, mock_conn


# ──────────────────────────────────────────────
# DAO Tests
# ──────────────────────────────────────────────

class TestUpsertWeekly:
    """Test upsert_weekly DAO function."""

    def test_empty_df_returns_zero(self, mock_db_engines):
        from app.domains.extdata.dao.tushare_dao import upsert_weekly
        assert upsert_weekly(None) == 0
        assert upsert_weekly(pd.DataFrame()) == 0

    def test_upserts_rows(self, mock_db_engines):
        mock_engine, mock_conn = mock_db_engines
        from app.domains.extdata.dao import tushare_dao
        tushare_dao.engine = mock_engine

        df = pd.DataFrame([{
            'ts_code': '000001.SZ', 'trade_date': '20250101',
            'open': 10.0, 'high': 11.0, 'low': 9.5, 'close': 10.5,
            'pre_close': 10.0, 'change': 0.5, 'pct_chg': 5.0,
            'vol': 100000, 'amount': 1050000.0
        }])
        result = tushare_dao.upsert_weekly(df)
        assert result == 1
        mock_conn.execute.assert_called_once()


class TestUpsertMonthly:
    """Test upsert_monthly DAO function."""

    def test_empty_df_returns_zero(self, mock_db_engines):
        from app.domains.extdata.dao.tushare_dao import upsert_monthly
        assert upsert_monthly(None) == 0
        assert upsert_monthly(pd.DataFrame()) == 0

    def test_upserts_rows(self, mock_db_engines):
        mock_engine, mock_conn = mock_db_engines
        from app.domains.extdata.dao import tushare_dao
        tushare_dao.engine = mock_engine

        df = pd.DataFrame([{
            'ts_code': '000001.SZ', 'trade_date': '20250101',
            'open': 10.0, 'high': 12.0, 'low': 9.0, 'close': 11.0,
            'pre_close': 10.0, 'change': 1.0, 'pct_chg': 10.0,
            'vol': 500000, 'amount': 5500000.0
        }])
        result = tushare_dao.upsert_monthly(df)
        assert result == 1
        mock_conn.execute.assert_called_once()


class TestUpsertIndexWeekly:
    """Test upsert_index_weekly_df DAO function."""

    def test_empty_df_returns_zero(self, mock_db_engines):
        from app.domains.extdata.dao.tushare_dao import upsert_index_weekly_df
        assert upsert_index_weekly_df(None) == 0
        assert upsert_index_weekly_df(pd.DataFrame()) == 0

    def test_upserts_rows(self, mock_db_engines):
        mock_engine, mock_conn = mock_db_engines
        from app.domains.extdata.dao import tushare_dao
        tushare_dao.engine = mock_engine

        df = pd.DataFrame([{
            'index_code': '000001.SH', 'trade_date': '20250103',
            'open': 3000.0, 'high': 3100.0, 'low': 2950.0, 'close': 3050.0,
            'vol': 99999999, 'amount': 99999999.0
        }])
        result = tushare_dao.upsert_index_weekly_df(df)
        assert result == 1
        mock_conn.execute.assert_called_once()

    def test_ts_code_used_as_index_code(self, mock_db_engines):
        mock_engine, mock_conn = mock_db_engines
        from app.domains.extdata.dao import tushare_dao
        tushare_dao.engine = mock_engine

        df = pd.DataFrame([{
            'ts_code': '399001.SZ', 'trade_date': '20250103',
            'open': 10000.0, 'high': 10100.0, 'low': 9900.0, 'close': 10050.0,
            'vol': 88888888, 'amount': 88888888.0
        }])
        result = tushare_dao.upsert_index_weekly_df(df)
        assert result == 1
        call_args = mock_conn.execute.call_args
        params = call_args[0][1]
        assert params['index_code'] == '399001.SZ'


# ──────────────────────────────────────────────
# Helper Tests
# ──────────────────────────────────────────────

class TestCleanRound2Helpers:
    """Test the _clean and _round2 helpers."""

    def test_clean_none(self, mock_db_engines):
        from app.domains.extdata.dao.tushare_dao import _clean
        assert _clean(None) is None

    def test_clean_nan(self, mock_db_engines):
        from app.domains.extdata.dao.tushare_dao import _clean
        assert _clean(float('nan')) is None

    def test_clean_np_integer(self, mock_db_engines):
        from app.domains.extdata.dao.tushare_dao import _clean
        assert _clean(np.int64(42)) == 42
        assert isinstance(_clean(np.int64(42)), int)

    def test_clean_np_float(self, mock_db_engines):
        from app.domains.extdata.dao.tushare_dao import _clean
        assert _clean(np.float64(3.14)) == 3.14
        assert isinstance(_clean(np.float64(3.14)), float)

    def test_round2_none(self, mock_db_engines):
        from app.domains.extdata.dao.tushare_dao import _round2
        assert _round2(None) is None

    def test_round2_nan(self, mock_db_engines):
        from app.domains.extdata.dao.tushare_dao import _round2
        assert _round2(float('nan')) is None

    def test_round2_normal(self, mock_db_engines):
        from app.domains.extdata.dao.tushare_dao import _round2
        assert _round2(3.14159) == 3.14


# ──────────────────────────────────────────────
# Ingest Function Tests
# ──────────────────────────────────────────────

class TestIngestWeekly:
    """Test ingest_weekly function."""

    def test_ingest_weekly_success(self, mock_db_engines):
        with patch("app.datasync.service.tushare_ingest.upsert_weekly") as mock_upsert, \
             patch("app.datasync.service.tushare_ingest.call_pro") as mock_call, \
             patch("app.datasync.service.tushare_ingest.audit_finish") as mock_audit_finish, \
             patch("app.datasync.service.tushare_ingest.audit_start", return_value=1):

            from app.datasync.service.tushare_ingest import ingest_weekly
            df = pd.DataFrame([{'ts_code': '000001.SZ', 'trade_date': '20250101',
                                'open': 10.0, 'high': 11.0, 'low': 9.0, 'close': 10.5, 'vol': 100}])
            mock_call.return_value = df
            mock_upsert.return_value = 1

            result = ingest_weekly(ts_code='000001.SZ')
            assert result == 1
            mock_call.assert_called_once()
            mock_upsert.assert_called_once_with(df)
            mock_audit_finish.assert_called_once_with(1, 'success', 1)

    def test_ingest_weekly_failure_retries(self, mock_db_engines):
        with patch("app.datasync.service.tushare_ingest.call_pro", side_effect=Exception("API error")), \
             patch("app.datasync.service.tushare_ingest.audit_finish") as mock_audit_finish, \
             patch("app.datasync.service.tushare_ingest.audit_start", return_value=1), \
             patch.dict('os.environ', {'MAX_RETRIES': '1'}), \
             patch("time.sleep"):

            from app.datasync.service.tushare_ingest import ingest_weekly
            result = ingest_weekly()
            assert result == 0
            mock_audit_finish.assert_called_with(1, 'error', 0)


class TestIngestMonthly:
    """Test ingest_monthly function."""

    def test_ingest_monthly_success(self, mock_db_engines):
        with patch("app.datasync.service.tushare_ingest.upsert_monthly") as mock_upsert, \
             patch("app.datasync.service.tushare_ingest.call_pro") as mock_call, \
             patch("app.datasync.service.tushare_ingest.audit_finish") as mock_audit_finish, \
             patch("app.datasync.service.tushare_ingest.audit_start", return_value=2):

            from app.datasync.service.tushare_ingest import ingest_monthly
            df = pd.DataFrame([{'ts_code': '000001.SZ', 'trade_date': '20250101',
                                'open': 10.0, 'high': 12.0, 'low': 9.0, 'close': 11.0, 'vol': 500}])
            mock_call.return_value = df
            mock_upsert.return_value = 1

            result = ingest_monthly(ts_code='000001.SZ')
            assert result == 1
            mock_audit_finish.assert_called_once_with(2, 'success', 1)


class TestIngestIndexWeekly:
    """Test ingest_index_weekly function."""

    def test_ingest_index_weekly_success(self, mock_db_engines):
        with patch("app.datasync.service.tushare_ingest.upsert_index_weekly_df") as mock_upsert, \
             patch("app.datasync.service.tushare_ingest.call_pro") as mock_call, \
             patch("app.datasync.service.tushare_ingest.audit_finish") as mock_audit_finish, \
             patch("app.datasync.service.tushare_ingest.audit_start", return_value=3):

            from app.datasync.service.tushare_ingest import ingest_index_weekly
            df = pd.DataFrame([{
                'ts_code': '000001.SH', 'trade_date': '20250103',
                'open': 3000.0, 'high': 3100.0, 'low': 2950.0, 'close': 3050.0,
                'vol': 99999999, 'amount': 99999999.0
            }])
            mock_call.return_value = df
            mock_upsert.return_value = 1

            result = ingest_index_weekly(ts_code='000001.SH')
            assert result == 1
            actual_df = mock_upsert.call_args[0][0]
            assert 'index_code' in actual_df.columns

    def test_ingest_index_weekly_empty(self, mock_db_engines):
        with patch("app.datasync.service.tushare_ingest.upsert_index_weekly_df") as mock_upsert, \
             patch("app.datasync.service.tushare_ingest.call_pro") as mock_call, \
             patch("app.datasync.service.tushare_ingest.audit_finish"), \
             patch("app.datasync.service.tushare_ingest.audit_start", return_value=3):

            from app.datasync.service.tushare_ingest import ingest_index_weekly
            mock_call.return_value = pd.DataFrame()

            result = ingest_index_weekly(ts_code='000001.SH')
            assert result == 0
            mock_upsert.assert_not_called()


# ──────────────────────────────────────────────
# SyncStep Enum Tests
# ──────────────────────────────────────────────

class TestSyncStepEnum:
    """Test that new SyncStep values exist."""

    def test_new_steps_in_enum(self, mock_db_engines):
        from app.datasync.service.data_sync_daemon import SyncStep
        assert SyncStep.TUSHARE_STOCK_WEEKLY.value == 'tushare_stock_weekly'
        assert SyncStep.TUSHARE_STOCK_MONTHLY.value == 'tushare_stock_monthly'
        assert SyncStep.TUSHARE_INDEX_DAILY.value == 'tushare_index_daily'
        assert SyncStep.TUSHARE_INDEX_WEEKLY.value == 'tushare_index_weekly'

    def test_original_steps_still_exist(self, mock_db_engines):
        from app.datasync.service.data_sync_daemon import SyncStep
        assert SyncStep.AKSHARE_INDEX.value == 'akshare_index'
        assert SyncStep.TUSHARE_STOCK_DAILY.value == 'tushare_stock_daily'
        assert SyncStep.VNPY_SYNC.value == 'vnpy_sync'


# ──────────────────────────────────────────────
# Step Runner Tests
# ──────────────────────────────────────────────

class TestStepRunners:
    """Test the new step runner functions."""

    def test_run_stock_weekly_step(self, mock_db_engines):
        with patch("app.datasync.service.data_sync_daemon.ingest_weekly", return_value=50):
            from app.datasync.service.data_sync_daemon import run_tushare_stock_weekly_step, SyncStatus
            status, rows, err = run_tushare_stock_weekly_step(date(2025, 1, 3))
            assert status == SyncStatus.SUCCESS
            assert rows == 50
            assert err is None

    def test_run_stock_weekly_step_error(self, mock_db_engines):
        with patch("app.datasync.service.data_sync_daemon.ingest_weekly", side_effect=Exception("fail")):
            from app.datasync.service.data_sync_daemon import run_tushare_stock_weekly_step, SyncStatus
            status, rows, err = run_tushare_stock_weekly_step(date(2025, 1, 3))
            assert status == SyncStatus.ERROR
            assert rows == 0

    def test_run_stock_monthly_step(self, mock_db_engines):
        with patch("app.datasync.service.data_sync_daemon.ingest_monthly", return_value=30):
            from app.datasync.service.data_sync_daemon import run_tushare_stock_monthly_step, SyncStatus
            status, rows, err = run_tushare_stock_monthly_step(date(2025, 1, 31))
            assert status == SyncStatus.SUCCESS
            assert rows == 30

    def test_run_index_daily_step_success(self, mock_db_engines):
        with patch("app.datasync.service.data_sync_daemon.ingest_index_daily", return_value=5):
            from app.datasync.service.data_sync_daemon import run_tushare_index_daily_step, SyncStatus
            status, rows, err = run_tushare_index_daily_step(date(2025, 1, 6))
            assert status == SyncStatus.SUCCESS
            assert rows == 25  # 5 indices * 5 rows each

    def test_run_index_weekly_step_success(self, mock_db_engines):
        with patch("app.datasync.service.data_sync_daemon.ingest_index_weekly", return_value=3):
            from app.datasync.service.data_sync_daemon import run_tushare_index_weekly_step, SyncStatus
            status, rows, err = run_tushare_index_weekly_step(date(2025, 1, 3))
            assert status == SyncStatus.SUCCESS
            assert rows == 15  # 5 indices * 3 rows each

    def test_run_index_weekly_partial(self, mock_db_engines):
        with patch("app.datasync.service.data_sync_daemon.ingest_index_weekly") as mock_ingest:
            mock_ingest.side_effect = [2, 2, Exception("fail"), Exception("fail"), Exception("fail")]
            from app.datasync.service.data_sync_daemon import run_tushare_index_weekly_step, SyncStatus
            status, rows, err = run_tushare_index_weekly_step(date(2025, 1, 3))
            assert status == SyncStatus.PARTIAL
            assert rows == 4
            assert "Failed" in err


# ──────────────────────────────────────────────
# Migration SQL Tests
# ──────────────────────────────────────────────

class TestMigrationSQL:
    """Verify migration file content."""

    def test_migration_file_exists(self):
        path = os.path.join(os.path.dirname(__file__), '..', '..', 'mysql', 'migrations', '008_add_weekly_monthly_index_tables.sql')
        assert os.path.exists(path)

    def test_migration_contains_tables(self):
        path = os.path.join(os.path.dirname(__file__), '..', '..', 'mysql', 'migrations', '008_add_weekly_monthly_index_tables.sql')
        with open(path) as f:
            sql = f.read()
        assert 'stock_weekly' in sql
        assert 'stock_monthly' in sql
        assert 'index_weekly' in sql
        assert 'ALTER TABLE quantmate.data_sync_status' in sql
