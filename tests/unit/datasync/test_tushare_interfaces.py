"""Unit tests for app.datasync.sources.tushare.interfaces."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from app.datasync.base import SyncStatus

_INGEST = "app.datasync.service.tushare_ingest"
_STATUS_DAO = "app.domains.extdata.dao.data_sync_status_dao"
_TS_DAO = "app.domains.extdata.dao.tushare_dao"
_MOD = "app.datasync.sources.tushare.interfaces"


class TestIndexCodes:
    def test_index_codes_defined(self):
        from app.datasync.sources.tushare.interfaces import INDEX_CODES
        assert len(INDEX_CODES) >= 5


class TestTushareStockBasicInterface:
    def test_info(self):
        from app.datasync.sources.tushare.interfaces import TushareStockBasicInterface
        iface = TushareStockBasicInterface()
        info = iface.info
        assert info.interface_key == "stock_basic"
        assert info.source_key == "tushare"

    def test_get_ddl(self):
        from app.datasync.sources.tushare.interfaces import TushareStockBasicInterface
        assert "stock_basic" in TushareStockBasicInterface().get_ddl().lower()

    def test_sync_date_success(self):
        from app.datasync.sources.tushare.interfaces import TushareStockBasicInterface
        with patch(f"{_INGEST}.ingest_stock_basic") as m_ingest, \
             patch(f"{_STATUS_DAO}.get_stock_basic_count", return_value=5000):
            result = TushareStockBasicInterface().sync_date(date(2024, 1, 5))
            assert result.status == SyncStatus.SUCCESS
            assert result.rows_synced == 5000
            m_ingest.assert_called_once()

    def test_sync_date_error(self):
        from app.datasync.sources.tushare.interfaces import TushareStockBasicInterface
        with patch(f"{_INGEST}.ingest_stock_basic", side_effect=RuntimeError("fail")):
            result = TushareStockBasicInterface().sync_date(date(2024, 1, 5))
            assert result.status == SyncStatus.ERROR


class TestTushareStockDailyInterface:
    def test_info(self):
        from app.datasync.sources.tushare.interfaces import TushareStockDailyInterface
        info = TushareStockDailyInterface().info
        assert info.interface_key == "stock_daily"

    def test_sync_date_success(self):
        from app.datasync.sources.tushare.interfaces import TushareStockDailyInterface
        df = pd.DataFrame({"ts_code": ["000001.SZ"], "close": [10.0]})
        with patch(f"{_INGEST}.call_pro", return_value=df) as m_call, \
             patch(f"{_INGEST}.upsert_daily", return_value=1):
            result = TushareStockDailyInterface().sync_date(date(2024, 1, 5))
            assert result.status == SyncStatus.SUCCESS
            assert result.rows_synced == 1

    def test_sync_date_empty(self):
        from app.datasync.sources.tushare.interfaces import TushareStockDailyInterface
        with patch(f"{_INGEST}.call_pro", return_value=pd.DataFrame()), \
             patch(f"{_INGEST}.upsert_daily"):
            result = TushareStockDailyInterface().sync_date(date(2024, 1, 5))
            assert result.status == SyncStatus.SUCCESS
            assert result.rows_synced == 0

    def test_sync_date_error(self):
        from app.datasync.sources.tushare.interfaces import TushareStockDailyInterface
        with patch(f"{_INGEST}.call_pro", side_effect=RuntimeError("boom")):
            result = TushareStockDailyInterface().sync_date(date(2024, 1, 5))
            assert result.status == SyncStatus.ERROR


class TestTushareBakDailyInterface:
    def test_info(self):
        from app.datasync.sources.tushare.interfaces import TushareBakDailyInterface
        info = TushareBakDailyInterface().info
        assert info.interface_key == "bak_daily"

    def test_sync_date_success(self):
        from app.datasync.sources.tushare.interfaces import TushareBakDailyInterface
        with patch(f"{_INGEST}.ingest_bak_daily", return_value=12):
            result = TushareBakDailyInterface().sync_date(date(2024, 1, 5))
            assert result.status == SyncStatus.SUCCESS
            assert result.rows_synced == 12


class TestTushareSuspendDInterface:
    def test_sync_date_success(self):
        from app.datasync.sources.tushare.interfaces import TushareSuspendDInterface
        with patch(f"{_INGEST}.ingest_suspend_d", return_value=3):
            result = TushareSuspendDInterface().sync_date(date(2024, 1, 5))
            assert result.status == SyncStatus.SUCCESS
            assert result.rows_synced == 3


class TestTushareMoneyflowInterface:
    def test_sync_date_success(self):
        from app.datasync.sources.tushare.interfaces import TushareMoneyflowInterface
        df = pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20240105"]})
        with patch(f"{_INGEST}.call_pro", return_value=df), \
             patch(f"{_INGEST}.upsert_moneyflow", return_value=9):
            result = TushareMoneyflowInterface().sync_date(date(2024, 1, 5))
            assert result.status == SyncStatus.SUCCESS
            assert result.rows_synced == 9


class TestTushareSuspendInterface:
    def test_sync_date_success(self):
        from app.datasync.sources.tushare.interfaces import TushareSuspendInterface
        with patch(f"{_INGEST}.ingest_suspend", return_value=2):
            result = TushareSuspendInterface().sync_date(date(2024, 1, 5))
            assert result.status == SyncStatus.SUCCESS
            assert result.rows_synced == 2


class TestTushareAdjFactorInterface:
    def test_sync_date(self):
        from app.datasync.sources.tushare.interfaces import TushareAdjFactorInterface
        with patch(f"{_INGEST}.ingest_adj_factor") as m, \
             patch(f"{_STATUS_DAO}.get_adj_factor_count_for_date", return_value=200):
            result = TushareAdjFactorInterface().sync_date(date(2024, 1, 5))
            assert result.status == SyncStatus.SUCCESS
            assert result.rows_synced == 200
            m.assert_called_once()


class TestTushareDividendInterface:
    def test_sync_date_success(self):
        from app.datasync.sources.tushare.interfaces import TushareDividendInterface
        df = pd.DataFrame({"ts_code": ["000001.SZ"], "div_proc": ["实施"]})
        with patch(f"{_INGEST}.call_pro", return_value=df), \
             patch(f"{_TS_DAO}.upsert_dividend_df", return_value=1):
            result = TushareDividendInterface().sync_date(date(2024, 1, 5))
            assert result.status == SyncStatus.SUCCESS

    def test_sync_date_permission_denied(self):
        from app.datasync.sources.tushare.interfaces import TushareDividendInterface
        with patch(f"{_INGEST}.call_pro", side_effect=RuntimeError("没有接口访问权限")):
            result = TushareDividendInterface().sync_date(date(2024, 1, 5))
            assert result.status == SyncStatus.PARTIAL

    def test_sync_date_empty(self):
        from app.datasync.sources.tushare.interfaces import TushareDividendInterface
        with patch(f"{_INGEST}.call_pro", return_value=pd.DataFrame()):
            result = TushareDividendInterface().sync_date(date(2024, 1, 5))
            assert result.status == SyncStatus.SUCCESS
            assert result.rows_synced == 0


class TestTushareStockWeeklyInterface:
    def test_sync_date(self):
        from app.datasync.sources.tushare.interfaces import TushareStockWeeklyInterface
        with patch(f"{_INGEST}.ingest_weekly", return_value=10) as mock_ingest:
            result = TushareStockWeeklyInterface().sync_date(date(2024, 1, 5))
            assert result.status == SyncStatus.SUCCESS
            assert result.rows_synced == 10
            mock_ingest.assert_called_once_with(trade_date="20240105")


class TestTushareStockMonthlyInterface:
    def test_sync_date(self):
        from app.datasync.sources.tushare.interfaces import TushareStockMonthlyInterface
        with patch(f"{_INGEST}.ingest_monthly", return_value=5) as mock_ingest:
            result = TushareStockMonthlyInterface().sync_date(date(2024, 1, 5))
            assert result.status == SyncStatus.SUCCESS
            assert result.rows_synced == 5
            mock_ingest.assert_called_once_with(trade_date="20240105")


class TestTushareIndexDailyInterface:
    def test_sync_date_all_succeed(self):
        from app.datasync.sources.tushare.interfaces import TushareIndexDailyInterface
        with patch(f"{_INGEST}.ingest_index_daily", return_value=10):
            result = TushareIndexDailyInterface().sync_date(date(2024, 1, 5))
            assert result.status == SyncStatus.SUCCESS
            assert result.rows_synced == 50  # 5 codes * 10

    def test_sync_date_partial_failure(self):
        from app.datasync.sources.tushare.interfaces import TushareIndexDailyInterface
        call_count = 0

        def _side(ts_code, start_date, end_date):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("fail")
            return 5

        with patch(f"{_INGEST}.ingest_index_daily", side_effect=_side):
            result = TushareIndexDailyInterface().sync_date(date(2024, 1, 5))
            assert result.status == SyncStatus.PARTIAL

    def test_sync_date_all_fail(self):
        from app.datasync.sources.tushare.interfaces import TushareIndexDailyInterface
        with patch(f"{_INGEST}.ingest_index_daily", side_effect=RuntimeError("fail")):
            result = TushareIndexDailyInterface().sync_date(date(2024, 1, 5))
            assert result.status == SyncStatus.ERROR


class TestTushareIndexWeeklyInterface:
    def test_sync_date(self):
        from app.datasync.sources.tushare.interfaces import TushareIndexWeeklyInterface
        with patch(f"{_INGEST}.ingest_index_weekly", return_value=10):
            result = TushareIndexWeeklyInterface().sync_date(date(2024, 1, 5))
            assert result.status == SyncStatus.SUCCESS
            assert result.rows_synced == 50  # 5 codes * 10


class TestTushareTop10HoldersInterface:
    def test_sync_date_success(self):
        from app.datasync.sources.tushare.interfaces import TushareTop10HoldersInterface
        with patch(f"{_INGEST}.get_all_ts_codes", return_value=["000001.SZ"] * 100), \
             patch(f"{_INGEST}.ingest_top10_holders"):
            result = TushareTop10HoldersInterface().sync_date(date(2024, 1, 5))
            assert result.status == SyncStatus.SUCCESS
            assert result.rows_synced == 50  # min(50, 100) sampled

    def test_sync_date_partial(self):
        from app.datasync.sources.tushare.interfaces import TushareTop10HoldersInterface
        with patch(f"{_INGEST}.get_all_ts_codes", return_value=["000001.SZ"] * 100), \
             patch(f"{_INGEST}.ingest_top10_holders", side_effect=RuntimeError("fail")):
            result = TushareTop10HoldersInterface().sync_date(date(2024, 1, 5))
            assert result.status == SyncStatus.PARTIAL

    def test_sync_date_error(self):
        from app.datasync.sources.tushare.interfaces import TushareTop10HoldersInterface
        with patch(f"{_INGEST}.get_all_ts_codes", side_effect=RuntimeError("boom")):
            result = TushareTop10HoldersInterface().sync_date(date(2024, 1, 5))
            assert result.status == SyncStatus.ERROR
