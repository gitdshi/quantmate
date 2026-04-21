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

    def test_is_latest_only_for_backfill(self):
        from app.datasync.sources.tushare.interfaces import TushareStockBasicInterface

        assert TushareStockBasicInterface().supports_backfill() is False

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


class TestTushareTradeCalInterface:
    def test_info(self):
        from app.datasync.sources.tushare.interfaces import TushareTradeCalInterface

        info = TushareTradeCalInterface().info
        assert info.interface_key == "trade_cal"
        assert info.target_table == "trade_cal"

    def test_backfill_mode(self):
        from app.datasync.sources.tushare.interfaces import TushareTradeCalInterface

        assert TushareTradeCalInterface().backfill_mode() == "date"

    def test_sync_date_success(self):
        from app.datasync.sources.tushare.interfaces import TushareTradeCalInterface

        df = pd.DataFrame(
            {
                "exchange": ["SSE"],
                "cal_date": ["20240105"],
                "is_open": [1],
                "pretrade_date": ["20240104"],
            }
        )
        engine = MagicMock()
        begin_conn = MagicMock()
        engine.begin.return_value.__enter__.return_value = begin_conn

        with patch(f"{_INGEST}.call_pro", return_value=df), \
             patch(f"{_MOD}.get_tushare_engine", return_value=engine):
            result = TushareTradeCalInterface().sync_date(date(2024, 1, 5))

        assert result.status == SyncStatus.SUCCESS
        assert result.rows_synced == 1
        begin_conn.execute.assert_called_once()

    def test_get_backfill_rows_by_date(self):
        from app.datasync.sources.tushare.interfaces import TushareTradeCalInterface

        engine = MagicMock()
        connect_conn = MagicMock()
        connect_conn.execute.return_value.fetchall.return_value = [(date(2024, 1, 5), 1)]
        engine.connect.return_value.__enter__.return_value = connect_conn

        with patch(f"{_MOD}.get_tushare_engine", return_value=engine):
            counts = TushareTradeCalInterface().get_backfill_rows_by_date(date(2024, 1, 5), date(2024, 1, 8))

        assert counts == {date(2024, 1, 5): 1}


class TestTushareStockDailyInterface:
    def test_info(self):
        from app.datasync.sources.tushare.interfaces import TushareStockDailyInterface
        info = TushareStockDailyInterface().info
        assert info.interface_key == "stock_daily"

    def test_requires_nonempty_trading_day_data(self):
        from app.datasync.sources.tushare.interfaces import TushareStockDailyInterface

        assert TushareStockDailyInterface().requires_nonempty_trading_day_data() is True

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

    def test_sync_date_quota_returns_pending(self):
        from app.datasync.service.tushare_ingest import TushareQuotaExceededError
        from app.datasync.sources.tushare.interfaces import TushareStockDailyInterface

        with patch(
            f"{_INGEST}.call_pro",
            side_effect=TushareQuotaExceededError("daily", "daily quota", scope="day"),
        ):
            result = TushareStockDailyInterface().sync_date(date(2024, 1, 5))

        assert result.status == SyncStatus.PENDING
        assert result.details["quota_exceeded"] is True
        assert result.details["quota_scope"] == "day"


class TestTushareBakDailyInterface:
    def test_info(self):
        from app.datasync.sources.tushare.interfaces import TushareBakDailyInterface
        info = TushareBakDailyInterface().info
        assert info.interface_key == "bak_daily"

    def test_requires_nonempty_trading_day_data(self):
        from app.datasync.sources.tushare.interfaces import TushareBakDailyInterface

        assert TushareBakDailyInterface().requires_nonempty_trading_day_data() is True

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
    def test_requires_nonempty_trading_day_data(self):
        from app.datasync.sources.tushare.interfaces import TushareMoneyflowInterface

        assert TushareMoneyflowInterface().requires_nonempty_trading_day_data() is True

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
    def test_requires_nonempty_trading_day_data(self):
        from app.datasync.sources.tushare.interfaces import TushareAdjFactorInterface

        assert TushareAdjFactorInterface().requires_nonempty_trading_day_data() is True

    def test_sync_date(self):
        from app.datasync.sources.tushare.interfaces import TushareAdjFactorInterface
        with patch(f"{_INGEST}.ingest_adj_factor") as m, \
             patch(f"{_STATUS_DAO}.get_adj_factor_count_for_date", return_value=200):
            result = TushareAdjFactorInterface().sync_date(date(2024, 1, 5))
            assert result.status == SyncStatus.SUCCESS
            assert result.rows_synced == 200
            m.assert_called_once()


class TestTushareDividendInterface:
    def test_backfill_mode(self):
        from app.datasync.sources.tushare.interfaces import TushareDividendInterface
        assert TushareDividendInterface().backfill_mode() == "range"

    def test_sync_date_success(self):
        from app.datasync.sources.tushare.interfaces import TushareDividendInterface
        df = pd.DataFrame({"ts_code": ["000001.SZ"], "div_proc": ["实施"]})
        with patch(f"{_INGEST}.call_pro", return_value=df), \
             patch(f"{_TS_DAO}.upsert_dividend_df", return_value=1):
            result = TushareDividendInterface().sync_date(date(2024, 1, 5))
            assert result.status == SyncStatus.SUCCESS

    def test_sync_range_success(self):
        from app.datasync.sources.tushare.interfaces import TushareDividendInterface
        with patch(f"{_INGEST}.ingest_dividend_by_ann_date_range", return_value=7) as mock_ingest:
            result = TushareDividendInterface().sync_range(date(2024, 1, 5), date(2024, 1, 8))
            assert result.status == SyncStatus.SUCCESS
            assert result.rows_synced == 7
            mock_ingest.assert_called_once_with("2024-01-05", "2024-01-08")

    def test_get_backfill_rows_by_date(self):
        from app.datasync.sources.tushare.interfaces import TushareDividendInterface
        counts = {date(2024, 1, 5): 2}
        with patch(f"{_STATUS_DAO}.get_dividend_counts", return_value=counts):
            result = TushareDividendInterface().get_backfill_rows_by_date(date(2024, 1, 5), date(2024, 1, 8))
        assert result == counts

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
    def test_requires_nonempty_trading_day_data(self):
        from app.datasync.sources.tushare.interfaces import TushareIndexDailyInterface

        assert TushareIndexDailyInterface().requires_nonempty_trading_day_data() is True

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
    def test_backfill_mode(self):
        from app.datasync.sources.tushare.interfaces import TushareTop10HoldersInterface
        assert TushareTop10HoldersInterface().backfill_mode() == "range"

    def test_sync_date_success(self):
        from app.datasync.sources.tushare.interfaces import TushareTop10HoldersInterface
        with patch(f"{_INGEST}.get_all_ts_codes", return_value=["000001.SZ"] * 100), \
             patch(f"{_INGEST}.ingest_top10_holders"):
            result = TushareTop10HoldersInterface().sync_date(date(2024, 1, 5))
            assert result.status == SyncStatus.SUCCESS
            assert result.rows_synced == 50  # min(50, 100) sampled

    def test_sync_range_success(self):
        from app.datasync.sources.tushare.interfaces import TushareTop10HoldersInterface
        with patch(f"{_INGEST}.ingest_top10_holders_marketwide_by_date_range", return_value=11) as mock_ingest:
            result = TushareTop10HoldersInterface().sync_range(date(2024, 1, 5), date(2024, 1, 8))
            assert result.status == SyncStatus.SUCCESS
            assert result.rows_synced == 11
            mock_ingest.assert_called_once_with("2024-01-05", "2024-01-08")

    def test_get_backfill_rows_by_date(self):
        from app.datasync.sources.tushare.interfaces import TushareTop10HoldersInterface
        counts = {date(2024, 1, 5): 3}
        with patch(f"{_STATUS_DAO}.get_top10_holders_counts", return_value=counts):
            result = TushareTop10HoldersInterface().get_backfill_rows_by_date(date(2024, 1, 5), date(2024, 1, 8))
        assert result == counts

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


class TestTushareCatalogInterface:
    def test_trade_date_catalog_sync(self):
        from app.datasync.sources.tushare.catalog_interfaces import TushareCatalogInterface, TushareCatalogSpec

        iface = TushareCatalogInterface(
            TushareCatalogSpec(
                interface_key="block_trade",
                display_name="大宗交易",
                api_name="block_trade",
                target_table="block_trade",
                sync_priority=90,
            )
        )

        df = pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20240105"]})
        with patch("app.datasync.service.tushare_ingest.call_pro", return_value=df) as mock_call, \
             patch("app.domains.extdata.dao.tushare_dao.insert_catalog_rows", return_value=1) as mock_insert:
            result = iface.sync_date(date(2024, 1, 5))

        assert iface.supports_backfill() is True
        assert iface.supports_scheduled_sync() is True
        assert iface.backfill_mode() == "date"
        assert iface.requires_nonempty_trading_day_data() is False
        assert result.status == SyncStatus.SUCCESS
        assert result.rows_synced == 1
        mock_call.assert_called_once_with("block_trade", trade_date="20240105")
        mock_insert.assert_called_once()

    def test_runtime_unsupported_catalog_item_disables_scheduled_sync(self):
        from app.datasync.sources.tushare.catalog_interfaces import TushareCatalogInterface, TushareCatalogSpec

        iface = TushareCatalogInterface(
            TushareCatalogSpec(
                interface_key="fund_portfolio",
                display_name="公募基金持仓数据",
                api_name="fund_portfolio",
                target_table="fund_portfolio",
                sync_priority=515,
            )
        )

        assert iface.supports_scheduled_sync() is False
        assert iface.supports_backfill() is False

    @pytest.mark.parametrize(
        ("interface_key", "api_name"),
        [
            ("index_dailybasic", "index_dailybasic"),
            ("stk_factor_pro", "stk_factor_pro"),
        ],
    )
    def test_trade_date_catalog_items_are_scheduler_enabled(self, interface_key, api_name):
        from app.datasync.sources.tushare.catalog_interfaces import TushareCatalogInterface, TushareCatalogSpec

        iface = TushareCatalogInterface(
            TushareCatalogSpec(
                interface_key=interface_key,
                display_name=interface_key,
                api_name=api_name,
                target_table=interface_key,
                sync_priority=999,
            )
        )

        df = pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20240105"]})
        with patch("app.datasync.service.tushare_ingest.call_pro", return_value=df) as mock_call, \
             patch("app.domains.extdata.dao.tushare_dao.insert_catalog_rows", return_value=1):
            result = iface.sync_date(date(2024, 1, 5))

        assert iface.supports_scheduled_sync() is True
        assert iface.supports_backfill() is True
        assert iface.backfill_mode() == "date"
        assert iface.requires_nonempty_trading_day_data() is True
        assert result.status == SyncStatus.SUCCESS
        assert result.rows_synced == 1
        mock_call.assert_called_once_with(api_name, trade_date="20240105")

    @pytest.mark.parametrize(
        ("interface_key", "api_name"),
        [
            ("pledge_detail", "pledge_detail"),
            ("fund_nav", "fund_nav"),
            ("fund_portfolio", "fund_portfolio"),
            ("index_weight", "index_weight"),
        ],
    )
    def test_known_generic_unsupported_catalog_items_are_scheduler_disabled(self, interface_key, api_name):
        from app.datasync.sources.tushare.catalog_interfaces import TushareCatalogInterface, TushareCatalogSpec

        iface = TushareCatalogInterface(
            TushareCatalogSpec(
                interface_key=interface_key,
                display_name=interface_key,
                api_name=api_name,
                target_table=interface_key,
                sync_priority=999,
            )
        )

        assert iface.supports_scheduled_sync() is False
        assert iface.supports_backfill() is False

    def test_range_catalog_sync_uses_range_mode(self):
        from app.datasync.sources.tushare.catalog_interfaces import TushareCatalogInterface, TushareCatalogSpec

        iface = TushareCatalogInterface(
            TushareCatalogSpec(
                interface_key="new_share",
                display_name="IPO新股列表",
                api_name="new_share",
                target_table="new_share",
                sync_priority=104,
            )
        )

        df = pd.DataFrame({"ts_code": ["000001.SZ"], "ipo_date": ["20240105"]})
        with patch("app.datasync.service.tushare_ingest.call_pro", return_value=df) as mock_call, \
             patch("app.domains.extdata.dao.tushare_dao.insert_catalog_rows", return_value=2):
            result = iface.sync_range(date(2024, 1, 5), date(2024, 1, 8))

        assert iface.supports_backfill() is True
        assert iface.backfill_mode() == "range"
        assert result.status == SyncStatus.SUCCESS
        assert result.rows_synced == 2
        mock_call.assert_called_once_with("new_share", start_date="20240105", end_date="20240108")

    def test_catalog_permission_denied_returns_partial(self):
        from app.datasync.sources.tushare.catalog_interfaces import TushareCatalogInterface, TushareCatalogSpec

        iface = TushareCatalogInterface(
            TushareCatalogSpec(
                interface_key="report_rc",
                display_name="盈利预测数据",
                api_name="report_rc",
                target_table="report_rc",
                sync_priority=315,
            )
        )

        with patch("app.datasync.service.tushare_ingest.call_pro", side_effect=RuntimeError("没有接口访问权限")):
            result = iface.sync_date(date(2024, 1, 5))

        assert result.status == SyncStatus.PARTIAL

    def test_catalog_quota_returns_pending(self):
        from app.datasync.service.tushare_ingest import TushareQuotaExceededError
        from app.datasync.sources.tushare.catalog_interfaces import TushareCatalogInterface, TushareCatalogSpec

        iface = TushareCatalogInterface(
            TushareCatalogSpec(
                interface_key="report_rc",
                display_name="盈利预测数据",
                api_name="report_rc",
                target_table="report_rc",
                sync_priority=315,
            )
        )

        with patch(
            "app.datasync.service.tushare_ingest.call_pro",
            side_effect=TushareQuotaExceededError("report_rc", "daily quota", scope="day"),
        ):
            result = iface.sync_date(date(2024, 1, 5))

        assert result.status == SyncStatus.PENDING
        assert result.details["quota_exceeded"] is True

    def test_build_catalog_interfaces_skips_existing_keys(self):
        from app.datasync.sources.tushare.catalog_interfaces import build_catalog_interfaces

        interfaces = build_catalog_interfaces({"hsgt_stk_hold", "moneyflow"})
        keys = {iface.info.interface_key for iface in interfaces}

        assert "hsgt_stk_hold" not in keys
        assert "moneyflow" not in keys
        assert "suspend_daily" not in keys
        assert "stock_st" in keys
