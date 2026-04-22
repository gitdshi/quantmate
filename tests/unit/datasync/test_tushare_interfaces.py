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
_CATALOG_MOD = "app.datasync.sources.tushare.catalog_interfaces"


def _catalog_spec(interface_key: str, api_name: str | None = None, *, sync_priority: int = 100):
    from app.datasync.sources.tushare.catalog_interfaces import TushareCatalogSpec

    return TushareCatalogSpec(
        interface_key=interface_key,
        display_name=interface_key,
        api_name=api_name or interface_key,
        target_table=interface_key,
        sync_priority=sync_priority,
        requires_permission="0",
    )


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


class TestTushareDynamicCatalogSchema:
    def test_infer_dynamic_table_schema_uses_date_and_code_key(self):
        from app.datasync.sources.tushare import ddl

        schema = ddl.infer_dynamic_table_schema(
            "report_rc",
            pd.DataFrame(
                {
                    "ts_code": ["000001.SZ"],
                    "ann_date": ["20240105"],
                    "name": ["Ping An"],
                    "eps": [1.23],
                }
            ),
            preferred_date_column="ann_date",
            preferred_key_fields=("ts_code",),
        )

        assert schema["key_columns"] == ("ann_date", "ts_code")
        assert "UNIQUE KEY" in schema["ddl"]
        assert "`ann_date` DATE NOT NULL" in schema["ddl"]
        assert "`ts_code` VARCHAR(32) NOT NULL" in schema["ddl"]

    def test_infer_dynamic_table_schema_uses_deterministic_date_priority_without_preference(self):
        from app.datasync.sources.tushare import ddl

        schema = ddl.infer_dynamic_table_schema(
            "example_dynamic",
            pd.DataFrame(
                {
                    "ts_code": ["000001.SZ"],
                    "ann_date": ["20240105"],
                    "end_date": ["20231231"],
                    "value": [1],
                }
            ),
        )

        assert schema["key_columns"] == ("ann_date", "ts_code")

    @pytest.mark.parametrize(
        ("api_name", "table_name"),
        [
            ("namechange", "namechange"),
            ("repurchase", "repurchase"),
        ],
    )
    def test_catalog_schema_overrides_key_date_column(self, api_name: str, table_name: str):
        from app.datasync.sources.tushare import ddl
        from app.datasync.sources.tushare.catalog_interfaces import TushareCatalogInterface, TushareCatalogSpec

        iface = TushareCatalogInterface(
            TushareCatalogSpec(
                interface_key=api_name,
                display_name=api_name,
                api_name=api_name,
                target_table=table_name,
                sync_priority=100,
            )
        )

        schema = ddl.infer_dynamic_table_schema(
            table_name,
            pd.DataFrame(
                {
                    "ts_code": ["000001.SZ"],
                    "ann_date": ["20240105"],
                    "end_date": ["20240131"],
                    "value": [1],
                }
            ),
            preferred_date_column=iface._schema_date_column(),
            preferred_key_fields=iface._payload_key_fields(),
        )

        assert iface._schema_date_column() == "end_date"
        assert schema["key_columns"] == ("end_date", "ts_code")

    def test_dynamic_catalog_skips_precreate_and_ensures_inferred_schema_on_sync(self):
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
        df = pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "ann_date": ["20240105"],
                "name": ["Ping An"],
                "eps": [1.23],
            }
        )

        with patch("app.datasync.service.tushare_ingest.call_pro", return_value=df), \
             patch("app.datasync.table_manager.ensure_inferred_table") as ensure_inferred_table, \
             patch("app.domains.extdata.dao.tushare_dao.insert_catalog_rows", return_value=1) as insert_catalog_rows:
            result = iface.sync_date(date(2024, 1, 5))

        assert iface.should_ensure_table_before_sync() is False
        assert result.status == SyncStatus.SUCCESS
        ensure_inferred_table.assert_called_once()
        _, insert_kwargs = insert_catalog_rows.call_args
        assert insert_kwargs["key_columns"] == ("ann_date", "ts_code")
        assert insert_kwargs["column_specs"]


class TestTushareSuspendDInterface:
    def test_sync_date_success(self):
        from app.datasync.sources.tushare.interfaces import TushareSuspendDInterface
        with patch(f"{_INGEST}.ingest_suspend_d", return_value=3):
            result = TushareSuspendDInterface().sync_date(date(2024, 1, 5))
            assert result.status == SyncStatus.SUCCESS
            assert result.rows_synced == 3


class TestTushareMoneyflowInterface:
    def test_info_targets_matching_table(self):
        from app.datasync.sources.tushare.interfaces import TushareMoneyflowInterface

        assert TushareMoneyflowInterface().info.target_table == "moneyflow"

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
    def test_info_targets_matching_table(self):
        from app.datasync.sources.tushare.interfaces import TushareDividendInterface

        assert TushareDividendInterface().info.target_table == "dividend"

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
             patch("app.datasync.table_manager.ensure_inferred_table") as mock_ensure_table, \
             patch("app.domains.extdata.dao.tushare_dao.insert_catalog_rows", return_value=1) as mock_insert:
            result = iface.sync_date(date(2024, 1, 5))

        assert iface.supports_backfill() is True
        assert iface.supports_scheduled_sync() is True
        assert iface.backfill_mode() == "date"
        assert iface.requires_nonempty_trading_day_data() is False
        assert result.status == SyncStatus.SUCCESS
        assert result.rows_synced == 1
        mock_call.assert_called_once_with("block_trade", trade_date="20240105")
        mock_ensure_table.assert_called_once()
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
             patch("app.datasync.table_manager.ensure_inferred_table") as mock_ensure_table, \
             patch("app.domains.extdata.dao.tushare_dao.insert_catalog_rows", return_value=1):
            result = iface.sync_date(date(2024, 1, 5))

        assert iface.supports_scheduled_sync() is True
        assert iface.supports_backfill() is True
        assert iface.backfill_mode() == "date"
        assert iface.requires_nonempty_trading_day_data() is True
        assert result.status == SyncStatus.SUCCESS
        assert result.rows_synced == 1
        mock_ensure_table.assert_called_once()
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

    def test_static_catalog_stock_basic_uses_default_params(self):
        from app.datasync.sources.tushare.catalog_interfaces import TushareCatalogInterface, TushareCatalogSpec

        iface = TushareCatalogInterface(
            TushareCatalogSpec(
                interface_key="stock_basic",
                display_name="股票基础列表",
                api_name="stock_basic",
                target_table="stock_basic",
                sync_priority=10,
            )
        )

        df = pd.DataFrame({"ts_code": ["000001.SZ"], "symbol": ["000001"], "name": ["平安银行"]})
        with patch("app.datasync.service.tushare_ingest.call_pro", return_value=df) as mock_call, \
             patch("app.domains.extdata.dao.tushare_dao.insert_catalog_rows", return_value=1) as mock_insert:
            result = iface.sync_date(date(2024, 1, 5))

        assert result.status == SyncStatus.SUCCESS
        assert result.rows_synced == 1
        mock_call.assert_called_once_with("stock_basic", list_status="L")
        assert mock_insert.call_args.kwargs["column_specs"] is None
        assert mock_insert.call_args.kwargs["key_columns"] is None

    def test_static_catalog_suspend_uses_suspend_date_param(self):
        from app.datasync.sources.tushare.catalog_interfaces import TushareCatalogInterface, TushareCatalogSpec

        iface = TushareCatalogInterface(
            TushareCatalogSpec(
                interface_key="suspend",
                display_name="停复牌历史",
                api_name="suspend",
                target_table="suspend",
                sync_priority=24,
            )
        )

        df = pd.DataFrame({"ts_code": ["000001.SZ"], "suspend_date": ["20240105"], "resume_date": ["20240108"]})
        with patch("app.datasync.service.tushare_ingest.call_pro", return_value=df) as mock_call, \
             patch("app.domains.extdata.dao.tushare_dao.insert_catalog_rows", return_value=1):
            result = iface.sync_date(date(2024, 1, 5))

        assert result.status == SyncStatus.SUCCESS
        assert result.rows_synced == 1
        mock_call.assert_called_once_with("suspend", suspend_date="20240105")

    def test_cyq_chips_sync_loops_symbols_with_price_key(self):
        from app.datasync.sources.tushare.interfaces import TushareCyqChipsInterface

        iface = TushareCyqChipsInterface()
        df = pd.DataFrame(
            {
                "ts_code": ["000001.SZ", "000001.SZ"],
                "trade_date": ["20240105", "20240105"],
                "price": [10.01, 10.02],
                "percent": [0.12, 0.18],
            }
        )
        inferred_schema = {
            "column_specs": [
                {"name": "ts_code", "normalizer": "clean"},
                {"name": "trade_date", "normalizer": "date"},
                {"name": "price", "normalizer": "float"},
                {"name": "percent", "normalizer": "float"},
            ],
            "key_columns": ("ts_code", "trade_date", "price"),
        }

        with patch(f"{_INGEST}.get_all_ts_codes", return_value=["000001.SZ", "000002.SZ"]), \
             patch(f"{_INGEST}.call_pro", side_effect=[df, pd.DataFrame()]) as mock_call, \
             patch("app.datasync.sources.tushare.catalog_interfaces.ddl.infer_dynamic_table_schema", return_value=inferred_schema) as mock_infer, \
             patch("app.datasync.table_manager.ensure_inferred_table") as mock_ensure_table, \
             patch(f"{_TS_DAO}.insert_catalog_rows", return_value=2) as mock_insert:
            result = iface.sync_date(date(2024, 1, 5))

        assert iface.supports_scheduled_sync() is True
        assert iface.supports_backfill() is True
        assert iface.backfill_mode() == "date"
        assert iface.requires_nonempty_trading_day_data() is True
        assert result.status == SyncStatus.SUCCESS
        assert result.rows_synced == 2
        mock_call.assert_any_call("cyq_chips", ts_code="000001.SZ", trade_date="20240105")
        mock_call.assert_any_call("cyq_chips", ts_code="000002.SZ", trade_date="20240105")
        mock_infer.assert_called_once()
        assert mock_infer.call_args.kwargs["preferred_key_fields"][:3] == ("ts_code", "trade_date", "price")
        mock_ensure_table.assert_called_once()
        assert mock_insert.call_args.kwargs["key_columns"] == ("ts_code", "trade_date", "price")

    def test_cyq_chips_quota_pause_preserves_partial_rows(self):
        from app.datasync.service.tushare_ingest import TushareQuotaExceededError
        from app.datasync.sources.tushare.interfaces import TushareCyqChipsInterface

        iface = TushareCyqChipsInterface()
        df = pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": ["20240105"],
                "price": [10.01],
                "percent": [0.12],
            }
        )
        inferred_schema = {
            "column_specs": [
                {"name": "ts_code", "normalizer": "clean"},
                {"name": "trade_date", "normalizer": "date"},
                {"name": "price", "normalizer": "float"},
                {"name": "percent", "normalizer": "float"},
            ],
            "key_columns": ("ts_code", "trade_date", "price"),
        }

        with patch(f"{_INGEST}.get_all_ts_codes", return_value=["000001.SZ", "000002.SZ"]), \
             patch(
                 f"{_INGEST}.call_pro",
                 side_effect=[df, TushareQuotaExceededError("cyq_chips", "daily quota", scope="day")],
             ), \
             patch("app.datasync.sources.tushare.catalog_interfaces.ddl.infer_dynamic_table_schema", return_value=inferred_schema), \
             patch("app.datasync.table_manager.ensure_inferred_table"), \
             patch(f"{_TS_DAO}.insert_catalog_rows", return_value=1):
            result = iface.sync_date(date(2024, 1, 5))

        assert result.status == SyncStatus.PENDING
        assert result.rows_synced == 1
        assert result.details["quota_exceeded"] is True
        assert result.details["processed_count"] == 1

    def test_build_catalog_interfaces_uses_cyq_chips_special_handler(self):
        from app.datasync.sources.tushare.catalog_interfaces import build_catalog_interfaces
        from app.datasync.sources.tushare.interfaces import TushareCyqChipsInterface

        with patch(f"{_CATALOG_MOD}._load_catalog_specs", return_value=(_catalog_spec("cyq_chips"),)):
            interfaces = build_catalog_interfaces(set())
        iface = next(item for item in interfaces if item.info.interface_key == "cyq_chips")

        assert isinstance(iface, TushareCyqChipsInterface)
        assert iface.supports_scheduled_sync() is True
        assert iface.supports_backfill() is True
        assert iface.backfill_mode() == "date"

    def test_build_catalog_interfaces_skips_existing_keys(self):
        from app.datasync.sources.tushare.catalog_interfaces import build_catalog_interfaces

        specs = (
            _catalog_spec("hsgt_stk_hold"),
            _catalog_spec("moneyflow"),
            _catalog_spec("stock_st"),
        )
        with patch(f"{_CATALOG_MOD}._load_catalog_specs", return_value=specs):
            interfaces = build_catalog_interfaces({"hsgt_stk_hold", "moneyflow"})
        keys = {iface.info.interface_key for iface in interfaces}

        assert "hsgt_stk_hold" not in keys
        assert "moneyflow" not in keys
        assert "suspend_daily" not in keys
        assert "stock_st" in keys


class TestTushareDataSourceRegistration:
    def test_simple_builtin_interfaces_now_use_catalog_syncer(self):
        from app.datasync.sources.tushare.catalog_interfaces import TushareCatalogInterface
        from app.datasync.sources.tushare.source import TushareDataSource

        specs = (
            _catalog_spec("stock_basic"),
            _catalog_spec("stock_daily", "daily"),
            _catalog_spec("bak_daily"),
            _catalog_spec("moneyflow"),
            _catalog_spec("suspend_d"),
            _catalog_spec("suspend"),
            _catalog_spec("adj_factor"),
            _catalog_spec("stock_weekly", "weekly"),
            _catalog_spec("stock_monthly", "monthly"),
        )
        with patch(f"{_CATALOG_MOD}._load_catalog_specs", return_value=specs):
            interfaces = {iface.info.interface_key: iface for iface in TushareDataSource().get_interfaces()}

        for key in (
            "stock_basic",
            "stock_daily",
            "bak_daily",
            "moneyflow",
            "suspend_d",
            "suspend",
            "adj_factor",
            "stock_weekly",
            "stock_monthly",
        ):
            assert isinstance(interfaces[key], TushareCatalogInterface)

        assert interfaces["stock_daily"].requires_nonempty_trading_day_data() is True
        assert interfaces["bak_daily"].requires_nonempty_trading_day_data() is True
        assert interfaces["moneyflow"].requires_nonempty_trading_day_data() is True
        assert interfaces["adj_factor"].requires_nonempty_trading_day_data() is True

    def test_complex_interfaces_keep_custom_syncers(self):
        from app.datasync.sources.tushare.interfaces import (
            TushareCyqChipsInterface,
            TushareDividendInterface,
            TushareIndexDailyInterface,
            TushareIndexWeeklyInterface,
            TushareStockCompanyInterface,
            TushareTop10HoldersInterface,
            TushareTradeCalInterface,
        )
        from app.datasync.sources.tushare.source import TushareDataSource

        with patch(f"{_CATALOG_MOD}._load_catalog_specs", return_value=tuple()):
            interfaces = {iface.info.interface_key: iface for iface in TushareDataSource().get_interfaces()}

        assert isinstance(interfaces["trade_cal"], TushareTradeCalInterface)
        assert isinstance(interfaces["stock_company"], TushareStockCompanyInterface)
        assert isinstance(interfaces["dividend"], TushareDividendInterface)
        assert isinstance(interfaces["top10_holders"], TushareTop10HoldersInterface)
        assert isinstance(interfaces["index_daily"], TushareIndexDailyInterface)
        assert isinstance(interfaces["index_weekly"], TushareIndexWeeklyInterface)
        assert isinstance(interfaces["cyq_chips"], TushareCyqChipsInterface)

    def test_falls_back_to_bundled_catalog_when_db_catalog_unavailable(self):
        from app.datasync.sources.tushare.source import TushareDataSource

        with patch(f"{_CATALOG_MOD}._fetch_catalog_rows", side_effect=RuntimeError("db unavailable")):
            interfaces = {iface.info.interface_key: iface for iface in TushareDataSource().get_interfaces()}

        assert "daily_basic" in interfaces
        assert "hsgt_top10" in interfaces
        assert "stock_daily" in interfaces
