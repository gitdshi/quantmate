"""Unit tests for TushareBrowserService."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import patch

import pytest
from sqlalchemy import Column, Date, Integer, MetaData, Numeric, String, Table, create_engine

_MOD = "app.domains.extdata.tushare_browser_service"


@pytest.fixture()
def sqlite_tushare_engine():
    engine = create_engine("sqlite:///:memory:")
    metadata = MetaData()
    stock_daily = Table(
        "stock_daily",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("ts_code", String(20), nullable=False),
        Column("trade_date", Date, nullable=False),
        Column("close", Numeric(10, 2), nullable=False),
        Column("volume", Integer, nullable=False),
    )
    basic = Table(
        "stock_basic",
        metadata,
        Column("ts_code", String(20), primary_key=True),
        Column("name", String(50), nullable=False),
    )
    metadata.create_all(engine)
    with engine.begin() as conn:
        conn.execute(
            stock_daily.insert(),
            [
                {"id": 1, "ts_code": "000001.SZ", "trade_date": date(2024, 1, 2), "close": Decimal("10.50"), "volume": 1000},
                {"id": 2, "ts_code": "000001.SZ", "trade_date": date(2024, 1, 3), "close": Decimal("11.20"), "volume": 1200},
                {"id": 3, "ts_code": "600519.SH", "trade_date": date(2024, 1, 3), "close": Decimal("1700.00"), "volume": 200},
            ],
        )
        conn.execute(
            basic.insert(),
            [
                {"ts_code": "000001.SZ", "name": "Ping An Bank"},
                {"ts_code": "600519.SH", "name": "Kweichow Moutai"},
            ],
        )
    return engine


class TestTushareBrowserService:
    def test_list_tables(self, sqlite_tushare_engine):
        from app.domains.extdata.tushare_browser_service import TushareBrowserService

        metadata_rows = [
            {
                "source": "tushare",
                "item_key": "stock_daily",
                "item_name": "日线行情",
                "target_database": "tushare",
                "target_table": "stock_daily",
                "table_created": 1,
                "category": "股票数据",
                "sub_category": "行情数据",
            },
            {
                "source": "tushare",
                "item_key": "stock_basic",
                "item_name": "股票基础列表",
                "target_database": "tushare",
                "target_table": "stock_basic",
                "table_created": 1,
                "category": "股票数据",
                "sub_category": "基础数据",
            },
            {
                "source": "tushare",
                "item_key": "adj_factor",
                "item_name": "复权因子",
                "target_database": "tushare",
                "target_table": "adj_factor",
                "table_created": 0,
                "category": "股票数据",
                "sub_category": "行情数据",
            },
        ]

        with patch(f"{_MOD}.get_tushare_engine", return_value=sqlite_tushare_engine), patch(
            f"{_MOD}.DataSourceItemDao"
        ) as MockDao:
            MockDao.return_value.list_with_categories.return_value = metadata_rows
            service = TushareBrowserService()
            tables = service.list_tables()

        names = [item["name"] for item in tables]
        assert "stock_daily" in names
        assert "stock_basic" in names
        assert all(item["table_created"] is True for item in tables)
        assert tables[0]["target_database"] == "tushare"
        assert all(item["column_count"] > 0 for item in tables)

    def test_list_tables_keyword(self, sqlite_tushare_engine):
        from app.domains.extdata.tushare_browser_service import TushareBrowserService

        metadata_rows = [
            {
                "source": "tushare",
                "item_key": "stock_daily",
                "item_name": "日线行情",
                "target_database": "tushare",
                "target_table": "stock_daily",
                "table_created": 1,
                "category": "股票数据",
                "sub_category": "行情数据",
            },
            {
                "source": "tushare",
                "item_key": "stock_basic",
                "item_name": "股票基础列表",
                "target_database": "tushare",
                "target_table": "stock_basic",
                "table_created": 1,
                "category": "股票数据",
                "sub_category": "基础数据",
            },
        ]

        with patch(f"{_MOD}.get_tushare_engine", return_value=sqlite_tushare_engine), patch(
            f"{_MOD}.DataSourceItemDao"
        ) as MockDao:
            MockDao.return_value.list_with_categories.return_value = metadata_rows
            service = TushareBrowserService()
            tables = service.list_tables(keyword="daily")

        assert [item["name"] for item in tables] == ["stock_daily"]

    def test_list_tables_skips_metadata_rows_without_physical_table(self, sqlite_tushare_engine):
        from app.domains.extdata.tushare_browser_service import TushareBrowserService

        metadata_rows = [
            {
                "source": "tushare",
                "item_key": "stock_daily",
                "item_name": "日线行情",
                "target_database": "tushare",
                "target_table": "stock_daily",
                "table_created": 1,
                "category": "股票数据",
                "sub_category": "行情数据",
            },
            {
                "source": "tushare",
                "item_key": "ghost_table",
                "item_name": "幽灵表",
                "target_database": "tushare",
                "target_table": "ghost_table",
                "table_created": 1,
                "category": "股票数据",
                "sub_category": "行情数据",
            },
        ]

        with patch(f"{_MOD}.get_tushare_engine", return_value=sqlite_tushare_engine), patch(
            f"{_MOD}.DataSourceItemDao"
        ) as MockDao:
            MockDao.return_value.list_with_categories.return_value = metadata_rows
            service = TushareBrowserService()
            tables = service.list_tables()

        assert [item["name"] for item in tables] == ["stock_daily"]

    def test_list_tables_filters_by_category_and_sub_category(self, sqlite_tushare_engine):
        from app.domains.extdata.tushare_browser_service import TushareBrowserService

        metadata_rows = [
            {
                "source": "tushare",
                "item_key": "stock_daily",
                "item_name": "日线行情",
                "target_database": "tushare",
                "target_table": "stock_daily",
                "table_created": 1,
                "category": "股票数据",
                "sub_category": "行情数据",
            },
            {
                "source": "tushare",
                "item_key": "stock_basic",
                "item_name": "股票基础列表",
                "target_database": "tushare",
                "target_table": "stock_basic",
                "table_created": 1,
                "category": "股票数据",
                "sub_category": "基础数据",
            },
        ]

        with patch(f"{_MOD}.get_tushare_engine", return_value=sqlite_tushare_engine), patch(
            f"{_MOD}.DataSourceItemDao"
        ) as MockDao:
            MockDao.return_value.list_with_categories.return_value = metadata_rows
            service = TushareBrowserService()
            tables = service.list_tables(category="股票数据", sub_category="行情数据")

        MockDao.return_value.list_with_categories.assert_called_once_with(source="tushare", category="股票数据")
        assert [item["name"] for item in tables] == ["stock_daily"]

    def test_list_tables_falls_back_to_physical_tables_for_legacy_metadata(self, sqlite_tushare_engine):
        from app.domains.extdata.tushare_browser_service import TushareBrowserService

        with patch(f"{_MOD}.get_tushare_engine", return_value=sqlite_tushare_engine), patch(
            f"{_MOD}.DataSourceItemDao"
        ) as MockDao:
            MockDao.return_value.list_with_categories.side_effect = RuntimeError("Unknown column 'category'")
            service = TushareBrowserService()
            tables = service.list_tables(keyword="daily")

        assert [item["name"] for item in tables] == ["stock_daily"]
        assert tables[0]["target_table"] == "stock_daily"

    def test_get_schema(self, sqlite_tushare_engine):
        from app.domains.extdata.tushare_browser_service import TushareBrowserService

        with patch(f"{_MOD}.get_tushare_engine", return_value=sqlite_tushare_engine):
            service = TushareBrowserService()
            schema = service.get_schema("stock_daily")

        assert schema["table"] == "stock_daily"
        column_names = [column["name"] for column in schema["columns"]]
        assert "trade_date" in column_names
        assert any(column["primary_key"] for column in schema["columns"])

    def test_query_rows_defaults_to_trade_date_desc(self, sqlite_tushare_engine):
        from app.domains.extdata.tushare_browser_service import TushareBrowserService

        with patch(f"{_MOD}.get_tushare_engine", return_value=sqlite_tushare_engine):
            service = TushareBrowserService()
            result = service.query_rows("stock_daily")

        assert result["meta"]["sort_by"] == "trade_date"
        assert result["meta"]["sort_dir"] == "desc"
        assert result["data"][0]["trade_date"] == "2024-01-03"

    def test_query_rows_supports_filters(self, sqlite_tushare_engine):
        from app.domains.extdata.tushare_browser_service import TushareBrowserService

        with patch(f"{_MOD}.get_tushare_engine", return_value=sqlite_tushare_engine):
            service = TushareBrowserService()
            result = service.query_rows(
                "stock_daily",
                filters=[
                    {"column": "ts_code", "operator": "eq", "value": "000001.SZ"},
                    {"column": "close", "operator": "gte", "value": "11.0"},
                ],
                sort_by="id",
                sort_dir="asc",
            )

        assert result["meta"]["total"] == 1
        assert result["data"][0]["id"] == 2

    def test_query_rows_rejects_unknown_table(self, sqlite_tushare_engine):
        from app.domains.extdata.tushare_browser_service import TushareBrowserError, TushareBrowserService

        with patch(f"{_MOD}.get_tushare_engine", return_value=sqlite_tushare_engine):
            service = TushareBrowserService()
            with pytest.raises(TushareBrowserError, match="unknown table"):
                service.query_rows("missing_table")

    def test_query_rows_rejects_invalid_sort_column(self, sqlite_tushare_engine):
        from app.domains.extdata.tushare_browser_service import TushareBrowserError, TushareBrowserService

        with patch(f"{_MOD}.get_tushare_engine", return_value=sqlite_tushare_engine):
            service = TushareBrowserService()
            with pytest.raises(TushareBrowserError, match="unknown sort column"):
                service.query_rows("stock_daily", sort_by="missing_column")
