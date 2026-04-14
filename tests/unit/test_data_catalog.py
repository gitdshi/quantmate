"""Tests for Data Catalog service — scan_database_columns, get_catalog, etc."""
import pytest
from unittest.mock import patch, MagicMock

from app.domains.factors.data_catalog import (
    _classify_column,
    _is_numeric_type,
    scan_database_columns,
    get_catalog,
    get_catalog_summary,
    get_feature_columns_for_qlib,
    _CATEGORY_MAP,
    _METADATA_COLUMNS,
    _NUMERIC_TYPES,
)


class TestClassifyColumn:
    """Tests for _classify_column helper."""

    def test_known_price_column(self):
        assert _classify_column("open") == "price"
        assert _classify_column("close") == "price"
        assert _classify_column("pre_close") == "price"

    def test_known_volume_column(self):
        assert _classify_column("vol") == "volume"
        assert _classify_column("amount") == "volume"
        assert _classify_column("turnover_rate") == "volume"

    def test_known_fundamental_column(self):
        assert _classify_column("pe_ttm") == "fundamental"
        assert _classify_column("pb") == "fundamental"
        assert _classify_column("total_mv") == "fundamental"

    def test_known_flow_column(self):
        assert _classify_column("net_mf") == "flow"
        assert _classify_column("buy_sm_vol") == "flow"

    def test_known_margin_column(self):
        assert _classify_column("financing_balance") == "margin"

    def test_known_dividend_column(self):
        assert _classify_column("div_cash") == "dividend"

    def test_known_technical_column(self):
        assert _classify_column("adj_factor") == "technical"

    def test_unknown_column_returns_other(self):
        assert _classify_column("some_random_field") == "other"

    def test_metadata_column_returns_metadata(self):
        assert _classify_column("ts_code") == "metadata"
        assert _classify_column("trade_date") == "metadata"

    def test_all_category_map_entries(self):
        """Every key in _CATEGORY_MAP should produce the expected category."""
        for col, expected_cat in _CATEGORY_MAP.items():
            assert _classify_column(col) == expected_cat


class TestIsNumericType:
    """Tests for _is_numeric_type helper."""

    def test_int_types(self):
        assert _is_numeric_type("int") is True
        assert _is_numeric_type("bigint") is True
        assert _is_numeric_type("tinyint") is True
        assert _is_numeric_type("mediumint") is True
        assert _is_numeric_type("smallint") is True

    def test_float_types(self):
        assert _is_numeric_type("float") is True
        assert _is_numeric_type("double") is True
        assert _is_numeric_type("decimal") is True

    def test_non_numeric_types(self):
        assert _is_numeric_type("varchar") is False
        assert _is_numeric_type("text") is False
        assert _is_numeric_type("datetime") is False
        assert _is_numeric_type("json") is False
        assert _is_numeric_type("blob") is False

    def test_case_insensitive(self):
        assert _is_numeric_type("FLOAT") is True
        assert _is_numeric_type("Float") is True
        assert _is_numeric_type("DOUBLE") is True

    def test_type_with_params(self):
        # e.g., decimal(10,2) — the function receives just the base type
        assert _is_numeric_type("decimal") is True

    def test_all_numeric_types(self):
        for t in _NUMERIC_TYPES:
            assert _is_numeric_type(t) is True


class TestScanDatabaseColumns:
    """Tests for scan_database_columns."""

    @patch("app.domains.factors.data_catalog.connection")
    def test_returns_column_list(self, mock_conn_ctx):
        mock_conn = MagicMock()
        mock_conn_ctx.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn_ctx.return_value.__exit__ = MagicMock(return_value=False)

        # Simulate INFORMATION_SCHEMA query result
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            ("daily", "ts_code", "varchar"),
            ("daily", "close", "double"),
            ("daily", "vol", "bigint"),
        ]
        mock_conn.execute.return_value = mock_result

        columns = scan_database_columns("tushare", "tushare")
        assert len(columns) == 3
        assert columns[0]["table_name"] == "daily"
        assert columns[0]["column_name"] == "ts_code"
        assert columns[1]["column_name"] == "close"
        assert columns[1]["data_type"] == "double"

    @patch("app.domains.factors.data_catalog.connection")
    def test_empty_database(self, mock_conn_ctx):
        mock_conn = MagicMock()
        mock_conn_ctx.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn_ctx.return_value.__exit__ = MagicMock(return_value=False)

        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_conn.execute.return_value = mock_result

        columns = scan_database_columns("tushare", "tushare")
        assert columns == []

    @patch("app.domains.factors.data_catalog.connection")
    def test_database_error_returns_empty(self, mock_conn_ctx):
        mock_conn_ctx.return_value.__enter__ = MagicMock(side_effect=Exception("DB error"))
        mock_conn_ctx.return_value.__exit__ = MagicMock(return_value=False)

        columns = scan_database_columns("tushare", "tushare")
        assert columns == []


class TestGetCatalog:
    """Tests for get_catalog."""

    @patch("app.domains.factors.data_catalog.scan_database_columns")
    def test_returns_combined_catalog(self, mock_scan):
        mock_scan.side_effect = [
            # tushare
            [
                {"source": "tushare", "table_name": "daily", "column_name": "close", "data_type": "double", "category": "price", "is_numeric": True},
                {"source": "tushare", "table_name": "daily", "column_name": "vol", "data_type": "bigint", "category": "volume", "is_numeric": True},
                {"source": "tushare", "table_name": "daily", "column_name": "ts_code", "data_type": "varchar", "category": "metadata", "is_numeric": False},
            ],
            # akshare
            [
                {"source": "akshare", "table_name": "stock_zh_a_hist", "column_name": "open", "data_type": "float", "category": "price", "is_numeric": True},
            ],
        ]

        catalog = get_catalog(numeric_only=False, include_metadata=True)
        assert len(catalog) == 4  # all 4 columns

    @patch("app.domains.factors.data_catalog.scan_database_columns")
    def test_numeric_only(self, mock_scan):
        mock_scan.side_effect = [
            [
                {"source": "tushare", "table_name": "daily", "column_name": "close", "data_type": "double", "category": "price", "is_numeric": True},
                {"source": "tushare", "table_name": "daily", "column_name": "ts_code", "data_type": "varchar", "category": "metadata", "is_numeric": False},
            ],
            [],
        ]

        catalog = get_catalog(numeric_only=True, include_metadata=False)
        # ts_code is varchar (not numeric) AND metadata, so excluded either way
        assert len(catalog) == 1
        assert catalog[0]["column_name"] == "close"

    @patch("app.domains.factors.data_catalog.scan_database_columns")
    def test_exclude_metadata(self, mock_scan):
        mock_scan.side_effect = [
            [
                {"source": "tushare", "table_name": "daily", "column_name": "ts_code", "data_type": "varchar", "category": "metadata", "is_numeric": False},
                {"source": "tushare", "table_name": "daily", "column_name": "trade_date", "data_type": "varchar", "category": "metadata", "is_numeric": False},
                {"source": "tushare", "table_name": "daily", "column_name": "close", "data_type": "double", "category": "price", "is_numeric": True},
            ],
            [],
        ]

        catalog = get_catalog(numeric_only=False, include_metadata=False)
        # ts_code and trade_date are metadata
        assert len(catalog) == 1

    @patch("app.domains.factors.data_catalog.scan_database_columns")
    def test_catalog_entries_have_category(self, mock_scan):
        mock_scan.side_effect = [
            [
                {"source": "tushare", "table_name": "daily", "column_name": "close", "data_type": "double", "category": "price", "is_numeric": True},
                {"source": "tushare", "table_name": "daily", "column_name": "vol", "data_type": "bigint", "category": "volume", "is_numeric": True},
            ],
            [],
        ]

        catalog = get_catalog()
        for entry in catalog:
            assert "category" in entry
        assert catalog[0]["category"] == "price"
        assert catalog[1]["category"] == "volume"

    @patch("app.domains.factors.data_catalog.scan_database_columns")
    def test_scan_exception_skips_source(self, mock_scan):
        """When scan_database_columns raises, that source is skipped silently."""
        mock_scan.side_effect = [
            Exception("tushare DB down"),
            [
                {"source": "akshare", "table_name": "stock", "column_name": "open", "data_type": "float", "category": "price", "is_numeric": True},
            ],
        ]

        catalog = get_catalog(numeric_only=False, include_metadata=True)
        assert len(catalog) == 1
        assert catalog[0]["source"] == "akshare"


class TestGetCatalogSummary:
    """Tests for get_catalog_summary."""

    @patch("app.domains.factors.data_catalog.get_catalog")
    def test_returns_summary_structure(self, mock_get_catalog):
        mock_get_catalog.return_value = [
            {"source": "tushare", "table_name": "daily", "column_name": "close", "data_type": "double", "category": "price", "is_numeric": True},
            {"source": "tushare", "table_name": "daily", "column_name": "vol", "data_type": "bigint", "category": "volume", "is_numeric": True},
            {"source": "akshare", "table_name": "stock", "column_name": "pe_ttm", "data_type": "float", "category": "fundamental", "is_numeric": True},
        ]

        summary = get_catalog_summary()
        assert "categories" in summary
        assert "total_fields" in summary
        assert "sources" in summary
        assert summary["total_fields"] == 3
        assert "price" in summary["categories"]
        assert "close" in summary["categories"]["price"]

    @patch("app.domains.factors.data_catalog.get_catalog")
    def test_empty_catalog(self, mock_get_catalog):
        mock_get_catalog.return_value = []

        summary = get_catalog_summary()
        assert summary["total_fields"] == 0
        assert summary["categories"] == {}


class TestGetFeatureColumnsForQlib:
    """Tests for get_feature_columns_for_qlib."""

    @patch("app.domains.factors.data_catalog.get_catalog")
    def test_returns_only_numeric_non_metadata(self, mock_get_catalog):
        mock_get_catalog.return_value = [
            {"source": "tushare", "table_name": "daily", "column_name": "close", "data_type": "double", "category": "price", "is_numeric": True},
            {"source": "tushare", "table_name": "daily", "column_name": "vol", "data_type": "bigint", "category": "volume", "is_numeric": True},
        ]

        cols = get_feature_columns_for_qlib()
        col_names = [c["column_name"] for c in cols]
        assert "close" in col_names
        assert "vol" in col_names

    @patch("app.domains.factors.data_catalog.get_catalog")
    def test_returns_dicts_with_expected_keys(self, mock_get_catalog):
        mock_get_catalog.return_value = [
            {"source": "tushare", "table_name": "daily", "column_name": "close", "data_type": "double", "category": "price", "is_numeric": True},
        ]

        cols = get_feature_columns_for_qlib()
        assert len(cols) == 1
        assert "source" in cols[0]
        assert "table_name" in cols[0]
        assert "column_name" in cols[0]
        assert "category" in cols[0]

    @patch("app.domains.factors.data_catalog.get_catalog")
    def test_empty_catalog(self, mock_get_catalog):
        mock_get_catalog.return_value = []
        cols = get_feature_columns_for_qlib()
        assert cols == []


class TestConstants:
    """Tests for module-level constants."""

    def test_metadata_columns_are_frozenset(self):
        assert isinstance(_METADATA_COLUMNS, frozenset)
        assert "ts_code" in _METADATA_COLUMNS
        assert "trade_date" in _METADATA_COLUMNS

    def test_numeric_types_are_frozenset(self):
        assert isinstance(_NUMERIC_TYPES, frozenset)
        assert "float" in _NUMERIC_TYPES
        assert "int" in _NUMERIC_TYPES

    def test_category_map_has_entries(self):
        assert len(_CATEGORY_MAP) > 20
        assert "close" in _CATEGORY_MAP
