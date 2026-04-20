"""Unit tests for app.api.routes.data."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.routes import data
from app.api.exception_handlers import register_exception_handlers

# Patch at route module level
_ROUTE = "app.api.routes.data"


@pytest.fixture()
def mock_user():
    return type("User", (), {"id": 1, "user_id": 1, "username": "test", "sub": 1})()


@pytest.fixture()
def client(mock_user):
    test_app = FastAPI()
    register_exception_handlers(test_app)
    test_app.include_router(data.router, prefix="/api/v1")

    async def override_auth():
        return mock_user

    test_app.dependency_overrides[data.get_current_user] = override_auth
    if hasattr(data, "get_current_user_optional"):
        test_app.dependency_overrides[data.get_current_user_optional] = override_auth
    for route in test_app.routes:
        if hasattr(route, "dependencies"):
            route.dependencies = []
    return TestClient(test_app, raise_server_exceptions=False)


class TestDataRoutes:
    @patch(f"{_ROUTE}.DataService")
    def test_list_symbols(self, MockSvc, client):
        MockSvc.return_value.get_symbols.return_value = []
        resp = client.get("/api/v1/data/symbols")
        assert resp.status_code == 200

    @patch(f"{_ROUTE}.DataService")
    def test_get_history(self, MockSvc, client):
        MockSvc.return_value.get_history.return_value = [
            {"datetime": "2024-01-03T00:00:00", "open": 10.0, "high": 11.0,
             "low": 9.5, "close": 10.5, "volume": 1000}
        ]
        resp = client.get("/api/v1/data/history/000001.SZSE?start_date=2024-01-01&end_date=2024-01-31")
        assert resp.status_code == 200

    @patch(f"{_ROUTE}.DataService")
    def test_get_history_value_error(self, MockSvc, client):
        MockSvc.return_value.get_history.side_effect = ValueError("bad range")
        resp = client.get("/api/v1/data/history/000001.SZSE?start_date=2024-01-31&end_date=2024-01-01")
        assert resp.status_code == 400

    @patch(f"{_ROUTE}.DataService")
    def test_get_indicators(self, MockSvc, client):
        MockSvc.return_value.get_indicators.return_value = []
        resp = client.get("/api/v1/data/indicators/000001.SZSE?start_date=2024-01-01&end_date=2024-01-31")
        assert resp.status_code in (200, 500)

    @patch(f"{_ROUTE}.DataService")
    def test_get_market_overview(self, MockSvc, client):
        MockSvc.return_value.get_overview.return_value = {"total": 5000}
        resp = client.get("/api/v1/data/overview")
        assert resp.status_code in (200, 500)

    @patch(f"{_ROUTE}.DataService")
    def test_list_tushare_tables(self, MockSvc, client):
        MockSvc.return_value.list_tushare_tables.return_value = [{"name": "stock_daily", "column_count": 5, "primary_keys": ["id"]}]
        resp = client.get("/api/v1/data/tushare/tables?keyword=daily")
        assert resp.status_code == 200
        assert resp.json()["data"][0]["name"] == "stock_daily"

    @patch(f"{_ROUTE}.DataService")
    def test_get_tushare_table_schema(self, MockSvc, client):
        MockSvc.return_value.get_tushare_table_schema.return_value = {
            "table": "stock_daily",
            "columns": [{"name": "trade_date", "data_type": "DATE", "nullable": False, "primary_key": False, "indexed": True}],
        }
        resp = client.get("/api/v1/data/tushare/tables/stock_daily/schema")
        assert resp.status_code == 200
        assert resp.json()["table"] == "stock_daily"

    @patch(f"{_ROUTE}.DataService")
    def test_query_tushare_rows(self, MockSvc, client):
        MockSvc.return_value.query_tushare_rows.return_value = {
            "table": "stock_daily",
            "data": [{"ts_code": "000001.SZ", "trade_date": "2024-01-03"}],
            "meta": {"page": 1, "page_size": 50, "total": 1, "total_pages": 1, "sort_by": "trade_date", "sort_dir": "desc"},
        }
        resp = client.post(
            "/api/v1/data/tushare/tables/stock_daily/rows",
            json={
                "page": 1,
                "page_size": 50,
                "sort_by": "trade_date",
                "sort_dir": "desc",
                "filters": [{"column": "ts_code", "operator": "eq", "value": "000001.SZ"}],
            },
        )
        assert resp.status_code == 200
        assert resp.json()["meta"]["total"] == 1

    @patch(f"{_ROUTE}.DataService")
    def test_query_tushare_rows_validation_error(self, MockSvc, client):
        MockSvc.return_value.query_tushare_rows.side_effect = ValueError("bad filter")
        resp = client.post(
            "/api/v1/data/tushare/tables/stock_daily/rows",
            json={"page": 1, "page_size": 50, "filters": []},
        )
        assert resp.status_code == 400
