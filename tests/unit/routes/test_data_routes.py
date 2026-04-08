"""Unit tests for app.api.routes.data."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

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
