"""Tests for multi-market routes (HK / US)."""
import pytest
from unittest.mock import patch
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.routes import multi_market
from app.api.exception_handlers import register_exception_handlers


@pytest.fixture
def mock_user():
    return type("User", (), {"id": 1, "user_id": 1, "username": "testuser"})()


@pytest.fixture
def client(mock_user):
    test_app = FastAPI()
    register_exception_handlers(test_app)
    test_app.include_router(multi_market.router, prefix="/api/v1")

    async def override_auth():
        return mock_user

    test_app.dependency_overrides[multi_market.get_current_user] = override_auth
    return TestClient(test_app)


MOCK_DAO = "app.domains.market.multi_market_dao.MultiMarketDao"


class TestMultiMarketRoutes:

    @patch(MOCK_DAO)
    def test_list_exchanges(self, MockDao, client):
        MockDao.return_value.list_exchanges.return_value = [
            {"id": 1, "code": "SSE", "name": "Shanghai Stock Exchange"}
        ]
        resp = client.get("/api/v1/market/exchanges")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["code"] == "SSE"

    @patch(MOCK_DAO)
    def test_list_hk_stocks(self, MockDao, client):
        MockDao.return_value.list_hk_stocks.return_value = [
            {"ts_code": "00700.HK", "name": "Tencent"}
        ]
        resp = client.get("/api/v1/market/hk/stocks?limit=100")
        assert resp.status_code == 200
        data = resp.json()
        assert data[0]["ts_code"] == "00700.HK"

    @patch(MOCK_DAO)
    def test_get_hk_daily(self, MockDao, client):
        MockDao.return_value.get_hk_daily.return_value = [
            {"trade_date": "20240101", "close": 350.0}
        ]
        resp = client.get("/api/v1/market/hk/daily?ts_code=00700.HK&start_date=20240101&end_date=20240131")
        assert resp.status_code == 200
        assert len(resp.json()) == 1

    @patch(MOCK_DAO)
    def test_list_us_stocks(self, MockDao, client):
        MockDao.return_value.list_us_stocks.return_value = [
            {"ts_code": "AAPL", "name": "Apple Inc"}
        ]
        resp = client.get("/api/v1/market/us/stocks")
        assert resp.status_code == 200
        assert resp.json()[0]["ts_code"] == "AAPL"

    @patch(MOCK_DAO)
    def test_get_us_daily(self, MockDao, client):
        MockDao.return_value.get_us_daily.return_value = [
            {"trade_date": "20240101", "close": 185.0}
        ]
        resp = client.get("/api/v1/market/us/daily?ts_code=AAPL&start_date=20240101&end_date=20240131")
        assert resp.status_code == 200

    @patch(MOCK_DAO)
    def test_hk_daily_requires_params(self, MockDao, client):
        resp = client.get("/api/v1/market/hk/daily")
        assert resp.status_code == 422  # Missing required query params

    @patch(MOCK_DAO)
    def test_hk_stocks_limit_validation(self, MockDao, client):
        MockDao.return_value.list_hk_stocks.return_value = []
        resp = client.get("/api/v1/market/hk/stocks?limit=3000")
        assert resp.status_code == 422  # Exceeds le=2000

