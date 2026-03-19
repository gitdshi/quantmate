"""Tests for P2: Trading routes (orders, paper trading)."""
import pytest
from unittest.mock import patch
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.routes import trading
from app.api.exception_handlers import register_exception_handlers


@pytest.fixture
def mock_user():
    return type("User", (), {"id": 1, "user_id": 1, "username": "testuser"})()


@pytest.fixture
def client(mock_user):
    test_app = FastAPI()
    register_exception_handlers(test_app)
    test_app.include_router(trading.router, prefix="/api/v1")

    async def override_auth():
        return mock_user

    test_app.dependency_overrides[trading.get_current_user] = override_auth
    return TestClient(test_app)


class TestOrders:
    @patch("app.api.routes.trading.OrderDao")
    def test_create_order(self, MockDao, client):
        instance = MockDao.return_value
        instance.create.return_value = 1
        instance.get_by_id.return_value = {"id": 1, "symbol": "000001.SZ", "status": "created"}
        resp = client.post("/api/v1/trade/orders", json={
            "symbol": "000001.SZ", "direction": "buy", "order_type": "limit",
            "quantity": 100, "price": 10.0, "mode": "paper"
        })
        assert resp.status_code == 201
        assert resp.json()["id"] == 1

    @patch("app.api.routes.trading.OrderDao")
    def test_create_market_order_paper_auto_fill(self, MockDao, client):
        instance = MockDao.return_value
        instance.create.return_value = 1
        instance.update_status.return_value = True
        instance.insert_trade.return_value = 1
        resp = client.post("/api/v1/trade/orders", json={
            "symbol": "000001.SZ", "direction": "buy", "order_type": "market",
            "quantity": 100, "mode": "paper"
        })
        assert resp.status_code == 201
        # Paper market orders are auto-filled
        instance.update_status.assert_called()
        instance.insert_trade.assert_called()

    @patch("app.api.routes.trading.OrderDao")
    def test_create_order_invalid_direction(self, MockDao, client):
        resp = client.post("/api/v1/trade/orders", json={
            "symbol": "000001.SZ", "direction": "invalid", "order_type": "limit",
            "quantity": 100, "price": 10.0
        })
        assert resp.status_code == 400

    @patch("app.api.routes.trading.OrderDao")
    def test_create_order_invalid_type(self, MockDao, client):
        resp = client.post("/api/v1/trade/orders", json={
            "symbol": "000001.SZ", "direction": "buy", "order_type": "invalid",
            "quantity": 100
        })
        assert resp.status_code == 400

    @patch("app.api.routes.trading.OrderDao")
    def test_list_orders(self, MockDao, client):
        instance = MockDao.return_value
        instance.list_by_user.return_value = (
            [{"id": 1, "symbol": "000001.SZ", "status": "filled"}], 1
        )
        resp = client.get("/api/v1/trade/orders")
        assert resp.status_code == 200
        assert resp.json()["meta"]["total"] == 1

    @patch("app.api.routes.trading.OrderDao")
    def test_get_order(self, MockDao, client):
        instance = MockDao.return_value
        instance.get_by_id.return_value = {"id": 1, "symbol": "000001.SZ"}
        resp = client.get("/api/v1/trade/orders/1")
        assert resp.status_code == 200

    @patch("app.api.routes.trading.OrderDao")
    def test_get_order_not_found(self, MockDao, client):
        instance = MockDao.return_value
        instance.get_by_id.return_value = None
        resp = client.get("/api/v1/trade/orders/999")
        assert resp.status_code == 404

    @patch("app.api.routes.trading.OrderDao")
    def test_cancel_order(self, MockDao, client):
        instance = MockDao.return_value
        instance.cancel.return_value = True
        resp = client.post("/api/v1/trade/orders/1/cancel")
        assert resp.status_code == 200

    @patch("app.api.routes.trading.OrderDao")
    def test_cancel_order_not_found(self, MockDao, client):
        instance = MockDao.return_value
        instance.cancel.return_value = False
        resp = client.post("/api/v1/trade/orders/999/cancel")
        assert resp.status_code == 400

