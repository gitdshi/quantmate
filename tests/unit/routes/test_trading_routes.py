"""Tests for Trading routes — live-only (paper trading moved to paper_trading routes)."""
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
    def test_create_paper_order_rejected(self, client):
        """Paper orders should be rejected with a redirect message."""
        resp = client.post("/api/v1/trade/orders", json={
            "symbol": "000001.SZ", "direction": "buy", "order_type": "limit",
            "quantity": 100, "price": 10.0, "mode": "paper"
        })
        assert resp.status_code == 400
        assert "paper-trade" in resp.json()["detail"].lower()

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

    @patch("app.domains.trading.vnpy_trading_service.VnpyTradingService")
    @patch("app.api.routes.trading.OrderDao")
    def test_create_live_order_submits_to_gateway(self, MockDao, MockVnpy, client):
        instance = MockDao.return_value
        instance.create.return_value = 1
        instance.get_by_id.return_value = {"id": 1, "symbol": "000001.SZ", "status": "submitted"}
        MockVnpy.return_value.send_order.return_value = "vt_order_001"
        resp = client.post("/api/v1/trade/orders", json={
            "symbol": "000001.SZ", "direction": "buy", "order_type": "market",
            "quantity": 100, "mode": "live", "gateway_name": "ctp",
        })
        assert resp.status_code == 201

    @patch("app.domains.trading.vnpy_trading_service.VnpyTradingService")
    @patch("app.api.routes.trading.OrderDao")
    def test_create_live_order_gateway_reject(self, MockDao, MockVnpy, client):
        instance = MockDao.return_value
        instance.create.return_value = 1
        MockVnpy.return_value.send_order.return_value = None
        resp = client.post("/api/v1/trade/orders", json={
            "symbol": "000001.SZ", "direction": "buy", "order_type": "market",
            "quantity": 100, "mode": "live",
        })
        assert resp.status_code == 502


class TestGatewayManagement:

    @patch("app.domains.trading.vnpy_trading_service.VnpyTradingService")
    def test_connect_gateway(self, MockVnpy, client):
        MockVnpy.return_value.connect_gateway.return_value = True
        resp = client.post("/api/v1/trade/gateway/connect", json={
            "gateway_type": "ctp", "config": {"host": "127.0.0.1"}, "gateway_name": "my_ctp",
        })
        assert resp.status_code == 200
        assert "connected" in resp.json()["message"].lower()

    @patch("app.domains.trading.vnpy_trading_service.VnpyTradingService")
    def test_connect_gateway_unknown_type(self, MockVnpy, client):
        resp = client.post("/api/v1/trade/gateway/connect", json={
            "gateway_type": "invalid_gw", "config": {},
        })
        assert resp.status_code == 400

    @patch("app.domains.trading.vnpy_trading_service.VnpyTradingService")
    def test_disconnect_gateway(self, MockVnpy, client):
        MockVnpy.return_value.disconnect_gateway.return_value = True
        resp = client.post("/api/v1/trade/gateway/disconnect?gateway_name=my_ctp")
        assert resp.status_code == 200

    @patch("app.domains.trading.vnpy_trading_service.VnpyTradingService")
    def test_disconnect_gateway_not_found(self, MockVnpy, client):
        MockVnpy.return_value.disconnect_gateway.return_value = False
        resp = client.post("/api/v1/trade/gateway/disconnect?gateway_name=no_such")
        assert resp.status_code == 404

    @patch("app.domains.trading.vnpy_trading_service.VnpyTradingService")
    def test_list_gateways(self, MockVnpy, client):
        MockVnpy.return_value.list_gateways.return_value = [
            {"name": "ctp_01", "type": "ctp", "connected": True}
        ]
        resp = client.get("/api/v1/trade/gateways")
        assert resp.status_code == 200
        assert len(resp.json()["gateways"]) == 1

    @patch("app.domains.trading.vnpy_trading_service.VnpyTradingService")
    def test_gateway_positions(self, MockVnpy, client):
        MockVnpy.return_value.query_positions.return_value = []
        resp = client.get("/api/v1/trade/gateway/positions")
        assert resp.status_code == 200
        assert resp.json()["positions"] == []

    @patch("app.domains.trading.vnpy_trading_service.VnpyTradingService")
    def test_gateway_account(self, MockVnpy, client):
        MockVnpy.return_value.query_account.return_value = None
        resp = client.get("/api/v1/trade/gateway/account")
        assert resp.status_code == 200
        assert resp.json()["account"] is None


class TestAutoStrategy:

    @patch("app.domains.trading.cta_strategy_runner.CtaStrategyRunner")
    def test_start_auto_strategy(self, MockRunner, client):
        MockRunner.return_value.start_strategy.return_value = {
            "success": True, "strategy_name": "test_001",
        }
        resp = client.post("/api/v1/trade/auto-strategy/start", json={
            "strategy_class_name": "DoubleMaStrategy",
            "vt_symbol": "IF2406.CFFEX",
            "parameters": {"fast_window": 10, "slow_window": 30},
        })
        assert resp.status_code == 200
        assert resp.json()["success"] is True

    @patch("app.domains.trading.cta_strategy_runner.CtaStrategyRunner")
    def test_start_auto_strategy_failure(self, MockRunner, client):
        MockRunner.return_value.start_strategy.return_value = {
            "success": False, "error": "Strategy class not found",
        }
        resp = client.post("/api/v1/trade/auto-strategy/start", json={
            "strategy_class_name": "NonExistent",
            "vt_symbol": "IF2406.CFFEX",
        })
        assert resp.status_code == 400

    @patch("app.domains.trading.cta_strategy_runner.CtaStrategyRunner")
    def test_stop_auto_strategy(self, MockRunner, client):
        MockRunner.return_value.stop_strategy.return_value = True
        resp = client.post("/api/v1/trade/auto-strategy/stop", json={
            "strategy_name": "test_001",
        })
        assert resp.status_code == 200

    @patch("app.domains.trading.cta_strategy_runner.CtaStrategyRunner")
    def test_stop_auto_strategy_not_found(self, MockRunner, client):
        MockRunner.return_value.stop_strategy.return_value = False
        resp = client.post("/api/v1/trade/auto-strategy/stop", json={
            "strategy_name": "no_such",
        })
        assert resp.status_code == 404

    @patch("app.domains.trading.cta_strategy_runner.CtaStrategyRunner")
    def test_auto_strategy_status(self, MockRunner, client):
        MockRunner.return_value.list_strategies.return_value = [
            {"name": "test_001", "class_name": "DoubleMaStrategy", "status": "running"},
        ]
        resp = client.get("/api/v1/trade/auto-strategy/status")
        assert resp.status_code == 200
        assert len(resp.json()["strategies"]) == 1

