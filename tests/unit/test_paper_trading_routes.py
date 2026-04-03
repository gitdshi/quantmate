"""Tests for paper trading routes — deploy, orders, positions, performance."""
import pytest
from unittest.mock import patch, MagicMock
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.routes import paper_trading
from app.api.exception_handlers import register_exception_handlers


@pytest.fixture
def mock_user():
    return type("User", (), {"id": 1, "user_id": 1, "username": "testuser"})()


@pytest.fixture
def client(mock_user):
    test_app = FastAPI()
    register_exception_handlers(test_app)
    test_app.include_router(paper_trading.router, prefix="/api/v1")

    async def override_auth():
        return mock_user

    test_app.dependency_overrides[paper_trading.get_current_user] = override_auth
    return TestClient(test_app)


class TestDeploy:
    @patch("app.api.routes.paper_trading.PaperTradingService")
    def test_deploy_strategy(self, MockSvc, client):
        MockSvc.return_value.deploy.return_value = {
            "success": True, "deployment_id": 10, "status": "running",
        }
        resp = client.post("/api/v1/paper-trade/deploy", json={
            "strategy_id": 1, "vt_symbol": "IF2406.CFFEX", "parameters": {"fast": 10},
        })
        assert resp.status_code == 201
        assert resp.json()["deployment_id"] == 10

    @patch("app.api.routes.paper_trading.PaperTradingService")
    def test_deploy_strategy_not_found(self, MockSvc, client):
        MockSvc.return_value.deploy.return_value = {
            "success": False, "error": "Strategy not found",
        }
        resp = client.post("/api/v1/paper-trade/deploy", json={
            "strategy_id": 999, "vt_symbol": "IF2406.CFFEX",
        })
        assert resp.status_code == 400
        assert "not found" in resp.json()["detail"].lower()

    @patch("app.api.routes.paper_trading.PaperTradingService")
    def test_list_deployments(self, MockSvc, client):
        MockSvc.return_value.list_deployments.return_value = [
            {"id": 1, "strategy_name": "DMA", "status": "running"},
        ]
        resp = client.get("/api/v1/paper-trade/deployments")
        assert resp.status_code == 200
        assert len(resp.json()["deployments"]) == 1

    @patch("app.api.routes.paper_trading.PaperTradingService")
    def test_stop_deployment(self, MockSvc, client):
        MockSvc.return_value.stop_deployment.return_value = True
        resp = client.post("/api/v1/paper-trade/deployments/1/stop")
        assert resp.status_code == 200

    @patch("app.api.routes.paper_trading.PaperTradingService")
    def test_stop_deployment_not_found(self, MockSvc, client):
        MockSvc.return_value.stop_deployment.return_value = False
        resp = client.post("/api/v1/paper-trade/deployments/999/stop")
        assert resp.status_code == 404


class TestPaperOrders:
    @patch("app.api.routes.paper_trading.OrderDao")
    def test_create_paper_order_limit(self, MockDao, client):
        instance = MockDao.return_value
        instance.create.return_value = 1
        instance.get_by_id.return_value = {"id": 1, "symbol": "000001.SZ", "status": "created"}
        resp = client.post("/api/v1/paper-trade/orders", json={
            "symbol": "000001.SZ", "direction": "buy",
            "order_type": "limit", "quantity": 100, "price": 10.5,
        })
        assert resp.status_code == 201
        instance.create.assert_called_once()
        # Limit orders should NOT be auto-filled
        instance.update_status.assert_not_called()

    @patch("app.api.routes.paper_trading.RealtimeQuoteService")
    @patch("app.api.routes.paper_trading.OrderDao")
    def test_create_paper_order_market_auto_fill(self, MockDao, MockQuoteSvc, client):
        instance = MockDao.return_value
        instance.create.return_value = 1
        instance.get_by_id.return_value = {"id": 1, "symbol": "000001.SZ", "status": "filled"}
        MockQuoteSvc.return_value.get_quote.return_value = {
            "last_price": 12.0,
            "prev_close": 12.0,
        }
        resp = client.post("/api/v1/paper-trade/orders", json={
            "symbol": "000001.SZ", "direction": "buy",
            "order_type": "market", "quantity": 100, "price": 10.0,
        })
        assert resp.status_code == 201
        instance.update_status.assert_called_once()
        instance.insert_trade.assert_called_once()

    @patch("app.api.routes.paper_trading.OrderDao")
    def test_create_paper_order_invalid_direction(self, MockDao, client):
        resp = client.post("/api/v1/paper-trade/orders", json={
            "symbol": "000001.SZ", "direction": "invalid",
            "order_type": "market", "quantity": 100,
        })
        assert resp.status_code == 400

    @patch("app.api.routes.paper_trading.OrderDao")
    def test_create_paper_order_invalid_type(self, MockDao, client):
        resp = client.post("/api/v1/paper-trade/orders", json={
            "symbol": "000001.SZ", "direction": "buy",
            "order_type": "stop", "quantity": 100,
        })
        assert resp.status_code == 400

    @patch("app.api.routes.paper_trading.OrderDao")
    def test_create_paper_order_zero_quantity(self, MockDao, client):
        resp = client.post("/api/v1/paper-trade/orders", json={
            "paper_account_id": 1,
            "symbol": "000001.SZ", "direction": "buy",
            "order_type": "market", "quantity": 0,
        })
        assert resp.status_code == 400

    @patch("app.api.routes.paper_trading.OrderDao")
    def test_list_paper_orders(self, MockDao, client):
        instance = MockDao.return_value
        instance.list_by_user.return_value = (
            [{"id": 1, "symbol": "000001.SZ", "status": "filled"}], 1,
        )
        resp = client.get("/api/v1/paper-trade/orders")
        assert resp.status_code == 200
        assert resp.json()["meta"]["total"] == 1
        # Verify mode=paper is passed
        _, kwargs = instance.list_by_user.call_args
        assert kwargs.get("mode") == "paper"

    @patch("app.api.routes.paper_trading.OrderDao")
    def test_cancel_paper_order(self, MockDao, client):
        MockDao.return_value.cancel.return_value = True
        resp = client.post("/api/v1/paper-trade/orders/1/cancel")
        assert resp.status_code == 200

    @patch("app.api.routes.paper_trading.OrderDao")
    def test_cancel_paper_order_not_found(self, MockDao, client):
        MockDao.return_value.cancel.return_value = False
        resp = client.post("/api/v1/paper-trade/orders/999/cancel")
        assert resp.status_code == 400


class TestPositionsAndPerformance:
    @patch("app.api.routes.paper_trading.PaperTradingService")
    def test_get_paper_positions(self, MockSvc, client):
        MockSvc.return_value.get_positions.return_value = [
            {"symbol": "000001.SZ", "direction": "buy", "quantity": 100, "avg_cost": 10.5},
        ]
        resp = client.get("/api/v1/paper-trade/positions")
        assert resp.status_code == 200
        assert len(resp.json()["positions"]) == 1

    @patch("app.api.routes.paper_trading.PaperTradingService")
    def test_get_paper_positions_empty(self, MockSvc, client):
        MockSvc.return_value.get_positions.return_value = []
        resp = client.get("/api/v1/paper-trade/positions")
        assert resp.status_code == 200
        assert resp.json()["positions"] == []

    @patch("app.api.routes.paper_trading.PaperTradingService")
    def test_get_paper_performance(self, MockSvc, client):
        MockSvc.return_value.get_performance.return_value = {
            "total_pnl": 1500.50,
            "total_trades": 20,
            "win_rate": 0.6,
            "max_drawdown": 0.05,
            "sharpe_ratio": None,
            "equity_curve": [{"date": "2026-01-01", "value": 100}],
        }
        resp = client.get("/api/v1/paper-trade/performance")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_pnl"] == 1500.50
        assert data["total_trades"] == 20

    @patch("app.api.routes.paper_trading.PaperTradingService")
    def test_get_paper_performance_empty(self, MockSvc, client):
        MockSvc.return_value.get_performance.return_value = {
            "total_pnl": 0.0,
            "total_trades": 0,
            "win_rate": 0.0,
            "max_drawdown": 0.0,
            "sharpe_ratio": None,
            "equity_curve": [],
        }
        resp = client.get("/api/v1/paper-trade/performance")
        assert resp.status_code == 200
        assert resp.json()["total_trades"] == 0
