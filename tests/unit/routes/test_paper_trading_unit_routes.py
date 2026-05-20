"""Unit tests for app.api.routes.paper_trading."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.routes import paper_trading
from app.api.exception_handlers import register_exception_handlers

# Patch at route module level
_ROUTE = "app.api.routes.paper_trading"


@pytest.fixture()
def mock_user():
    return type("User", (), {"id": 1, "user_id": 1, "username": "test", "sub": 1})()


@pytest.fixture()
def client(mock_user):
    test_app = FastAPI()
    register_exception_handlers(test_app)
    test_app.include_router(paper_trading.router, prefix="/api/v1")

    async def override_auth():
        return mock_user

    test_app.dependency_overrides[paper_trading.get_current_user] = override_auth
    for route in test_app.routes:
        if hasattr(route, "dependencies"):
            route.dependencies = []
    return TestClient(test_app, raise_server_exceptions=False)


class TestPaperTradingRoutes:
    @patch(f"{_ROUTE}.PaperTradingService")
    def test_list_deployments(self, MockSvc, client):
        MockSvc.return_value.list_deployments.return_value = [{"id": 1}]
        resp = client.get("/api/v1/paper-trade/deployments")
        assert resp.status_code == 200
        assert resp.json()["deployments"] == [{"id": 1}]

    @patch(f"{_ROUTE}.PaperTradingService")
    def test_list_deployments_with_account_filter(self, MockSvc, client):
        MockSvc.return_value.list_deployments.return_value = [{"id": 1}]

        resp = client.get("/api/v1/paper-trade/deployments?paper_account_id=8")

        assert resp.status_code == 200
        MockSvc.return_value.list_deployments.assert_called_once_with(user_id=1, paper_account_id=8)

    @patch(f"{_ROUTE}.PaperTradingService")
    def test_get_deployment_runtime(self, MockSvc, client):
        MockSvc.return_value.get_deployment_runtime.return_value = {
            "deployment_id": 1,
            "runtime_status": "running",
        }
        resp = client.get("/api/v1/paper-trade/deployments/1/runtime")
        assert resp.status_code == 200

    @patch(f"{_ROUTE}.PaperTradingService")
    @patch(f"{_ROUTE}.PaperAccountService")
    @patch(f"{_ROUTE}.PaperRuntimeService")
    def test_deploy_strategy(self, MockRuntime, MockAcct, MockSvc, client):
        MockAcct.return_value.get_account.return_value = {"status": "active"}
        MockRuntime.return_value.preview_runtime.return_value = {
            "status": "pending",
            "runtime_mode": "legacy_executor_bridge",
        }
        MockSvc.return_value.deploy.return_value = {
            "success": True, "deployment_id": 1, "strategy_name": "MA",
        }
        resp = client.post("/api/v1/paper-trade/deploy", json={
            "strategy_id": 1, "vt_symbol": "000001.SZSE",
            "paper_account_id": 1, "parameters": {},
        })
        assert resp.status_code in (200, 201)

    @patch(f"{_ROUTE}.PaperTradingService")
    @patch(f"{_ROUTE}.PaperAccountService")
    @patch(f"{_ROUTE}.PaperRuntimeService")
    def test_deploy_strategy_accepts_vt_symbols(self, MockRuntime, MockAcct, MockSvc, client):
        MockAcct.return_value.get_account.return_value = {"status": "active"}
        MockRuntime.return_value.preview_runtime.return_value = {
            "status": "pending",
            "runtime_mode": "portfolio_strategy_bridge",
        }
        MockSvc.return_value.deploy.return_value = {
            "success": True, "deployment_id": 2, "strategy_name": "Pairs",
        }
        resp = client.post("/api/v1/paper-trade/deploy", json={
            "strategy_id": 1,
            "vt_symbols": ["000001.SZSE", "000002.SZSE"],
            "paper_account_id": 1,
            "parameters": {},
        })
        assert resp.status_code in (200, 201)
        _, kwargs = MockSvc.return_value.deploy.call_args
        assert kwargs["vt_symbol"] == "000001.SZSE,000002.SZSE"

    @patch(f"{_ROUTE}.PaperTradingService")
    def test_stop_deployment(self, MockSvc, client):
        MockSvc.return_value.stop_deployment.return_value = True
        resp = client.post("/api/v1/paper-trade/deployments/1/stop")
        assert resp.status_code == 200

    @patch(f"{_ROUTE}.PaperTradingService")
    def test_get_positions(self, MockSvc, client):
        MockSvc.return_value.get_positions.return_value = [{"symbol": "000001.SZ"}]
        resp = client.get("/api/v1/paper-trade/positions")
        assert resp.status_code == 200

    @patch(f"{_ROUTE}.PaperTradingService")
    def test_get_positions_with_account_filter(self, MockSvc, client):
        MockSvc.return_value.get_positions.return_value = [{"symbol": "000001.SZ"}]

        resp = client.get("/api/v1/paper-trade/positions?paper_account_id=8")

        assert resp.status_code == 200
        MockSvc.return_value.get_positions.assert_called_once_with(user_id=1, paper_account_id=8)

    @patch(f"{_ROUTE}.PaperTradingService")
    def test_get_performance(self, MockSvc, client):
        MockSvc.return_value.get_performance.return_value = {"pnl": 1000}
        resp = client.get("/api/v1/paper-trade/performance")
        assert resp.status_code == 200

    @patch(f"{_ROUTE}.connection")
    def test_list_signals(self, mock_conn, client):
        ctx = MagicMock()
        mock_conn.return_value.__enter__ = MagicMock(return_value=ctx)
        mock_conn.return_value.__exit__ = MagicMock(return_value=False)
        ctx.execute.return_value.fetchall.return_value = []
        resp = client.get("/api/v1/paper-trade/signals")
        assert resp.status_code == 200

    @patch(f"{_ROUTE}.PaperExecutionLedger")
    @patch(f"{_ROUTE}.OrderDao")
    @patch(f"{_ROUTE}.PaperAccountService")
    @patch(f"{_ROUTE}.RealtimeQuoteService")
    @patch(f"{_ROUTE}.validate_order")
    @patch(f"{_ROUTE}.try_fill_market_order")
    def test_create_paper_order(self, mock_fill, mock_validate, mock_quote_svc, MockAcct, MockDao, MockLedger, client):
        # Setup mocks
        MockAcct.return_value.get_account.return_value = {
            "status": "active", "market": "CN", "balance": 100000,
        }
        vr = MagicMock()
        vr.valid = True
        mock_validate.return_value = vr
        mock_quote_svc.return_value.get_quote.return_value = {"last_price": 10.0}
        fill = MagicMock()
        fill.filled = True
        fill.fill_price = 10.0
        fill.fill_quantity = 100
        fill.fee = MagicMock(total=1.0)
        mock_fill.return_value = fill
        MockDao.return_value.create.return_value = 1
        MockDao.return_value.get_by_id.return_value = {"id": 1, "status": "filled"}

        resp = client.post("/api/v1/paper-trade/orders", json={
            "symbol": "000001.SZSE", "direction": "buy",
            "quantity": 100, "price": 10.0, "paper_account_id": 1,
        })
        assert resp.status_code in (200, 201)
        MockLedger.return_value.record_order_event.assert_called()

    @patch(f"{_ROUTE}.OrderDao")
    def test_list_orders(self, MockDao, client):
        MockDao.return_value.list_by_user.return_value = ([], 0)
        resp = client.get("/api/v1/paper-trade/orders")
        assert resp.status_code == 200

    @patch(f"{_ROUTE}.OrderDao")
    def test_list_orders_with_account_filter(self, MockDao, client):
        MockDao.return_value.list_by_user.return_value = ([], 0)

        resp = client.get("/api/v1/paper-trade/orders?paper_account_id=8")

        assert resp.status_code == 200
        MockDao.return_value.list_by_user.assert_called_once_with(
            user_id=1,
            status=None,
            mode="paper",
            page=1,
            page_size=50,
            paper_account_id=8,
        )
