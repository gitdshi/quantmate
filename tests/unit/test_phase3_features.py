"""Tests for Data Source Toggle, Watchlists, Portfolio, Analytics, Trade Log (Phase 3)."""
import pytest
import json
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch
from fastapi import FastAPI, Depends
from fastapi.testclient import TestClient

from app.api.services.auth_service import get_current_user
from app.api.models.user import TokenData
from app.api.exception_handlers import register_exception_handlers

future_exp = datetime.utcnow() + timedelta(hours=1)
TEST_USER = TokenData(user_id=10, username="alice", exp=future_exp)


def _make_client(*routers):
    app = FastAPI()
    register_exception_handlers(app)
    app.dependency_overrides[get_current_user] = lambda: TEST_USER
    # Route-level Depends(require_permission(...)) resolves to nested dependency
    # callables, so override the concrete dependency objects attached to routes.
    for r in routers:
        for route in r.routes:
            dependant = getattr(route, "dependant", None)
            if not dependant:
                continue
            for dep in dependant.dependencies:
                call = getattr(dep, "call", None)
                if callable(call):
                    module = getattr(call, "__module__", "")
                    qualname = getattr(call, "__qualname__", "")
                    if module == "app.api.dependencies.permissions" and "require_permission" in qualname:
                        app.dependency_overrides[call] = lambda: TEST_USER
        app.include_router(r)
    return TestClient(app, raise_server_exceptions=False)


# ==================================================================
# Data Source Toggle
# ==================================================================
class TestDataSourceToggle:
    @pytest.fixture
    def client(self):
        from app.api.routes.settings import router
        return _make_client(router)

    def test_list_items(self, client):
        with patch("app.domains.market.dao.data_source_item_dao.DataSourceItemDao") as MockDao:
            MockDao.return_value.list_all.return_value = [
                {"id": 1, "source": "tushare", "item_key": "stock_basic", "enabled": 1}
            ]
            resp = client.get("/settings/datasource-items")
        assert resp.status_code == 200
        assert len(resp.json()["data"]) == 1

    def test_update_item(self, client):
        with patch("app.domains.market.dao.data_source_item_dao.DataSourceItemDao") as MockDao:
            MockDao.return_value.get_by_key.return_value = {"id": 1}
            MockDao.return_value.update_enabled.return_value = True
            resp = client.put(
                "/settings/datasource-items/stock_basic?source=tushare",
                json={"enabled": False},
            )
        assert resp.status_code == 200
        assert resp.json()["enabled"] is False

    def test_update_not_found(self, client):
        with patch("app.domains.market.dao.data_source_item_dao.DataSourceItemDao") as MockDao:
            MockDao.return_value.get_by_key.return_value = None
            resp = client.put(
                "/settings/datasource-items/nope?source=tushare",
                json={"enabled": True},
            )
        assert resp.status_code == 404

    def test_batch_update(self, client):
        with patch("app.domains.market.dao.data_source_item_dao.DataSourceItemDao") as MockDao:
            MockDao.return_value.batch_update.return_value = 2
            resp = client.put("/settings/datasource-items/batch", json={
                "items": [
                    {"source": "tushare", "item_key": "stock_basic", "enabled": True},
                    {"source": "tushare", "item_key": "stock_daily", "enabled": False},
                ]
            })
        assert resp.status_code == 200
        assert resp.json()["updated"] == 2


# ==================================================================
# Watchlist
# ==================================================================
class TestWatchlist:
    @pytest.fixture
    def client(self):
        from app.api.routes.watchlist import router
        return _make_client(router)

    def test_list_empty(self, client):
        with patch("app.domains.market.dao.watchlist_dao.WatchlistDao") as MockDao:
            MockDao.return_value.list_for_user.return_value = []
            resp = client.get("/data/watchlists")
        assert resp.status_code == 200
        assert resp.json()["data"] == []

    def test_create(self, client):
        with patch("app.domains.market.dao.watchlist_dao.WatchlistDao") as MockDao:
            MockDao.return_value.create.return_value = 1
            resp = client.post("/data/watchlists", json={"name": "My Picks"})
        assert resp.status_code == 200
        assert resp.json()["id"] == 1

    def test_update_own(self, client):
        with patch("app.domains.market.dao.watchlist_dao.WatchlistDao") as MockDao:
            MockDao.return_value.get.return_value = {"id": 1, "user_id": 10}
            resp = client.put("/data/watchlists/1", json={"name": "Renamed"})
        assert resp.status_code == 200

    def test_update_other_user_forbidden(self, client):
        with patch("app.domains.market.dao.watchlist_dao.WatchlistDao") as MockDao:
            MockDao.return_value.get.return_value = {"id": 1, "user_id": 999}
            resp = client.put("/data/watchlists/1", json={"name": "Steal"})
        assert resp.status_code == 403

    def test_delete(self, client):
        with patch("app.domains.market.dao.watchlist_dao.WatchlistDao") as MockDao:
            MockDao.return_value.get.return_value = {"id": 1, "user_id": 10}
            resp = client.delete("/data/watchlists/1")
        assert resp.status_code == 200

    def test_add_item(self, client):
        with patch("app.domains.market.dao.watchlist_dao.WatchlistDao") as MockDao:
            MockDao.return_value.get.return_value = {"id": 1, "user_id": 10}
            MockDao.return_value.add_item.return_value = 1
            resp = client.post("/data/watchlists/1/items", json={"symbol": "000001.SZ"})
        assert resp.status_code == 200
        assert resp.json()["symbol"] == "000001.SZ"

    def test_remove_item(self, client):
        with patch("app.domains.market.dao.watchlist_dao.WatchlistDao") as MockDao:
            MockDao.return_value.get.return_value = {"id": 1, "user_id": 10}
            MockDao.return_value.remove_item.return_value = True
            resp = client.delete("/data/watchlists/1/items/000001.SZ")
        assert resp.status_code == 200


# ==================================================================
# Portfolio
# ==================================================================
class TestPortfolio:
    @pytest.fixture
    def client(self):
        from app.api.routes.portfolio import router
        return _make_client(router)

    def test_get_positions(self, client):
        with patch("app.domains.portfolio.dao.portfolio_dao.PortfolioDao") as MockDao:
            MockDao.return_value.get_or_create.return_value = {"id": 1, "cash": 1000000}
            MockDao.return_value.list_positions.return_value = []
            resp = client.get("/portfolio/positions")
        assert resp.status_code == 200
        assert resp.json()["cash"] == 1000000

    def test_close_position_success(self, client):
        with patch("app.domains.portfolio.dao.portfolio_dao.PortfolioDao") as MockDao:
            MockDao.return_value.get_or_create.return_value = {"id": 1, "cash": 500000}
            MockDao.return_value.get_position.return_value = {"quantity": 1000, "avg_cost": 10.0}
            resp = client.post("/portfolio/close", json={
                "symbol": "000001.SZ", "quantity": 500, "price": 12.0
            })
        assert resp.status_code == 200
        body = resp.json()
        assert body["sold"] == 500
        assert body["proceeds"] == 6000.0

    def test_close_insufficient(self, client):
        with patch("app.domains.portfolio.dao.portfolio_dao.PortfolioDao") as MockDao:
            MockDao.return_value.get_or_create.return_value = {"id": 1, "cash": 500000}
            MockDao.return_value.get_position.return_value = {"quantity": 100, "avg_cost": 10.0}
            resp = client.post("/portfolio/close", json={
                "symbol": "000001.SZ", "quantity": 500, "price": 12.0
            })
        assert resp.status_code == 400

    def test_transactions(self, client):
        with patch("app.domains.portfolio.dao.portfolio_dao.PortfolioDao") as MockDao:
            MockDao.return_value.get_or_create.return_value = {"id": 1, "cash": 0}
            MockDao.return_value.count_transactions.return_value = 0
            MockDao.return_value.list_transactions.return_value = []
            resp = client.get("/portfolio/1/transactions")
        assert resp.status_code == 200

    def test_snapshots(self, client):
        with patch("app.domains.portfolio.dao.portfolio_dao.PortfolioDao") as MockDao:
            MockDao.return_value.get_or_create.return_value = {"id": 1, "cash": 0}
            MockDao.return_value.list_snapshots.return_value = []
            resp = client.get("/portfolio/1/snapshots")
        assert resp.status_code == 200


# ==================================================================
# Analytics
# ==================================================================
class TestAnalytics:
    @pytest.fixture
    def client(self):
        from app.api.routes.analytics import router
        return _make_client(router)

    def test_dashboard(self, client):
        with patch("app.domains.portfolio.dao.portfolio_dao.PortfolioDao") as MockDao:
            MockDao.return_value.get_or_create.return_value = {"id": 1, "cash": 1000000}
            MockDao.return_value.list_positions.return_value = []
            resp = client.get("/analytics/dashboard")
        assert resp.status_code == 200
        body = resp.json()
        assert body["portfolio_stats"]["total_value"] == 1000000
        assert body["portfolio_stats"]["positions_count"] == 0

    def test_risk_metrics_no_snapshots(self, client):
        with patch("app.domains.portfolio.dao.portfolio_dao.PortfolioDao") as MockDao:
            MockDao.return_value.get_or_create.return_value = {"id": 1, "cash": 1000000}
            MockDao.return_value.list_snapshots.return_value = []
            resp = client.get("/analytics/risk-metrics")
        assert resp.status_code == 200
        assert resp.json()["volatility"]["daily"] == 0.0


# ==================================================================
# Trade Log
# ==================================================================
class TestTradeLog:
    @pytest.fixture
    def client(self):
        from app.api.routes.trade_log import router
        return _make_client(router)

    def test_query(self, client):
        with patch("app.domains.market.dao.trade_log_dao.TradeLogDao") as MockDao:
            MockDao.return_value.count.return_value = 0
            MockDao.return_value.query.return_value = []
            resp = client.get("/reports/trade-logs")
        assert resp.status_code == 200

    def test_export_csv(self, client):
        with patch("app.domains.market.dao.trade_log_dao.TradeLogDao") as MockDao:
            MockDao.return_value.query.return_value = [
                {"id": 1, "timestamp": datetime(2025, 1, 1), "event_type": "signal",
                 "symbol": "000001.SZ", "direction": "buy", "quantity": 100,
                 "price": 10.0, "strategy_id": 1, "status": "created", "notes": None}
            ]
            resp = client.get("/reports/trade-logs/export?format=csv")
        assert resp.status_code == 200
        assert "text/csv" in resp.headers["content-type"]
        assert "000001.SZ" in resp.text

    def test_export_json(self, client):
        with patch("app.domains.market.dao.trade_log_dao.TradeLogDao") as MockDao:
            MockDao.return_value.query.return_value = []
            resp = client.get("/reports/trade-logs/export?format=json")
        assert resp.status_code == 200
