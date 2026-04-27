from datetime import datetime, timedelta

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.routes import analytics
from app.api.models.user import TokenData


def _override_auth():
    return TokenData(user_id=7, username="tester", exp=datetime.utcnow() + timedelta(minutes=5))


def _build_client():
    app = FastAPI()
    app.include_router(analytics.router, prefix="/api/v1")
    app.dependency_overrides[analytics.get_current_user] = _override_auth
    return TestClient(app)


class TestAnalyticsRoutes:
    def test_dashboard_aggregates_portfolio_stats(self, monkeypatch):
        client = _build_client()

        class FakeDao:
            def get_or_create(self, user_id):
                assert user_id == 7
                return {"id": 1, "cash": 100.0}

            def list_positions(self, portfolio_id):
                assert portfolio_id == 1
                return [{"quantity": 2, "avg_cost": 5.0}, {"quantity": 3, "avg_cost": 10.0}]

        monkeypatch.setattr("app.domains.portfolio.dao.portfolio_dao.PortfolioDao", lambda: FakeDao())

        resp = client.get("/api/v1/analytics/dashboard")
        body = resp.json()

        assert resp.status_code == 200
        assert body["portfolio_stats"]["total_value"] == 140.0
        assert body["portfolio_stats"]["positions_count"] == 2
        assert body["risk_metrics"]["beta"] == 0.0

    def test_risk_metrics_uses_cash_ratio(self, monkeypatch):
        client = _build_client()

        class FakeDao:
            def get_or_create(self, user_id):
                return {"id": 1, "cash": 50.0}

            def list_positions(self, portfolio_id):
                return [{"quantity": 5, "avg_cost": 10.0}]

        monkeypatch.setattr("app.domains.portfolio.dao.portfolio_dao.PortfolioDao", lambda: FakeDao())

        resp = client.get("/api/v1/analytics/risk-metrics")

        assert resp.status_code == 200
        assert resp.json()["liquidity"]["cash_ratio"] == 0.5

    def test_live_pnl_passes_current_prices_to_service(self, monkeypatch):
        client = _build_client()
        captured = {}

        class FakeDao:
            def get_or_create(self, user_id):
                return {"id": 1, "cash": 12.0}

            def list_positions(self, portfolio_id):
                return [
                    {"symbol": "AAA", "quantity": 2, "last_price": 8.0, "avg_cost": 7.0},
                    {"symbol": "BBB", "quantity": 1, "avg_cost": 5.0},
                ]

        class FakeSvc:
            def calculate_live_pnl(self, positions, current_prices, cash):
                captured.update({"positions": positions, "current_prices": current_prices, "cash": cash})
                return {"ok": True}

        monkeypatch.setattr("app.domains.portfolio.dao.portfolio_dao.PortfolioDao", lambda: FakeDao())
        monkeypatch.setattr("app.domains.monitoring.pnl_monitor_service.PnLMonitorService", lambda: FakeSvc())

        resp = client.get("/api/v1/analytics/live-pnl")

        assert resp.status_code == 200
        assert resp.json() == {"ok": True}
        assert captured["current_prices"] == {"AAA": 8.0, "BBB": 5.0}
        assert captured["cash"] == 12.0

    def test_anomalies_builds_market_values_and_returns_alerts(self, monkeypatch):
        client = _build_client()
        captured = {}

        class FakeDao:
            def get_or_create(self, user_id):
                return {"id": 1, "cash": 20.0}

            def list_positions(self, portfolio_id):
                return [{"symbol": "AAA", "quantity": 2, "avg_cost": 5.0, "last_price": 6.0}]

            def list_snapshots(self, portfolio_id, limit=30):
                return [{"returns_1d": 0.1}, {"returns_1d": -0.2}]

        class FakeSvc:
            def detect_anomalies(self, daily_returns, positions, total_value):
                captured.update({"daily_returns": daily_returns, "positions": positions, "total_value": total_value})
                return [{"kind": "drawdown"}]

            def get_rules(self):
                return ["rule"]

        monkeypatch.setattr("app.domains.portfolio.dao.portfolio_dao.PortfolioDao", lambda: FakeDao())
        monkeypatch.setattr("app.domains.monitoring.pnl_monitor_service.PnLMonitorService", lambda: FakeSvc())

        resp = client.get("/api/v1/analytics/anomalies")

        assert resp.status_code == 200
        assert resp.json() == {"alerts": [{"kind": "drawdown"}], "rules": ["rule"]}
        assert captured["daily_returns"] == [-0.2, 0.1]
        assert captured["positions"][0]["market_value"] == 12.0
        assert captured["total_value"] == 32.0
