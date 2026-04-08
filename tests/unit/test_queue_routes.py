import sys
import types

from fastapi import FastAPI
from fastapi.testclient import TestClient

sys.modules.setdefault("vnpy", types.ModuleType("vnpy"))
sys.modules.setdefault("vnpy.trader", types.ModuleType("vnpy.trader"))
sys.modules.setdefault("vnpy.trader.constant", types.SimpleNamespace(Interval=object()))
sys.modules.setdefault("vnpy.trader.optimize", types.SimpleNamespace(OptimizationSetting=object()))
sys.modules.setdefault("vnpy_ctastrategy", types.ModuleType("vnpy_ctastrategy"))
sys.modules.setdefault("vnpy_ctastrategy.backtesting", types.SimpleNamespace(BacktestingEngine=object, BacktestingMode=object))

from app.api.exception_handlers import register_exception_handlers
from app.api.routes import queue


async def _override_auth():
    return type("User", (), {"user_id": 1, "username": "tester"})()


def _build_client():
    app = FastAPI()
    register_exception_handlers(app)
    app.include_router(queue.router, prefix="/api/v1")
    app.dependency_overrides[queue.get_current_user] = _override_auth
    return TestClient(app, raise_server_exceptions=False)


class TestQueueBacktestRoutes:
    def test_submit_backtest_forwards_version_and_source(self, monkeypatch):
        client = _build_client()
        captured = {}

        class FakeService:
            def submit_backtest(self, **kwargs):
                captured.update(kwargs)
                return "job-1"

        monkeypatch.setattr(queue, "get_backtest_service", lambda: FakeService())

        resp = client.post(
            "/api/v1/queue/backtest",
            json={
                "strategy_id": 5,
                "version_id": 12,
                "source": "strategy_research",
                "strategy_class": "DemoStrategy",
                "symbol": "000001.SZSE",
                "start_date": "2025-01-01",
                "end_date": "2025-01-31",
            },
        )

        assert resp.status_code == 200
        assert resp.json()["job_id"] == "job-1"
        assert captured["strategy_id"] == 5
        assert captured["version_id"] == 12
        assert captured["source"] == "strategy_research"

    def test_submit_bulk_backtest_forwards_version_and_source(self, monkeypatch):
        client = _build_client()
        captured = {}

        class FakeService:
            def submit_batch_backtest(self, **kwargs):
                captured.update(kwargs)
                return "bulk-1"

        monkeypatch.setattr(queue, "get_backtest_service", lambda: FakeService())

        resp = client.post(
            "/api/v1/queue/bulk-backtest",
            json={
                "strategy_id": 5,
                "version_id": 12,
                "source": "strategy_research",
                "strategy_class": "DemoStrategy",
                "symbols": ["000001.SZSE"],
                "start_date": "2025-01-01",
                "end_date": "2025-01-31",
            },
        )

        assert resp.status_code == 200
        assert resp.json()["job_id"] == "bulk-1"
        assert captured["version_id"] == 12
        assert captured["source"] == "strategy_research"
