"""Unit tests for app.api.routes.backtest."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.routes import backtest
from app.api.exception_handlers import register_exception_handlers

# Patch at point-of-use (the route module), NOT the service definition module
_ROUTE = "app.api.routes.backtest"


@pytest.fixture()
def mock_user():
    return type("User", (), {"id": 1, "user_id": 1, "username": "test", "sub": 1})()


@pytest.fixture()
def client(mock_user):
    test_app = FastAPI()
    register_exception_handlers(test_app)
    test_app.include_router(backtest.router, prefix="/api/v1")

    async def override_auth():
        return mock_user

    test_app.dependency_overrides[backtest.get_current_user] = override_auth
    # remove permission dependency
    for route in test_app.routes:
        if hasattr(route, "dependencies"):
            route.dependencies = []
    return TestClient(test_app, raise_server_exceptions=False)


class TestBacktestRoutes:
    def test_submit_backtest(self, client):
        # Handler uses in-memory dict + background task
        resp = client.post("/api/v1/backtest", json={
            "strategy_id": 1,
            "strategy_class": "TripleMA",
            "symbol": "000001.SZSE",
            "start_date": "2023-01-01",
            "end_date": "2023-12-31",
        })
        assert resp.status_code in (200, 422)

    def test_get_status_not_found(self, client):
        # Not in _jobs → 404
        resp = client.get("/api/v1/backtest/nonexistent")
        assert resp.status_code == 404

    @patch(f"{_ROUTE}.BacktestHistoryDao")
    def test_list_history(self, MockDao, client):
        dao = MockDao.return_value
        dao.count_for_user.return_value = 1
        dao.list_for_user.return_value = [
            {
                "id": 1, "job_id": "j1", "strategy_id": 1,
                "strategy_class": "TripleMA", "strategy_version": "1",
                "vt_symbol": "000001.SZSE",
                "start_date": "2023-01-01", "end_date": "2023-12-31",
                "status": "completed", "result": None,
                "created_at": None, "completed_at": None,
            }
        ]
        resp = client.get("/api/v1/backtest/history/list")
        assert resp.status_code == 200

    def test_cancel_not_in_jobs(self, client):
        # _jobs empty → 404
        resp = client.delete("/api/v1/backtest/j1")
        assert resp.status_code == 404

    def test_submit_batch(self, client):
        resp = client.post("/api/v1/backtest/batch", json={
            "strategy_id": 1,
            "strategy_class": "TripleMA",
            "symbols": ["000001.SZSE", "000002.SZSE"],
            "start_date": "2023-01-01",
            "end_date": "2023-12-31",
        })
        assert resp.status_code == 200

    def test_get_batch_status_not_found(self, client):
        resp = client.get("/api/v1/backtest/batch/b1")
        assert resp.status_code == 404

    @patch(f"{_ROUTE}.BacktestHistoryDao")
    def test_get_history_detail(self, MockDao, client):
        dao = MockDao.return_value
        dao.get_detail_for_user.return_value = {
            "id": 1, "job_id": "j1", "strategy_id": 1,
            "strategy_class": "TripleMA", "strategy_version": "1",
            "vt_symbol": "000001.SZSE",
            "start_date": "2023-01-01", "end_date": "2023-12-31",
            "parameters": None, "status": "completed",
            "result": '{"statistics": {"total_return": 0.1}}',
            "error": None,
            "created_at": None, "completed_at": None,
        }
        resp = client.get("/api/v1/backtest/history/j1")
        assert resp.status_code == 200
