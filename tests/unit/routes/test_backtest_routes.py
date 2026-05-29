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
    @patch(f"{_ROUTE}.run_backtest_task")
    @patch(f"{_ROUTE}.BacktestHistoryDao")
    def test_submit_unified_strategy_run_validation(self, MockDao, _mock_task, client):
        resp = client.post(
            "/api/v1/backtest/runs",
            json={
                "subject_type": "strategy",
                "subject_id": 1,
                "subject_name": "TripleMA",
                "start_date": "2023-01-01",
                "end_date": "2023-12-31",
                "profile": {"vt_symbol": "000001.SZSE", "parameters": {}},
            },
        )
        assert resp.status_code == 200

    def test_submit_unified_factor_run_requires_expression_or_factor(self, client):
        resp = client.post(
            "/api/v1/backtest/runs",
            json={
                "subject_type": "factor",
                "start_date": "2023-01-01",
                "end_date": "2023-12-31",
            },
        )
        assert resp.status_code == 400

    @patch(f"{_ROUTE}.CompositeStrategyService")
    def test_submit_unified_composite_run(self, MockCompositeService, client):
        MockCompositeService.return_value.submit_backtest.return_value = {"job_id": "cbt_abc", "status": "queued"}
        resp = client.post(
            "/api/v1/backtest/runs",
            json={
                "subject_type": "composite",
                "subject_id": 8,
                "subject_name": "Composite Alpha",
                "start_date": "2023-01-01",
                "end_date": "2023-12-31",
                "benchmark": "000300.SH",
                "profile": {"composite_strategy_id": 8},
            },
        )
        assert resp.status_code == 200
        payload = resp.json()
        assert payload["job_id"] == "cbt_abc"

    @patch(f"{_ROUTE}.run_factor_backtest_task")
    @patch(f"{_ROUTE}.BacktestHistoryDao")
    @patch(f"{_ROUTE}.FactorService")
    def test_submit_unified_factor_run(self, MockFactorService, MockDao, _mock_task, client):
        MockFactorService.return_value.get_factor.return_value = {
            "id": 7,
            "name": "Quality",
            "expression": "close",
        }
        resp = client.post(
            "/api/v1/backtest/runs",
            json={
                "subject_type": "factor",
                "subject_id": 7,
                "start_date": "2024-01-01",
                "end_date": "2024-03-31",
                "profile": {"instruments": ["000001.SZ", "000002.SZ"], "top_n": 1},
            },
        )
        assert resp.status_code == 200
        payload = resp.json()
        assert payload["subject_type"] == "factor"
        MockDao.return_value.upsert_history.assert_called_once()

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

    @patch(f"{_ROUTE}.BacktestHistoryDao")
    def test_list_runs(self, MockDao, client):
        dao = MockDao.return_value
        dao.count_runs_for_user.return_value = 1
        dao.list_runs_for_user.return_value = [
            {
                "id": 1,
                "job_id": "j1",
                "subject_type": "strategy",
                "subject_id": 1,
                "subject_name": "TripleMA",
                "engine_type": "vnpy",
                "scope_type": "single_symbol",
                "strategy_id": 1,
                "strategy_class": "TripleMA",
                "vt_symbol": "000001.SZSE",
                "start_date": "2023-01-01",
                "end_date": "2023-12-31",
                "status": "completed",
                "summary_json": '{"total_return": 0.1}',
                "result": None,
                "created_at": None,
                "completed_at": None,
            }
        ]
        resp = client.get("/api/v1/backtest/runs")
        assert resp.status_code == 200

    @patch(f"{_ROUTE}.BacktestHistoryDao")
    def test_get_run_detail(self, MockDao, client):
        dao = MockDao.return_value
        dao.get_run_detail_for_user.return_value = {
            "id": 1,
            "job_id": "j1",
            "subject_type": "strategy",
            "subject_id": 1,
            "subject_name": "TripleMA",
            "engine_type": "vnpy",
            "scope_type": "single_symbol",
            "strategy_id": 1,
            "strategy_class": "TripleMA",
            "vt_symbol": "000001.SZSE",
            "start_date": "2023-01-01",
            "end_date": "2023-12-31",
            "parameters": "{}",
            "status": "completed",
            "result": '{"total_return": 0.1}',
            "error": None,
            "request_payload": '{"subject_type": "strategy"}',
            "summary_json": '{"total_return": 0.1}',
            "artifacts_json": '{}',
            "diagnostics_json": '{}',
            "extensions_json": '{}',
            "result_schema_version": 2,
            "created_at": None,
            "completed_at": None,
        }
        resp = client.get("/api/v1/backtest/runs/j1")
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
