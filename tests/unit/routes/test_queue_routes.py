"""Unit tests for app.api.routes.queue."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.routes import queue
from app.api.exception_handlers import register_exception_handlers

# Patch at the ROUTE module, not the source module
_ROUTE = "app.api.routes.queue"


@pytest.fixture()
def mock_user():
    return type("User", (), {"id": 1, "user_id": 1, "username": "test", "sub": 1})()


@pytest.fixture()
def client(mock_user):
    test_app = FastAPI()
    register_exception_handlers(test_app)
    test_app.include_router(queue.router, prefix="/api/v1")

    async def override_auth():
        return mock_user

    test_app.dependency_overrides[queue.get_current_user] = override_auth
    for route in test_app.routes:
        if hasattr(route, "dependencies"):
            route.dependencies = []
    return TestClient(test_app, raise_server_exceptions=False)


class TestQueueRoutes:
    @patch(f"{_ROUTE}.get_job_storage")
    def test_get_queue_stats(self, mock_js, client):
        mock_js.return_value.get_queue_stats.return_value = {"queued": 5}
        resp = client.get("/api/v1/queue/stats")
        assert resp.status_code == 200

    @patch(f"{_ROUTE}.JobsService")
    def test_list_jobs(self, MockSvc, client):
        MockSvc.return_value.list_jobs.return_value = [{"job_id": "j1"}]
        resp = client.get("/api/v1/queue/jobs")
        assert resp.status_code == 200

    @patch(f"{_ROUTE}.get_backtest_service")
    def test_get_job_detail(self, mock_svc, client):
        mock_svc.return_value.get_job_status.return_value = {"status": "completed"}
        resp = client.get("/api/v1/queue/jobs/j1")
        assert resp.status_code == 200

    @patch(f"{_ROUTE}.get_backtest_service")
    def test_cancel_job(self, mock_svc, client):
        mock_svc.return_value.cancel_job.return_value = True
        resp = client.post("/api/v1/queue/jobs/j1/cancel")
        assert resp.status_code == 200

    @patch(f"{_ROUTE}.JobsService")
    @patch(f"{_ROUTE}.get_backtest_service")
    def test_delete_job(self, mock_bt_svc, MockJobs, client):
        mock_bt_svc.return_value.get_job_status.return_value = {"job_id": "j1"}
        MockJobs.return_value.delete_job_and_results.return_value = None
        resp = client.delete("/api/v1/queue/jobs/j1")
        assert resp.status_code == 200

    @patch(f"{_ROUTE}.get_backtest_service")
    def test_submit_backtest_to_queue(self, mock_svc, client):
        mock_svc.return_value.submit_backtest.return_value = "job-1"
        resp = client.post("/api/v1/queue/backtest", json={
            "strategy_id": 1, "strategy_class": "TripleMA",
            "symbol": "000001.SZSE",
            "start_date": "2023-01-01", "end_date": "2023-12-31",
        })
        assert resp.status_code == 200
