"""Tests for P2: Optimization task routes."""
import pytest
from unittest.mock import patch
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.routes import optimization
from app.api.exception_handlers import register_exception_handlers


@pytest.fixture
def mock_user():
    return {"id": 1, "username": "testuser"}


@pytest.fixture
def client(mock_user):
    test_app = FastAPI()
    register_exception_handlers(test_app)
    test_app.include_router(optimization.router, prefix="/api/v1")

    async def override_auth():
        return mock_user

    test_app.dependency_overrides[optimization.get_current_user] = override_auth
    return TestClient(test_app)


class TestOptimizationTasks:
    @patch("app.api.routes.optimization.OptimizationTaskDao")
    def test_list_tasks(self, MockDao, client):
        instance = MockDao.return_value
        instance.list_by_user.return_value = (
            [{"id": 1, "strategy_id": 1, "search_method": "grid", "status": "pending"}], 1
        )
        resp = client.get("/api/v1/optimization/tasks")
        assert resp.status_code == 200
        assert resp.json()["meta"]["total"] == 1

    @patch("app.api.routes.optimization.OptimizationTaskDao")
    def test_get_task(self, MockDao, client):
        instance = MockDao.return_value
        instance.get_by_id.return_value = {
            "id": 1, "strategy_id": 1, "search_method": "grid",
            "param_space": {"sma_period": [10, 20, 30]}, "status": "completed"
        }
        resp = client.get("/api/v1/optimization/tasks/1")
        assert resp.status_code == 200
        assert resp.json()["search_method"] == "grid"

    @patch("app.api.routes.optimization.OptimizationTaskDao")
    def test_get_task_not_found(self, MockDao, client):
        instance = MockDao.return_value
        instance.get_by_id.return_value = None
        resp = client.get("/api/v1/optimization/tasks/999")
        assert resp.status_code == 404

    @patch("app.api.routes.optimization.OptimizationTaskDao")
    def test_create_task(self, MockDao, client):
        instance = MockDao.return_value
        instance.create.return_value = 1
        resp = client.post("/api/v1/optimization/tasks", json={
            "strategy_id": 1, "search_method": "grid",
            "param_space": {"sma_period": [10, 20, 30]},
            "objective_metric": "sharpe_ratio"
        })
        assert resp.status_code == 201
        assert resp.json()["id"] == 1

    @patch("app.api.routes.optimization.OptimizationTaskDao")
    def test_create_task_invalid_method(self, MockDao, client):
        resp = client.post("/api/v1/optimization/tasks", json={
            "strategy_id": 1, "search_method": "invalid",
            "param_space": {"sma_period": [10, 20, 30]}
        })
        assert resp.status_code == 400

    @patch("app.api.routes.optimization.OptimizationTaskDao")
    def test_get_results(self, MockDao, client):
        instance = MockDao.return_value
        instance.get_results.return_value = [
            {"id": 1, "task_id": 1, "params": {"sma_period": 20}, "metrics": {"sharpe_ratio": 1.5}, "rank_order": 1}
        ]
        resp = client.get("/api/v1/optimization/tasks/1/results")
        assert resp.status_code == 200
        assert len(resp.json()["results"]) == 1
