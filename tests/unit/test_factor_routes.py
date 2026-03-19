"""Tests for Factor Lab routes."""
import pytest
from unittest.mock import patch
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.routes import factors
from app.api.exception_handlers import register_exception_handlers


@pytest.fixture
def mock_user():
    return type("User", (), {"id": 1, "user_id": 1, "username": "testuser"})()


@pytest.fixture
def client(mock_user):
    test_app = FastAPI()
    register_exception_handlers(test_app)
    test_app.include_router(factors.router, prefix="/api/v1")

    async def override_auth():
        return mock_user

    test_app.dependency_overrides[factors.get_current_user] = override_auth
    return TestClient(test_app)


class TestFactorRoutes:

    @patch("app.domains.factors.service.FactorDefinitionDao")
    def test_list_factors(self, MockDao, client):
        instance = MockDao.return_value
        instance.list_for_user.return_value = [
            {"id": 1, "name": "Momentum", "category": "momentum", "status": "draft"}
        ]
        instance.count_for_user.return_value = 1
        resp = client.get("/api/v1/factors")
        assert resp.status_code == 200
        assert "data" in resp.json()

    @patch("app.domains.factors.service.FactorDefinitionDao")
    def test_list_factors_with_category(self, MockDao, client):
        instance = MockDao.return_value
        instance.list_for_user.return_value = []
        instance.count_for_user.return_value = 0
        resp = client.get("/api/v1/factors?category=momentum")
        assert resp.status_code == 200

    @patch("app.domains.factors.service.FactorDefinitionDao")
    def test_create_factor(self, MockDao, client):
        instance = MockDao.return_value
        instance.create.return_value = 1
        instance.get.return_value = {
            "id": 1, "user_id": 1, "name": "RSI Factor",
            "expression": "RSI(close, 14)", "category": "momentum",
        }
        resp = client.post("/api/v1/factors", json={
            "name": "RSI Factor", "expression": "RSI(close, 14)", "category": "momentum",
        })
        assert resp.status_code == 201
        assert resp.json()["name"] == "RSI Factor"

    @patch("app.domains.factors.service.FactorDefinitionDao")
    def test_get_factor(self, MockDao, client):
        instance = MockDao.return_value
        instance.get.return_value = {"id": 1, "user_id": 1, "name": "Test"}
        resp = client.get("/api/v1/factors/1")
        assert resp.status_code == 200

    @patch("app.domains.factors.service.FactorDefinitionDao")
    def test_get_factor_not_found(self, MockDao, client):
        instance = MockDao.return_value
        instance.get.return_value = None
        resp = client.get("/api/v1/factors/999")
        assert resp.status_code == 404

    @patch("app.domains.factors.service.FactorDefinitionDao")
    def test_update_factor(self, MockDao, client):
        instance = MockDao.return_value
        instance.get.return_value = {"id": 1, "user_id": 1, "name": "Updated Factor"}
        instance.update.return_value = None
        resp = client.put("/api/v1/factors/1", json={"name": "Updated Factor"})
        assert resp.status_code == 200

    @patch("app.domains.factors.service.FactorDefinitionDao")
    def test_delete_factor(self, MockDao, client):
        instance = MockDao.return_value
        instance.delete.return_value = True
        resp = client.delete("/api/v1/factors/1")
        assert resp.status_code == 204

    @patch("app.domains.factors.service.FactorDefinitionDao")
    def test_delete_factor_not_found(self, MockDao, client):
        instance = MockDao.return_value
        instance.delete.return_value = False
        resp = client.delete("/api/v1/factors/999")
        assert resp.status_code == 404


class TestFactorEvaluationRoutes:

    @patch("app.domains.factors.service.FactorEvaluationDao")
    @patch("app.domains.factors.service.FactorDefinitionDao")
    def test_list_evaluations(self, MockFactorDao, MockEvalDao, client):
        MockFactorDao.return_value.get.return_value = {"id": 1, "user_id": 1}
        MockEvalDao.return_value.list_for_factor.return_value = [
            {"id": 1, "factor_id": 1, "ic_mean": 0.035}
        ]
        resp = client.get("/api/v1/factors/1/evaluations")
        assert resp.status_code == 200

    @patch("app.domains.factors.service.FactorEvaluationDao")
    @patch("app.domains.factors.service.FactorDefinitionDao")
    def test_run_evaluation(self, MockFactorDao, MockEvalDao, client):
        MockFactorDao.return_value.get.return_value = {"id": 1, "user_id": 1}
        MockEvalDao.return_value.create.return_value = 1
        MockEvalDao.return_value.get.return_value = {
            "id": 1, "factor_id": 1, "ic_mean": 0.035, "ic_ir": 0.42,
        }
        resp = client.post("/api/v1/factors/1/evaluations", json={
            "start_date": "2024-01-01", "end_date": "2024-12-31",
        })
        assert resp.status_code == 201

    @patch("app.domains.factors.service.FactorEvaluationDao")
    @patch("app.domains.factors.service.FactorDefinitionDao")
    def test_delete_evaluation(self, MockFactorDao, MockEvalDao, client):
        MockFactorDao.return_value.get.return_value = {"id": 1, "user_id": 1}
        MockEvalDao.return_value.delete.return_value = True
        resp = client.delete("/api/v1/factors/1/evaluations/1")
        assert resp.status_code == 204

