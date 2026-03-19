"""Tests for P2: Indicator library routes."""
import pytest
from unittest.mock import patch
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.routes import indicators
from app.api.exception_handlers import register_exception_handlers


@pytest.fixture
def mock_user():
    return type("User", (), {"id": 1, "user_id": 1, "username": "testuser"})()


@pytest.fixture
def client(mock_user):
    test_app = FastAPI()
    register_exception_handlers(test_app)
    test_app.include_router(indicators.router, prefix="/api/v1")

    async def override_auth():
        return mock_user

    test_app.dependency_overrides[indicators.get_current_user] = override_auth
    return TestClient(test_app)


class TestIndicators:
    @patch("app.api.routes.indicators.IndicatorConfigDao")
    def test_list_indicators(self, MockDao, client):
        instance = MockDao.return_value
        instance.list_all.return_value = [
            {"id": 1, "name": "SMA", "display_name": "Simple Moving Average", "category": "trend"}
        ]
        resp = client.get("/api/v1/indicators")
        assert resp.status_code == 200
        assert len(resp.json()["indicators"]) == 1

    @patch("app.api.routes.indicators.IndicatorConfigDao")
    def test_list_indicators_by_category(self, MockDao, client):
        instance = MockDao.return_value
        instance.list_all.return_value = []
        resp = client.get("/api/v1/indicators?category=trend")
        assert resp.status_code == 200
        instance.list_all.assert_called_once_with(category="trend")

    @patch("app.api.routes.indicators.IndicatorConfigDao")
    def test_get_indicator(self, MockDao, client):
        instance = MockDao.return_value
        instance.get_by_id.return_value = {"id": 1, "name": "SMA", "default_params": {"period": 20}}
        resp = client.get("/api/v1/indicators/1")
        assert resp.status_code == 200
        assert resp.json()["name"] == "SMA"

    @patch("app.api.routes.indicators.IndicatorConfigDao")
    def test_get_indicator_not_found(self, MockDao, client):
        instance = MockDao.return_value
        instance.get_by_id.return_value = None
        resp = client.get("/api/v1/indicators/999")
        assert resp.status_code == 404

    @patch("app.api.routes.indicators.IndicatorConfigDao")
    def test_create_indicator(self, MockDao, client):
        instance = MockDao.return_value
        instance.create.return_value = 17
        resp = client.post("/api/v1/indicators", json={
            "name": "MY_IND", "display_name": "My Indicator",
            "category": "custom", "default_params": {"period": 14}
        })
        assert resp.status_code == 201
        assert resp.json()["id"] == 17

    @patch("app.api.routes.indicators.IndicatorConfigDao")
    def test_create_indicator_invalid_category(self, MockDao, client):
        resp = client.post("/api/v1/indicators", json={
            "name": "BAD", "display_name": "Bad", "category": "invalid"
        })
        assert resp.status_code == 400

    @patch("app.api.routes.indicators.IndicatorConfigDao")
    def test_update_indicator(self, MockDao, client):
        instance = MockDao.return_value
        instance.update.return_value = True
        resp = client.put("/api/v1/indicators/1", json={"display_name": "Updated"})
        assert resp.status_code == 200

    @patch("app.api.routes.indicators.IndicatorConfigDao")
    def test_update_indicator_not_found(self, MockDao, client):
        instance = MockDao.return_value
        instance.update.return_value = False
        resp = client.put("/api/v1/indicators/999", json={"display_name": "X"})
        assert resp.status_code == 404

    @patch("app.api.routes.indicators.IndicatorConfigDao")
    def test_update_indicator_no_fields(self, MockDao, client):
        resp = client.put("/api/v1/indicators/1", json={})
        assert resp.status_code == 400

    @patch("app.api.routes.indicators.IndicatorConfigDao")
    def test_delete_indicator(self, MockDao, client):
        instance = MockDao.return_value
        instance.delete.return_value = True
        resp = client.delete("/api/v1/indicators/1")
        assert resp.status_code == 200

    @patch("app.api.routes.indicators.IndicatorConfigDao")
    def test_delete_builtin_indicator(self, MockDao, client):
        instance = MockDao.return_value
        instance.delete.return_value = False  # Built-in cannot be deleted
        resp = client.delete("/api/v1/indicators/1")
        assert resp.status_code == 400

