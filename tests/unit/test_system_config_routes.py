"""Tests for P2: System configuration routes."""
import pytest
from unittest.mock import patch
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.routes import system_config
from app.api.exception_handlers import register_exception_handlers


@pytest.fixture
def mock_user():
    return type("User", (), {"id": 1, "user_id": 1, "username": "admin"})()


@pytest.fixture
def client(mock_user):
    test_app = FastAPI()
    register_exception_handlers(test_app)
    test_app.include_router(system_config.router, prefix="/api/v1")

    async def override_auth():
        return mock_user

    test_app.dependency_overrides[system_config.get_current_user] = override_auth
    return TestClient(test_app)


class TestSystemConfigs:
    @patch("app.api.routes.system_config.SystemConfigDao")
    def test_list_configs(self, MockDao, client):
        instance = MockDao.return_value
        instance.list_all.return_value = [
            {"config_key": "theme", "config_value": "dark", "category": "ui"}
        ]
        resp = client.get("/api/v1/system/configs")
        assert resp.status_code == 200
        assert len(resp.json()["configs"]) == 1

    @patch("app.api.routes.system_config.SystemConfigDao")
    def test_list_configs_by_category(self, MockDao, client):
        instance = MockDao.return_value
        instance.list_all.return_value = []
        resp = client.get("/api/v1/system/configs?category=ui")
        assert resp.status_code == 200
        instance.list_all.assert_called_once_with(category="ui")

    @patch("app.api.routes.system_config.SystemConfigDao")
    def test_get_config(self, MockDao, client):
        instance = MockDao.return_value
        instance.get.return_value = {"config_key": "theme", "config_value": "dark"}
        resp = client.get("/api/v1/system/configs/theme")
        assert resp.status_code == 200
        assert resp.json()["config_value"] == "dark"

    @patch("app.api.routes.system_config.SystemConfigDao")
    def test_get_config_not_found(self, MockDao, client):
        instance = MockDao.return_value
        instance.get.return_value = None
        resp = client.get("/api/v1/system/configs/nonexistent")
        assert resp.status_code == 404

    @patch("app.api.routes.system_config.SystemConfigDao")
    def test_upsert_config(self, MockDao, client):
        instance = MockDao.return_value
        resp = client.put("/api/v1/system/configs", json={
            "config_key": "theme", "config_value": "dark", "category": "ui"
        })
        assert resp.status_code == 200
        instance.upsert.assert_called_once()

    @patch("app.api.routes.system_config.SystemConfigDao")
    def test_delete_config(self, MockDao, client):
        instance = MockDao.return_value
        instance.delete.return_value = True
        resp = client.delete("/api/v1/system/configs/theme")
        assert resp.status_code == 200

    @patch("app.api.routes.system_config.SystemConfigDao")
    def test_delete_config_not_found(self, MockDao, client):
        instance = MockDao.return_value
        instance.delete.return_value = False
        resp = client.delete("/api/v1/system/configs/nonexistent")
        assert resp.status_code == 404


class TestDataSourceConfigs:
    @patch("app.api.routes.system_config.DataSourceConfigDao")
    def test_list_data_sources(self, MockDao, client):
        instance = MockDao.return_value
        instance.list_all.return_value = [{"source_name": "tushare", "is_enabled": True}]
        resp = client.get("/api/v1/system/data-sources")
        assert resp.status_code == 200

    @patch("app.api.routes.system_config.DataSourceConfigDao")
    def test_upsert_data_source(self, MockDao, client):
        instance = MockDao.return_value
        resp = client.put("/api/v1/system/data-sources", json={
            "source_name": "tushare", "is_enabled": True, "rate_limit_per_min": 200
        })
        assert resp.status_code == 200
        instance.upsert.assert_called_once()

