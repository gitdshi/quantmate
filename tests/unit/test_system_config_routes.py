"""Tests for P2: System configuration routes."""
import pytest
from unittest.mock import patch
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.routes import system_config
from app.api.exception_handlers import register_exception_handlers
from app.infrastructure.config.system_config_registry import SystemConfigDefinition


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

    @patch("app.api.routes.system_config.resolve_runtime_config_value")
    @patch("app.api.routes.system_config.list_db_system_config_definitions")
    @patch("app.api.routes.system_config.SystemConfigDao")
    def test_list_runtime_config_catalog(self, MockDao, mock_list_defs, mock_resolve, client):
        instance = MockDao.return_value
        instance.list_all.return_value = [
            {"config_key": "datasync.sync_hour", "config_value": "5", "updated_at": "2026-04-22T12:00:00"}
        ]
        mock_list_defs.return_value = [
            SystemConfigDefinition(
                key="datasync.sync_hour",
                category="datasync",
                label="Daily sync hour",
                description="Daily sync schedule hour in 24-hour local time.",
                value_type="int",
                default_value="2",
                legacy_env_keys=("SYNC_HOUR",),
            )
        ]
        mock_resolve.return_value = ("5", "db")

        resp = client.get("/api/v1/system/configs/catalog")
        assert resp.status_code == 200
        assert resp.json()["configs"] == [
            {
                "key": "datasync.sync_hour",
                "category": "datasync",
                "label": "Daily sync hour",
                "description": "Daily sync schedule hour in 24-hour local time.",
                "value_type": "int",
                "default_value": "2",
                "legacy_env_keys": ["SYNC_HOUR"],
                "current_value": "5",
                "stored_value": "5",
                "is_overridden": True,
                "value_source": "db",
                "updated_at": "2026-04-22T12:00:00",
            }
        ]

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
