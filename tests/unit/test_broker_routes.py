"""Tests for P2: Broker configuration routes."""
import pytest
from unittest.mock import patch
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.routes import broker
from app.api.exception_handlers import register_exception_handlers


@pytest.fixture
def mock_user():
    return {"id": 1, "username": "testuser"}


@pytest.fixture
def client(mock_user):
    test_app = FastAPI()
    register_exception_handlers(test_app)
    test_app.include_router(broker.router, prefix="/api/v1")

    async def override_auth():
        return mock_user

    test_app.dependency_overrides[broker.get_current_user] = override_auth
    return TestClient(test_app)


class TestBrokerConfigs:
    @patch("app.api.routes.broker.BrokerConfigDao")
    def test_list_configs(self, MockDao, client):
        instance = MockDao.return_value
        instance.list_by_user.return_value = [
            {"id": 1, "broker_name": "simnow", "config": {"api_key": "abc", "secret": "xyz"}}
        ]
        resp = client.get("/api/v1/broker/configs")
        assert resp.status_code == 200
        configs = resp.json()["configs"]
        assert len(configs) == 1
        # Secret fields should be masked
        assert configs[0]["config"]["secret"] == "***"

    @patch("app.api.routes.broker.BrokerConfigDao")
    def test_create_config(self, MockDao, client):
        instance = MockDao.return_value
        instance.create.return_value = 1
        resp = client.post("/api/v1/broker/configs", json={
            "broker_name": "simnow", "config": {"api_key": "abc"}, "is_paper": True
        })
        assert resp.status_code == 201
        assert resp.json()["id"] == 1

    @patch("app.api.routes.broker.BrokerConfigDao")
    def test_update_config(self, MockDao, client):
        instance = MockDao.return_value
        instance.update.return_value = True
        resp = client.put("/api/v1/broker/configs/1", json={"broker_name": "updated"})
        assert resp.status_code == 200

    @patch("app.api.routes.broker.BrokerConfigDao")
    def test_update_config_not_found(self, MockDao, client):
        instance = MockDao.return_value
        instance.update.return_value = False
        resp = client.put("/api/v1/broker/configs/999", json={"broker_name": "updated"})
        assert resp.status_code == 404

    @patch("app.api.routes.broker.BrokerConfigDao")
    def test_update_config_no_fields(self, MockDao, client):
        resp = client.put("/api/v1/broker/configs/1", json={})
        assert resp.status_code == 400

    @patch("app.api.routes.broker.BrokerConfigDao")
    def test_delete_config(self, MockDao, client):
        instance = MockDao.return_value
        instance.delete.return_value = True
        resp = client.delete("/api/v1/broker/configs/1")
        assert resp.status_code == 200

    @patch("app.api.routes.broker.BrokerConfigDao")
    def test_delete_config_not_found(self, MockDao, client):
        instance = MockDao.return_value
        instance.delete.return_value = False
        resp = client.delete("/api/v1/broker/configs/999")
        assert resp.status_code == 404
