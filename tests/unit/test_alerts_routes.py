"""Tests for P2: Alert management routes."""
import pytest
from unittest.mock import patch, MagicMock
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.routes import alerts
from app.api.exception_handlers import register_exception_handlers


@pytest.fixture
def mock_user():
    return type("User", (), {"id": 1, "user_id": 1, "username": "testuser"})()


@pytest.fixture
def client(mock_user):
    test_app = FastAPI()
    register_exception_handlers(test_app)
    test_app.include_router(alerts.router, prefix="/api/v1")

    async def override_auth():
        return mock_user

    test_app.dependency_overrides[alerts.get_current_user] = override_auth
    return TestClient(test_app)


class TestAlertRules:
    @patch("app.api.routes.alerts.AlertRuleDao")
    def test_list_alert_rules(self, MockDao, client):
        instance = MockDao.return_value
        instance.list_by_user.return_value = [
            {"id": 1, "name": "High RSI", "metric": "rsi", "comparator": "gt", "threshold": 80}
        ]
        resp = client.get("/api/v1/alerts/rules")
        assert resp.status_code == 200
        assert len(resp.json()["rules"]) == 1

    @patch("app.api.routes.alerts.AlertRuleDao")
    def test_create_alert_rule(self, MockDao, client):
        instance = MockDao.return_value
        instance.create.return_value = 1
        resp = client.post("/api/v1/alerts/rules", json={
            "name": "High RSI", "metric": "rsi", "comparator": "gt",
            "threshold": 80.0, "level": "warning"
        })
        assert resp.status_code == 201
        assert resp.json()["id"] == 1

    @patch("app.api.routes.alerts.AlertRuleDao")
    def test_create_alert_rule_invalid_comparator(self, MockDao, client):
        resp = client.post("/api/v1/alerts/rules", json={
            "name": "Test", "metric": "rsi", "comparator": "invalid", "threshold": 80.0
        })
        assert resp.status_code == 400

    @patch("app.api.routes.alerts.AlertRuleDao")
    def test_create_alert_rule_invalid_level(self, MockDao, client):
        resp = client.post("/api/v1/alerts/rules", json={
            "name": "Test", "metric": "rsi", "comparator": "gt", "threshold": 80.0, "level": "bad"
        })
        assert resp.status_code == 400

    @patch("app.api.routes.alerts.AlertRuleDao")
    def test_update_alert_rule(self, MockDao, client):
        instance = MockDao.return_value
        instance.update.return_value = True
        resp = client.put("/api/v1/alerts/rules/1", json={"name": "Updated"})
        assert resp.status_code == 200

    @patch("app.api.routes.alerts.AlertRuleDao")
    def test_update_alert_rule_not_found(self, MockDao, client):
        instance = MockDao.return_value
        instance.update.return_value = False
        resp = client.put("/api/v1/alerts/rules/999", json={"name": "Updated"})
        assert resp.status_code == 404

    @patch("app.api.routes.alerts.AlertRuleDao")
    def test_update_alert_rule_no_fields(self, MockDao, client):
        resp = client.put("/api/v1/alerts/rules/1", json={})
        assert resp.status_code == 400

    @patch("app.api.routes.alerts.AlertRuleDao")
    def test_delete_alert_rule(self, MockDao, client):
        instance = MockDao.return_value
        instance.delete.return_value = True
        resp = client.delete("/api/v1/alerts/rules/1")
        assert resp.status_code == 200

    @patch("app.api.routes.alerts.AlertRuleDao")
    def test_delete_alert_rule_not_found(self, MockDao, client):
        instance = MockDao.return_value
        instance.delete.return_value = False
        resp = client.delete("/api/v1/alerts/rules/999")
        assert resp.status_code == 404


class TestAlertHistory:
    @patch("app.api.routes.alerts.AlertHistoryDao")
    def test_list_alert_history(self, MockDao, client):
        instance = MockDao.return_value
        instance.list_by_user.return_value = (
            [{"id": 1, "message": "RSI > 80", "level": "warning"}], 1
        )
        resp = client.get("/api/v1/alerts/history")
        assert resp.status_code == 200
        assert resp.json()["meta"]["total"] == 1

    @patch("app.api.routes.alerts.AlertHistoryDao")
    def test_acknowledge_alert(self, MockDao, client):
        instance = MockDao.return_value
        instance.acknowledge.return_value = True
        resp = client.post("/api/v1/alerts/history/1/acknowledge")
        assert resp.status_code == 200

    @patch("app.api.routes.alerts.AlertHistoryDao")
    def test_acknowledge_alert_not_found(self, MockDao, client):
        instance = MockDao.return_value
        instance.acknowledge.return_value = False
        resp = client.post("/api/v1/alerts/history/999/acknowledge")
        assert resp.status_code == 404


class TestNotificationChannels:
    @patch("app.api.routes.alerts.NotificationChannelDao")
    def test_list_channels(self, MockDao, client):
        instance = MockDao.return_value
        instance.list_by_user.return_value = [
            {"id": 1, "channel_type": "email", "config": {}}
        ]
        resp = client.get("/api/v1/alerts/channels")
        assert resp.status_code == 200
        assert len(resp.json()["channels"]) == 1

    @patch("app.api.routes.alerts.NotificationChannelDao")
    def test_create_channel(self, MockDao, client):
        instance = MockDao.return_value
        instance.create.return_value = 1
        resp = client.post("/api/v1/alerts/channels", json={
            "channel_type": "email", "config": {"address": "test@test.com"}
        })
        assert resp.status_code == 201

    @patch("app.api.routes.alerts.NotificationChannelDao")
    def test_create_channel_invalid_type(self, MockDao, client):
        resp = client.post("/api/v1/alerts/channels", json={
            "channel_type": "invalid", "config": {}
        })
        assert resp.status_code == 400

    @patch("app.api.routes.alerts.NotificationChannelDao")
    def test_delete_channel(self, MockDao, client):
        instance = MockDao.return_value
        instance.delete.return_value = True
        resp = client.delete("/api/v1/alerts/channels/1")
        assert resp.status_code == 200

    @patch("app.api.routes.alerts.NotificationChannelDao")
    def test_delete_channel_not_found(self, MockDao, client):
        instance = MockDao.return_value
        instance.delete.return_value = False
        resp = client.delete("/api/v1/alerts/channels/999")
        assert resp.status_code == 404

