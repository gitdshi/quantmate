"""Tests for P2: Risk management routes."""
import pytest
from unittest.mock import patch, MagicMock
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.routes import risk
from app.api.exception_handlers import register_exception_handlers


@pytest.fixture
def mock_user():
    return type("User", (), {"id": 1, "user_id": 1, "username": "testuser"})()


@pytest.fixture
def client(mock_user):
    test_app = FastAPI()
    register_exception_handlers(test_app)
    test_app.include_router(risk.router, prefix="/api/v1")

    async def override_auth():
        return mock_user

    test_app.dependency_overrides[risk.get_current_user] = override_auth
    return TestClient(test_app)


class TestRiskRules:
    @patch("app.api.routes.risk.RiskRuleDao")
    def test_list_risk_rules(self, MockDao, client):
        instance = MockDao.return_value
        instance.list_by_user.return_value = [
            {"id": 1, "rule_type": "position_limit", "params": {"max_position": 100000}}
        ]
        resp = client.get("/api/v1/risk/rules")
        assert resp.status_code == 200
        assert len(resp.json()["rules"]) == 1

    @patch("app.api.routes.risk.RiskRuleDao")
    def test_create_risk_rule(self, MockDao, client):
        instance = MockDao.return_value
        instance.create.return_value = 1
        resp = client.post("/api/v1/risk/rules", json={
            "name": "Max Position", "rule_type": "position_limit",
            "threshold": 100000.0, "action": "block"
        })
        assert resp.status_code == 201
        assert resp.json()["id"] == 1

    @patch("app.api.routes.risk.RiskRuleDao")
    def test_create_risk_rule_invalid_type(self, MockDao, client):
        resp = client.post("/api/v1/risk/rules", json={
            "name": "Bad", "rule_type": "invalid", "threshold": 1.0, "action": "block"
        })
        assert resp.status_code == 400

    @patch("app.api.routes.risk.RiskRuleDao")
    def test_create_risk_rule_invalid_action(self, MockDao, client):
        resp = client.post("/api/v1/risk/rules", json={
            "name": "Bad", "rule_type": "position_limit", "threshold": 1.0, "action": "invalid"
        })
        assert resp.status_code == 400

    @patch("app.api.routes.risk.RiskRuleDao")
    def test_update_risk_rule(self, MockDao, client):
        instance = MockDao.return_value
        instance.update.return_value = True
        resp = client.put("/api/v1/risk/rules/1", json={
            "threshold": 200000.0
        })
        assert resp.status_code == 200

    @patch("app.api.routes.risk.RiskRuleDao")
    def test_update_risk_rule_not_found(self, MockDao, client):
        instance = MockDao.return_value
        instance.update.return_value = False
        resp = client.put("/api/v1/risk/rules/999", json={"threshold": 1.0})
        assert resp.status_code == 404

    @patch("app.api.routes.risk.RiskRuleDao")
    def test_delete_risk_rule(self, MockDao, client):
        instance = MockDao.return_value
        instance.delete.return_value = True
        resp = client.delete("/api/v1/risk/rules/1")
        assert resp.status_code == 200

    @patch("app.api.routes.risk.RiskRuleDao")
    def test_delete_risk_rule_not_found(self, MockDao, client):
        instance = MockDao.return_value
        instance.delete.return_value = False
        resp = client.delete("/api/v1/risk/rules/999")
        assert resp.status_code == 404


class TestPreTradeRiskCheck:
    @patch("app.api.routes.risk.RiskRuleDao")
    def test_risk_check_passes(self, MockDao, client):
        instance = MockDao.return_value
        instance.list_by_user.return_value = [
            {"id": 1, "name": "Max Pos", "rule_type": "position_limit", "action": "block"}
        ]
        resp = client.post("/api/v1/risk/check?symbol=000001.SZ&direction=buy&quantity=100")
        assert resp.status_code == 200
        data = resp.json()
        assert "overall" in data

    @patch("app.api.routes.risk.RiskRuleDao")
    def test_risk_check_no_rules(self, MockDao, client):
        instance = MockDao.return_value
        instance.list_by_user.return_value = []
        resp = client.post("/api/v1/risk/check?symbol=000001.SZ&direction=buy&quantity=100")
        assert resp.status_code == 200
        assert resp.json()["overall"] == "pass"

    @patch("app.domains.audit.service.get_audit_service", return_value=MagicMock())
    @patch("app.api.routes.risk.RiskRuleDao")
    def test_risk_check_warn_and_block_with_structured_body(self, MockDao, _mock_audit, client):
        instance = MockDao.return_value
        instance.list_by_user.return_value = [
            {"id": 1, "name": "Warn Upgrade", "rule_type": "frequency", "action": "warn", "threshold": 0},
            {"id": 2, "name": "Max Qty", "rule_type": "position_limit", "action": "block", "threshold": 10},
        ]
        resp = client.post(
            "/api/v1/risk/check",
            json={
                "scope_type": "paper_deployment",
                "scope_id": 3,
                "strategy_id": 5,
                "version_id": 7,
                "projected_action": "prepare_live_upgrade",
                "symbol": "000001.SZ",
                "direction": "buy",
                "quantity": 100,
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["result"] == "block"
        assert len(body["triggered_rules"]) == 2
        assert body["context"]["version_id"] == 7

