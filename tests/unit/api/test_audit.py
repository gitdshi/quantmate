"""Tests for Issue #2: Audit Logging System."""
import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.audit_middleware import (
    _classify_request,
    _extract_resource_id,
    AuditMiddleware,
)


class TestClassifyRequest:
    def test_login(self):
        op, res = _classify_request("/api/v1/auth/login", "POST")
        assert op == "AUTH_LOGIN"
        assert res == "user"

    def test_strategy_create(self):
        op, res = _classify_request("/api/v1/strategies", "POST")
        assert op == "STRATEGY_CREATE"
        assert res == "strategy"

    def test_strategy_delete(self):
        op, res = _classify_request("/api/v1/strategies/42", "DELETE")
        assert op == "STRATEGY_DELETE"
        assert res == "strategy"

    def test_data_access(self):
        op, res = _classify_request("/api/v1/data/history/000001.SZ", "GET")
        assert op == "DATA_ACCESS"
        assert res == "data"

    def test_unknown_defaults_to_method(self):
        op, res = _classify_request("/api/v1/unknown", "PATCH")
        assert op == "API_PATCH"
        assert res is None


class TestExtractResourceId:
    def test_numeric_id(self):
        assert _extract_resource_id("/api/v1/strategies/42") == "42"

    def test_uuid(self):
        uid = "abc-1234-def"
        assert _extract_resource_id(f"/api/v1/backtest/{uid}") == uid

    def test_no_id(self):
        assert _extract_resource_id("/api/v1/strategies") is None


class TestAuditMiddleware:
    @pytest.fixture
    def client(self):
        app = FastAPI()
        app.add_middleware(AuditMiddleware)

        @app.get("/api/v1/test")
        async def test_endpoint():
            return {"ok": True}

        @app.get("/health")
        async def health():
            return {"ok": True}

        return TestClient(app, raise_server_exceptions=False)

    def test_skip_non_api(self, client):
        """Non-API paths should not trigger audit logging."""
        with patch("app.domains.audit.dao.audit_log_dao.AuditLogDao") as MockDao:
            resp = client.get("/health")
        assert resp.status_code == 200
        MockDao.return_value.insert.assert_not_called()

    def test_audit_logs_api_request(self, client):
        """API requests should be logged."""
        mock_dao = MagicMock()
        with patch("app.domains.audit.dao.audit_log_dao.AuditLogDao", return_value=mock_dao):
            resp = client.get("/api/v1/test")
        assert resp.status_code == 200
        mock_dao.insert.assert_called_once()
        call_kwargs = mock_dao.insert.call_args[1]
        assert call_kwargs["http_method"] == "GET"
        assert call_kwargs["http_path"] == "/api/v1/test"
        assert call_kwargs["http_status"] == 200

    def test_audit_failure_doesnt_break_request(self, client):
        """If audit logging fails, the request should still succeed."""
        mock_dao = MagicMock()
        mock_dao.insert.side_effect = Exception("DB down")
        with patch("app.domains.audit.dao.audit_log_dao.AuditLogDao", return_value=mock_dao):
            resp = client.get("/api/v1/test")
        assert resp.status_code == 200


class TestAuditRoutes:
    """Test the audit query/export API."""

    @pytest.fixture
    def admin_client(self):
        from app.api.routes.audit import router, _require_admin
        from app.api.models.user import TokenData

        app = FastAPI()

        # Override admin dependency
        future_exp = datetime.utcnow() + timedelta(hours=1)
        admin_user = TokenData(user_id=1, username="admin", exp=future_exp, must_change_password=False)

        def override_admin():
            return admin_user

        app.include_router(router)
        app.dependency_overrides[_require_admin] = override_admin
        return TestClient(app, raise_server_exceptions=False)

    @pytest.fixture
    def non_admin_client(self):
        from app.api.routes.audit import router
        from app.api.exception_handlers import register_exception_handlers

        app = FastAPI()
        register_exception_handlers(app)

        # Don't override — will need proper auth
        app.include_router(router)
        return TestClient(app, raise_server_exceptions=False)

    def test_query_logs_admin(self, admin_client):
        mock_dao = MagicMock()
        mock_dao.count.return_value = 1
        mock_dao.query.return_value = [
            {
                "id": 1,
                "timestamp": None,
                "user_id": 1,
                "username": "admin",
                "operation_type": "AUTH_LOGIN",
                "resource_type": "user",
                "resource_id": None,
                "details": '{"duration_ms": 50}',
                "ip_address": "127.0.0.1",
                "user_agent": "test",
                "http_method": "POST",
                "http_path": "/api/v1/auth/login",
                "http_status": 200,
            }
        ]
        with patch("app.api.routes.audit.AuditLogDao", return_value=mock_dao):
            resp = admin_client.get("/audit/logs")
        assert resp.status_code == 200
        body = resp.json()
        assert body["meta"]["total"] == 1
        assert len(body["data"]) == 1

    def test_export_csv(self, admin_client):
        mock_dao = MagicMock()
        mock_dao.query.return_value = [
            {
                "id": 1,
                "timestamp": None,
                "user_id": 1,
                "username": "admin",
                "operation_type": "AUTH_LOGIN",
                "resource_type": "user",
                "resource_id": None,
                "details": None,
                "ip_address": "127.0.0.1",
                "user_agent": "test",
                "http_method": "POST",
                "http_path": "/api/v1/auth/login",
                "http_status": 200,
            }
        ]
        with patch("app.api.routes.audit.AuditLogDao", return_value=mock_dao):
            resp = admin_client.get("/audit/logs/export?format=csv")
        assert resp.status_code == 200
        assert "text/csv" in resp.headers["content-type"]
        assert "AUTH_LOGIN" in resp.text

    def test_export_json(self, admin_client):
        mock_dao = MagicMock()
        mock_dao.query.return_value = []
        with patch("app.api.routes.audit.AuditLogDao", return_value=mock_dao):
            resp = admin_client.get("/audit/logs/export?format=json")
        assert resp.status_code == 200
        assert "application/json" in resp.headers["content-type"]
