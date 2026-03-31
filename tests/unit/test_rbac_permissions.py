"""RBAC permission dependency coverage for admin, strategy, and audit routes."""

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.exception_handlers import register_exception_handlers
from app.api.models.user import TokenData
from app.api.services.auth_service import get_current_user


future_exp = datetime.utcnow() + timedelta(hours=1)
TEST_USER = TokenData(user_id=10, username="alice", exp=future_exp)


def _make_client(*routers) -> TestClient:
    app = FastAPI()
    register_exception_handlers(app)
    app.dependency_overrides[get_current_user] = lambda: TEST_USER
    for router in routers:
        app.include_router(router, prefix="/api/v1")
    return TestClient(app, raise_server_exceptions=False)


def test_strategy_create_returns_403_when_permission_denied():
    from app.api.routes.strategies import router

    client = _make_client(router)

    with patch("app.api.dependencies.permissions.RbacService.check_permission", return_value=False), patch(
        "app.domains.audit.dao.audit_log_dao.AuditLogDao"
    ) as MockAuditDao:
        resp = client.post(
            "/api/v1/strategies",
            json={
                "name": "Momentum Strategy",
                "class_name": "MomentumStrategy",
                "description": "Test strategy",
                "parameters": {},
                "code": "class MomentumStrategy: pass",
            },
        )

    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "FORBIDDEN"
    MockAuditDao.return_value.insert.assert_called_once()


def test_admin_roles_returns_403_when_account_manage_missing():
    from app.api.routes.admin import router

    client = _make_client(router)

    with patch("app.api.dependencies.permissions.RbacService.check_permission", return_value=False):
        resp = client.get("/api/v1/admin/roles")

    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "FORBIDDEN"


def test_admin_roles_returns_data_when_permission_granted():
    from app.api.routes.admin import router

    client = _make_client(router)

    role_dao = MagicMock()
    role_dao.list_all.return_value = [
        {"id": 1, "name": "admin", "description": "Administrator", "is_system": True}
    ]
    role_dao.list_role_permissions.return_value = ["account.manage", "system.manage"]

    with patch("app.api.dependencies.permissions.RbacService.check_permission", return_value=True), patch(
        "app.api.routes.admin.RoleDao", return_value=role_dao
    ):
        resp = client.get("/api/v1/admin/roles")

    assert resp.status_code == 200
    assert resp.json()["roles"][0]["permissions"] == ["account.manage", "system.manage"]


def test_audit_logs_accept_non_admin_username_when_rbac_allows():
    from app.api.routes.audit import router

    client = _make_client(router)

    mock_dao = MagicMock()
    mock_dao.count.return_value = 1
    mock_dao.query.return_value = [
        {
            "id": 1,
            "timestamp": None,
            "user_id": 10,
            "username": "alice",
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

    with patch("app.api.dependencies.permissions.RbacService.check_permission", return_value=True), patch(
        "app.api.routes.audit.AuditLogDao", return_value=mock_dao
    ):
        resp = client.get("/api/v1/audit/logs")

    assert resp.status_code == 200
    assert resp.json()["meta"]["total"] == 1
    assert resp.json()["data"][0]["username"] == "alice"
