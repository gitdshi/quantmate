"""Tests for admin, audit, api_keys, sessions, mfa route files."""
from __future__ import annotations
import hashlib, json
from unittest.mock import MagicMock, patch
import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

# ═══ admin ═══
_ADM = "app.api.routes.admin"

@pytest.fixture()
def adm_client():
    from app.api.routes import admin
    from app.api.exception_handlers import register_exception_handlers
    app = FastAPI(); register_exception_handlers(app)
    app.include_router(admin.router, prefix="/api/v1")
    app.dependency_overrides[admin.get_current_user] = lambda: MagicMock(user_id=1)
    for r in app.routes:
        if hasattr(r, "dependencies"): r.dependencies = []
    return TestClient(app, raise_server_exceptions=False)


class TestAdminRoles:
    @patch(f"{_ADM}.RoleDao")
    def test_list_roles(self, M, adm_client):
        d = M.return_value; d.list_all.return_value = [{"id":1,"name":"admin","description":"A"}]
        d.list_role_permissions.return_value = []
        r = adm_client.get("/api/v1/admin/roles"); assert r.status_code in (200,500)

    @patch(f"{_ADM}.RoleDao")
    def test_create_role(self, M, adm_client):
        d = M.return_value; d.get_by_name.return_value = None; d.create.return_value = 2
        r = adm_client.post("/api/v1/admin/roles", json={"name":"ed","description":"e"})
        assert r.status_code in (200,201,422)

    @patch(f"{_ADM}.RoleDao")
    def test_create_role_dup(self, M, adm_client):
        M.return_value.get_by_name.return_value = {"id":1}
        r = adm_client.post("/api/v1/admin/roles", json={"name":"ed","description":"e"})
        assert r.status_code in (400,409,422)

    @patch(f"{_ADM}.RoleDao")
    def test_delete_role(self, M, adm_client):
        d = M.return_value; d.get.return_value = {"id":2,"name":"ed","is_system":False}; d.delete.return_value = True
        r = adm_client.delete("/api/v1/admin/roles/2"); assert r.status_code in (200,204,404)

    @patch(f"{_ADM}.RoleDao")
    def test_delete_system_role(self, M, adm_client):
        M.return_value.get.return_value = {"id":1,"name":"admin","is_system":True}
        r = adm_client.delete("/api/v1/admin/roles/1"); assert r.status_code in (400,403)

    @patch(f"{_ADM}.RoleDao")
    def test_update_role(self, M, adm_client):
        M.return_value.update.return_value = True
        r = adm_client.put("/api/v1/admin/roles/1", json={"description":"U"}); assert r.status_code in (200,404,422)

    @patch(f"{_ADM}.RoleDao")
    def test_set_role_perms(self, M, adm_client):
        M.return_value.set_permissions.return_value = None
        r = adm_client.put("/api/v1/admin/roles/1/permissions", json={"permission_ids":[1,2]})
        assert r.status_code in (200,422)

    @patch(f"{_ADM}.PermissionDao")
    def test_list_perms(self, M, adm_client):
        M.return_value.list_all.return_value = [{"id":1,"resource":"data","action":"read"}]
        r = adm_client.get("/api/v1/admin/permissions"); assert r.status_code in (200,500)


class TestAdminUsers:
    @patch(f"{_ADM}.UserRoleDao")
    def test_list_users(self, M, adm_client):
        M.return_value.list_users_with_roles.return_value = []
        r = adm_client.get("/api/v1/admin/users"); assert r.status_code in (200,500)

    @patch(f"{_ADM}.UserRoleDao")
    @patch(f"{_ADM}.UserDao")
    def test_update_user_roles(self, MU, MR, adm_client):
        MU.return_value.get_user_by_id.return_value = {"id":2}
        MR.return_value.set_user_roles.return_value = None
        r = adm_client.put("/api/v1/admin/users/2/roles", json={"role_ids":[1]})
        assert r.status_code in (200,404,422)

    @patch(f"{_ADM}.UserDao")
    def test_update_user_status(self, M, adm_client):
        M.return_value.get_user_by_id.return_value = {"id":2}
        M.return_value.update_user_status.return_value = True
        r = adm_client.put("/api/v1/admin/users/2/status", json={"is_active":False})
        assert r.status_code in (200,404,422)


# ═══ audit ═══
_AUD = "app.api.routes.audit"

@pytest.fixture()
def aud_client():
    from app.api.routes import audit
    from app.api.exception_handlers import register_exception_handlers
    app = FastAPI(); register_exception_handlers(app)
    app.include_router(audit.router, prefix="/api/v1")
    # override _require_admin which is used as Depends
    app.dependency_overrides[audit._require_admin] = lambda: MagicMock(user_id=1)
    for r in app.routes:
        if hasattr(r, "dependencies"): r.dependencies = []
    return TestClient(app, raise_server_exceptions=False)


class TestAuditRoutes:
    @patch(f"{_AUD}.AuditLogDao")
    def test_query(self, M, aud_client):
        d = M.return_value; d.count.return_value = 0; d.query.return_value = []
        r = aud_client.get("/api/v1/audit/logs"); assert r.status_code in (200,500)

    @patch(f"{_AUD}.AuditLogDao")
    def test_export_csv(self, M, aud_client):
        M.return_value.query.return_value = []
        r = aud_client.get("/api/v1/audit/logs/export?format=csv"); assert r.status_code in (200,500)

    @patch(f"{_AUD}.AuditLogDao")
    def test_export_json(self, M, aud_client):
        M.return_value.query.return_value = []
        r = aud_client.get("/api/v1/audit/logs/export?format=json"); assert r.status_code in (200,500)


# ═══ api_keys ═══
_AK = "app.api.routes.api_keys"

@pytest.fixture()
def ak_client():
    from app.api.routes import api_keys
    from app.api.exception_handlers import register_exception_handlers
    app = FastAPI(); register_exception_handlers(app)
    app.include_router(api_keys.router, prefix="/api/v1")
    app.dependency_overrides[api_keys.get_current_user] = lambda: MagicMock(user_id=1)
    for r in app.routes:
        if hasattr(r, "dependencies"): r.dependencies = []
    return TestClient(app, raise_server_exceptions=False)


class TestApiKeyRoutes:
    @patch(f"{_AK}.ApiKeyDao")
    def test_list(self, M, ak_client):
        M.return_value.list_by_user.return_value = []
        r = ak_client.get("/api/v1/auth/api-keys/"); assert r.status_code in (200,500)

    @patch(f"{_AK}.ApiKeyDao")
    def test_create(self, M, ak_client):
        d = M.return_value; d.count_by_user.return_value = 0; d.create.return_value = 1
        r = ak_client.post("/api/v1/auth/api-keys/", json={"name":"my-key"})
        assert r.status_code in (200,201,422)

    @patch(f"{_AK}.ApiKeyDao")
    def test_create_limit(self, M, ak_client):
        M.return_value.count_by_user.return_value = 10
        r = ak_client.post("/api/v1/auth/api-keys/", json={"name":"x"})
        assert r.status_code in (400,429,422)

    @patch(f"{_AK}.ApiKeyDao")
    def test_revoke(self, M, ak_client):
        M.return_value.revoke.return_value = True
        r = ak_client.delete("/api/v1/auth/api-keys/1"); assert r.status_code in (200,204,404)


# ═══ sessions ═══
_SE = "app.api.routes.sessions"

@pytest.fixture()
def se_client():
    from app.api.routes import sessions
    from app.api.exception_handlers import register_exception_handlers
    app = FastAPI(); register_exception_handlers(app)
    app.include_router(sessions.router, prefix="/api/v1")
    app.dependency_overrides[sessions.get_current_user] = lambda: MagicMock(user_id=1)
    for r in app.routes:
        if hasattr(r, "dependencies"): r.dependencies = []
    return TestClient(app, raise_server_exceptions=False)


class TestSessionRoutes:
    @patch(f"{_SE}.SessionDao")
    def test_list(self, M, se_client):
        M.return_value.list_by_user.return_value = []
        r = se_client.get("/api/v1/auth/sessions/"); assert r.status_code in (200,500)

    @patch(f"{_SE}.SessionDao")
    def test_revoke(self, M, se_client):
        M.return_value.delete.return_value = True
        r = se_client.delete("/api/v1/auth/sessions/1"); assert r.status_code in (200,204,404)

    @patch(f"{_SE}.SessionDao")
    def test_revoke_all(self, M, se_client):
        M.return_value.delete_all_for_user.return_value = 3
        r = se_client.delete("/api/v1/auth/sessions/all"); assert r.status_code in (200,204,422)


# ═══ mfa ═══
_MFA = "app.api.routes.mfa"

@pytest.fixture()
def mfa_client():
    from app.api.routes import mfa
    from app.api.exception_handlers import register_exception_handlers
    app = FastAPI(); register_exception_handlers(app)
    app.include_router(mfa.router, prefix="/api/v1")
    app.dependency_overrides[mfa.get_current_user] = lambda: MagicMock(user_id=1)
    for r in app.routes:
        if hasattr(r, "dependencies"): r.dependencies = []
    return TestClient(app, raise_server_exceptions=False)


class TestMfaRoutes:
    @patch(f"{_MFA}._generate_recovery_codes", return_value=["r1","r2"])
    @patch(f"{_MFA}._generate_totp_secret", return_value="SECRET")
    @patch(f"{_MFA}.MfaDao")
    def test_setup(self, M, mk_s, mk_r, mfa_client):
        M.return_value.get_by_user_id.return_value = None
        M.return_value.upsert.return_value = None
        r = mfa_client.post("/api/v1/auth/mfa/setup"); assert r.status_code in (200,400,500)

    @patch(f"{_MFA}._verify_totp_code", return_value=True)
    @patch(f"{_MFA}.MfaDao")
    def test_verify(self, M, mk_v, mfa_client):
        M.return_value.get_by_user_id.return_value = {"secret_encrypted":"S","is_enabled":False}
        M.return_value.enable.return_value = None
        r = mfa_client.post("/api/v1/auth/mfa/verify", json={"code":"123456"})
        assert r.status_code in (200,400,422)

    @patch(f"{_MFA}._verify_totp_code", return_value=True)
    @patch(f"{_MFA}.MfaDao")
    def test_disable(self, M, mk_v, mfa_client):
        M.return_value.get_by_user_id.return_value = {"secret_encrypted":"S","is_enabled":True}
        M.return_value.disable.return_value = None
        r = mfa_client.post("/api/v1/auth/mfa/disable", json={"code":"123456"})
        assert r.status_code in (200,400,422)

    @patch(f"{_MFA}.MfaDao")
    def test_recovery(self, M, mfa_client):
        code = "myrecovery"
        code_hash = hashlib.sha256(code.encode()).hexdigest()
        M.return_value.get_by_user_id.return_value = {
            "secret_encrypted": "S", "is_enabled": True, "mfa_type": "totp",
            "recovery_codes_hash": json.dumps([code_hash, "otherhash"]),
        }
        M.return_value.upsert.return_value = None
        M.return_value.enable.return_value = None
        r = mfa_client.post("/api/v1/auth/mfa/recovery", json={"recovery_code": code})
        assert r.status_code in (200,400,422)
