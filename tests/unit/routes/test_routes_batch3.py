"""Coverage batch for auth, factor, mfa services and their DAOs.

Direct service/domain unit tests that patch DAO constructors to avoid DB.
Also tests route endpoints where feasible.
"""
from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.api.main import app
from app.api.services.auth_service import get_current_user
from app.api.models.user import TokenData


def _user(**kw):
    defaults = dict(user_id=1, username="tester", exp=datetime(2099, 1, 1))
    defaults.update(kw)
    return TokenData(**defaults)


@pytest.fixture(autouse=True)
def _override():
    app.dependency_overrides[get_current_user] = lambda: _user()
    yield
    app.dependency_overrides.clear()


@pytest.fixture(autouse=True)
def _rbac_bypass(monkeypatch):
    """Bypass RBAC permission checks that hit the DB."""
    from app.domains.rbac.service.rbac_service import RbacService
    monkeypatch.setattr(RbacService, "check_permission",
                        lambda self, user_id, resource, action, username=None: True)


_c = None

def client():
    global _c
    if _c is None:
        _c = TestClient(app, raise_server_exceptions=False)
    return _c


# ═══════════════════════════════════════════════════════════════════════
# AuthService (direct unit tests — patch UserDao to avoid DB)
# ═══════════════════════════════════════════════════════════════════════

class TestAuthService:

    @patch("app.domains.auth.service.UserDao")
    @patch("app.domains.auth.service.get_password_hash", return_value="$hash")
    def test_register(self, _hash, dao_cls):
        from app.domains.auth.service import AuthService
        dao = dao_cls.return_value
        dao.username_exists.return_value = False
        dao.email_exists.return_value = False
        dao.insert_user.return_value = 1
        dao.get_user_by_id.return_value = {
            "id": 1, "username": "u", "email": "u@e.com",
            "is_active": True, "created_at": datetime.now(),
            "must_change_password": False,
        }
        svc = AuthService()
        r = svc.register("u", "u@e.com", "pass")
        assert r["id"] == 1

    @patch("app.domains.auth.service.UserDao")
    def test_register_username_exists(self, dao_cls):
        from app.domains.auth.service import AuthService
        dao = dao_cls.return_value
        dao.username_exists.return_value = True
        svc = AuthService()
        with pytest.raises(ValueError):
            svc.register("dup", "d@e.com", "pass")

    @patch("app.domains.auth.service.UserDao")
    def test_register_email_exists(self, dao_cls):
        from app.domains.auth.service import AuthService
        dao = dao_cls.return_value
        dao.username_exists.return_value = False
        dao.email_exists.return_value = True
        svc = AuthService()
        with pytest.raises(ValueError):
            svc.register("u2", "dup@e.com", "pass")

    @patch("app.domains.auth.service.create_refresh_token", return_value="refresh")
    @patch("app.domains.auth.service.create_access_token", return_value="access")
    @patch("app.domains.auth.service.verify_password", return_value=True)
    @patch("app.domains.auth.service.UserDao")
    def test_login_ok(self, dao_cls, _vp, _cat, _crt):
        from app.domains.auth.service import AuthService
        dao = dao_cls.return_value
        dao.get_user_for_login.return_value = {
            "id": 1, "username": "admin", "email": "a@b.com",
            "hashed_password": "hash", "is_active": True,
            "must_change_password": False,
        }
        svc = AuthService()
        r = svc.login("admin", "pass")
        assert "access_token" in r

    @patch("app.domains.auth.service.verify_password", return_value=False)
    @patch("app.domains.auth.service.UserDao")
    def test_login_wrong_pass(self, dao_cls, _vp):
        from app.domains.auth.service import AuthService
        dao = dao_cls.return_value
        dao.get_user_for_login.return_value = {
            "id": 1, "username": "admin", "hashed_password": "hash",
            "is_active": True, "must_change_password": False,
        }
        svc = AuthService()
        with pytest.raises(PermissionError):
            svc.login("admin", "wrong")

    @patch("app.domains.auth.service.UserDao")
    def test_login_not_found(self, dao_cls):
        from app.domains.auth.service import AuthService
        dao = dao_cls.return_value
        dao.get_user_for_login.return_value = None
        svc = AuthService()
        with pytest.raises(PermissionError):
            svc.login("nope", "pass")

    @patch("app.domains.auth.service.verify_password", return_value=True)
    @patch("app.domains.auth.service.UserDao")
    def test_login_inactive(self, dao_cls, _vp):
        from app.domains.auth.service import AuthService
        dao = dao_cls.return_value
        dao.get_user_for_login.return_value = {
            "id": 1, "username": "admin", "hashed_password": "hash",
            "is_active": False, "must_change_password": False,
        }
        svc = AuthService()
        with pytest.raises(PermissionError):
            svc.login("admin", "pass")

    @patch("app.domains.auth.service.UserDao")
    def test_me(self, dao_cls):
        from app.domains.auth.service import AuthService
        dao = dao_cls.return_value
        dao.get_user_by_id.return_value = {
            "id": 1, "username": "admin", "email": "a@b.com",
            "is_active": True, "created_at": datetime.now(),
            "must_change_password": False,
        }
        svc = AuthService()
        r = svc.me(1)
        assert r["username"] == "admin"

    @patch("app.domains.auth.service.UserDao")
    def test_me_not_found(self, dao_cls):
        from app.domains.auth.service import AuthService
        dao = dao_cls.return_value
        dao.get_user_by_id.return_value = None
        svc = AuthService()
        with pytest.raises(KeyError):
            svc.me(999)

    @patch("app.domains.auth.service.get_password_hash", return_value="$new")
    @patch("app.domains.auth.service.verify_password", return_value=True)
    @patch("app.domains.auth.service.UserDao")
    def test_change_password(self, dao_cls, _vp, _hash):
        from app.domains.auth.service import AuthService
        dao = dao_cls.return_value
        dao.get_user_by_id.return_value = {"id": 1, "hashed_password": "old"}
        svc = AuthService()
        svc.change_password(1, "old", "new")
        dao.update_user_password.assert_called_once()

    @patch("app.domains.auth.service.verify_password", return_value=False)
    @patch("app.domains.auth.service.UserDao")
    def test_change_password_wrong_current(self, dao_cls, _vp):
        from app.domains.auth.service import AuthService
        dao = dao_cls.return_value
        dao.get_user_by_id.return_value = {"id": 1, "hashed_password": "old"}
        svc = AuthService()
        with pytest.raises(PermissionError):
            svc.change_password(1, "wrong", "new")

    @patch("app.domains.auth.service.create_access_token", return_value="new_tok")
    @patch("app.domains.auth.service.decode_token")
    @patch("app.domains.auth.service.UserDao")
    def test_refresh(self, dao_cls, decode, _cat):
        from app.domains.auth.service import AuthService
        decode.return_value = _user()
        dao_cls.return_value.get_user_by_id.return_value = {
            "id": 1, "username": "tester", "is_active": True,
            "must_change_password": False,
        }
        svc = AuthService()
        r = svc.refresh("old_refresh_token")
        assert "access_token" in r


# ═══════════════════════════════════════════════════════════════════════
# FactorService (direct unit tests — patch DAO classes)
# ═══════════════════════════════════════════════════════════════════════

class TestFactorService:

    @patch("app.domains.factors.service.FactorEvaluationDao")
    @patch("app.domains.factors.service.FactorDefinitionDao")
    def test_list_factors(self, def_dao, eval_dao):
        from app.domains.factors.service import FactorService
        def_dao.return_value.list_for_user.return_value = []
        svc = FactorService()
        r = svc.list_factors(user_id=1)
        assert isinstance(r, list)

    @patch("app.domains.factors.service.FactorEvaluationDao")
    @patch("app.domains.factors.service.FactorDefinitionDao")
    def test_count_factors(self, def_dao, eval_dao):
        from app.domains.factors.service import FactorService
        def_dao.return_value.count_for_user.return_value = 5
        svc = FactorService()
        r = svc.count_factors(user_id=1)
        assert r == 5

    @patch("app.domains.factors.service.FactorEvaluationDao")
    @patch("app.domains.factors.service.FactorDefinitionDao")
    def test_create_factor(self, def_dao, eval_dao):
        from app.domains.factors.service import FactorService
        def_dao.return_value.create_factor.return_value = 1
        def_dao.return_value.get_factor.return_value = {"id": 1, "name": "rsi"}
        svc = FactorService()
        r = svc.create_factor(user_id=1, name="rsi", expression="ta.RSI(close, 14)")
        assert r is not None

    @patch("app.domains.factors.service.FactorEvaluationDao")
    @patch("app.domains.factors.service.FactorDefinitionDao")
    def test_get_factor(self, def_dao, eval_dao):
        from app.domains.factors.service import FactorService
        def_dao.return_value.get_by_id.return_value = {"id": 1, "name": "rsi"}
        svc = FactorService()
        r = svc.get_factor(user_id=1, factor_id=1)
        assert r is not None

    @patch("app.domains.factors.service.FactorEvaluationDao")
    @patch("app.domains.factors.service.FactorDefinitionDao")
    def test_update_factor(self, def_dao, eval_dao):
        from app.domains.factors.service import FactorService
        def_dao.return_value.update_factor.return_value = True
        def_dao.return_value.get_factor.return_value = {"id": 1, "name": "r2"}
        svc = FactorService()
        r = svc.update_factor(user_id=1, factor_id=1, name="r2")
        assert r is not None

    @patch("app.domains.factors.service.FactorEvaluationDao")
    @patch("app.domains.factors.service.FactorDefinitionDao")
    def test_delete_factor(self, def_dao, eval_dao):
        from app.domains.factors.service import FactorService
        svc = FactorService()
        svc.delete_factor(user_id=1, factor_id=1)
        # Just verify it ran without error - the mock auto-accepts the call

    @patch("app.domains.factors.service.FactorEvaluationDao")
    @patch("app.domains.factors.service.FactorDefinitionDao")
    def test_list_evaluations(self, def_dao, eval_dao):
        from app.domains.factors.service import FactorService
        eval_dao.return_value.list_for_factor.return_value = []
        svc = FactorService()
        r = svc.list_evaluations(user_id=1, factor_id=1)
        assert isinstance(r, list)

    @patch("app.domains.factors.service.FactorEvaluationDao")
    @patch("app.domains.factors.service.FactorDefinitionDao")
    def test_run_evaluation(self, def_dao, eval_dao):
        from app.domains.factors.service import FactorService
        eval_dao.return_value.create_evaluation.return_value = 10
        eval_dao.return_value.get_evaluation.return_value = {"id": 10, "status": "pending"}
        svc = FactorService()
        r = svc.run_evaluation(user_id=1, factor_id=1,
                               start_date="2024-01-01", end_date="2024-06-01")
        assert r is not None

    @patch("app.domains.factors.service.FactorEvaluationDao")
    @patch("app.domains.factors.service.FactorDefinitionDao")
    def test_delete_evaluation(self, def_dao, eval_dao):
        from app.domains.factors.service import FactorService
        svc = FactorService()
        svc.delete_evaluation(user_id=1, factor_id=1, eval_id=10)
        # Just verify it ran without error


# ═══════════════════════════════════════════════════════════════════════
# MFA helper functions (no DB needed)
# ═══════════════════════════════════════════════════════════════════════

class TestMfaHelpers:

    def test_generate_totp_secret(self):
        from app.api.routes.mfa import _generate_totp_secret
        s = _generate_totp_secret()
        assert isinstance(s, str) and len(s) > 10

    def test_generate_recovery_codes(self):
        from app.api.routes.mfa import _generate_recovery_codes
        codes = _generate_recovery_codes(8)
        assert len(codes) == 8
        assert all(isinstance(c, str) for c in codes)

    def test_hash_recovery_codes(self):
        from app.api.routes.mfa import _hash_recovery_codes
        codes = ["ABCD-1234", "EFGH-5678"]
        hashed = _hash_recovery_codes(codes)
        assert isinstance(hashed, str)
        assert hashed != codes[0]

    def test_verify_totp_code_invalid(self):
        from app.api.routes.mfa import _verify_totp_code
        # Secret must be hex-encoded (from secrets.token_hex(20))
        import secrets
        hex_secret = secrets.token_hex(20)
        r = _verify_totp_code(hex_secret, "000000")
        assert isinstance(r, bool)


# ═══════════════════════════════════════════════════════════════════════
# MfaDao (direct unit tests - correct method names)
# ═══════════════════════════════════════════════════════════════════════

class TestMfaDao:

    def _mock_conn(self, conn_fn):
        """Set up mock connection context manager."""
        ctx = MagicMock()
        conn_fn.return_value.__enter__ = lambda s: ctx
        conn_fn.return_value.__exit__ = MagicMock(return_value=False)
        return ctx

    @patch("app.domains.auth.dao.mfa_dao.connection")
    def test_get_by_user_id(self, conn_fn):
        from app.domains.auth.dao.mfa_dao import MfaDao
        ctx = self._mock_conn(conn_fn)
        row = MagicMock()
        row._mapping = {"id": 1, "user_id": 1, "mfa_type": "totp",
                        "secret_encrypted": "abc", "is_enabled": True,
                        "recovery_codes_hash": "hash", "created_at": datetime.now()}
        ctx.execute.return_value.fetchone.return_value = row
        dao = MfaDao()
        r = dao.get_by_user_id(1)
        assert r is not None

    @patch("app.domains.auth.dao.mfa_dao.connection")
    def test_get_by_user_id_not_found(self, conn_fn):
        from app.domains.auth.dao.mfa_dao import MfaDao
        ctx = self._mock_conn(conn_fn)
        ctx.execute.return_value.fetchone.return_value = None
        dao = MfaDao()
        r = dao.get_by_user_id(999)
        assert r is None

    @patch("app.domains.auth.dao.mfa_dao.connection")
    def test_upsert(self, conn_fn):
        from app.domains.auth.dao.mfa_dao import MfaDao
        ctx = self._mock_conn(conn_fn)
        ctx.execute.return_value.lastrowid = 1
        dao = MfaDao()
        r = dao.upsert(1, "totp", "encrypted_secret", "hashed_codes")
        assert r is not None

    @patch("app.domains.auth.dao.mfa_dao.connection")
    def test_enable(self, conn_fn):
        from app.domains.auth.dao.mfa_dao import MfaDao
        ctx = self._mock_conn(conn_fn)
        ctx.execute.return_value.rowcount = 1
        dao = MfaDao()
        r = dao.enable(1)
        assert r is True

    @patch("app.domains.auth.dao.mfa_dao.connection")
    def test_disable(self, conn_fn):
        from app.domains.auth.dao.mfa_dao import MfaDao
        ctx = self._mock_conn(conn_fn)
        ctx.execute.return_value.rowcount = 1
        dao = MfaDao()
        r = dao.disable(1)
        assert r is True

    @patch("app.domains.auth.dao.mfa_dao.connection")
    def test_delete(self, conn_fn):
        from app.domains.auth.dao.mfa_dao import MfaDao
        ctx = self._mock_conn(conn_fn)
        ctx.execute.return_value.rowcount = 1
        dao = MfaDao()
        r = dao.delete(1)
        assert r is True


# ═══════════════════════════════════════════════════════════════════════
# Auth route endpoints (login/register don't need RBAC)
# ═══════════════════════════════════════════════════════════════════════

class TestAuthRoutes:

    @patch("app.domains.auth.service.UserDao")
    @patch("app.domains.auth.service.get_password_hash", return_value="$hash")
    def test_register_success(self, _hash, dao_cls):
        dao = dao_cls.return_value
        dao.username_exists.return_value = False
        dao.email_exists.return_value = False
        dao.insert_user.return_value = 1
        dao.get_user_by_id.return_value = {
            "id": 1, "username": "newuser", "email": "new@test.com",
            "is_active": True, "created_at": datetime.now(),
            "must_change_password": False,
        }
        r = client().post("/api/v1/auth/register", json={
            "username": "newuser", "email": "new@test.com", "password": "Str0ng!Pass"
        })
        assert r.status_code in (200, 201)

    @patch("app.domains.auth.service.UserDao")
    def test_register_conflict(self, dao_cls):
        dao = dao_cls.return_value
        dao.username_exists.return_value = True
        r = client().post("/api/v1/auth/register", json={
            "username": "existing", "email": "e@test.com", "password": "Str0ng!Pass"
        })
        assert r.status_code in (400, 409)

    @patch("app.domains.rbac.service.rbac_service.RbacService.get_user_permissions", return_value=[])
    @patch("app.domains.rbac.service.rbac_service.RbacService.get_primary_role", return_value="viewer")
    @patch("app.domains.auth.service.create_refresh_token", return_value="ref")
    @patch("app.domains.auth.service.create_access_token", return_value="tok")
    @patch("app.domains.auth.service.verify_password", return_value=True)
    @patch("app.domains.auth.service.UserDao")
    def test_login_success(self, dao_cls, _vp, _cat, _crt, _role, _perms):
        dao = dao_cls.return_value
        dao.get_user_for_login.return_value = {
            "id": 1, "username": "admin", "email": "a@b.com",
            "hashed_password": "hash", "is_active": True,
            "must_change_password": False,
        }
        dao.get_user_by_id.return_value = {
            "id": 1, "username": "admin", "email": "a@b.com",
            "is_active": True, "created_at": datetime.now(),
            "must_change_password": False,
        }
        r = client().post("/api/v1/auth/login", json={
            "username": "admin", "password": "pass"
        })
        assert r.status_code == 200

    @patch("app.domains.auth.service.UserDao")
    def test_login_bad_credentials(self, dao_cls):
        dao = dao_cls.return_value
        dao.get_user_for_login.return_value = None
        r = client().post("/api/v1/auth/login", json={
            "username": "nobody", "password": "pass"
        })
        assert r.status_code == 401

    @patch("app.domains.auth.service.UserDao")
    def test_get_me(self, dao_cls):
        dao = dao_cls.return_value
        dao.get_user_by_id.return_value = {
            "id": 1, "username": "tester", "email": "a@b.com",
            "is_active": True, "created_at": datetime.now(),
            "must_change_password": False,
        }
        r = client().get("/api/v1/auth/me")
        assert r.status_code == 200

    @patch("app.domains.auth.service.get_password_hash", return_value="$new")
    @patch("app.domains.auth.service.verify_password", return_value=True)
    @patch("app.domains.auth.service.UserDao")
    def test_change_password(self, dao_cls, _vp, _hash):
        dao = dao_cls.return_value
        dao.get_user_by_id.return_value = {"id": 1, "hashed_password": "old"}
        r = client().post("/api/v1/auth/change-password", json={
            "current_password": "old", "new_password": "New!Str0ng"
        })
        assert r.status_code == 200
