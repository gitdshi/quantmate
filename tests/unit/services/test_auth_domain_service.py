"""Unit tests for AuthService."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

# Patch settings before importing AuthService
with patch("app.infrastructure.config.get_settings") as _gs:
    _gs.return_value = MagicMock(
        secret_key="test-secret-key-for-unit-tests-only-0123456789abcdef",
        algorithm="HS256",
        access_token_expire_minutes=30,
        refresh_token_expire_days=7,
        mysql_host="localhost",
        mysql_port=3306,
        mysql_user="root",
        mysql_password="test",
    )

from app.domains.auth.service import AuthService, _frontend_role


# ── _frontend_role ────────────────────────────────────────────────

def test_frontend_role_admin():
    assert _frontend_role("admin") == "admin"


def test_frontend_role_viewer():
    assert _frontend_role("viewer") == "viewer"


def test_frontend_role_other():
    assert _frontend_role("editor") == "user"
    assert _frontend_role("member") == "user"


# ── AuthService ───────────────────────────────────────────────────

@pytest.fixture
def auth_svc():
    with patch("app.domains.auth.service.UserDao") as DaoCls, \
         patch("app.domains.auth.service.SessionDao") as SessionDaoCls, \
         patch("app.domains.auth.service.get_password_hash", return_value="hashed"), \
         patch("app.domains.auth.service.verify_password") as mock_verify, \
         patch("app.domains.auth.service.create_access_token", return_value="access_tok"), \
         patch("app.domains.auth.service.create_refresh_token", return_value="refresh_tok"), \
         patch("app.domains.auth.service.decode_token") as mock_decode:
        svc = AuthService()
        svc._users = DaoCls.return_value
        svc._sessions = SessionDaoCls.return_value
        svc._sessions.create.return_value = 11
        svc._sessions.get_active.return_value = {"id": 11, "user_id": 1}
        svc._sessions.touch_by_id.return_value = True
        yield svc, mock_verify, mock_decode


def _mock_rbac(monkeypatch):
    """Stub RbacService used by _enrich_user."""
    mock_rbac = MagicMock()
    mock_rbac.get_primary_role.return_value = "user"
    mock_rbac.get_user_permissions.return_value = ["read"]
    monkeypatch.setattr(
        "app.domains.rbac.service.rbac_service.RbacService",
        lambda: mock_rbac,
    )


# ── register ──────────────────────────────────────────────────────

def test_register_success(auth_svc, monkeypatch):
    svc, _, _ = auth_svc
    _mock_rbac(monkeypatch)
    svc._users.username_exists.return_value = False
    svc._users.email_exists.return_value = False
    svc._users.insert_user.return_value = 42
    result = svc.register("alice", "a@b.com", "pass")
    assert result["id"] == 42
    assert result["role"] == "user"
    svc._users.insert_user.assert_called_once()


def test_register_duplicate_username(auth_svc, monkeypatch):
    svc, _, _ = auth_svc
    svc._users.username_exists.return_value = True
    with pytest.raises(ValueError, match="Username already registered"):
        svc.register("dup", "a@b.com", "pass")


def test_register_duplicate_email(auth_svc, monkeypatch):
    svc, _, _ = auth_svc
    svc._users.username_exists.return_value = False
    svc._users.email_exists.return_value = True
    with pytest.raises(ValueError, match="Email already registered"):
        svc.register("new", "dup@b.com", "pass")


# ── login ─────────────────────────────────────────────────────────

def test_login_success(auth_svc, monkeypatch):
    svc, mock_verify, _ = auth_svc
    _mock_rbac(monkeypatch)
    mock_verify.return_value = True
    svc._users.get_user_for_login.return_value = {
        "id": 1, "username": "alice", "hashed_password": "h",
        "is_active": True, "must_change_password": False,
    }
    svc._users.get_user_by_id.return_value = {
        "id": 1, "username": "alice", "email": "a@b.com",
        "is_active": True, "created_at": datetime(2025, 1, 1),
    }
    result = svc.login("alice", "pass")
    assert result["access_token"] == "access_tok"
    assert result["refresh_token"] == "refresh_tok"
    assert result["token_type"] == "bearer"


def test_login_wrong_password(auth_svc, monkeypatch):
    svc, mock_verify, _ = auth_svc
    mock_verify.return_value = False
    svc._users.get_user_for_login.return_value = {
        "id": 1, "username": "alice", "hashed_password": "h",
        "is_active": True,
    }
    with pytest.raises(PermissionError, match="Incorrect"):
        svc.login("alice", "wrong")


def test_login_user_not_found(auth_svc, monkeypatch):
    svc, _, _ = auth_svc
    svc._users.get_user_for_login.return_value = None
    with pytest.raises(PermissionError, match="Incorrect"):
        svc.login("nobody", "pass")


def test_login_inactive_user(auth_svc, monkeypatch):
    svc, mock_verify, _ = auth_svc
    mock_verify.return_value = True
    svc._users.get_user_for_login.return_value = {
        "id": 1, "username": "alice", "hashed_password": "h",
        "is_active": False,
    }
    with pytest.raises(PermissionError, match="disabled"):
        svc.login("alice", "pass")


# ── refresh ───────────────────────────────────────────────────────

def test_refresh_success(auth_svc, monkeypatch):
    svc, _, mock_decode = auth_svc
    _mock_rbac(monkeypatch)
    td = MagicMock()
    td.user_id = 1
    td.session_id = None
    mock_decode.return_value = td
    svc._users.get_user_by_id.return_value = {
        "id": 1, "username": "alice", "is_active": True,
        "must_change_password": False,
    }
    result = svc.refresh("tok")
    assert result["access_token"] == "access_tok"


def test_refresh_invalid_token(auth_svc, monkeypatch):
    svc, _, mock_decode = auth_svc
    mock_decode.return_value = None
    with pytest.raises(PermissionError, match="Invalid"):
        svc.refresh("bad")


def test_refresh_inactive_user(auth_svc, monkeypatch):
    svc, _, mock_decode = auth_svc
    td = MagicMock()
    td.user_id = 1
    td.session_id = None
    mock_decode.return_value = td
    svc._users.get_user_by_id.return_value = {"id": 1, "is_active": False}
    with pytest.raises(PermissionError, match="inactive"):
        svc.refresh("tok")


# ── me ────────────────────────────────────────────────────────────

def test_me_found(auth_svc, monkeypatch):
    svc, _, _ = auth_svc
    _mock_rbac(monkeypatch)
    svc._users.get_user_by_id.return_value = {
        "id": 1, "username": "alice",
    }
    result = svc.me(1)
    assert result["username"] == "alice"


def test_me_not_found(auth_svc, monkeypatch):
    svc, _, _ = auth_svc
    svc._users.get_user_by_id.return_value = None
    with pytest.raises(KeyError, match="not found"):
        svc.me(999)


# ── change_password ───────────────────────────────────────────────

def test_change_password_success(auth_svc, monkeypatch):
    svc, mock_verify, _ = auth_svc
    mock_verify.return_value = True
    svc._users.get_user_by_id.return_value = {
        "id": 1, "hashed_password": "old_hash",
    }
    svc.change_password(1, "old", "new")
    svc._users.update_user_password.assert_called_once()


def test_change_password_wrong_current(auth_svc, monkeypatch):
    svc, mock_verify, _ = auth_svc
    mock_verify.return_value = False
    svc._users.get_user_by_id.return_value = {
        "id": 1, "hashed_password": "h",
    }
    with pytest.raises(PermissionError, match="Incorrect"):
        svc.change_password(1, "wrong", "new")


def test_change_password_user_not_found(auth_svc, monkeypatch):
    svc, _, _ = auth_svc
    svc._users.get_user_by_id.return_value = None
    with pytest.raises(KeyError):
        svc.change_password(999, "a", "b")
