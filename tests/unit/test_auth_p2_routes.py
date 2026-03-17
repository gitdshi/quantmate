"""Tests for P2: MFA, API Key, and Session routes."""
import pytest
from unittest.mock import patch, MagicMock
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.routes import mfa, api_keys, sessions
from app.api.exception_handlers import register_exception_handlers


@pytest.fixture
def mock_user():
    return {"id": 1, "username": "testuser"}


@pytest.fixture
def mfa_client(mock_user):
    test_app = FastAPI()
    register_exception_handlers(test_app)
    test_app.include_router(mfa.router, prefix="/api/v1")

    async def override_auth():
        return mock_user

    test_app.dependency_overrides[mfa.get_current_user] = override_auth
    return TestClient(test_app)


@pytest.fixture
def apikey_client(mock_user):
    test_app = FastAPI()
    register_exception_handlers(test_app)
    test_app.include_router(api_keys.router, prefix="/api/v1")

    async def override_auth():
        return mock_user

    test_app.dependency_overrides[api_keys.get_current_user] = override_auth
    return TestClient(test_app)


@pytest.fixture
def session_client(mock_user):
    test_app = FastAPI()
    register_exception_handlers(test_app)
    test_app.include_router(sessions.router, prefix="/api/v1")

    async def override_auth():
        return mock_user

    test_app.dependency_overrides[sessions.get_current_user] = override_auth
    return TestClient(test_app)


class TestMFA:
    @patch("app.api.routes.mfa.MfaDao")
    @patch("app.api.routes.mfa._generate_totp_secret")
    @patch("app.api.routes.mfa._generate_recovery_codes")
    @patch("app.api.routes.mfa._hash_recovery_codes")
    def test_mfa_setup(self, mock_hash, mock_codes, mock_secret, MockDao, mfa_client):
        instance = MockDao.return_value
        instance.get_by_user_id.return_value = None
        mock_secret.return_value = "TESTSECRET"
        mock_codes.return_value = ["code1", "code2", "code3", "code4", "code5", "code6", "code7", "code8"]
        mock_hash.return_value = "hashed"
        instance.upsert.return_value = None

        resp = mfa_client.post("/api/v1/auth/mfa/setup")
        assert resp.status_code == 200
        data = resp.json()
        assert data["secret"] == "TESTSECRET"
        assert len(data["recovery_codes"]) == 8

    @patch("app.api.routes.mfa.MfaDao")
    @patch("app.api.routes.mfa._verify_totp_code")
    def test_mfa_verify(self, mock_verify, MockDao, mfa_client):
        instance = MockDao.return_value
        instance.get_by_user_id.return_value = {"secret_encrypted": "SECRET", "is_enabled": False}
        mock_verify.return_value = True
        instance.enable.return_value = None

        resp = mfa_client.post("/api/v1/auth/mfa/verify", json={"code": "123456"})
        assert resp.status_code == 200

    @patch("app.api.routes.mfa.MfaDao")
    @patch("app.api.routes.mfa._verify_totp_code")
    def test_mfa_disable(self, mock_verify, MockDao, mfa_client):
        instance = MockDao.return_value
        instance.get_by_user_id.return_value = {"is_enabled": True, "secret_encrypted": "SEC"}
        mock_verify.return_value = True
        instance.disable.return_value = None

        resp = mfa_client.post("/api/v1/auth/mfa/disable", json={"code": "123456"})
        assert resp.status_code == 200


class TestAPIKeys:
    @patch("app.api.routes.api_keys.ApiKeyDao")
    def test_list_api_keys(self, MockDao, apikey_client):
        instance = MockDao.return_value
        instance.list_by_user.return_value = [
            {"id": 1, "key_id": "qm_abc123", "name": "Test Key",
             "permissions": ["read"], "expires_at": None, "ip_whitelist": None,
             "rate_limit": 60, "is_active": True,
             "created_at": "2026-01-01T00:00:00", "last_used_at": None}
        ]
        resp = apikey_client.get("/api/v1/auth/api-keys")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["key_id"] == "qm_abc123"

    @patch("app.api.routes.api_keys.ApiKeyDao")
    def test_create_api_key(self, MockDao, apikey_client):
        instance = MockDao.return_value
        instance.count_by_user.return_value = 0
        instance.create.return_value = 1

        resp = apikey_client.post("/api/v1/auth/api-keys", json={
            "name": "Test Key", "permissions": ["read"]
        })
        assert resp.status_code == 201
        data = resp.json()
        assert "key_id" in data
        assert "secret" in data

    @patch("app.api.routes.api_keys.ApiKeyDao")
    def test_create_api_key_limit_exceeded(self, MockDao, apikey_client):
        instance = MockDao.return_value
        instance.count_by_user.return_value = 5

        resp = apikey_client.post("/api/v1/auth/api-keys", json={
            "name": "Test Key", "permissions": ["read"]
        })
        assert resp.status_code == 400

    @patch("app.api.routes.api_keys.ApiKeyDao")
    def test_delete_api_key(self, MockDao, apikey_client):
        instance = MockDao.return_value
        instance.revoke.return_value = True
        resp = apikey_client.delete("/api/v1/auth/api-keys/1")
        assert resp.status_code == 200

    @patch("app.api.routes.api_keys.ApiKeyDao")
    def test_delete_api_key_not_found(self, MockDao, apikey_client):
        instance = MockDao.return_value
        instance.revoke.return_value = False
        resp = apikey_client.delete("/api/v1/auth/api-keys/999")
        assert resp.status_code == 404


class TestSessions:
    @patch("app.api.routes.sessions.SessionDao")
    def test_list_sessions(self, MockDao, session_client):
        instance = MockDao.return_value
        instance.list_by_user.return_value = [
            {"id": 1, "device_info": "Chrome", "ip_address": "127.0.0.1"}
        ]
        resp = session_client.get("/api/v1/auth/sessions")
        assert resp.status_code == 200
        assert len(resp.json()["sessions"]) == 1

    @patch("app.api.routes.sessions.SessionDao")
    def test_revoke_session(self, MockDao, session_client):
        instance = MockDao.return_value
        instance.delete.return_value = True
        resp = session_client.delete("/api/v1/auth/sessions/1")
        assert resp.status_code == 200

    @patch("app.api.routes.sessions.SessionDao")
    def test_revoke_session_not_found(self, MockDao, session_client):
        instance = MockDao.return_value
        instance.delete.return_value = False
        resp = session_client.delete("/api/v1/auth/sessions/999")
        assert resp.status_code == 404

    @patch("app.api.routes.sessions.SessionDao")
    def test_revoke_all_sessions(self, MockDao, session_client):
        instance = MockDao.return_value
        instance.delete_all_for_user.return_value = 3
        resp = session_client.delete("/api/v1/auth/sessions/all")
        assert resp.status_code == 200
