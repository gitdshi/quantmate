"""Tests for Issue #3: Brute-force Login Protection."""
import pytest
from unittest.mock import MagicMock, patch

from app.api.brute_force import (
    MAX_ATTEMPTS,
    LOCKOUT_SECONDS,
    WINDOW_SECONDS,
    _PREFIX_IP,
    _PREFIX_USER,
    _LOCK_PREFIX_IP,
    _LOCK_PREFIX_USER,
    is_locked,
    remaining_lockout,
    record_failure,
    reset,
)


@pytest.fixture
def mock_redis():
    r = MagicMock()
    r.exists.return_value = False
    r.ttl.return_value = -2
    r.incr.return_value = 1
    with patch("app.api.brute_force._get_redis", return_value=r):
        yield r


class TestIsLocked:
    def test_not_locked(self, mock_redis):
        assert is_locked(ip="1.2.3.4", username="alice") is False
        assert mock_redis.exists.call_count == 2

    def test_locked_by_ip(self, mock_redis):
        mock_redis.exists.side_effect = lambda k: k == f"{_LOCK_PREFIX_IP}1.2.3.4"
        assert is_locked(ip="1.2.3.4") is True

    def test_locked_by_username(self, mock_redis):
        mock_redis.exists.side_effect = lambda k: k == f"{_LOCK_PREFIX_USER}alice"
        assert is_locked(username="alice") is True

    def test_fail_open_on_redis_error(self, mock_redis):
        mock_redis.exists.side_effect = Exception("Redis down")
        assert is_locked(ip="1.2.3.4") is False


class TestRemainingLockout:
    def test_not_locked(self, mock_redis):
        mock_redis.ttl.return_value = -2
        assert remaining_lockout(ip="1.2.3.4") == 0

    def test_returns_ttl(self, mock_redis):
        mock_redis.ttl.return_value = 600
        assert remaining_lockout(ip="1.2.3.4") == 600

    def test_max_of_ip_and_user(self, mock_redis):
        def ttl_side(key):
            if "ip:" in key:
                return 300
            return 600
        mock_redis.ttl.side_effect = ttl_side
        assert remaining_lockout(ip="1.2.3.4", username="alice") == 600


class TestRecordFailure:
    def test_increments_counter(self, mock_redis):
        mock_redis.incr.return_value = 1
        count = record_failure(ip="1.2.3.4")
        assert count == 1
        mock_redis.incr.assert_called_with(f"{_PREFIX_IP}1.2.3.4")

    def test_sets_expiry_on_first(self, mock_redis):
        mock_redis.incr.return_value = 1
        record_failure(ip="1.2.3.4")
        mock_redis.expire.assert_called_with(f"{_PREFIX_IP}1.2.3.4", WINDOW_SECONDS)

    def test_no_expiry_after_first(self, mock_redis):
        mock_redis.incr.return_value = 3
        record_failure(ip="1.2.3.4")
        mock_redis.expire.assert_not_called()

    def test_lockout_set_at_max_attempts(self, mock_redis):
        mock_redis.incr.return_value = MAX_ATTEMPTS
        record_failure(ip="1.2.3.4")
        mock_redis.setex.assert_called_with(f"{_LOCK_PREFIX_IP}1.2.3.4", LOCKOUT_SECONDS, "1")

    def test_both_ip_and_username(self, mock_redis):
        mock_redis.incr.return_value = 2
        count = record_failure(ip="1.2.3.4", username="alice")
        assert count == 2
        assert mock_redis.incr.call_count == 2

    def test_fail_open(self, mock_redis):
        mock_redis.incr.side_effect = Exception("Redis down")
        count = record_failure(ip="1.2.3.4")
        assert count == 0


class TestReset:
    def test_deletes_all_keys(self, mock_redis):
        reset(ip="1.2.3.4", username="alice")
        deleted_keys = mock_redis.delete.call_args[0]
        assert f"{_PREFIX_IP}1.2.3.4" in deleted_keys
        assert f"{_LOCK_PREFIX_IP}1.2.3.4" in deleted_keys
        assert f"{_PREFIX_USER}alice" in deleted_keys
        assert f"{_LOCK_PREFIX_USER}alice" in deleted_keys

    def test_no_crash_on_redis_error(self, mock_redis):
        mock_redis.delete.side_effect = Exception("Redis down")
        reset(ip="1.2.3.4")  # Should not raise


class TestLoginIntegration:
    """Test brute-force checks integrated in the login route."""

    @pytest.fixture
    def client(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from app.api.routes.auth import router
        from app.api.exception_handlers import register_exception_handlers

        app = FastAPI()
        register_exception_handlers(app)
        app.include_router(router, prefix="/api/v1")
        return TestClient(app, raise_server_exceptions=False)

    def test_locked_user_gets_429(self, client):
        with patch("app.api.routes.auth.brute_force") as mock_bf:
            mock_bf.is_locked.return_value = True
            mock_bf.remaining_lockout.return_value = 900
            resp = client.post("/api/v1/auth/login", json={"username": "alice", "password": "wrong"})
        assert resp.status_code == 429
        assert "AUTH_ACCOUNT_LOCKED" in resp.text

    def test_failed_login_records_failure(self, client):
        with patch("app.api.routes.auth.brute_force") as mock_bf, \
             patch("app.api.routes.auth.AuthService") as MockSvc:
            mock_bf.is_locked.return_value = False
            MockSvc.return_value.login.side_effect = PermissionError("Invalid credentials")
            resp = client.post("/api/v1/auth/login", json={"username": "alice", "password": "wrong"})
        assert resp.status_code == 401
        mock_bf.record_failure.assert_called_once()

    def test_success_resets_counters(self, client):
        with patch("app.api.routes.auth.brute_force") as mock_bf, \
             patch("app.api.routes.auth.AuthService") as MockSvc:
            mock_bf.is_locked.return_value = False
            MockSvc.return_value.login.return_value = {
                "access_token": "tok",
                "refresh_token": "ref",
                "token_type": "bearer",
                "expires_in": 3600,
                "must_change_password": False,
            }
            resp = client.post("/api/v1/auth/login", json={"username": "alice", "password": "correct"})
        assert resp.status_code == 200
        mock_bf.reset.assert_called_once()
