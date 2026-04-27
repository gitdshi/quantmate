"""Tests for Issue #14: Rate Limiting middleware."""
import pytest
import time
from unittest.mock import MagicMock, patch

from app.api.rate_limit import (
    _check_rate_limit,
    _get_limit_for_path,
    _rate_limit_key,
    _extract_identifier,
)


class TestRateLimitKey:
    def test_key_format(self):
        key = _rate_limit_key("user:1", "global")
        assert key == "quantmate:ratelimit:global:user:1"

    def test_key_with_path_tag(self):
        key = _rate_limit_key("ip:127.0.0.1", "api_v1_auth_login")
        assert "api_v1_auth_login" in key


class TestGetLimitForPath:
    def test_login_limit(self):
        limit, tag = _get_limit_for_path("/api/v1/auth/login")
        assert limit == 10
        assert "login" in tag

    def test_register_limit(self):
        limit, tag = _get_limit_for_path("/api/v1/auth/register")
        assert limit == 10

    def test_default_limit(self):
        limit, tag = _get_limit_for_path("/api/v1/strategies")
        assert limit == 60
        assert tag == "global"


class TestCheckRateLimit:
    """Test the sliding window logic with a mock Redis."""

    def _make_mock_redis(self, current_count: int, oldest_score: float = 0):
        mock = MagicMock()
        pipe = MagicMock()
        mock.pipeline.return_value = pipe
        # pipeline returns: [zremrangebyscore, zadd, zcard, expire]
        pipe.execute.return_value = [0, 1, current_count, True]
        mock.zrange.return_value = [(b"ts", oldest_score)]
        return mock

    def test_allowed_when_under_limit(self):
        mock = self._make_mock_redis(current_count=5)
        allowed, remaining, retry = _check_rate_limit(mock, "key", max_requests=60)
        assert allowed is True
        assert remaining == 55
        assert retry == 0

    def test_blocked_when_over_limit(self):
        now = time.time()
        mock = self._make_mock_redis(current_count=61, oldest_score=now - 30)
        allowed, remaining, retry = _check_rate_limit(mock, "key", max_requests=60)
        assert allowed is False
        assert remaining == 0
        assert retry > 0

    def test_exactly_at_limit_is_allowed(self):
        mock = self._make_mock_redis(current_count=60)
        allowed, remaining, retry = _check_rate_limit(mock, "key", max_requests=60)
        assert allowed is True
        assert remaining == 0


class TestExtractIdentifier:
    def test_ip_from_client(self):
        request = MagicMock()
        request.headers = {}
        request.client = MagicMock()
        request.client.host = "192.168.1.1"
        ident = _extract_identifier(request)
        assert ident == "ip:192.168.1.1"

    def test_ip_from_forwarded_header(self):
        request = MagicMock()
        request.headers = {"x-forwarded-for": "10.0.0.1, 10.0.0.2"}
        ident = _extract_identifier(request)
        assert ident == "ip:10.0.0.1"

    def test_user_from_bearer_token(self):
        request = MagicMock()
        request.headers = {"authorization": "Bearer sometoken"}
        request.client = MagicMock()
        request.client.host = "127.0.0.1"

        mock_data = MagicMock()
        mock_data.user_id = 42
        import app.api.services.auth_service as auth_mod
        with patch.object(auth_mod, "decode_token", return_value=mock_data):
            ident = _extract_identifier(request)
        assert ident == "user:42"

    def test_fallback_to_ip_on_bad_token(self):
        request = MagicMock()
        request.headers = {"authorization": "Bearer badtoken"}
        request.client = MagicMock()
        request.client.host = "127.0.0.1"

        import app.api.services.auth_service as auth_mod
        with patch.object(auth_mod, "decode_token", side_effect=Exception("bad")):
            ident = _extract_identifier(request)
        assert ident == "ip:127.0.0.1"


class TestRateLimitMiddlewareIntegration:
    """Test the middleware end-to-end with a mock Redis."""

    @pytest.fixture
    def client(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from app.api.rate_limit import RateLimitMiddleware

        app = FastAPI()
        app.add_middleware(RateLimitMiddleware)

        @app.get("/api/v1/test")
        async def test_endpoint():
            return {"ok": True}

        @app.get("/health")
        async def health():
            return {"status": "ok"}

        return TestClient(app, raise_server_exceptions=False)

    def test_headers_present_on_success(self, client):
        mock_redis = self._make_mock_redis(current_count=1)
        with patch("app.api.rate_limit._get_redis", return_value=mock_redis):
            resp = client.get("/api/v1/test")
        assert resp.status_code == 200
        assert "X-RateLimit-Limit" in resp.headers
        assert "X-RateLimit-Remaining" in resp.headers

    def test_429_on_exceeded(self, client):
        now = time.time()
        mock_redis = self._make_mock_redis(current_count=61, oldest_score=now - 10)
        with patch("app.api.rate_limit._get_redis", return_value=mock_redis):
            resp = client.get("/api/v1/test")
        assert resp.status_code == 429
        assert "Retry-After" in resp.headers
        body = resp.json()
        assert body["error"]["code"] == "RATE_LIMIT_EXCEEDED"

    def test_non_api_path_not_rate_limited(self, client):
        """Health endpoint should bypass rate limiting."""
        resp = client.get("/health")
        assert resp.status_code == 200
        assert "X-RateLimit-Limit" not in resp.headers

    def test_fail_open_on_redis_error(self, client):
        """If Redis is unreachable, requests should still pass."""
        mock_redis = MagicMock()
        mock_redis.pipeline.side_effect = Exception("Redis down")
        with patch("app.api.rate_limit._get_redis", return_value=mock_redis):
            resp = client.get("/api/v1/test")
        assert resp.status_code == 200

    def _make_mock_redis(self, current_count: int, oldest_score: float = 0):
        mock = MagicMock()
        pipe = MagicMock()
        mock.pipeline.return_value = pipe
        pipe.execute.return_value = [0, 1, current_count, True]
        mock.zrange.return_value = [(b"ts", oldest_score)]
        return mock
