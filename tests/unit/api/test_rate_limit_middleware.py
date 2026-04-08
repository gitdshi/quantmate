"""Unit tests for app.api.rate_limit — Redis-based sliding-window rate limiter."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.api.rate_limit import (
    _check_rate_limit,
    _extract_identifier,
    _get_limit_for_path,
    _rate_limit_key,
    RateLimitMiddleware,
)


# ── _rate_limit_key ──────────────────────────────────────────────

def test_rate_limit_key():
    key = _rate_limit_key("user:42", "global")
    assert key == "quantmate:ratelimit:global:user:42"


# ── _get_limit_for_path ──────────────────────────────────────────

def test_get_limit_login():
    limit, tag = _get_limit_for_path("/api/v1/auth/login")
    assert limit == 10
    assert "auth" in tag


def test_get_limit_register():
    limit, tag = _get_limit_for_path("/api/v1/auth/register")
    assert limit == 10


def test_get_limit_default():
    limit, tag = _get_limit_for_path("/api/v1/strategies")
    assert limit == 60
    assert tag == "global"


# ── _check_rate_limit ────────────────────────────────────────────

def test_check_rate_limit_allowed():
    redis_mock = MagicMock()
    pipe = MagicMock()
    redis_mock.pipeline.return_value = pipe
    pipe.execute.return_value = [None, None, 5, None]  # zremrangebyscore, zadd, zcard=5, expire
    allowed, remaining, retry = _check_rate_limit(redis_mock, "key", max_requests=10)
    assert allowed is True
    assert remaining == 5
    assert retry == 0


def test_check_rate_limit_exceeded():
    redis_mock = MagicMock()
    pipe = MagicMock()
    redis_mock.pipeline.return_value = pipe
    pipe.execute.return_value = [None, None, 11, None]  # count > max
    redis_mock.zrange.return_value = [("ts", 1000.0)]
    allowed, remaining, retry = _check_rate_limit(redis_mock, "key", max_requests=10)
    assert allowed is False
    assert remaining == 0
    assert retry >= 1


def test_check_rate_limit_exceeded_no_oldest():
    redis_mock = MagicMock()
    pipe = MagicMock()
    redis_mock.pipeline.return_value = pipe
    pipe.execute.return_value = [None, None, 11, None]
    redis_mock.zrange.return_value = []
    allowed, remaining, retry = _check_rate_limit(redis_mock, "key", max_requests=10)
    assert allowed is False
    assert retry == 60  # window


def test_check_rate_limit_at_exact_limit():
    redis_mock = MagicMock()
    pipe = MagicMock()
    redis_mock.pipeline.return_value = pipe
    pipe.execute.return_value = [None, None, 10, None]  # count == max
    allowed, remaining, _retry = _check_rate_limit(redis_mock, "key", max_requests=10)
    assert allowed is True
    assert remaining == 0


# ── _extract_identifier ──────────────────────────────────────────

def test_extract_identifier_from_jwt():
    from app.api.services.auth_service import create_access_token

    tok = create_access_token(42, "alice")
    req = MagicMock()
    req.headers.get.side_effect = lambda k, d="": f"Bearer {tok}" if k == "authorization" else d
    ident = _extract_identifier(req)
    assert ident == "user:42"


def test_extract_identifier_from_ip():
    req = MagicMock()
    req.headers.get.side_effect = lambda k, d="": "" if k == "authorization" else d
    req.client.host = "10.0.0.1"
    ident = _extract_identifier(req)
    assert ident == "ip:10.0.0.1"


def test_extract_identifier_from_forwarded():
    req = MagicMock()
    req.headers.get.side_effect = lambda k, d="": {
        "authorization": "",
        "x-forwarded-for": "1.2.3.4, 5.6.7.8",
    }.get(k, d)
    ident = _extract_identifier(req)
    assert ident == "ip:1.2.3.4"


def test_extract_identifier_no_client():
    req = MagicMock()
    req.headers.get.side_effect = lambda k, d="": d
    req.client = None
    ident = _extract_identifier(req)
    assert ident == "ip:unknown"


# ── RateLimitMiddleware ──────────────────────────────────────────

@pytest.mark.asyncio
async def test_middleware_skips_non_api():
    app = MagicMock()
    mw = RateLimitMiddleware(app)
    req = MagicMock()
    req.url.path = "/health"
    mock_next = AsyncMock(return_value=MagicMock(status_code=200))
    response = await mw.dispatch(req, mock_next)
    mock_next.assert_awaited_once()


@pytest.mark.asyncio
async def test_middleware_allows_request():
    app = MagicMock()
    mw = RateLimitMiddleware(app)
    req = MagicMock()
    req.url.path = "/api/v1/strategies"
    req.method = "GET"
    req.headers.get.side_effect = lambda k, d="": d
    req.client.host = "127.0.0.1"
    mock_response = MagicMock(status_code=200, headers={})
    mock_next = AsyncMock(return_value=mock_response)

    mock_redis = MagicMock()
    pipe = MagicMock()
    mock_redis.pipeline.return_value = pipe
    pipe.execute.return_value = [None, None, 1, None]

    with patch("app.api.rate_limit._get_redis", return_value=mock_redis):
        response = await mw.dispatch(req, mock_next)
    mock_next.assert_awaited_once()


@pytest.mark.asyncio
async def test_middleware_blocks_when_exceeded():
    app = MagicMock()
    mw = RateLimitMiddleware(app)
    req = MagicMock()
    req.url.path = "/api/v1/auth/login"
    req.method = "POST"
    req.headers.get.side_effect = lambda k, d="": d
    req.client.host = "127.0.0.1"
    mock_next = AsyncMock()

    mock_redis = MagicMock()
    pipe = MagicMock()
    mock_redis.pipeline.return_value = pipe
    pipe.execute.return_value = [None, None, 11, None]
    mock_redis.zrange.return_value = []

    with patch("app.api.rate_limit._get_redis", return_value=mock_redis):
        response = await mw.dispatch(req, mock_next)
    assert response.status_code == 429
    mock_next.assert_not_awaited()


@pytest.mark.asyncio
async def test_middleware_fails_open_on_redis_error():
    app = MagicMock()
    mw = RateLimitMiddleware(app)
    req = MagicMock()
    req.url.path = "/api/v1/strategies"
    req.method = "GET"
    mock_next = AsyncMock(return_value=MagicMock(status_code=200))

    with patch("app.api.rate_limit._get_redis", side_effect=Exception("redis down")):
        response = await mw.dispatch(req, mock_next)
    mock_next.assert_awaited_once()
