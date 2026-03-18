"""Redis-based sliding-window rate limiting middleware (Issue #14).

Default limits
- General: 60 req/min per user (or per IP for unauthenticated).
- /auth/login: 10 req/min per IP.

Returns 429 Too Many Requests with Retry-After header when exceeded.
"""

from __future__ import annotations

import time

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from app.infrastructure.config import get_settings

# ---------------------------------------------------------------------------
# Redis client (lazy singleton)
# ---------------------------------------------------------------------------
_redis_client = None


def _get_redis():
    global _redis_client
    if _redis_client is None:
        import redis as _redis_module

        settings = get_settings()
        _redis_client = _redis_module.Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            db=settings.redis_db,
            decode_responses=True,
        )
    return _redis_client


# ---------------------------------------------------------------------------
# Sliding-window counter helpers
# ---------------------------------------------------------------------------
_KEY_PREFIX = "quantmate:ratelimit:"
_WINDOW_SECONDS = 60  # 1-minute window


def _rate_limit_key(identifier: str, path_tag: str) -> str:
    return f"{_KEY_PREFIX}{path_tag}:{identifier}"


def _check_rate_limit(
    redis_client, key: str, max_requests: int, window: int = _WINDOW_SECONDS
) -> tuple[bool, int, int]:
    """Check and increment the sliding-window counter.

    Returns:
        (allowed, remaining, retry_after_seconds)
    """
    now = time.time()
    window_start = now - window

    pipe = redis_client.pipeline(True)
    # Remove old entries outside the window
    pipe.zremrangebyscore(key, 0, window_start)
    # Add current request
    pipe.zadd(key, {str(now): now})
    # Count requests in window
    pipe.zcard(key)
    # Set TTL so keys expire automatically
    pipe.expire(key, window)
    results = pipe.execute()

    current_count = results[2]
    if current_count > max_requests:
        # Compute retry-after: time until the oldest entry expires out of the window
        oldest = redis_client.zrange(key, 0, 0, withscores=True)
        if oldest:
            retry_after = int(oldest[0][1] + window - now) + 1
        else:
            retry_after = window
        return False, 0, max(1, retry_after)

    remaining = max(0, max_requests - current_count)
    return True, remaining, 0


# ---------------------------------------------------------------------------
# Path-based limit configuration
# ---------------------------------------------------------------------------
# Paths mapped to their per-minute limits (matched by prefix).
_PATH_LIMITS: dict[str, int] = {
    "/api/v1/auth/login": 10,
    "/api/v1/auth/register": 10,
}
_DEFAULT_LIMIT = 60


def _get_limit_for_path(path: str) -> tuple[int, str]:
    """Return (max_requests, tag) for the given path."""
    for prefix, limit in _PATH_LIMITS.items():
        if path.startswith(prefix):
            tag = prefix.replace("/", "_").strip("_")
            return limit, tag
    return _DEFAULT_LIMIT, "global"


# ---------------------------------------------------------------------------
# Identifier extraction
# ---------------------------------------------------------------------------


def _extract_identifier(request: Request) -> str:
    """Get rate-limit key identifier: user_id from JWT if available, else client IP."""
    auth = request.headers.get("authorization", "")
    if auth.startswith("Bearer "):
        token = auth[7:]
        try:
            from app.api.services.auth_service import decode_token

            data = decode_token(token)
            if data and data.user_id:
                return f"user:{data.user_id}"
        except Exception:
            pass
    # Fall back to IP
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return f"ip:{forwarded.split(',')[0].strip()}"
    client = request.client
    return f"ip:{client.host}" if client else "ip:unknown"


# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Sliding-window rate limiter backed by Redis sorted sets."""

    async def dispatch(self, request: Request, call_next):
        # Skip non-API paths (docs, health, root)
        path = request.url.path
        if not path.startswith("/api/"):
            return await call_next(request)

        try:
            r = _get_redis()
            max_req, tag = _get_limit_for_path(path)
            identifier = _extract_identifier(request)
            key = _rate_limit_key(identifier, tag)
            allowed, remaining, retry_after = _check_rate_limit(r, key, max_req)

            if not allowed:
                return JSONResponse(
                    status_code=429,
                    content={
                        "error": {
                            "code": "RATE_LIMIT_EXCEEDED",
                            "message": "Too many requests. Please try again later.",
                        }
                    },
                    headers={
                        "Retry-After": str(retry_after),
                        "X-RateLimit-Limit": str(max_req),
                        "X-RateLimit-Remaining": "0",
                    },
                )

            response = await call_next(request)
            response.headers["X-RateLimit-Limit"] = str(max_req)
            response.headers["X-RateLimit-Remaining"] = str(remaining)
            return response

        except Exception:
            # If Redis is unavailable, allow the request (fail open).
            return await call_next(request)
