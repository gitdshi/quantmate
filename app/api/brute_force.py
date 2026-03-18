"""Brute-force login protection (Issue #3).

Uses Redis to track failed login attempts per IP and per username.
After MAX_ATTEMPTS failures within the WINDOW, the IP/username is locked
for LOCKOUT_SECONDS. Successful login resets the counters.
"""

from __future__ import annotations

from typing import Optional

from app.infrastructure.logging import get_logger

logger = get_logger(__name__)

MAX_ATTEMPTS = 5
LOCKOUT_SECONDS = 15 * 60  # 15 minutes
WINDOW_SECONDS = 15 * 60  # sliding window = lockout window

_PREFIX_IP = "bf:ip:"
_PREFIX_USER = "bf:user:"
_LOCK_PREFIX_IP = "bf:lock:ip:"
_LOCK_PREFIX_USER = "bf:lock:user:"


def _get_redis():
    """Reuse the same lazy singleton as rate_limit.py."""
    from app.api.rate_limit import _get_redis as _rl_redis

    return _rl_redis()


def is_locked(*, ip: Optional[str] = None, username: Optional[str] = None) -> bool:
    """Return True if the IP or username is currently locked out."""
    try:
        r = _get_redis()
        if ip and r.exists(f"{_LOCK_PREFIX_IP}{ip}"):
            return True
        if username and r.exists(f"{_LOCK_PREFIX_USER}{username}"):
            return True
    except Exception as e:
        logger.warning(f"Brute-force check failed (fail-open): {e}")
    return False


def remaining_lockout(*, ip: Optional[str] = None, username: Optional[str] = None) -> int:
    """Return seconds remaining on lockout, or 0 if not locked."""
    try:
        r = _get_redis()
        ttls = []
        if ip:
            t = r.ttl(f"{_LOCK_PREFIX_IP}{ip}")
            if t and t > 0:
                ttls.append(t)
        if username:
            t = r.ttl(f"{_LOCK_PREFIX_USER}{username}")
            if t and t > 0:
                ttls.append(t)
        return max(ttls) if ttls else 0
    except Exception:
        return 0


def record_failure(*, ip: Optional[str] = None, username: Optional[str] = None) -> int:
    """Record a failed login attempt. Returns the new failure count.

    If the count crosses MAX_ATTEMPTS, a lockout key is set.
    """
    try:
        r = _get_redis()
        count = 0
        for prefix, lock_prefix, key in [
            (_PREFIX_IP, _LOCK_PREFIX_IP, ip),
            (_PREFIX_USER, _LOCK_PREFIX_USER, username),
        ]:
            if not key:
                continue
            redis_key = f"{prefix}{key}"
            new_count = r.incr(redis_key)
            # Set expiry on first increment
            if new_count == 1:
                r.expire(redis_key, WINDOW_SECONDS)
            count = max(count, new_count)
            if new_count >= MAX_ATTEMPTS:
                r.setex(f"{lock_prefix}{key}", LOCKOUT_SECONDS, "1")
                logger.warning(f"Brute-force lockout: {prefix}{key} ({new_count} attempts)")
        return count
    except Exception as e:
        logger.warning(f"Brute-force record failed (fail-open): {e}")
        return 0


def reset(*, ip: Optional[str] = None, username: Optional[str] = None) -> None:
    """Reset counters after a successful login."""
    try:
        r = _get_redis()
        keys = []
        if ip:
            keys.extend([f"{_PREFIX_IP}{ip}", f"{_LOCK_PREFIX_IP}{ip}"])
        if username:
            keys.extend([f"{_PREFIX_USER}{username}", f"{_LOCK_PREFIX_USER}{username}"])
        if keys:
            r.delete(*keys)
    except Exception as e:
        logger.warning(f"Brute-force reset failed: {e}")
