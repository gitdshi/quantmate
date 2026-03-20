"""Redis-backed cache for realtime quote series (24h)."""

from __future__ import annotations

import json
import time
from typing import Any


DEFAULT_TTL_SECONDS = 60 * 60 * 24


class RealtimeQuoteCache:
    """Store and retrieve intraday quote series in Redis."""

    def __init__(self, ttl_seconds: int = DEFAULT_TTL_SECONDS) -> None:
        self.ttl_seconds = ttl_seconds

    @staticmethod
    def _get_redis():
        # Reuse the shared Redis client (lazy singleton).
        from app.api.rate_limit import _get_redis as _rl_redis

        return _rl_redis()

    @staticmethod
    def _key(market: str, symbol: str) -> str:
        return f"quantmate:realtime:quote:{market.upper()}:{symbol.upper()}"

    def record(self, *, market: str, symbol: str, quote: dict[str, Any]) -> None:
        r = self._get_redis()
        if r is None:
            return

        price = quote.get("price")
        if price is None:
            return

        ts = int(time.time())
        key = self._key(market, symbol)
        payload = json.dumps({"ts": ts, "price": price}, separators=(",", ":"))

        try:
            r.zadd(key, {payload: ts})
            r.zremrangebyscore(key, 0, ts - self.ttl_seconds)
            r.expire(key, self.ttl_seconds)
        except Exception:
            # Fail open if Redis is unavailable
            return

    def get_series(
        self,
        *,
        market: str,
        symbol: str,
        start_ts: int | None = None,
        end_ts: int | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        r = self._get_redis()
        if r is None:
            return []

        now = int(time.time())
        start = start_ts if start_ts is not None else now - self.ttl_seconds
        end = end_ts if end_ts is not None else now

        key = self._key(market, symbol)
        try:
            values = r.zrangebyscore(key, start, end)
        except Exception:
            return []

        points: list[dict[str, Any]] = []
        for raw in values:
            try:
                data = json.loads(raw)
                if "ts" in data and "price" in data:
                    points.append({"ts": int(data["ts"]), "price": float(data["price"])})
            except Exception:
                continue

        if limit and len(points) > limit:
            return points[-limit:]
        return points
