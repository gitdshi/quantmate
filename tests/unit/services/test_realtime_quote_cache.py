"""Unit tests for app.domains.market.realtime_quote_cache."""

from __future__ import annotations

import json
import time
from unittest.mock import MagicMock, patch

import pytest

import app.domains.market.realtime_quote_cache as _mod


@pytest.fixture()
def cache():
    redis_mock = MagicMock()
    with patch.object(_mod.RealtimeQuoteCache, "_get_redis", return_value=redis_mock):
        c = _mod.RealtimeQuoteCache(ttl_seconds=3600)
        yield c, redis_mock


class TestRealtimeQuoteCache:
    def test_key_format(self):
        k = _mod.RealtimeQuoteCache._key("CN", "000001.SZ")
        assert "CN" in k
        assert "000001.SZ" in k

    def test_record(self, cache):
        c, r = cache
        c.record(market="CN", symbol="000001.SZ", quote={"price": 10.5, "volume": 1000})
        r.zadd.assert_called_once()
        r.expire.assert_called_once()

    def test_get_latest_found(self, cache):
        c, r = cache
        ts = time.time()
        data = json.dumps({"price": 10.5, "ts": ts})
        r.zrevrange.return_value = [data.encode()]
        result = c.get_latest(market="CN", symbol="000001.SZ")
        assert result is not None
        assert result["price"] == 10.5

    def test_get_latest_not_found(self, cache):
        c, r = cache
        r.zrevrange.return_value = []
        result = c.get_latest(market="CN", symbol="000001.SZ")
        assert result is None

    def test_get_series_empty(self, cache):
        c, r = cache
        r.zrangebyscore.return_value = []
        result = c.get_series(market="CN", symbol="000001.SZ")
        assert result == []

    def test_get_series_with_data(self, cache):
        c, r = cache
        ts = time.time()
        items = [json.dumps({"price": i, "ts": ts + i}).encode() for i in range(3)]
        r.zrangebyscore.return_value = items
        result = c.get_series(market="CN", symbol="000001.SZ")
        assert len(result) == 3

    def test_get_series_with_limit(self, cache):
        c, r = cache
        r.zrangebyscore.return_value = []
        c.get_series(market="CN", symbol="000001.SZ", limit=10)
        # limit should affect the call
        assert r.zrangebyscore.called or r.zrevrange.called

    def test_default_ttl(self):
        assert _mod.DEFAULT_TTL_SECONDS == 60 * 60 * 24
