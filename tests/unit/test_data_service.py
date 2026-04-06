from datetime import date

from app.api.services.data_service import DataService
from app.domains.market.service import MarketService


class TestDataServiceIndexOverview:
    def test_get_index_overview_uses_canonical_hs300_symbol(self, monkeypatch):
        captured = []

        def fake_get_quote(symbol: str, market: str):
            captured.append((symbol, market))
            return {"symbol": symbol, "market": market, "price": 1.0}

        svc = DataService()
        monkeypatch.setattr(svc._realtime, "get_quote", fake_get_quote)

        result = svc.get_index_overview()

        assert result["csi300"]["symbol"] == "399300.SZ"
        assert ("399300.SZ", "CN_INDEX") in captured

    def test_get_index_overview_returns_error_payload_when_quote_fails(self, monkeypatch):
        svc = DataService()
        monkeypatch.setattr(svc._realtime, "get_quote", lambda **_: (_ for _ in ()).throw(RuntimeError("boom")))

        result = svc.get_index_overview()

        assert result["csi300"]["error"] == "boom"
        assert result["csi300"]["display_name"] == "CSI 300"


class TestMarketServiceIndexes:
    def test_list_benchmark_indexes_normalizes_hs300_alias(self, monkeypatch):
        svc = MarketService()
        monkeypatch.setattr(
            svc._index_dao,
            "list_index_codes",
            lambda: ["000300.SH", "399300.SZ", "000001.SH"],
        )

        result = svc.list_benchmark_indexes()

        assert result == [
            {"value": "399300.SZ", "label": "HS300 (沪深300)"},
            {"value": "000001.SH", "label": "SSE Composite (上证综指)"},
        ]


class TestDataService:
    def test_get_symbols_delegates_to_market_service(self, monkeypatch):
        svc = DataService()
        monkeypatch.setattr(svc._market, "list_symbols", lambda **kwargs: [kwargs])

        result = svc.get_symbols(exchange="SZSE", keyword="bank", limit=5, offset=2)

        assert result == [{"exchange": "SZSE", "keyword": "bank", "limit": 5, "offset": 2}]

    def test_get_history_delegates_to_market_service(self, monkeypatch):
        svc = DataService()
        monkeypatch.setattr(svc._market, "get_history", lambda *args: [{"args": args}])

        result = svc.get_history("000001.SZSE", date(2025, 1, 1), date(2025, 1, 2))

        assert result == [{"args": ("000001.SZSE", date(2025, 1, 1), date(2025, 1, 2))}]

    def test_get_indicators_handles_multiple_indicator_types(self, monkeypatch):
        svc = DataService()
        monkeypatch.setattr(
            svc,
            "get_history",
            lambda *args, **kwargs: [
                {"datetime": "2025-01-01", "close": 10.0, "volume": 100},
                {"datetime": "2025-01-02", "close": 12.0, "volume": 110},
                {"datetime": "2025-01-03", "close": 14.0, "volume": 120},
            ],
        )
        monkeypatch.setattr(
            "app.api.services.data_service.moving_average",
            lambda series, window, method=None: series.rolling(window).mean(),
        )

        result = svc.get_indicators("000001.SZSE", date(2025, 1, 1), date(2025, 1, 3), ["ma_2", "ema_2", "returns", "volume_ma_20", "ma_bad"])

        assert set(result.keys()) == {"ma_2", "ema_2", "returns", "volume_ma_20"}
        assert result["ma_2"][-1]["name"] == "ma_2"
        assert result["returns"][-1]["name"] == "returns"

    def test_get_indicators_returns_empty_when_no_history(self, monkeypatch):
        svc = DataService()
        monkeypatch.setattr(svc, "get_history", lambda *args, **kwargs: [])

        assert svc.get_indicators("000001.SZSE", date(2025, 1, 1), date(2025, 1, 3), ["ma_5"]) == {}

    def test_get_market_overview_sectors_exchanges_symbols_by_filter_and_indexes_delegate(self, monkeypatch):
        svc = DataService()
        monkeypatch.setattr(svc._market, "market_overview", lambda: {"total": 1})
        monkeypatch.setattr(svc._market, "sectors", lambda: [{"sector": "Bank"}])
        monkeypatch.setattr(svc._market, "exchanges", lambda: [{"code": "SZSE"}])
        monkeypatch.setattr(svc._market, "symbols_by_filter", lambda **kwargs: [kwargs])
        monkeypatch.setattr(MarketService, "list_benchmark_indexes", lambda self: [{"value": "399300.SZ"}])

        assert svc.get_market_overview() == {"total": 1}
        assert svc.get_sectors() == [{"sector": "Bank"}]
        assert svc.get_exchanges() == [{"code": "SZSE"}]
        assert svc.get_symbols_by_filter(industry="Bank", exchange="SZSE", limit=9) == [{"industry": "Bank", "exchange": "SZSE", "limit": 9}]
        assert svc.get_indexes() == [{"value": "399300.SZ"}]

    def test_get_realtime_quote_records_cache_on_success(self, monkeypatch):
        svc = DataService()
        monkeypatch.setattr(svc, "_realtime_enabled", lambda: True)
        monkeypatch.setattr(svc, "_market_enabled", lambda market: True)
        monkeypatch.setattr(svc, "_cache_enabled", lambda: True)
        monkeypatch.setattr(svc._realtime, "get_quote", lambda **kwargs: {"price": 10, **kwargs})
        recorded = {}
        monkeypatch.setattr(svc._realtime_cache, "record", lambda **kwargs: recorded.update(kwargs))

        result = svc.get_realtime_quote("000001", "CN")

        assert result["price"] == 10
        assert recorded["symbol"] == "000001"
        assert recorded["market"] == "CN"

    def test_get_realtime_quote_raises_when_realtime_or_market_disabled(self, monkeypatch):
        svc = DataService()
        monkeypatch.setattr(svc, "_realtime_enabled", lambda: False)

        try:
            svc.get_realtime_quote("000001", "CN")
        except PermissionError as exc:
            assert "disabled" in str(exc)
        else:
            raise AssertionError("expected PermissionError")

        monkeypatch.setattr(svc, "_realtime_enabled", lambda: True)
        monkeypatch.setattr(svc, "_market_enabled", lambda market: False)
        try:
            svc.get_realtime_quote("000001", "HK")
        except PermissionError as exc:
            assert "HK" in str(exc)
        else:
            raise AssertionError("expected PermissionError")

    def test_get_realtime_quote_returns_stale_cache_on_fetch_error(self, monkeypatch):
        svc = DataService()
        monkeypatch.setattr(svc, "_realtime_enabled", lambda: True)
        monkeypatch.setattr(svc, "_market_enabled", lambda market: True)
        monkeypatch.setattr(svc, "_cache_enabled", lambda: True)
        monkeypatch.setattr(svc._realtime, "get_quote", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("fetch failed")))
        monkeypatch.setattr(svc._realtime_cache, "get_latest", lambda **kwargs: {"price": 9.9, "ts": 100})

        result = svc.get_realtime_quote("000001", "CN")

        assert result["source"] == "cache"
        assert result["stale"] is True
        assert result["error"] == "fetch failed"

    def test_get_realtime_series_handles_disabled_cache_and_returns_points(self, monkeypatch):
        svc = DataService()
        monkeypatch.setattr(svc, "_realtime_enabled", lambda: True)
        monkeypatch.setattr(svc, "_market_enabled", lambda market: True)
        monkeypatch.setattr(svc, "_cache_enabled", lambda: True)
        monkeypatch.setattr(svc._realtime_cache, "get_series", lambda **kwargs: [{"price": 1}])

        assert svc.get_realtime_series("000001", "CN")["points"] == [{"price": 1}]

        monkeypatch.setattr(svc, "_realtime_enabled", lambda: False)
        assert svc.get_realtime_series("000001", "CN") == {"symbol": "000001", "market": "CN", "cached": False, "points": []}

        monkeypatch.setattr(svc, "_realtime_enabled", lambda: True)
        monkeypatch.setattr(svc, "_market_enabled", lambda market: True)
        monkeypatch.setattr(svc, "_cache_enabled", lambda: False)
        assert svc.get_realtime_series("000001", "CN") == {"symbol": "000001", "market": "CN", "cached": False, "points": []}

    def test_realtime_and_cache_settings_default_true_and_parse_values(self, monkeypatch):
        svc = DataService()
        monkeypatch.setattr(svc._system_config, "get", lambda key: None)
        assert svc._realtime_enabled() is True
        assert svc._cache_enabled() is True
        assert svc._market_enabled("CN") is True

        values = {
            "realtime_quote_enabled": {"config_value": "off"},
            "realtime_quote_cache_enabled": {"config_value": "0"},
            "realtime_quote_markets": {"config_value": "HK, US"},
        }
        monkeypatch.setattr(svc._system_config, "get", lambda key: values.get(key))
        assert svc._realtime_enabled() is False
        assert svc._cache_enabled() is False
        assert svc._market_enabled("HK") is True
        assert svc._market_enabled("CN_INDEX") is False
