
import pandas as pd
import pytest
import requests

from app.domains.market.realtime_quote_service import RealtimeQuoteService
import app.domains.market.realtime_quote_service as quote_module


class TestRealtimeQuoteServiceHelpers:
    def test_to_float_to_int_normalize_and_pick(self):
        svc = RealtimeQuoteService()
        row = pd.Series({"a": "", "b": "42"})

        assert svc._to_float("1,234.5%") == 1234.5
        assert svc._to_float(3) == 3.0
        assert svc._to_float("") is None
        assert svc._to_int("1,234") == 1234
        assert svc._to_int(None) is None
        assert svc._normalize_symbol("btc/usdt") == "BTCUSDT"
        assert svc._pick(row, ["a", "b"]) == "42"

    def test_build_tencent_quote_parses_prices_and_amount(self):
        svc = RealtimeQuoteService()
        parts = ["v", "Name", "", "10", "8", "9", "100", *([""] * 26), "11", "7", "a/b/123.45"]

        result = svc._build_tencent_quote("000001", parts, market="CN")

        assert result["change"] == 2.0
        assert result["change_percent"] == 25.0
        assert result["amount"] == 123.45
        assert result["volume"] == 100

    def test_tencent_request_with_retry_success_and_invalid_payload(self, monkeypatch):
        class Resp:
            def __init__(self, text):
                self.text = text

            def raise_for_status(self):
                return None

        monkeypatch.setattr(requests, "get", lambda *args, **kwargs: Resp('v_sz000001="1~Name~~10~8~9~100";'))
        assert RealtimeQuoteService._tencent_request_with_retry("url", "000001")[1] == "Name"

        monkeypatch.setattr(requests, "get", lambda *args, **kwargs: Resp("bad"))
        with pytest.raises(ValueError, match="Invalid Tencent quote response"):
            RealtimeQuoteService._tencent_request_with_retry("url", "000001")

    def test_tencent_request_retries_timeout_and_raises_last_exception(self, monkeypatch):
        calls = {"n": 0}

        def fake_get(*args, **kwargs):
            calls["n"] += 1
            raise requests.Timeout("timeout")

        monkeypatch.setattr(requests, "get", fake_get)
        monkeypatch.setattr(quote_module.time, "sleep", lambda *_: None)

        with pytest.raises(requests.Timeout):
            RealtimeQuoteService._tencent_request_with_retry("url", "000001")
        assert calls["n"] == quote_module._TENCENT_RETRIES + 1

    def test_fetch_akshare_with_timeout_uses_cache_and_returns_stale_on_timeout(self, monkeypatch):
        df = pd.DataFrame([{"x": 1}])
        key = "demo-cache"
        quote_module._BULK_CACHE.clear()
        quote_module._set_cached_df(key, df)
        assert quote_module._fetch_akshare_with_timeout(lambda: pd.DataFrame(), key).equals(df)

        class Future:
            def result(self, timeout=None):
                raise quote_module.FuturesTimeoutError()

            def cancel(self):
                return None

        monkeypatch.setattr(quote_module, "_get_cached_df", lambda cache_key: None)
        monkeypatch.setattr(quote_module._executor, "submit", lambda fn: Future())
        quote_module._BULK_CACHE[key] = (quote_module.time.monotonic(), df)
        assert quote_module._fetch_akshare_with_timeout(lambda: df, key).equals(df)

    def test_get_quote_routes_by_market_and_rejects_empty_symbol(self, monkeypatch):
        svc = RealtimeQuoteService()
        monkeypatch.setattr(svc, "_quote_cn", lambda symbol: {"market": "CN", "symbol": symbol})
        monkeypatch.setattr(svc, "_quote_cn_index", lambda symbol: {"market": "CN_INDEX", "symbol": symbol})
        monkeypatch.setattr(svc, "_quote_hk", lambda symbol: {"market": "HK", "symbol": symbol})
        monkeypatch.setattr(svc, "_quote_us", lambda symbol: {"market": "US", "symbol": symbol})
        monkeypatch.setattr(svc, "_quote_fx", lambda symbol: {"market": "FX", "symbol": symbol})
        monkeypatch.setattr(svc, "_quote_futures", lambda symbol: {"market": "FUTURES", "symbol": symbol})
        monkeypatch.setattr(svc, "_quote_crypto", lambda symbol: {"market": "CRYPTO", "symbol": symbol})

        assert svc.get_quote("000001", "CN")["market"] == "CN"
        assert svc.get_quote("000300.SH", "INDEX")["market"] == "CN_INDEX"
        assert svc.get_quote("00700", "HK")["market"] == "HK"
        assert svc.get_quote("AAPL", "US")["market"] == "US"
        assert svc.get_quote("EURUSD", "FX")["market"] == "FX"
        assert svc.get_quote("IF2401", "FUTURES")["market"] == "FUTURES"
        assert svc.get_quote("BTCUSDT", "CRYPTO")["market"] == "CRYPTO"

        with pytest.raises(ValueError, match="Symbol is required"):
            svc.get_quote(" ", "CN")
        with pytest.raises(ValueError, match="Unsupported market"):
            svc.get_quote("AAPL", "UNKNOWN")


class TestRealtimeQuoteServiceMarkets:
    def test_quote_cn_and_cn_index_use_expected_prefixes(self, monkeypatch):
        svc = RealtimeQuoteService()
        monkeypatch.setattr(svc, "_fetch_tencent_quote", lambda code: ["v", "Name", "", "10", "9", "9", "100", *([""] * 29)])
        monkeypatch.setattr(svc, "_fetch_tencent_quote_with_prefix", lambda code, prefix: [prefix, "Idx", "", "10", "9", "9", "100", *([""] * 29)])

        assert svc._quote_cn("000001.SZ")["symbol"] == "000001"
        assert svc._quote_cn_index("000001.SH")["name"] == "Idx"
        assert svc._quote_cn_index("399001.SZ")["name"] == "Idx"

    def test_quote_hk_tencent_and_fallback_bulk(self, monkeypatch):
        svc = RealtimeQuoteService()
        monkeypatch.setattr(svc, "_tencent_request_with_retry", lambda url, code: ["v", "Tencent HK", "", "10", "8", "9", "100", *([""] * 28)])
        result = svc._quote_hk_tencent("00700")
        assert result["market"] == "HK"
        assert result["symbol"] == "00700"

        monkeypatch.setattr(svc, "_quote_hk_tencent", lambda code: (_ for _ in ()).throw(RuntimeError("fail")))
        monkeypatch.setattr(quote_module, "ak", type("AK", (), {"stock_hk_spot": staticmethod(lambda: pd.DataFrame([{"symbol": "00700", "name": "HK Corp", "lasttrade": 11, "pricechange": 1, "changepercent": 10, "open": 10, "high": 12, "low": 9, "prevclose": 10, "volume": 100, "amount": 200, "ticktime": "now"}]))})())
        monkeypatch.setattr(quote_module, "_fetch_akshare_with_timeout", lambda func, key: func())
        bulk = svc._quote_hk("700")
        assert bulk["symbol"] == "00700"
        assert bulk["source"] == "akshare:stock_hk_spot"

    def test_quote_us_fx_futures_crypto_and_missing_symbol_paths(self, monkeypatch):
        svc = RealtimeQuoteService()
        monkeypatch.setattr(quote_module, "_fetch_akshare_with_timeout", lambda func, key: func())
        quote_module.ak = type(
            "AK",
            (),
            {
                "stock_us_spot_em": staticmethod(lambda: pd.DataFrame([{"代码": "US.AAPL", "名称": "Apple", "最新价": 10, "涨跌额": 1, "涨跌幅": 2, "开盘价": 9, "最高价": 11, "最低价": 8, "昨收价": 9, "成交量": 100, "成交额": 200}])),
                "forex_spot_em": staticmethod(lambda: pd.DataFrame([{"代码": "EURUSD", "名称": "EUR/USD", "最新价": 1.1, "涨跌额": 0.1, "涨跌幅": 1, "今开": 1.0, "最高": 1.2, "最低": 0.9, "昨收": 1.0}])),
                "futures_zh_spot": staticmethod(lambda: pd.DataFrame([{"symbol": "IF2401", "name": "IF", "current_price": 10, "change": 1, "change_pct": 2, "open": 9, "high": 11, "low": 8, "prev_close": 9, "volume": 100, "time": "now"}])),
                "crypto_js_spot": staticmethod(lambda: pd.DataFrame([{"交易品种": "BTCUSDT", "市场": "Binance", "最近报价": 10, "涨跌额": 1, "涨跌幅": 2, "24小时最高": 11, "24小时最低": 9, "24小时成交量": 100, "更新时间": "now"}])),
            },
        )()

        assert svc._quote_us("aapl")["symbol"] == "AAPL"
        assert svc._quote_fx("eur/usd")["symbol"] == "EURUSD"
        assert svc._quote_futures("if2401")["symbol"] == "IF2401"
        assert svc._quote_crypto("btc/usdt")["symbol"] == "BTCUSDT"

        quote_module.ak = type("AK", (), {"stock_us_spot_em": staticmethod(lambda: pd.DataFrame([{"代码": "US.MSFT"}]))})()
        with pytest.raises(ValueError, match="Symbol not found"):
            svc._quote_us("AAPL")
