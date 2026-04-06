from datetime import date

import pytest

from app.domains.market.service import MarketService


class TestMarketService:
    def test_resolve_symbol_name_delegates_to_symbol_dao(self, monkeypatch):
        svc = MarketService()
        monkeypatch.setattr(svc._symbol_dao, "get_symbol_name", lambda symbol: f"name:{symbol}")

        assert svc.resolve_symbol_name("000001.SZ") == "name:000001.SZ"

    def test_list_symbols_maps_exchange_and_builds_vt_fields(self, monkeypatch):
        svc = MarketService()
        captured = {}

        def fake_list_stock_basic(exchange=None, keyword=None, limit=100, offset=0):
            captured.update({"exchange": exchange, "keyword": keyword, "limit": limit, "offset": offset})
            return [
                {"ts_code": "000001.SZ", "name": "Ping An", "industry": "Bank", "list_date": "1991-01-01"},
                {"ts_code": "600000.SH", "name": "SPDB", "industry": "Bank", "list_date": "1999-01-01"},
            ]

        monkeypatch.setattr(svc._market_dao, "list_stock_basic", fake_list_stock_basic)

        result = svc.list_symbols(exchange="SZSE", keyword="bank", limit=2, offset=3)

        assert captured == {"exchange": "SZ", "keyword": "bank", "limit": 2, "offset": 3}
        assert result == [
            {
                "symbol": "000001",
                "name": "Ping An",
                "exchange": "SZSE",
                "vt_symbol": "000001.SZSE",
                "industry": "Bank",
                "list_date": "1991-01-01",
            },
            {
                "symbol": "600000",
                "name": "SPDB",
                "exchange": "SSE",
                "vt_symbol": "600000.SSE",
                "industry": "Bank",
                "list_date": "1999-01-01",
            },
        ]

    def test_get_history_supports_dash_and_compact_dates(self, monkeypatch):
        svc = MarketService()
        monkeypatch.setattr(
            svc._market_dao,
            "get_stock_daily_history",
            lambda **_: [
                {"trade_date": "2025-01-02", "open": 1, "high": 2, "low": 0.5, "close": 1.5, "vol": 10, "amount": 20},
                {"trade_date": "20250103", "open": 2, "high": 3, "low": 1.5, "close": 2.5, "vol": 11, "amount": 21},
            ],
        )

        result = svc.get_history("000001.SZSE", date(2025, 1, 2), date(2025, 1, 3))

        assert result[0]["datetime"].strftime("%Y-%m-%d") == "2025-01-02"
        assert result[1]["datetime"].strftime("%Y-%m-%d") == "2025-01-03"
        assert result[1]["close"] == 2.5

    def test_get_history_rejects_invalid_vt_symbol(self):
        svc = MarketService()

        with pytest.raises(ValueError, match="Invalid vt_symbol format"):
            svc.get_history("bad-symbol", date(2025, 1, 1), date(2025, 1, 2))

    def test_market_overview_aggregates_counts(self, monkeypatch):
        svc = MarketService()
        monkeypatch.setattr(svc._market_dao, "exchange_counts", lambda: {"SZSE": 2, "SSE": 3})
        monkeypatch.setattr(svc._market_dao, "stock_daily_date_range", lambda: {"min_date": "2020-01-01", "max_date": "2025-01-01"})

        assert svc.market_overview() == {
            "exchanges": {"SZSE": 2, "SSE": 3},
            "total_symbols": 5,
            "data_start_date": "2020-01-01",
            "data_end_date": "2025-01-01",
        }

    def test_sectors_passthrough(self, monkeypatch):
        svc = MarketService()
        monkeypatch.setattr(svc._market_dao, "sectors", lambda: [{"sector": "Bank"}])

        assert svc.sectors() == [{"sector": "Bank"}]

    def test_exchanges_localizes_names(self, monkeypatch):
        svc = MarketService()
        monkeypatch.setattr(
            svc._market_dao,
            "exchanges",
            lambda: [{"exchange": "SZSE", "count": 1}, {"exchange": "OTHER", "count": 2}],
        )

        assert svc.exchanges() == [
            {"code": "SZSE", "name": "深圳证券交易所", "count": 1},
            {"code": "OTHER", "name": "OTHER", "count": 2},
        ]

    def test_symbols_by_filter_maps_suffixes(self, monkeypatch):
        svc = MarketService()
        monkeypatch.setattr(
            svc._market_dao,
            "symbols_by_filter",
            lambda **_: [
                {"ts_code": "000001.SZ", "name": "Ping An", "industry": "Bank"},
                {"ts_code": "430001.BJ", "name": "BJ Co", "industry": "Tech"},
            ],
        )

        result = svc.symbols_by_filter(industry="Bank", exchange="sz", limit=10)

        assert result == [
            {
                "ts_code": "000001.SZ",
                "symbol": "000001",
                "name": "Ping An",
                "exchange": "SZSE",
                "vt_symbol": "000001.SZSE",
                "industry": "Bank",
            },
            {
                "ts_code": "430001.BJ",
                "symbol": "430001",
                "name": "BJ Co",
                "exchange": "BSE",
                "vt_symbol": "430001.BSE",
                "industry": "Tech",
            },
        ]
