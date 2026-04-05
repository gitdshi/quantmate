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
