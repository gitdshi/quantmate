"""Tests for BacktestExportService."""
import json
import pytest
from app.domains.backtests.export_service import BacktestExportService


@pytest.fixture
def svc():
    return BacktestExportService()


@pytest.fixture
def sample_result():
    return {
        "parameters": {"symbol": "000001.SZ", "period": "2020-2023"},
        "metrics": {"total_return": 0.25, "sharpe_ratio": 1.5, "max_drawdown": -0.12},
        "trades": [
            {"date": "2020-01-15", "side": "buy", "price": 10.5, "qty": 100},
            {"date": "2020-02-20", "side": "sell", "price": 11.0, "qty": 100},
        ],
    }


class TestExportCSV:
    def test_csv_contains_metrics(self, svc, sample_result):
        csv_str = svc.to_csv(sample_result)
        assert "total_return" in csv_str
        assert "0.25" in csv_str

    def test_csv_contains_trades(self, svc, sample_result):
        csv_str = svc.to_csv(sample_result)
        assert "2020-01-15" in csv_str
        assert "buy" in csv_str

    def test_csv_empty_trades(self, svc):
        result = {"metrics": {"return": 0.1}, "trades": []}
        csv_str = svc.to_csv(result)
        assert "return" in csv_str
        assert "Trades" not in csv_str


class TestExportHTML:
    def test_html_structure(self, svc, sample_result):
        html = svc.to_html(sample_result)
        assert "<!DOCTYPE html>" in html
        assert "Backtest Report" in html

    def test_html_contains_params(self, svc, sample_result):
        html = svc.to_html(sample_result)
        assert "000001.SZ" in html

    def test_html_contains_metrics(self, svc, sample_result):
        html = svc.to_html(sample_result)
        assert "total_return" in html
        assert "1.5" in html

    def test_html_contains_trades(self, svc, sample_result):
        html = svc.to_html(sample_result)
        assert "2020-01-15" in html


class TestExportJSON:
    def test_json_roundtrip(self, svc, sample_result):
        json_str = svc.to_json(sample_result)
        parsed = json.loads(json_str)
        assert parsed["metrics"]["sharpe_ratio"] == 1.5

    def test_json_formatted(self, svc, sample_result):
        json_str = svc.to_json(sample_result)
        assert "\n" in json_str  # pretty-printed
