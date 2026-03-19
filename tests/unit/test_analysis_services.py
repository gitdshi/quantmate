"""Tests for WalkForwardService and MonteCarloService."""
import pytest
from app.domains.backtests.analysis_service import WalkForwardService, MonteCarloService


class TestWalkForward:
    @pytest.fixture
    def svc(self):
        return WalkForwardService()

    def test_window_count(self, svc):
        r = svc.run(total_bars=1000, num_windows=5)
        assert r["num_windows"] == 5
        assert len(r["windows"]) == 5

    def test_window_boundaries(self, svc):
        r = svc.run(total_bars=1000, in_sample_pct=0.7, num_windows=5)
        w = r["windows"][0]
        assert w["in_sample_range"] == [0, 140]
        assert w["out_of_sample_range"] == [140, 200]

    def test_with_metrics(self, svc):
        metrics = [{"oos_return": 0.05}, {"oos_return": -0.02}, {"oos_return": 0.03}]
        r = svc.run(total_bars=600, num_windows=3, metrics_per_window=metrics)
        assert "avg_oos_return" in r
        assert r["oos_consistency"] == pytest.approx(2 / 3, abs=0.01)

    def test_no_metrics(self, svc):
        r = svc.run(total_bars=500, num_windows=5)
        assert "avg_oos_return" not in r


class TestMonteCarlo:
    @pytest.fixture
    def svc(self):
        return MonteCarloService()

    def test_basic_run(self, svc):
        returns = [0.01, -0.005, 0.02, -0.01, 0.015, 0.005, -0.003, 0.01]
        r = svc.run(trade_returns=returns, num_simulations=100)
        assert r["num_trades"] == 8
        assert "percentiles" in r
        assert r["percentiles"]["p50"] > 0

    def test_empty_returns(self, svc):
        r = svc.run(trade_returns=[], num_simulations=100)
        assert "error" in r

    def test_prob_profit(self, svc):
        # All positive returns → prob_profit should be ~1.0
        returns = [0.02, 0.03, 0.01, 0.015, 0.02]
        r = svc.run(trade_returns=returns, num_simulations=200)
        assert r["prob_profit"] >= 0.9

    def test_percentile_ordering(self, svc):
        returns = [0.01, -0.02, 0.03, -0.005, 0.01]
        r = svc.run(trade_returns=returns, num_simulations=500)
        p = r["percentiles"]
        assert p["p5"] <= p["p25"] <= p["p50"] <= p["p75"] <= p["p95"]
