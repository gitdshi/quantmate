"""Tests for RiskAnalysisService."""
import pytest
from app.domains.portfolio.risk_analysis_service import RiskAnalysisService, _norm_ppf


class TestRiskAnalysisService:
    def setup_method(self):
        self.svc = RiskAnalysisService()
        # 100 synthetic daily returns ~N(0.001, 0.02)
        self.returns = [0.001 + 0.02 * (i % 7 - 3) / 3 for i in range(100)]

    # ── parametric_var ────────────────────────────────────────────

    def test_parametric_var_basic(self):
        result = self.svc.parametric_var(self.returns, confidence=0.95)
        assert result["method"] == "parametric"
        assert result["confidence"] == 0.95
        assert result["var_dollar"] > 0

    def test_parametric_var_too_few_points(self):
        result = self.svc.parametric_var([0.01])
        assert "error" in result

    def test_parametric_var_holding_period_scales(self):
        r1 = self.svc.parametric_var(self.returns, holding_period=1)
        r5 = self.svc.parametric_var(self.returns, holding_period=5)
        # sqrt(5)-day VaR > 1-day VaR
        assert r5["var_dollar"] > r1["var_dollar"]

    # ── historical_var ────────────────────────────────────────────

    def test_historical_var_basic(self):
        result = self.svc.historical_var(self.returns, confidence=0.95)
        assert result["method"] == "historical"
        assert result["observations"] == 100
        assert "cvar_dollar" in result

    def test_historical_var_too_few(self):
        result = self.svc.historical_var([0.01] * 5)
        assert "error" in result

    def test_cvar_gte_var(self):
        result = self.svc.historical_var(self.returns, confidence=0.95)
        # CVaR (expected shortfall) ≥ VaR
        assert result["cvar_dollar"] >= result["var_dollar"]

    # ── stress_test ───────────────────────────────────────────────

    def test_stress_test_default_scenarios(self):
        weights = {"AAPL": 0.4, "GOOG": 0.3, "MSFT": 0.3}
        results = self.svc.stress_test(1_000_000, weights)
        assert len(results) == 4
        for r in results:
            assert "total_impact" in r
            assert r["total_impact"] < 0  # All default scenarios are negative

    def test_stress_test_custom_scenario(self):
        weights = {"banks": 0.5, "tech": 0.5}
        scenario = [{"name": "Custom", "shocks": {"banks": -0.30, "tech": -0.10}}]
        results = self.svc.stress_test(1_000_000, weights, scenarios=scenario)
        assert len(results) == 1
        assert results[0]["total_impact"] == -200_000  # -300k banks + -100k tech... wait
        # banks: 1M * 0.5 * -0.30 = -150k, tech: 1M * 0.5 * -0.10 = -50k => -200k
        assert results[0]["total_impact"] == -200_000

    def test_stress_test_empty_portfolio(self):
        results = self.svc.stress_test(1_000_000, {})
        assert all(r["total_impact"] == 0 for r in results)

    # ── _norm_ppf ─────────────────────────────────────────────────

    def test_norm_ppf_95(self):
        z = _norm_ppf(0.95)
        assert abs(z - 1.645) < 0.01

    def test_norm_ppf_99(self):
        z = _norm_ppf(0.99)
        assert abs(z - 2.326) < 0.02

    def test_norm_ppf_50(self):
        z = _norm_ppf(0.50)
        assert abs(z) < 0.05  # Should be ~0
