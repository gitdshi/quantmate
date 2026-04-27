"""Tests for PerformanceAttributionService."""
import pytest
from app.domains.portfolio.attribution_service import PerformanceAttributionService


@pytest.fixture
def svc():
    return PerformanceAttributionService()


class TestBrinsonAttribution:
    def test_basic_attribution(self, svc):
        r = svc.brinson_attribution(
            portfolio_weights={"tech": 0.4, "finance": 0.3, "health": 0.3},
            benchmark_weights={"tech": 0.3, "finance": 0.4, "health": 0.3},
            portfolio_returns={"tech": 0.15, "finance": 0.08, "health": 0.05},
            benchmark_returns={"tech": 0.10, "finance": 0.06, "health": 0.04},
        )
        assert "allocation_effect" in r
        assert "selection_effect" in r
        assert "interaction_effect" in r
        assert "total_active_return" in r
        assert len(r["details"]) == 3

    def test_total_equals_sum(self, svc):
        r = svc.brinson_attribution(
            portfolio_weights={"a": 0.5, "b": 0.5},
            benchmark_weights={"a": 0.6, "b": 0.4},
            portfolio_returns={"a": 0.10, "b": 0.05},
            benchmark_returns={"a": 0.08, "b": 0.04},
        )
        expected = r["allocation_effect"] + r["selection_effect"] + r["interaction_effect"]
        assert r["total_active_return"] == pytest.approx(expected, abs=1e-6)

    def test_identical_weights_no_allocation(self, svc):
        r = svc.brinson_attribution(
            portfolio_weights={"a": 0.5, "b": 0.5},
            benchmark_weights={"a": 0.5, "b": 0.5},
            portfolio_returns={"a": 0.10, "b": 0.05},
            benchmark_returns={"a": 0.08, "b": 0.04},
        )
        assert r["allocation_effect"] == pytest.approx(0.0, abs=1e-6)
        assert r["interaction_effect"] == pytest.approx(0.0, abs=1e-6)

    def test_missing_sector_in_portfolio(self, svc):
        r = svc.brinson_attribution(
            portfolio_weights={"a": 1.0},
            benchmark_weights={"a": 0.5, "b": 0.5},
            portfolio_returns={"a": 0.10},
            benchmark_returns={"a": 0.08, "b": 0.04},
        )
        assert len(r["details"]) == 2  # both a and b

    def test_detail_fields(self, svc):
        r = svc.brinson_attribution(
            portfolio_weights={"tech": 0.6, "fin": 0.4},
            benchmark_weights={"tech": 0.5, "fin": 0.5},
            portfolio_returns={"tech": 0.12, "fin": 0.06},
            benchmark_returns={"tech": 0.10, "fin": 0.05},
        )
        detail = r["details"][0]
        for field in ["sector", "portfolio_weight", "benchmark_weight",
                       "portfolio_return", "benchmark_return",
                       "allocation_effect", "selection_effect", "interaction_effect"]:
            assert field in detail
