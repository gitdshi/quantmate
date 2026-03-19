"""Tests for PositionSizingService."""
import pytest
from app.domains.portfolio.position_sizing_service import PositionSizingService


@pytest.fixture
def svc():
    return PositionSizingService()


CAPITAL = 1_000_000


class TestFixedAmount:
    def test_basic(self, svc):
        r = svc.calculate("fixed_amount", CAPITAL, {"amount": 50000})
        assert r["position_amount"] == 50000
        assert r["method"] == "fixed_amount"

    def test_capped_by_max_position(self, svc):
        # max_position_pct=20% of 1M = 200k, amount=300k → capped at 200k
        r = svc.calculate("fixed_amount", CAPITAL, {"amount": 300000})
        assert r["position_amount"] == 200000


class TestFixedPercent:
    def test_basic(self, svc):
        r = svc.calculate("fixed_pct", CAPITAL, {"percent": 5.0})
        assert r["position_amount"] == 50000

    def test_respects_remaining_capacity(self, svc):
        positions = [{"market_value": 790000}]  # 79% already used, 80% cap
        r = svc.calculate("fixed_pct", CAPITAL, {"percent": 5.0}, current_positions=positions)
        assert r["position_amount"] == 10000  # only 10k remaining


class TestKelly:
    def test_positive_edge(self, svc):
        r = svc.calculate("kelly", CAPITAL, {"win_rate": 0.6, "win_loss_ratio": 2.0})
        # kelly = 0.6 - 0.4/2 = 0.4, half-kelly = 0.2 → 200k
        assert r["position_amount"] == 200000

    def test_negative_edge(self, svc):
        r = svc.calculate("kelly", CAPITAL, {"win_rate": 0.3, "win_loss_ratio": 1.0})
        # kelly = 0.3 - 0.7/1 = -0.4 → 0
        assert r["position_amount"] == 0


class TestEqualRisk:
    def test_basic(self, svc):
        r = svc.calculate("equal_risk", CAPITAL, {"risk_per_trade_pct": 1.0, "stop_loss_pct": 5.0})
        # risk = 10000, stop = 5% → 10000/0.05 = 200k
        assert r["position_amount"] == 200000

    def test_zero_stop_loss(self, svc):
        r = svc.calculate("equal_risk", CAPITAL, {"risk_per_trade_pct": 1.0, "stop_loss_pct": 0.0})
        assert r["position_amount"] == 0


class TestRiskParity:
    def test_basic(self, svc):
        r = svc.calculate("risk_parity", CAPITAL, {
            "target_portfolio_vol": 0.15, "asset_vol": 0.30, "num_assets": 10,
        })
        assert r["position_amount"] > 0

    def test_zero_vol(self, svc):
        r = svc.calculate("risk_parity", CAPITAL, {
            "target_portfolio_vol": 0.15, "asset_vol": 0, "num_assets": 10,
        })
        assert r["position_amount"] == 0


class TestUnknownMethod:
    def test_raises(self, svc):
        with pytest.raises(ValueError, match="Unknown sizing method"):
            svc.calculate("unknown", CAPITAL, {})
