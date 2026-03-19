"""Tests for AlgoExecutionService."""
import pytest
from datetime import datetime
from app.domains.trading.algo_execution_service import AlgoExecutionService


class TestAlgoExecutionService:
    def setup_method(self):
        self.svc = AlgoExecutionService()
        self.start = datetime(2024, 6, 1, 9, 30)
        self.end = datetime(2024, 6, 1, 15, 0)

    # ── TWAP ──────────────────────────────────────────────────────

    def test_twap_equal_split(self):
        slices = self.svc.twap(1000, 5, self.start, self.end)
        assert len(slices) == 5
        assert all(s["quantity"] == 200 for s in slices)
        assert sum(s["quantity"] for s in slices) == 1000

    def test_twap_remainder_distributed(self):
        slices = self.svc.twap(103, 5, self.start, self.end)
        assert sum(s["quantity"] for s in slices) == 103
        # First 3 slices get +1
        assert slices[0]["quantity"] == 21
        assert slices[4]["quantity"] == 20

    def test_twap_single_slice(self):
        slices = self.svc.twap(500, 1, self.start, self.end)
        assert len(slices) == 1
        assert slices[0]["quantity"] == 500

    def test_twap_invalid_slices_raises(self):
        with pytest.raises(ValueError, match="num_slices"):
            self.svc.twap(100, 0, self.start, self.end)

    def test_twap_invalid_quantity_raises(self):
        with pytest.raises(ValueError, match="total_quantity"):
            self.svc.twap(0, 5, self.start, self.end)

    def test_twap_price_limit(self):
        slices = self.svc.twap(100, 2, self.start, self.end, price_limit=50.0)
        assert all(s["price_limit"] == 50.0 for s in slices)

    # ── VWAP ──────────────────────────────────────────────────────

    def test_vwap_proportional(self):
        profile = [0.1, 0.3, 0.6]
        slices = self.svc.vwap(1000, profile, self.start)
        assert len(slices) == 3
        assert sum(s["quantity"] for s in slices) == 1000
        # First slice roughly 10%
        assert slices[0]["quantity"] == 100

    def test_vwap_rounding_absorbed_by_last(self):
        profile = [1, 1, 1]
        slices = self.svc.vwap(100, profile, self.start)
        total = sum(s["quantity"] for s in slices)
        assert total == 100

    def test_vwap_empty_profile_raises(self):
        with pytest.raises(ValueError, match="empty"):
            self.svc.vwap(100, [], self.start)

    def test_vwap_zero_weights_raises(self):
        with pytest.raises(ValueError, match="positive"):
            self.svc.vwap(100, [0, 0, 0], self.start)

    def test_vwap_scheduled_times(self):
        slices = self.svc.vwap(100, [1, 1], self.start, interval_minutes=60)
        t0 = datetime.fromisoformat(slices[0]["scheduled_time"])
        t1 = datetime.fromisoformat(slices[1]["scheduled_time"])
        assert (t1 - t0).seconds == 3600

    # ── Iceberg ───────────────────────────────────────────────────

    def test_iceberg_basic(self):
        slices = self.svc.iceberg(1000, 200, 50.0)
        assert len(slices) == 5
        assert all(s["quantity"] == 200 for s in slices)
        assert slices[-1]["hidden_remaining"] == 0

    def test_iceberg_uneven(self):
        slices = self.svc.iceberg(550, 200, 50.0)
        assert len(slices) == 3
        assert slices[0]["quantity"] == 200
        assert slices[2]["quantity"] == 150
        assert sum(s["quantity"] for s in slices) == 550

    def test_iceberg_single_chunk(self):
        slices = self.svc.iceberg(100, 200, 50.0)
        assert len(slices) == 1
        assert slices[0]["quantity"] == 100

    def test_iceberg_invalid_display_raises(self):
        with pytest.raises(ValueError, match="display_quantity"):
            self.svc.iceberg(100, 0, 50.0)

    def test_iceberg_price_limit(self):
        slices = self.svc.iceberg(100, 50, 25.5)
        assert all(s["price_limit"] == 25.5 for s in slices)
