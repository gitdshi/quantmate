"""Tests for PnLMonitorService."""
import pytest
from app.domains.monitoring.pnl_monitor_service import PnLMonitorService


class TestPnLMonitorService:
    def setup_method(self):
        self.svc = PnLMonitorService()

    # ── calculate_live_pnl ────────────────────────────────────────

    def test_live_pnl_single_position(self):
        positions = [{"symbol": "600000.SH", "quantity": 100, "avg_cost": 10.0}]
        prices = {"600000.SH": 12.0}
        result = self.svc.calculate_live_pnl(positions, prices, cash=5000.0)

        assert result["total_cost"] == 1000.0
        assert result["total_market_value"] == 1200.0
        assert result["total_unrealized_pnl"] == 200.0
        assert result["total_value"] == 6200.0
        assert result["cash"] == 5000.0

    def test_live_pnl_missing_price_falls_back_to_avg_cost(self):
        positions = [{"symbol": "600000.SH", "quantity": 100, "avg_cost": 10.0}]
        result = self.svc.calculate_live_pnl(positions, {}, cash=0)
        assert result["total_unrealized_pnl"] == 0.0

    def test_live_pnl_empty_positions(self):
        result = self.svc.calculate_live_pnl([], {}, cash=10000.0)
        assert result["total_value"] == 10000.0
        assert result["positions"] == []

    def test_live_pnl_position_details(self):
        positions = [{"symbol": "AAPL", "quantity": 50, "avg_cost": 100.0}]
        prices = {"AAPL": 120.0}
        result = self.svc.calculate_live_pnl(positions, prices, cash=0)
        p = result["positions"][0]
        assert p["unrealized_pnl"] == 1000.0
        assert p["pnl_pct"] == 20.0

    # ── detect_anomalies ──────────────────────────────────────────

    def test_detect_drawdown_alert(self):
        alerts = self.svc.detect_anomalies(
            daily_returns=[0.01, 0.02, -0.06],
            positions=[],
            total_value=100000,
        )
        assert len(alerts) == 1
        assert alerts[0]["rule"] == "daily_drawdown"
        assert alerts[0]["severity"] == "high"

    def test_no_drawdown_alert_when_above_threshold(self):
        alerts = self.svc.detect_anomalies(
            daily_returns=[0.01, 0.02, -0.03],
            positions=[],
            total_value=100000,
        )
        dd_alerts = [a for a in alerts if a["rule"] == "daily_drawdown"]
        assert len(dd_alerts) == 0

    def test_detect_concentration_alert(self):
        positions = [{"symbol": "X", "market_value": 50000}]
        alerts = self.svc.detect_anomalies([], positions, total_value=100000)
        conc = [a for a in alerts if a["rule"] == "position_concentration"]
        assert len(conc) == 1

    def test_no_concentration_alert_when_below(self):
        positions = [{"symbol": "X", "market_value": 30000}]
        alerts = self.svc.detect_anomalies([], positions, total_value=100000)
        conc = [a for a in alerts if a["rule"] == "position_concentration"]
        assert len(conc) == 0

    def test_detect_pnl_spike(self):
        # 20 stable returns then one extreme
        returns = [0.01] * 20 + [0.20]
        alerts = self.svc.detect_anomalies(returns, [], total_value=100000)
        spike = [a for a in alerts if a["rule"] == "pnl_spike"]
        assert len(spike) == 1
        assert spike[0]["severity"] == "high"

    def test_no_spike_with_short_history(self):
        alerts = self.svc.detect_anomalies([0.01, 0.20], [], total_value=100000)
        spike = [a for a in alerts if a["rule"] == "pnl_spike"]
        assert len(spike) == 0

    # ── get_rules ─────────────────────────────────────────────────

    def test_get_rules_returns_3(self):
        rules = self.svc.get_rules()
        assert len(rules) == 3
        names = {r["name"] for r in rules}
        assert names == {"daily_drawdown", "position_concentration", "pnl_spike"}
