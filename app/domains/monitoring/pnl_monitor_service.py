"""Real-time P&L monitoring and anomaly detection service.

Tracks portfolio P&L in real-time, detects anomalous drawdowns,
position concentration spikes, and sudden P&L swings.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


class PnLMonitorService:
    """Live P&L tracker with anomaly rules."""

    def __init__(self) -> None:
        self._alert_rules: list[dict[str, Any]] = [
            {"name": "daily_drawdown", "threshold": -0.05, "description": "Daily drawdown exceeds 5%"},
            {"name": "position_concentration", "threshold": 0.40, "description": "Single position > 40% of portfolio"},
            {"name": "pnl_spike", "threshold": 3.0, "description": "P&L change exceeds 3 std deviations"},
        ]

    # ── Snapshot-based PnL calculation ────────────────────────────────

    def calculate_live_pnl(
        self,
        positions: list[dict[str, Any]],
        current_prices: dict[str, float],
        cash: float,
    ) -> dict[str, Any]:
        """Compute current portfolio value and per-position P&L.

        Args:
            positions: List of dicts with symbol, quantity, avg_cost
            current_prices: Map of symbol -> current market price
            cash: Available cash
        """
        total_cost = 0.0
        total_market = 0.0
        position_pnls: list[dict[str, Any]] = []

        for p in positions:
            symbol = p["symbol"]
            qty = float(p["quantity"])
            avg_cost = float(p["avg_cost"])
            price = current_prices.get(symbol, avg_cost)
            cost_value = qty * avg_cost
            market_value = qty * price
            unrealized = market_value - cost_value
            pnl_pct = (unrealized / cost_value * 100) if cost_value else 0

            total_cost += cost_value
            total_market += market_value

            position_pnls.append({
                "symbol": symbol,
                "quantity": qty,
                "avg_cost": avg_cost,
                "current_price": price,
                "cost_value": round(cost_value, 2),
                "market_value": round(market_value, 2),
                "unrealized_pnl": round(unrealized, 2),
                "pnl_pct": round(pnl_pct, 2),
            })

        total_value = cash + total_market
        total_pnl = total_market - total_cost

        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "cash": round(cash, 2),
            "total_cost": round(total_cost, 2),
            "total_market_value": round(total_market, 2),
            "total_value": round(total_value, 2),
            "total_unrealized_pnl": round(total_pnl, 2),
            "total_pnl_pct": round((total_pnl / total_cost * 100) if total_cost else 0, 2),
            "positions": position_pnls,
        }

    # ── Anomaly detection ─────────────────────────────────────────────

    def detect_anomalies(
        self,
        daily_returns: list[float],
        positions: list[dict[str, Any]],
        total_value: float,
    ) -> list[dict[str, Any]]:
        """Run anomaly rules against current portfolio state."""
        alerts: list[dict[str, Any]] = []

        # Rule 1: Daily drawdown
        if daily_returns:
            latest_return = daily_returns[-1]
            dd_rule = self._alert_rules[0]
            if latest_return <= dd_rule["threshold"]:
                alerts.append({
                    "rule": dd_rule["name"],
                    "severity": "high",
                    "message": f"Daily return {latest_return:.2%} exceeds drawdown threshold {dd_rule['threshold']:.2%}",
                    "value": latest_return,
                    "threshold": dd_rule["threshold"],
                })

        # Rule 2: Position concentration
        if total_value > 0 and positions:
            conc_rule = self._alert_rules[1]
            for p in positions:
                mv = float(p.get("market_value", 0))
                weight = mv / total_value
                if weight >= conc_rule["threshold"]:
                    alerts.append({
                        "rule": conc_rule["name"],
                        "severity": "medium",
                        "message": f"{p.get('symbol', '?')} weight {weight:.1%} exceeds {conc_rule['threshold']:.0%}",
                        "value": weight,
                        "threshold": conc_rule["threshold"],
                    })

        # Rule 3: P&L spike (z-score)
        if len(daily_returns) >= 20:
            spike_rule = self._alert_rules[2]
            import statistics
            mean = statistics.mean(daily_returns)
            std = statistics.stdev(daily_returns)
            if std > 0:
                z = (daily_returns[-1] - mean) / std
                if abs(z) >= spike_rule["threshold"]:
                    alerts.append({
                        "rule": spike_rule["name"],
                        "severity": "high",
                        "message": f"P&L z-score {z:.2f} exceeds {spike_rule['threshold']} std deviations",
                        "value": z,
                        "threshold": spike_rule["threshold"],
                    })

        return alerts

    def get_rules(self) -> list[dict[str, Any]]:
        """Return configured alert rules."""
        return self._alert_rules
