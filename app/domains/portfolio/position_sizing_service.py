"""Position sizing service — various position sizing methods."""
from __future__ import annotations

import math
from typing import Any, Optional


class PositionSizingService:
    """Calculate position sizes using various methods."""

    def calculate(
        self,
        method: str,
        total_capital: float,
        params: dict[str, Any],
        current_positions: Optional[list[dict]] = None,
        max_position_pct: float = 20.0,
        max_total_pct: float = 80.0,
    ) -> dict[str, Any]:
        """Calculate position size based on the chosen method."""
        current_positions = current_positions or []
        current_total = sum(p.get("market_value", 0) for p in current_positions)
        remaining_capacity = total_capital * (max_total_pct / 100) - current_total
        max_single = total_capital * (max_position_pct / 100)

        if method == "fixed_amount":
            size = self._fixed_amount(params, max_single, remaining_capacity)
        elif method == "fixed_pct":
            size = self._fixed_percent(total_capital, params, max_single, remaining_capacity)
        elif method == "kelly":
            size = self._kelly(total_capital, params, max_single, remaining_capacity)
        elif method == "equal_risk":
            size = self._equal_risk(total_capital, params, max_single, remaining_capacity)
        elif method == "risk_parity":
            size = self._risk_parity(total_capital, params, max_single, remaining_capacity)
        else:
            raise ValueError(f"Unknown sizing method: {method}")

        return {
            "method": method,
            "position_amount": round(size, 2),
            "position_pct": round(size / total_capital * 100, 2) if total_capital > 0 else 0,
            "remaining_capacity": round(remaining_capacity, 2),
        }

    def _fixed_amount(self, params: dict, max_single: float, remaining: float) -> float:
        amount = params.get("amount", 10000)
        return min(amount, max_single, max(remaining, 0))

    def _fixed_percent(self, capital: float, params: dict, max_single: float, remaining: float) -> float:
        pct = params.get("percent", 5.0) / 100
        amount = capital * pct
        return min(amount, max_single, max(remaining, 0))

    def _kelly(self, capital: float, params: dict, max_single: float, remaining: float) -> float:
        win_rate = params.get("win_rate", 0.55)
        win_loss_ratio = params.get("win_loss_ratio", 1.5)
        if win_loss_ratio <= 0:
            return 0
        kelly_pct = win_rate - (1 - win_rate) / win_loss_ratio
        # Half-Kelly for safety
        kelly_pct = max(0, kelly_pct * 0.5)
        amount = capital * kelly_pct
        return min(amount, max_single, max(remaining, 0))

    def _equal_risk(self, capital: float, params: dict, max_single: float, remaining: float) -> float:
        risk_per_trade = params.get("risk_per_trade_pct", 1.0) / 100
        stop_loss_pct = params.get("stop_loss_pct", 5.0) / 100
        if stop_loss_pct <= 0:
            return 0
        amount = (capital * risk_per_trade) / stop_loss_pct
        return min(amount, max_single, max(remaining, 0))

    def _risk_parity(self, capital: float, params: dict, max_single: float, remaining: float) -> float:
        target_vol = params.get("target_portfolio_vol", 0.15)
        asset_vol = params.get("asset_vol", 0.25)
        num_assets = params.get("num_assets", 10)
        if asset_vol <= 0 or num_assets <= 0:
            return 0
        # Simple risk parity: allocate proportional to inverse volatility
        weight = (target_vol / (asset_vol * math.sqrt(num_assets)))
        amount = capital * min(weight, 1.0)
        return min(amount, max_single, max(remaining, 0))
