"""Drawdown Control — risk component.

Reduces or blocks new entries when portfolio drawdown exceeds
configurable thresholds.
"""

from typing import Any, Dict, List


def filter_and_size(
    signals: List[Dict[str, Any]],
    cash: float,
    positions: Dict[str, Any],
    prices: Dict[str, float],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return orders throttled by current drawdown level."""
    cfg = config or {}
    max_dd = cfg.get("max_drawdown", 0.15)
    reduce_dd = cfg.get("reduce_at_drawdown", 0.10)
    scale_factor = cfg.get("reduce_scale", 0.5)
    alloc_pct = cfg.get("alloc_pct_per_trade", 0.05)

    # compute current drawdown
    peak = cfg.get("portfolio_peak", cash)
    current_value = cash + sum(
        positions.get(s, {}).get("volume", 0) * prices.get(s, 0)
        for s in positions
        if isinstance(positions.get(s), dict)
    )
    dd = (peak - current_value) / peak if peak > 0 else 0

    if dd >= max_dd:
        # drawdown too deep — reject all new entries
        return []

    scale = scale_factor if dd >= reduce_dd else 1.0

    actionable = [s for s in signals if s.get("direction") in ("long", "short")]

    orders: List[Dict[str, Any]] = []
    for sig in actionable:
        symbol = sig["symbol"]
        price = prices.get(symbol, 0)
        if price <= 0:
            continue
        alloc = cash * alloc_pct * scale
        volume = int(alloc / price / 100) * 100
        if volume <= 0:
            continue
        orders.append(
            {
                "symbol": symbol,
                "direction": sig["direction"],
                "volume": volume,
                "price": price,
                "reason": sig.get("reason", ""),
            }
        )
    return orders
