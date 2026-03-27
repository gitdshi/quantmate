"""Trailing Stop — risk component.

Attaches trailing stop‑loss orders that ratchet with price movement.
"""

from typing import Any, Dict, List


def filter_and_size(
    signals: List[Dict[str, Any]],
    cash: float,
    positions: Dict[str, Any],
    prices: Dict[str, float],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return orders with trailing-stop metadata."""
    cfg = config or {}
    trail_pct = cfg.get("trail_pct", 0.03)
    max_positions = cfg.get("max_positions", 20)
    alloc_pct = cfg.get("alloc_pct_per_trade", 0.05)

    actionable = [s for s in signals if s.get("direction") in ("long", "short")]
    actionable = actionable[:max_positions]

    orders: List[Dict[str, Any]] = []
    for sig in actionable:
        symbol = sig["symbol"]
        price = prices.get(symbol, 0)
        if price <= 0:
            continue

        alloc = cash * alloc_pct
        volume = int(alloc / price / 100) * 100
        if volume <= 0:
            continue

        if sig["direction"] == "long":
            trail_stop = price * (1 - trail_pct)
        else:
            trail_stop = price * (1 + trail_pct)

        orders.append(
            {
                "symbol": symbol,
                "direction": sig["direction"],
                "volume": volume,
                "price": price,
                "trail_stop": round(trail_stop, 2),
                "trail_pct": trail_pct,
                "reason": sig.get("reason", ""),
            }
        )
    return orders
