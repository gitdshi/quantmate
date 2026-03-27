"""Equal Weight — risk component.

Allocates equal capital weight to every signal that passes through.
"""

from typing import Any, Dict, List


def filter_and_size(
    signals: List[Dict[str, Any]],
    cash: float,
    positions: Dict[str, Any],
    prices: Dict[str, float],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return sized orders with equal weight allocation."""
    cfg = config or {}
    max_positions = cfg.get("max_positions", 10)

    # filter to actionable signals only
    actionable = [s for s in signals if s.get("direction") in ("long", "short")]
    actionable = actionable[:max_positions]

    if not actionable:
        return []

    weight = 1.0 / len(actionable)
    alloc = cash * weight

    orders: List[Dict[str, Any]] = []
    for sig in actionable:
        symbol = sig["symbol"]
        price = prices.get(symbol, 0)
        if price <= 0:
            continue
        volume = int(alloc / price / 100) * 100  # round to board lot
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
