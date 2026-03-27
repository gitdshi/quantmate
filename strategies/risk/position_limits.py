"""Position Limits — risk component.

Enforces per‑symbol and portfolio‑level position limits.
"""

from typing import Any, Dict, List


def filter_and_size(
    signals: List[Dict[str, Any]],
    cash: float,
    positions: Dict[str, Any],
    prices: Dict[str, float],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return orders capped by position-limit constraints."""
    cfg = config or {}
    max_single_pct = cfg.get("max_single_position_pct", 0.10)
    max_total_positions = cfg.get("max_total_positions", 20)
    alloc_pct = cfg.get("alloc_pct_per_trade", 0.05)

    current_count = len(positions)
    remaining_slots = max(0, max_total_positions - current_count)

    actionable = [s for s in signals if s.get("direction") in ("long", "short")]
    actionable = actionable[:remaining_slots]

    portfolio_value = cash + sum(
        positions.get(s, {}).get("volume", 0) * prices.get(s, 0)
        for s in positions
        if isinstance(positions.get(s), dict)
    )
    max_single = portfolio_value * max_single_pct

    orders: List[Dict[str, Any]] = []
    for sig in actionable:
        symbol = sig["symbol"]
        price = prices.get(symbol, 0)
        if price <= 0:
            continue

        alloc = min(cash * alloc_pct, max_single)

        # subtract existing exposure
        existing = positions.get(symbol, {})
        if isinstance(existing, dict):
            existing_value = existing.get("volume", 0) * price
            alloc = min(alloc, max_single - existing_value)

        if alloc <= 0:
            continue
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
