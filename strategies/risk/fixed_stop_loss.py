"""Fixed Stop Loss — risk component.

Rejects signals that have already moved beyond the stop threshold
and attaches stop‑loss prices to surviving orders.
"""

from typing import Any, Dict, List


def filter_and_size(
    signals: List[Dict[str, Any]],
    cash: float,
    positions: Dict[str, Any],
    prices: Dict[str, float],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return orders with fixed stop‑loss prices attached."""
    cfg = config or {}
    stop_pct = cfg.get("stop_pct", 0.05)
    max_positions = cfg.get("max_positions", 20)
    risk_per_trade = cfg.get("risk_per_trade_pct", 0.02)

    actionable = [s for s in signals if s.get("direction") in ("long", "short")]
    actionable = actionable[:max_positions]

    orders: List[Dict[str, Any]] = []
    for sig in actionable:
        symbol = sig["symbol"]
        price = prices.get(symbol, 0)
        if price <= 0:
            continue

        if sig["direction"] == "long":
            stop = price * (1 - stop_pct)
        else:
            stop = price * (1 + stop_pct)

        risk_per_share = abs(price - stop)
        if risk_per_share == 0:
            continue
        max_loss = cash * risk_per_trade
        volume = int(max_loss / risk_per_share / 100) * 100
        if volume <= 0:
            continue

        orders.append(
            {
                "symbol": symbol,
                "direction": sig["direction"],
                "volume": volume,
                "price": price,
                "stop_price": round(stop, 2),
                "reason": sig.get("reason", ""),
            }
        )
    return orders
