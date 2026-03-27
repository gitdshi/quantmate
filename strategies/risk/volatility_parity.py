"""Volatility Parity — risk component.

Sizes positions inversely proportional to each asset's recent
volatility so that each contributes equal risk.
"""

from typing import Any, Dict, List


def filter_and_size(
    signals: List[Dict[str, Any]],
    cash: float,
    positions: Dict[str, Any],
    prices: Dict[str, float],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return orders sized by inverse volatility."""
    cfg = config or {}
    max_positions = cfg.get("max_positions", 10)
    vol_key = cfg.get("vol_key", "volatility_20d")
    target_vol = cfg.get("target_portfolio_vol", 0.15)

    actionable = [s for s in signals if s.get("direction") in ("long", "short")]
    actionable = actionable[:max_positions]
    if not actionable:
        return []

    # compute inverse-vol weights
    inv_vols: List[float] = []
    for sig in actionable:
        vol = sig.get(vol_key, 0.3)
        inv_vols.append(1.0 / max(vol, 0.01))
    total_inv = sum(inv_vols) or 1.0

    orders: List[Dict[str, Any]] = []
    for sig, inv_v in zip(actionable, inv_vols):
        weight = inv_v / total_inv
        alloc = cash * weight
        symbol = sig["symbol"]
        price = prices.get(symbol, 0)
        if price <= 0:
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
