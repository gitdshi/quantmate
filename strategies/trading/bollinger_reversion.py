"""Bollinger Reversion — trading component.

Mean‑reversion signals triggered when price touches or exceeds
Bollinger Bands.
"""

from typing import Any, Dict, List


def generate_signals(
    trading_day: str,
    universe: List[str],
    market_data: Dict[str, Dict[str, float]],
    positions: Dict[str, Any],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return reversion signals at Bollinger extremes."""
    cfg = config or {}
    bb_period = cfg.get("bb_period", 20)
    bb_std = cfg.get("bb_std", 2.0)
    _ = bb_period  # field pre‑computed

    signals: List[Dict[str, Any]] = []
    for symbol in universe:
        bar = market_data.get(symbol, {})
        close = bar.get("close", 0)
        upper = bar.get("bb_upper", 0)
        lower = bar.get("bb_lower", 0)
        mid = bar.get("bb_mid", 0)

        if close == 0 or mid == 0:
            continue

        pct_b = (close - lower) / (upper - lower) if upper != lower else 0.5

        if close <= lower:
            signals.append(
                {
                    "symbol": symbol,
                    "direction": "long",
                    "strength": min(1.0, (1 - pct_b)),
                    "reason": f"Price touched lower BB ({bb_std}σ)",
                }
            )
        elif close >= upper:
            signals.append(
                {
                    "symbol": symbol,
                    "direction": "short",
                    "strength": min(1.0, pct_b),
                    "reason": f"Price touched upper BB ({bb_std}σ)",
                }
            )
    return signals
