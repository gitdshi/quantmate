"""Momentum Signal — trading component.

Cross‑sectional momentum: go long stocks with strongest N‑day
returns, short the weakest.
"""

from typing import Any, Dict, List


def generate_signals(
    trading_day: str,
    universe: List[str],
    market_data: Dict[str, Dict[str, float]],
    positions: Dict[str, Any],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return momentum‑ranked long/short signals."""
    cfg = config or {}
    lookback = cfg.get("momentum_days", 20)
    top_pct = cfg.get("long_pct", 0.1)
    bottom_pct = cfg.get("short_pct", 0.1)

    returns: List[tuple[str, float]] = []
    for symbol in universe:
        bar = market_data.get(symbol, {})
        ret = bar.get(f"return_{lookback}d", 0)
        returns.append((symbol, ret))

    returns.sort(key=lambda x: x[1], reverse=True)
    n = len(returns)
    long_n = max(1, int(n * top_pct))
    short_n = max(1, int(n * bottom_pct))

    signals: List[Dict[str, Any]] = []
    for symbol, ret in returns[:long_n]:
        signals.append(
            {
                "symbol": symbol,
                "direction": "long",
                "strength": min(abs(ret) * 5, 1.0),
                "reason": f"{lookback}d momentum top decile ({ret:+.2%})",
            }
        )
    for symbol, ret in returns[-short_n:]:
        signals.append(
            {
                "symbol": symbol,
                "direction": "short",
                "strength": min(abs(ret) * 5, 1.0),
                "reason": f"{lookback}d momentum bottom decile ({ret:+.2%})",
            }
        )
    return signals
