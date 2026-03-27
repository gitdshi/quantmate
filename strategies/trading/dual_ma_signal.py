"""Dual MA Signal — trading component.

Generates buy/sell signals based on fast/slow moving average crossovers.
"""

from typing import Any, Dict, List


def generate_signals(
    trading_day: str,
    universe: List[str],
    market_data: Dict[str, Dict[str, float]],
    positions: Dict[str, Any],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return a list of signal dicts {symbol, direction, strength, reason}."""
    cfg = config or {}
    fast_period = cfg.get("fast_period", 5)
    slow_period = cfg.get("slow_period", 20)

    signals: List[Dict[str, Any]] = []
    for symbol in universe:
        bar = market_data.get(symbol, {})
        fast_ma = bar.get(f"ma_{fast_period}", 0)
        slow_ma = bar.get(f"ma_{slow_period}", 0)
        close = bar.get("close", 0)

        if fast_ma == 0 or slow_ma == 0 or close == 0:
            continue

        if fast_ma > slow_ma and close > fast_ma:
            signals.append(
                {
                    "symbol": symbol,
                    "direction": "long",
                    "strength": min((fast_ma - slow_ma) / slow_ma * 10, 1.0),
                    "reason": f"MA{fast_period} crossed above MA{slow_period}",
                }
            )
        elif fast_ma < slow_ma and close < fast_ma:
            signals.append(
                {
                    "symbol": symbol,
                    "direction": "short",
                    "strength": min((slow_ma - fast_ma) / slow_ma * 10, 1.0),
                    "reason": f"MA{fast_period} crossed below MA{slow_period}",
                }
            )
    return signals
