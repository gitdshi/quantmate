"""Donchian Breakout — trading component.

Generates signals when price breaks above/below the Donchian channel.
"""

from typing import Any, Dict, List


def generate_signals(
    trading_day: str,
    universe: List[str],
    market_data: Dict[str, Dict[str, float]],
    positions: Dict[str, Any],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return breakout signals based on Donchian channels."""
    cfg = config or {}
    entry_period = cfg.get("entry_period", 20)
    exit_period = cfg.get("exit_period", 10)

    signals: List[Dict[str, Any]] = []
    for symbol in universe:
        bar = market_data.get(symbol, {})
        high = bar.get("close", 0)
        upper = bar.get(f"donchian_upper_{entry_period}", 0)
        lower = bar.get(f"donchian_lower_{entry_period}", 0)
        exit_lower = bar.get(f"donchian_lower_{exit_period}", 0)

        held = symbol in positions

        if high >= upper and upper > 0 and not held:
            signals.append(
                {
                    "symbol": symbol,
                    "direction": "long",
                    "strength": 0.8,
                    "reason": f"Breakout above {entry_period}‑day high",
                }
            )
        elif high <= exit_lower and exit_lower > 0 and held:
            signals.append(
                {
                    "symbol": symbol,
                    "direction": "close",
                    "strength": 0.9,
                    "reason": f"Broke below {exit_period}‑day low — exit",
                }
            )
    return signals
