"""MACD Signal — trading component.

Generates signals from MACD histogram flips and zero‑line crosses.
"""

from typing import Any, Dict, List


def generate_signals(
    trading_day: str,
    universe: List[str],
    market_data: Dict[str, Dict[str, float]],
    positions: Dict[str, Any],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return MACD‑based signals."""
    cfg = config or {}
    fast = cfg.get("fast_period", 12)
    slow = cfg.get("slow_period", 26)
    signal_period = cfg.get("signal_period", 9)
    _ = (fast, slow, signal_period)  # used to select the right pre-computed field

    signals: List[Dict[str, Any]] = []
    for symbol in universe:
        bar = market_data.get(symbol, {})
        macd_val = bar.get("macd", 0)
        signal_val = bar.get("macd_signal", 0)
        hist = bar.get("macd_hist", 0)
        prev_hist = bar.get("macd_hist_prev", 0)

        # histogram flip
        if prev_hist <= 0 < hist:
            signals.append(
                {
                    "symbol": symbol,
                    "direction": "long",
                    "strength": min(abs(hist) * 5, 1.0),
                    "reason": "MACD histogram flipped positive",
                }
            )
        elif prev_hist >= 0 > hist:
            signals.append(
                {
                    "symbol": symbol,
                    "direction": "short",
                    "strength": min(abs(hist) * 5, 1.0),
                    "reason": "MACD histogram flipped negative",
                }
            )
        # zero‑line cross
        elif macd_val > 0 and signal_val < 0:
            signals.append(
                {
                    "symbol": symbol,
                    "direction": "long",
                    "strength": 0.6,
                    "reason": "MACD crossed zero line upward",
                }
            )
    return signals
