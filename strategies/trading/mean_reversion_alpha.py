"""Mean Reversion Alpha — trading component.

Identifies over‑extended price deviations from a rolling mean and
generates reversion entry signals.
"""

from typing import Any, Dict, List


def generate_signals(
    trading_day: str,
    universe: List[str],
    market_data: Dict[str, Dict[str, float]],
    positions: Dict[str, Any],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return mean‑reversion signals for over‑extended stocks."""
    cfg = config or {}
    lookback = cfg.get("lookback", 20)
    entry_z = cfg.get("entry_z_threshold", 2.0)

    signals: List[Dict[str, Any]] = []
    for symbol in universe:
        bar = market_data.get(symbol, {})
        close = bar.get("close", 0)
        mean = bar.get(f"ma_{lookback}", 0)
        std = bar.get(f"std_{lookback}", 0)

        if std == 0 or mean == 0:
            continue

        z = (close - mean) / std

        if z <= -entry_z:
            signals.append(
                {
                    "symbol": symbol,
                    "direction": "long",
                    "strength": min(abs(z) / 4.0, 1.0),
                    "reason": f"Price {z:.1f}σ below mean — reversion long",
                }
            )
        elif z >= entry_z:
            signals.append(
                {
                    "symbol": symbol,
                    "direction": "short",
                    "strength": min(abs(z) / 4.0, 1.0),
                    "reason": f"Price {z:.1f}σ above mean — reversion short",
                }
            )
    return signals
