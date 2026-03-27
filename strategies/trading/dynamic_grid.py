"""Dynamic Grid — trading component.

Like fixed grid but adapts spacing based on ATR (Average True Range).
"""

from typing import Any, Dict, List


def generate_signals(
    trading_day: str,
    universe: List[str],
    market_data: Dict[str, Dict[str, float]],
    positions: Dict[str, Any],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return ATR‑adaptive grid signals."""
    cfg = config or {}
    atr_multiplier = cfg.get("atr_multiplier", 1.0)
    max_layers = cfg.get("max_layers", 5)
    atr_period = cfg.get("atr_period", 14)

    signals: List[Dict[str, Any]] = []
    for symbol in universe:
        bar = market_data.get(symbol, {})
        close = bar.get("close", 0)
        base = bar.get("grid_base_price", close)
        atr = bar.get(f"atr_{atr_period}", 0)

        if close == 0 or base == 0 or atr == 0:
            continue

        grid_size = atr * atr_multiplier
        deviation = close - base
        layer = int(abs(deviation) / grid_size)

        if layer == 0 or layer > max_layers:
            continue

        direction = "long" if deviation < 0 else "short"
        signals.append(
            {
                "symbol": symbol,
                "direction": direction,
                "strength": min(layer / max_layers, 1.0),
                "reason": f"Dynamic grid L{layer} (ATR={atr:.2f}, Δ={deviation:+.2f})",
            }
        )
    return signals
