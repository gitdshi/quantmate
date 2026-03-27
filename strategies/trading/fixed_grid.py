"""Fixed Grid — trading component.

Places buy/sell signals at fixed price intervals around a
configurable base price.
"""

from typing import Any, Dict, List


def generate_signals(
    trading_day: str,
    universe: List[str],
    market_data: Dict[str, Dict[str, float]],
    positions: Dict[str, Any],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return grid‑level entry/exit signals."""
    cfg = config or {}
    grid_pct = cfg.get("grid_pct", 0.02)
    max_layers = cfg.get("max_layers", 5)

    signals: List[Dict[str, Any]] = []
    for symbol in universe:
        bar = market_data.get(symbol, {})
        close = bar.get("close", 0)
        base = bar.get("grid_base_price", close)

        if close == 0 or base == 0:
            continue

        deviation = (close - base) / base
        layer = int(abs(deviation) / grid_pct)

        if layer == 0 or layer > max_layers:
            continue

        direction = "long" if deviation < 0 else "short"
        signals.append(
            {
                "symbol": symbol,
                "direction": direction,
                "strength": min(layer / max_layers, 1.0),
                "reason": f"Grid layer {layer} ({deviation:+.1%} from base)",
            }
        )
    return signals
