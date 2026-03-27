"""Multi‑Factor Alpha — trading component.

Combines value, momentum, and quality z‑scores into a composite
alpha signal.
"""

from typing import Any, Dict, List


def generate_signals(
    trading_day: str,
    universe: List[str],
    market_data: Dict[str, Dict[str, float]],
    positions: Dict[str, Any],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return alpha signals sorted by composite z‑score."""
    cfg = config or {}
    w_value = cfg.get("weight_value", 0.4)
    w_momentum = cfg.get("weight_momentum", 0.3)
    w_quality = cfg.get("weight_quality", 0.3)
    top_k = cfg.get("top_k", 10)
    threshold = cfg.get("alpha_threshold", 0.5)

    scored: List[tuple[str, float]] = []
    for symbol in universe:
        bar = market_data.get(symbol, {})
        z_val = bar.get("z_value", 0)
        z_mom = bar.get("z_momentum", 0)
        z_qual = bar.get("z_quality", 0)
        composite = w_value * z_val + w_momentum * z_mom + w_quality * z_qual
        scored.append((symbol, composite))

    scored.sort(key=lambda x: x[1], reverse=True)

    signals: List[Dict[str, Any]] = []
    for symbol, score in scored[:top_k]:
        if score < threshold:
            break
        signals.append(
            {
                "symbol": symbol,
                "direction": "long",
                "strength": min(score / 3.0, 1.0),
                "reason": f"Multi‑factor alpha z={score:.2f}",
            }
        )
    return signals
