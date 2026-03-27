"""Pair Trading Signal — trading component.

Identifies co‑integrated pairs and generates spread‑reversion signals.
"""

from typing import Any, Dict, List


def generate_signals(
    trading_day: str,
    universe: List[str],
    market_data: Dict[str, Dict[str, float]],
    positions: Dict[str, Any],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return pair spread‑reversion signals."""
    cfg = config or {}
    entry_z = cfg.get("entry_z", 2.0)
    exit_z = cfg.get("exit_z", 0.5)
    pairs = cfg.get("pairs", [])

    signals: List[Dict[str, Any]] = []
    for pair in pairs:
        leg_a = pair.get("leg_a", "")
        leg_b = pair.get("leg_b", "")
        if leg_a not in universe or leg_b not in universe:
            continue

        bar_a = market_data.get(leg_a, {})
        bar_b = market_data.get(leg_b, {})
        spread_z = bar_a.get(f"pair_z_{leg_b}", 0)

        if abs(spread_z) >= entry_z:
            # spread too wide — expect reversion
            if spread_z > 0:
                signals.append(
                    {
                        "symbol": leg_a,
                        "direction": "short",
                        "strength": min(abs(spread_z) / 4, 1.0),
                        "reason": f"Pair {leg_a}/{leg_b} spread z={spread_z:.1f} — short A",
                    }
                )
                signals.append(
                    {
                        "symbol": leg_b,
                        "direction": "long",
                        "strength": min(abs(spread_z) / 4, 1.0),
                        "reason": f"Pair {leg_a}/{leg_b} spread z={spread_z:.1f} — long B",
                    }
                )
            else:
                signals.append(
                    {
                        "symbol": leg_a,
                        "direction": "long",
                        "strength": min(abs(spread_z) / 4, 1.0),
                        "reason": f"Pair {leg_a}/{leg_b} spread z={spread_z:.1f} — long A",
                    }
                )
                signals.append(
                    {
                        "symbol": leg_b,
                        "direction": "short",
                        "strength": min(abs(spread_z) / 4, 1.0),
                        "reason": f"Pair {leg_a}/{leg_b} spread z={spread_z:.1f} — short B",
                    }
                )
        elif abs(spread_z) <= exit_z:
            held_a = leg_a in positions
            held_b = leg_b in positions
            if held_a:
                signals.append(
                    {
                        "symbol": leg_a,
                        "direction": "close",
                        "strength": 0.9,
                        "reason": f"Pair spread converged z={spread_z:.1f} — close A",
                    }
                )
            if held_b:
                signals.append(
                    {
                        "symbol": leg_b,
                        "direction": "close",
                        "strength": 0.9,
                        "reason": f"Pair spread converged z={spread_z:.1f} — close B",
                    }
                )
    return signals
