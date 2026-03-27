"""ETF Arbitrage — trading component.

Exploits premium/discount between an ETF and its underlying basket.
"""

from typing import Any, Dict, List


def generate_signals(
    trading_day: str,
    universe: List[str],
    market_data: Dict[str, Dict[str, float]],
    positions: Dict[str, Any],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return ETF–basket arbitrage signals."""
    cfg = config or {}
    premium_threshold = cfg.get("premium_threshold", 0.005)
    discount_threshold = cfg.get("discount_threshold", -0.005)

    signals: List[Dict[str, Any]] = []
    for symbol in universe:
        bar = market_data.get(symbol, {})
        premium = bar.get("etf_premium", 0)

        if premium == 0:
            continue

        if premium >= premium_threshold:
            # ETF over‑priced vs basket — short ETF, long basket
            signals.append(
                {
                    "symbol": symbol,
                    "direction": "short",
                    "strength": min(abs(premium) / 0.02, 1.0),
                    "reason": f"ETF premium {premium:+.2%} — arbitrage short",
                }
            )
        elif premium <= discount_threshold:
            # ETF under‑priced — long ETF, short basket
            signals.append(
                {
                    "symbol": symbol,
                    "direction": "long",
                    "strength": min(abs(premium) / 0.02, 1.0),
                    "reason": f"ETF discount {premium:+.2%} — arbitrage long",
                }
            )
    return signals
