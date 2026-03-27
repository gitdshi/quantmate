"""Market Cap Filter — universe component.

Filters the tradable universe by market capitalisation range.
"""

from typing import Any, Dict, List


def select(
    trading_day: str,
    all_symbols: List[str],
    market_data: Dict[str, Dict[str, float]],
    config: Dict[str, Any] | None = None,
) -> List[str]:
    """Return symbols whose market cap falls within [min, max]."""
    cfg = config or {}
    min_cap = cfg.get("min_market_cap", 5_000_000_000)
    max_cap = cfg.get("max_market_cap", 1_000_000_000_000)

    result: List[str] = []
    for symbol in all_symbols:
        bar = market_data.get(symbol, {})
        cap = bar.get("market_cap", 0)
        if min_cap <= cap <= max_cap:
            result.append(symbol)
    return result
