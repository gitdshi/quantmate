"""Liquidity Filter — universe component.

Filters based on average daily volume and turnover rate.
"""

from typing import Any, Dict, List


def select(
    trading_day: str,
    all_symbols: List[str],
    market_data: Dict[str, Dict[str, float]],
    config: Dict[str, Any] | None = None,
) -> List[str]:
    """Return symbols meeting minimum liquidity thresholds."""
    cfg = config or {}
    min_volume = cfg.get("min_avg_volume", 1_000_000)
    min_turnover = cfg.get("min_turnover_rate", 0.005)

    result: List[str] = []
    for symbol in all_symbols:
        bar = market_data.get(symbol, {})
        vol = bar.get("avg_volume_20d", 0)
        turnover = bar.get("turnover_rate", 0)
        if vol >= min_volume and turnover >= min_turnover:
            result.append(symbol)
    return result
