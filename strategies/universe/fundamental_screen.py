"""Fundamental Screen — universe component.

Screens stocks by PE, PB, ROE, revenue growth and other
fundamental metrics.
"""

from typing import Any, Dict, List


def select(
    trading_day: str,
    all_symbols: List[str],
    market_data: Dict[str, Dict[str, float]],
    config: Dict[str, Any] | None = None,
) -> List[str]:
    """Return symbols passing all fundamental filters."""
    cfg = config or {}
    max_pe = cfg.get("max_pe", 40.0)
    max_pb = cfg.get("max_pb", 8.0)
    min_roe = cfg.get("min_roe", 0.08)
    min_revenue_growth = cfg.get("min_revenue_growth", 0.0)

    result: List[str] = []
    for symbol in all_symbols:
        bar = market_data.get(symbol, {})
        pe = bar.get("pe_ratio", float("inf"))
        pb = bar.get("pb_ratio", float("inf"))
        roe = bar.get("roe", 0.0)
        rev_g = bar.get("revenue_growth_yoy", 0.0)
        if pe <= max_pe and pb <= max_pb and roe >= min_roe and rev_g >= min_revenue_growth:
            result.append(symbol)
    return result
