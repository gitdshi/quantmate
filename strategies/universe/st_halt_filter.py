"""ST / Halt Filter — universe component.

Excludes ST‑flagged, suspended, and newly‑listed stocks.
Essential for A‑share compliance.
"""

from typing import Any, Dict, List


def select(
    trading_day: str,
    all_symbols: List[str],
    market_data: Dict[str, Dict[str, float]],
    config: Dict[str, Any] | None = None,
) -> List[str]:
    """Return symbols that are NOT ST, suspended, or too new."""
    cfg = config or {}
    min_list_days = cfg.get("min_list_days", 60)

    result: List[str] = []
    for symbol in all_symbols:
        bar = market_data.get(symbol, {})
        if bar.get("is_st", False):
            continue
        if bar.get("is_suspended", False):
            continue
        if bar.get("list_days", 0) < min_list_days:
            continue
        result.append(symbol)
    return result
