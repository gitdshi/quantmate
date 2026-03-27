"""Index Constituents — universe component.

Selects universe from major index constituent lists
(e.g. CSI 300, CSI 500, S&P 500).
"""

from typing import Any, Dict, List


def select(
    trading_day: str,
    all_symbols: List[str],
    market_data: Dict[str, Dict[str, float]],
    config: Dict[str, Any] | None = None,
) -> List[str]:
    """Return symbols that belong to the configured index."""
    cfg = config or {}
    index_name = cfg.get("index", "csi300")
    index_key = f"is_{index_name}"

    result: List[str] = []
    for symbol in all_symbols:
        bar = market_data.get(symbol, {})
        if bar.get(index_key, False):
            result.append(symbol)
    return result
