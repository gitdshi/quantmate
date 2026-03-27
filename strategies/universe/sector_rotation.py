"""Sector Rotation — universe component.

Selects stocks from the top‑performing industry sectors based on
rolling relative‑strength momentum.
"""

from typing import Any, Dict, List
from collections import defaultdict


def select(
    trading_day: str,
    all_symbols: List[str],
    market_data: Dict[str, Dict[str, float]],
    config: Dict[str, Any] | None = None,
) -> List[str]:
    """Return symbols belonging to the top‑N momentum sectors."""
    cfg = config or {}
    top_n = cfg.get("top_sectors", 3)
    momentum_key = cfg.get("momentum_key", "sector_momentum_20d")

    # group symbols by sector
    sectors: Dict[str, List[str]] = defaultdict(list)
    sector_scores: Dict[str, float] = {}
    for symbol in all_symbols:
        bar = market_data.get(symbol, {})
        sector = bar.get("sector", "Unknown")
        sectors[sector].append(symbol)
        # take the max momentum as sector score
        score = bar.get(momentum_key, 0.0)
        sector_scores[sector] = max(sector_scores.get(sector, float("-inf")), score)

    # pick top sectors
    ranked = sorted(sector_scores, key=sector_scores.get, reverse=True)  # type: ignore[arg-type]
    top_sectors = set(ranked[:top_n])

    result: List[str] = []
    for sector in top_sectors:
        result.extend(sectors[sector])
    return result
