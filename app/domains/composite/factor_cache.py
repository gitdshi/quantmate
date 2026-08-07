"""Factor computation cache (TASK-010).

Avoids recomputing the same factor expression across backtest days and across
backtest runs. Uses an in-memory LRU plus an optional on-disk pickle cache.
"""

from __future__ import annotations

import hashlib
import logging
import os
import pickle
import tempfile
from collections import OrderedDict
from pathlib import Path
from typing import List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


class FactorCache:
    """LRU cache for factor computation results (in-memory + optional disk)."""

    def __init__(
        self,
        max_size: int = 500,
        cache_dir: Optional[Path] = None,
    ) -> None:
        self._cache: "OrderedDict[str, pd.Series]" = OrderedDict()
        self._max_size = max_size
        self._cache_dir = cache_dir
        if cache_dir:
            try:
                cache_dir.mkdir(parents=True, exist_ok=True)
            except OSError:
                logger.debug("[factor-cache] Could not create cache dir %s", cache_dir, exc_info=True)
                self._cache_dir = None

    # ── key ────────────────────────────────────────────────────────

    @staticmethod
    def make_key(
        expression: str,
        universe: List[str],
        start_date: str,
        end_date: str,
        adjust: str = "hfq",
    ) -> str:
        raw = f"{expression}|{','.join(sorted(universe))}|{start_date}|{end_date}|{adjust}"
        return hashlib.md5(raw.encode("utf-8")).hexdigest()

    # ── get / set ──────────────────────────────────────────────────

    def get(self, key: str) -> Optional[pd.Series]:
        if key in self._cache:
            # Mark as recently used.
            value = self._cache.pop(key)
            self._cache[key] = value
            return value

        if self._cache_dir:
            cache_file = self._cache_dir / f"{key}.pkl"
            if cache_file.exists():
                try:
                    with open(cache_file, "rb") as fh:
                        value = pickle.load(fh)
                    self._set(key, value)
                    return value
                except (OSError, pickle.UnpicklingError):
                    logger.debug("[factor-cache] Failed to load %s", cache_file, exc_info=True)

        return None

    def set(self, key: str, value: pd.Series) -> None:
        self._set(key, value)
        if self._cache_dir:
            cache_file = self._cache_dir / f"{key}.pkl"
            try:
                with open(cache_file, "wb") as fh:
                    pickle.dump(value, fh, protocol=pickle.HIGHEST_PROTOCOL)
            except OSError:
                logger.debug("[factor-cache] Failed to write %s", cache_file, exc_info=True)

    def _set(self, key: str, value: pd.Series) -> None:
        if key in self._cache:
            self._cache.pop(key)
        while len(self._cache) >= self._max_size:
            self._cache.popitem(last=False)
        self._cache[key] = value

    def clear(self) -> None:
        self._cache.clear()
        if self._cache_dir:
            for f in self._cache_dir.glob("*.pkl"):
                try:
                    f.unlink()
                except OSError:
                    pass

    def stats(self) -> dict:
        hits = getattr(self, "_hits", 0)
        misses = getattr(self, "_misses", 0)
        return {
            "size": len(self._cache),
            "max_size": self._max_size,
            "disk_enabled": self._cache_dir is not None,
            "hits": hits,
            "misses": misses,
        }


# ── Singleton ────────────────────────────────────────────────────────

_default_cache_dir: Optional[Path] = None
_candidates = [
    Path("/app/data/factor_cache"),
    Path(os.environ.get("FACTOR_CACHE_DIR", "")) if os.environ.get("FACTOR_CACHE_DIR") else None,
    Path(tempfile.gettempdir()) / "quantmate_factor_cache",
]
for c in _candidates:
    if c:
        try:
            c.mkdir(parents=True, exist_ok=True)
            _default_cache_dir = c
            break
        except OSError:
            continue

_factor_cache = FactorCache(max_size=500, cache_dir=_default_cache_dir)


def get_factor_cache() -> FactorCache:
    return _factor_cache
