"""Factor screening — batch evaluate + deduplicate factors by correlation.

Two main workflows:
  1. screen_factor_pool(): Evaluate IC/ICIR for a pool of factor expressions,
     rank them, and filter redundant ones by pairwise correlation.
  2. mine_alpha158_factors(): Screen all Alpha158 features against forward returns,
     persist top results to factor_screening_results.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any, Optional

import numpy as np
import pandas as pd
from scipy import stats

from app.domains.factors.expression_engine import (
    compute_custom_factor,
    compute_factor_metrics,
    compute_forward_returns,
    compute_qlib_factor_set,
    fetch_ohlcv,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Batch screening
# ---------------------------------------------------------------------------


def screen_factor_pool(
    expressions: list[str],
    start_date: date,
    end_date: date,
    instruments: Optional[list[str]] = None,
    ic_threshold: float = 0.02,
    corr_threshold: float = 0.7,
    forward_periods: int = 1,
) -> list[dict[str, Any]]:
    """Evaluate multiple custom expressions and return ranked, deduplicated results.

    Returns list of dicts sorted by |IC_mean| descending, after removing
    highly-correlated factors (keeping the one with higher |IC|).
    """
    ohlcv = fetch_ohlcv(start_date=start_date, end_date=end_date, instruments=instruments)
    if ohlcv.empty:
        logger.warning("[screening] No OHLCV data for %s–%s", start_date, end_date)
        return []

    fwd_returns = compute_forward_returns(ohlcv, periods=forward_periods)

    # Evaluate each expression
    results: list[dict[str, Any]] = []
    factor_series: dict[str, pd.Series] = {}

    for expr in expressions:
        try:
            fv = compute_custom_factor(expr, ohlcv)
            metrics = compute_factor_metrics(fv, fwd_returns)
            if abs(metrics["ic_mean"]) < ic_threshold:
                continue
            results.append({"expression": expr, **metrics})
            factor_series[expr] = fv
        except Exception:
            logger.debug("[screening] Failed to evaluate: %s", expr[:60], exc_info=True)
            continue

    if not results:
        return []

    # Sort by |IC_mean| descending
    results.sort(key=lambda r: abs(r["ic_mean"]), reverse=True)

    # Dedup by pairwise correlation
    kept: list[dict[str, Any]] = []
    kept_series: list[pd.Series] = []

    for r in results:
        expr = r["expression"]
        fv = factor_series[expr]
        is_redundant = False

        for ks in kept_series:
            try:
                # Align indices
                aligned = pd.DataFrame({"a": fv, "b": ks}).dropna()
                if len(aligned) < 10:
                    continue
                corr = abs(aligned["a"].corr(aligned["b"]))
                if corr > corr_threshold:
                    is_redundant = True
                    break
            except Exception:
                continue

        if not is_redundant:
            kept.append(r)
            kept_series.append(fv)

    return kept


# ---------------------------------------------------------------------------
# Mine Qlib Alpha158 factors
# ---------------------------------------------------------------------------


def mine_alpha158_factors(
    start_date: str = "2023-01-01",
    end_date: str = "2024-12-31",
    instruments: str = "csi300",
    ic_threshold: float = 0.02,
    corr_threshold: float = 0.7,
    top_n: int = 30,
) -> list[dict[str, Any]]:
    """Screen all Alpha158 features, rank by IC, and deduplicate.

    Returns top_n factors after filtering and deduplication.
    """
    try:
        df = compute_qlib_factor_set(
            factor_set="Alpha158",
            instruments=instruments,
            start_date=start_date,
            end_date=end_date,
        )
    except RuntimeError:
        logger.warning("[mining] Qlib not available, cannot mine Alpha158")
        return []

    if df.empty:
        return []

    # Compute forward returns from close price column
    close_col = None
    for col_name in df.columns:
        if isinstance(col_name, tuple):
            if "CLOSE" in str(col_name[-1]).upper():
                close_col = col_name
                break
        elif "CLOSE" in str(col_name).upper():
            close_col = col_name
            break

    if close_col is None:
        # Use last column as proxy or skip
        logger.warning("[mining] Could not find close price column in Alpha158")
        return []

    close_prices = df[close_col]
    # Forward return per instrument
    fwd_ret = close_prices.groupby(level=0).apply(lambda g: g.pct_change(1).shift(-1))
    if isinstance(fwd_ret.index, pd.MultiIndex) and fwd_ret.index.nlevels > 2:
        fwd_ret = fwd_ret.droplevel(0)
    fwd_ret.name = "forward_return"

    results: list[dict[str, Any]] = []
    factor_values: dict[str, pd.Series] = {}

    for col in df.columns:
        col_name = str(col)
        fv = df[col]
        try:
            metrics = compute_factor_metrics(fv, fwd_ret)
        except Exception:
            continue

        if abs(metrics["ic_mean"]) < ic_threshold:
            continue

        results.append({"factor_name": col_name, "factor_set": "Alpha158", **metrics})
        factor_values[col_name] = fv

    # Sort by |IC|
    results.sort(key=lambda r: abs(r["ic_mean"]), reverse=True)

    # Dedup
    kept: list[dict[str, Any]] = []
    kept_series: list[pd.Series] = []

    for r in results:
        fv = factor_values[r["factor_name"]]
        is_redundant = False
        for ks in kept_series:
            try:
                aligned = pd.DataFrame({"a": fv, "b": ks}).dropna()
                if len(aligned) < 10:
                    continue
                if abs(aligned["a"].corr(aligned["b"])) > corr_threshold:
                    is_redundant = True
                    break
            except Exception:
                continue
        if not is_redundant:
            kept.append(r)
            kept_series.append(fv)

        if len(kept) >= top_n:
            break

    logger.info("[mining] Alpha158 screening: %d raw → %d after IC filter → %d after dedup",
                len(df.columns), len(results), len(kept))
    return kept


# ---------------------------------------------------------------------------
# Persist screening results to DB
# ---------------------------------------------------------------------------


def save_screening_results(
    user_id: int,
    run_label: str,
    results: list[dict[str, Any]],
    config: Optional[dict[str, Any]] = None,
) -> int:
    """Save screening results to `quantmate.factor_screening_results`.

    Returns the run_id.
    """
    import json
    from sqlalchemy import text
    from app.infrastructure.db.connections import connection

    with connection("quantmate") as conn:
        row = conn.execute(
            text(
                "INSERT INTO factor_screening_results "
                "(user_id, run_label, config, result_count) "
                "VALUES (:uid, :label, :cfg, :cnt)"
            ),
            {
                "uid": user_id,
                "label": run_label,
                "cfg": json.dumps(config) if config else None,
                "cnt": len(results),
            },
        )
        run_id = row.lastrowid

        # Save individual factor results
        for rank_idx, r in enumerate(results, 1):
            conn.execute(
                text(
                    "INSERT INTO factor_screening_details "
                    "(run_id, rank_order, factor_name, factor_set, expression, "
                    "ic_mean, ic_std, ic_ir, turnover, long_ret, short_ret, long_short_ret) "
                    "VALUES (:rid, :rank, :fname, :fset, :expr, "
                    ":ic_mean, :ic_std, :ic_ir, :turnover, :long_ret, :short_ret, :lsr)"
                ),
                {
                    "rid": run_id,
                    "rank": rank_idx,
                    "fname": r.get("factor_name", r.get("expression", "")[:100]),
                    "fset": r.get("factor_set", "custom"),
                    "expr": r.get("expression", ""),
                    "ic_mean": r.get("ic_mean", 0),
                    "ic_std": r.get("ic_std", 0),
                    "ic_ir": r.get("ic_ir", 0),
                    "turnover": r.get("turnover", 0),
                    "long_ret": r.get("long_ret", 0),
                    "short_ret": r.get("short_ret", 0),
                    "lsr": r.get("long_short_ret", 0),
                },
            )

        conn.commit()

    logger.info("[screening] Saved run %d (%s) with %d results", run_id, run_label, len(results))
    return run_id
