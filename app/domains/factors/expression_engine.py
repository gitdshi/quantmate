"""Factor expression engine — compute factor values from expressions or Qlib datasets.

Supports two modes:
  1. Qlib built-in factor sets (Alpha158 / Alpha360) via DataHandlerLP
  2. Custom expressions evaluated with pandas (safe subset)

Results are written to `qlib.alpha_factor_values` and returned as DataFrames.
"""

from __future__ import annotations

import logging
import re
from datetime import date
from typing import Any, Optional

import numpy as np
import pandas as pd
from scipy import stats
from sqlalchemy import bindparam, text

from app.infrastructure.db.connections import connection

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Safe expression helpers
# ---------------------------------------------------------------------------

# Allowed function names in custom expressions (whitelist approach)
_SAFE_FUNCTIONS = {
    "abs", "log", "sqrt", "sign", "rank", "mean", "std", "sum",
    "min", "max", "median", "delay", "delta", "corr", "cov",
    "shift", "rolling", "pct_change", "clip", "where",
}

_UNSAFE_PATTERN = re.compile(
    r"(__\w+__|import\s|exec\s*\(|eval\s*\(|open\s*\(|compile\s*\(|getattr|setattr|delattr|globals|locals)",
    re.IGNORECASE,
)
_VALID_IDENTIFIER = re.compile(r"^[A-Za-z_]\w*$")


def _validate_expression(expr: str) -> None:
    """Reject obviously unsafe expressions."""
    if _UNSAFE_PATTERN.search(expr):
        raise ValueError(f"Expression contains forbidden constructs: {expr[:80]}")


def normalize_factor_expression(expression: str) -> str:
    """Normalize discovered-factor syntax into the engine's pandas-eval subset."""
    normalized = expression.strip().replace("$", "")
    normalized = normalized.replace(r"\_", "_")
    compact = re.sub(r"\s+", " ", normalized)
    compact = re.sub(r"\s*where\s+.*$", "", compact, flags=re.IGNORECASE)
    compact = re.sub(r"\\text\{([^{}]+)\}", r"\1", compact)
    compact = re.sub(r"\s*,?\s*where\s+.*$", "", compact, flags=re.IGNORECASE)
    compact = re.sub(
        r"\\sigma_\{t\}\^\{(\d+)d\}\s*=.*$",
        lambda match: f"ts_std(ret_1d, {match.group(1)})",
        compact,
    )
    compact = re.sub(
        r"\\sigma_\{(\d+)\}\(t\)\s*=.*$",
        lambda match: f"ts_std(ret_1d, {match.group(1)})",
        compact,
    )
    compact = compact.replace("}]", "}")
    compact = re.sub(r"([A-Za-z]+(?:_t|_\{[^}]+\}))\^\{[^}]+\}", r"\1", compact)

    for source, target in {
        "Close": "close",
        "High": "high",
        "Low": "low",
        "Open": "open",
        "Volume": "volume",
        "VWAP": "vwap",
        "Mean": "mean",
        "Max": "max",
        "Min": "min",
        "Momentum": "momentum",
        "Volatility": "volatility",
        "VolumeRatio": "volume_ratio",
        "HighLowRange": "high_low_range",
        "VWAPCloseRatio": "vwap_close_ratio",
        "MA": "ma",
    }.items():
        compact = re.sub(rf"\b{source}\b", target, compact)

    compact = re.sub(r"^[A-Za-z_][A-Za-z0-9_]*_\{[^}]+\}\s*=\s*", "", compact)
    compact = re.sub(r"^[A-Za-z_]+_\{[^}]+\}\s*=\s*", "", compact)
    compact = re.sub(r"^[A-Za-z_][A-Za-z0-9_]*\s*=\s*", "", compact)
    if " = " in compact and not compact.lstrip().startswith(r"\sigma"):
        compact = compact.rsplit(" = ", 1)[-1].strip()
    compact = re.sub(
        r"\\sigma\s*\(\s*R_\{t-(\d+):t\}\s*\)",
        lambda match: f"ts_std(ret_1d, {int(match.group(1)) + 1})",
        compact,
    )
    compact = re.sub(
        r"\\frac\{volume(?:_t)?\}\{\\frac\{1\}\{(\d+)\}\s*\\sum_\{i=0\}\^\{\d+\}\s*volume_\{t-i\}\}",
        lambda match: f"volume / ts_mean(volume, {match.group(1)})",
        compact,
    )
    compact = re.sub(
        r"\\frac\{(?:Volume|volume)(?:_t)?\}\{\\frac\{1\}\{(\d+)\}\s*\\sum_\{i=0\}\^\{\d+\}\s*(?:Volume|volume)_\{t-i\}\}",
        lambda match: f"volume / ts_mean(volume, {match.group(1)})",
        compact,
    )
    compact = re.sub(
        r"\\frac\{V_t\}\{\\frac\{1\}\{(\d+)\}\s*\\sum_\{i=0\}\^\{\d+\}\s*V_\{t-i\}\}",
        lambda match: f"volume / ts_mean(volume, {match.group(1)})",
        compact,
    )
    compact = re.sub(
        r"\\frac\{V_t\}\{\\frac\{1\}\{(\d+)\}.*?V_\{t-i\}\}",
        lambda match: f"volume / ts_mean(volume, {match.group(1)})",
        compact,
    )
    compact = re.sub(
        r"\\frac\{volume(?:_t)?\}\{mean\((\w+)_\{t-(\d+):t\}\}\s*",
        lambda match: f"(volume) / (ts_mean({match.group(1).lower()}, {int(match.group(2)) + 1}))",
        compact,
    )
    compact = re.sub(
        r"mean\((\w+)_\{t-(\d+)\}:(\w+)_\{t\}\)",
        lambda match: (
            f"ts_mean({match.group(1).lower()}, {int(match.group(2)) + 1})"
            if match.group(1).lower() == match.group(3).lower()
            else match.group(0)
        ),
        compact,
    )
    compact = re.sub(
        r"\\sqrt\{\\frac\{1\}\{\d+\}\s*\\sum_\{i=0\}\^\{\d+\}\s*\(r_\{t-i\}\s*-\s*\\bar\{r\}_\{(\d+)d\}\)\^2\}",
        lambda match: f"ts_std(ret_1d, {match.group(1)})",
        compact,
    )
    compact = re.sub(
        r"\\sqrt\{\\frac\{1\}\{\d+\}.*?\\bar\{r\}_\{(\d+)d\}.*?\}",
        lambda match: f"ts_std(ret_1d, {match.group(1)})",
        compact,
    )
    compact = re.sub(
        r"mean\((\w+)_\{i,t-(\d+)\}:(\w+)_\{i,t\}\)",
        lambda match: (
            f"ts_mean({match.group(1).lower()}, {int(match.group(2)) + 1})"
            if match.group(1).lower() == match.group(3).lower()
            else match.group(0)
        ),
        compact,
    )
    compact = re.sub(
        r"\\frac\{close_t\}\{\\frac\{1\}\{(\d+)\}\s*\\sum_\{i=0\}\^\{\d+\}\s*close_\{t-i\}\}\s*-\s*1",
        lambda match: f"(close) / (ts_mean(close, {match.group(1)})) - 1",
        compact,
    )
    compact = re.sub(
        r"([A-Za-z]+)_\{(\d+),t\}",
        lambda match: f"{match.group(1).lower()}_{match.group(2)}d",
        compact,
    )
    compact = re.sub(
        r"R_\{(\d+)\}\(t\)\s*=\s*\\frac\{Close\(t\)\}\{Close\(t-(\d+)\)\}\s*-\s*1",
        lambda match: f"(close) / (delay(close, {match.group(2)})) - 1",
        compact,
    )
    compact = re.sub(
        r"VR_\{(\d+)\}\(t\)\s*=\s*\\frac\{Volume\(t\)\}\{MA_\{(\d+)\}\(Volume\)\}",
        lambda match: f"volume / ts_mean(volume, {match.group(2)})",
        compact,
    )
    compact = re.sub(
        r"SMA_\{ratio\}\(t\)\s*=\s*\\frac\{Close\(t\)\}\{SMA_\{(\d+)\}\(Close\)\}",
        lambda match: f"close / ts_mean(close, {match.group(1)})",
        compact,
    )
    compact = re.sub(
        r"HL_\{(\d+)\}\(t\)\s*=\s*\\frac\{1\}\{\d+\}\s*\\sum_\{i=0\}\^\{\d+\}\s*\\frac\{High\(t-i\)\s*-\s*Low\(t-i\)\}\{Close\(t-i\)\}",
        lambda match: f"ts_mean((high - low) / close, {match.group(1)})",
        compact,
    )
    compact = re.sub(
        r"\\frac\{close_t\}\{ma_(\d+)d\}\s*-\s*1",
        lambda match: f"(close) / (ts_mean(close, {match.group(1)})) - 1",
        compact,
    )
    compact = re.sub(
        r"(?:MA|SMA)_\{(\d+)\}\((\w+)\)",
        lambda match: f"ts_mean({match.group(2).lower()}, {match.group(1)})",
        compact,
    )

    window_functions = {
        "mean": "ts_mean",
        "max": "ts_max",
        "min": "ts_min",
    }
    for function_name, replacement in window_functions.items():
        compact = re.sub(
            rf"{function_name}\((\w+)_\{{t-(\d+):t(?:\+\d+)?\}}\)",
            lambda match: f"{replacement}({match.group(1).lower()}, {int(match.group(2)) + 1})",
            compact,
        )

    compact = re.sub(
        r"([A-Za-z]+)_\{i,t-(\d+)\}",
        lambda match: f"delay({match.group(1).lower()}, {match.group(2)})",
        compact,
    )
    compact = re.sub(
        r"([A-Za-z]+)_\{i,t\}",
        lambda match: match.group(1).lower(),
        compact,
    )
    compact = re.sub(
        r"([A-Za-z]+)_\{t-(\d+)\}",
        lambda match: f"delay({match.group(1).lower()}, {match.group(2)})",
        compact,
    )
    compact = re.sub(
        r"([A-Za-z]+)_\{t\}",
        lambda match: match.group(1).lower(),
        compact,
    )
    compact = re.sub(
        r"([A-Za-z]+)\(t-(\d+)\)",
        lambda match: f"delay({match.group(1).lower() if match.group(1) != 'R' else 'ret_1d'}, {match.group(2)})",
        compact,
    )
    compact = re.sub(
        r"([A-Za-z]+)\(t\)",
        lambda match: "ret_1d" if match.group(1) == "R" else match.group(1).lower(),
        compact,
    )
    compact = re.sub(
        r"([A-Za-z]+)_t",
        lambda match: match.group(1).lower(),
        compact,
    )

    while True:
        updated = re.sub(r"\\frac\{([^{}]+)\}\{([^{}]+)\}", r"(\1) / (\2)", compact)
        if updated == compact:
            break
        compact = updated

    compact = re.sub(
        r"\\frac\{volume\}\{\(1\)\s*/\s*\((\d+)\)\s*\\sum_\{i=0\}(?:\^\{\d+\})?\s*volume_\{t-i\}\}",
        lambda match: f"volume / ts_mean(volume, {match.group(1)})",
        compact,
    )
    compact = re.sub(
        r"\\frac\{volume\}\{\(1\)\s*/\s*\((\d+)\)\s*\\sum_\{i=0\}(?:\^\{\d+\})?\s*(?:Volume|volume)_\{t-i\}\}",
        lambda match: f"volume / ts_mean(volume, {match.group(1)})",
        compact,
    )
    compact = re.sub(
        r"\\frac\{volume\}\{\(1\)\s*/\s*\((\d+)\)\s*\\sum_\{i=0\}(?:\^\{\d+\})?\s*V_\{t-i\}\}",
        lambda match: f"volume / ts_mean(volume, {match.group(1)})",
        compact,
    )
    compact = re.sub(
        r"\\frac\{close\}\{\(1\)\s*/\s*\((\d+)\)\s*\\sum_\{i=0\}(?:\^\{\d+\})?\s*close_\{t-i\}\}\s*-\s*1",
        lambda match: f"(close) / (ts_mean(close, {match.group(1)})) - 1",
        compact,
    )
    compact = re.sub(
        r"\(1\)\s*/\s*\((\d+)\)\s*\\sum_\{i=0\}(?:\^\{\d+\})?\s*\(high\(t-i\)\s*-\s*low\(t-i\)\)\s*/\s*\(close\(t-i\)\)",
        lambda match: f"ts_mean((high - low) / close, {match.group(1)})",
        compact,
    )

    latex_patterns = (
        (
            re.compile(r"^M_\{(\d+)d\}\s*=\s*\\frac\{close_t\}\{close_\{t-(\d+)\}\}\s*-\s*1$"),
            lambda match: f"ret_{match.group(1)}d" if match.group(1) == match.group(2) else normalized,
        ),
        (
            re.compile(
                r"^VR_\{(\d+)d\}\s*=\s*\\frac\{volume_t\}\{\\frac\{1\}\{\d+\}\s*\\sum_\{i=0\}\^\{\d+\}\s*volume_\{t-i\}\}$"
            ),
            lambda match: f"volume / ts_mean(volume, {match.group(1)})",
        ),
        (
            re.compile(r"^CR_\{HL\}\s*=\s*\\frac\{close_t\s*-\s*low_t\}\{high_t\s*-\s*low_t\}$"),
            lambda match: "(close - low) / (high - low)",
        ),
        (
            re.compile(r"^\\sigma_\{(\d+)d\}\s*=.*$"),
            lambda match: f"ts_std(ret_1d, {match.group(1)})",
        ),
    )
    for pattern, replacement in latex_patterns:
        match = pattern.match(compact)
        if match:
            replaced = replacement(match)
            if isinstance(replaced, str) and replaced != normalized:
                normalized = replaced
                break
    else:
        normalized = compact

    normalized = re.sub(r"\$(\w+)", r"\1", normalized)
    replacements = {
        r"(?<![\w.])rolling_mean\s*\(": "ts_mean(",
        r"(?<![\w.])rolling_std\s*\(": "ts_std(",
        r"(?<![\w.])rolling_sum\s*\(": "ts_sum(",
        r"(?<![\w.])rolling_max\s*\(": "ts_max(",
        r"(?<![\w.])rolling_min\s*\(": "ts_min(",
        r"(?<![\w.])mean\s*\(": "ts_mean(",
        r"(?<![\w.])std\s*\(": "ts_std(",
        r"(?<![\w.])sum\s*\(": "ts_sum(",
        r"(?<![\w.])max\s*\(": "ts_max(",
        r"(?<![\w.])min\s*\(": "ts_min(",
        r"(?<![\w.])corr\s*\(": "ts_corr(",
    }
    for pattern, replacement in replacements.items():
        normalized = re.sub(pattern, replacement, normalized)

    aliases = {
        r"\bdaily_return\b": "ret_1d",
        r"\breturn_1d\b": "ret_1d",
        r"\breturn_5d\b": "ret_5d",
        r"\breturn_10d\b": "ret_10d",
        r"\breturn_20d\b": "ret_20d",
        r"\bmomentum_5d\b": "ret_5d",
        r"\bmomentum_10d\b": "ret_10d",
        r"\bmomentum_20d\b": "ret_20d",
        r"\\bar\{R\}_\{(\d+)\}": lambda match: f"ts_mean(ret_1d, {match.group(1)})",
        r"\\bar\{R\}": "ts_mean(ret_1d, 1)",
        r"\breturn\b": "ret_1d",
    }
    for pattern, replacement in aliases.items():
        normalized = re.sub(pattern, replacement, normalized)

    symbol_aliases = {
        r"(?<![\w.])p(?![\w.])": "close",
        r"(?<![\w.])v(?![\w.])": "volume",
        r"(?<![\w.])o(?![\w.])": "open",
        r"(?<![\w.])h(?![\w.])": "high",
        r"(?<![\w.])l(?![\w.])": "low",
        r"(?<![\w.])c(?![\w.])": "close",
    }
    for pattern, replacement in symbol_aliases.items():
        normalized = re.sub(pattern, replacement, normalized)

    return normalized


def augment_factor_eval_ohlcv(ohlcv: pd.DataFrame) -> pd.DataFrame:
    """Add derived return columns used by discovered-factor expressions."""
    if ohlcv.empty:
        return ohlcv.copy()

    eval_ohlcv = ohlcv.copy()
    for periods in (1, 5, 10, 20):
        eval_ohlcv[f"ret_{periods}d"] = eval_ohlcv.groupby(level=0)["close"].pct_change(periods=periods)
    return eval_ohlcv


def _instrument_level(index: pd.Index) -> int | None:
    if not isinstance(index, pd.MultiIndex):
        return None
    if "instrument" in index.names:
        return index.names.index("instrument")
    return 0 if index.nlevels >= 2 else None


def _date_level(index: pd.Index) -> int | None:
    if not isinstance(index, pd.MultiIndex):
        return None
    if "date" in index.names:
        return index.names.index("date")
    if "datetime" in index.names:
        return index.names.index("datetime")
    return 1 if index.nlevels >= 2 else None


def _apply_by_instrument(series: pd.Series, operation) -> pd.Series:
    level = _instrument_level(series.index)
    if level is None:
        return operation(series)
    return series.groupby(level=level, group_keys=False).apply(operation)


# ---------------------------------------------------------------------------
# Custom expression evaluation
# ---------------------------------------------------------------------------


def _delay(series: pd.Series, periods: int = 1) -> pd.Series:
    """Shift series forward (lag)."""
    return _apply_by_instrument(series, lambda value: value.shift(periods))


def _delta(series: pd.Series, periods: int = 1) -> pd.Series:
    """Difference: series - delay(series, periods)."""
    return series - _delay(series, periods)


def _rank(series: pd.Series) -> pd.Series:
    """Cross-sectional rank (percentile)."""
    level = _date_level(series.index)
    if level is None:
        return series.rank(pct=True)
    return series.groupby(level=level, group_keys=False).rank(pct=True)


def _ts_mean(series: pd.Series, window: int = 5) -> pd.Series:
    return _apply_by_instrument(series, lambda value: value.rolling(window, min_periods=1).mean())


def _ts_std(series: pd.Series, window: int = 5) -> pd.Series:
    return _apply_by_instrument(series, lambda value: value.rolling(window, min_periods=1).std())


def _ts_max(series: pd.Series, window: int = 5) -> pd.Series:
    return _apply_by_instrument(series, lambda value: value.rolling(window, min_periods=1).max())


def _ts_min(series: pd.Series, window: int = 5) -> pd.Series:
    return _apply_by_instrument(series, lambda value: value.rolling(window, min_periods=1).min())


def _ts_sum(series: pd.Series, window: int = 5) -> pd.Series:
    return _apply_by_instrument(series, lambda value: value.rolling(window, min_periods=1).sum())


def _ts_corr(x: pd.Series, y: pd.Series, window: int = 5) -> pd.Series:
    level = _instrument_level(x.index)
    if level is not None and _instrument_level(y.index) is not None:
        aligned = pd.DataFrame({"x": x, "y": y})
        return aligned.groupby(level=level, group_keys=False).apply(
            lambda group: group["x"].rolling(window, min_periods=2).corr(group["y"])
        )
    return x.rolling(window, min_periods=2).corr(y)


# Build locals dict for pandas eval context
_EVAL_LOCALS = {
    "delay": _delay,
    "delta": _delta,
    "rank": _rank,
    "ts_mean": _ts_mean,
    "ts_std": _ts_std,
    "ts_max": _ts_max,
    "ts_min": _ts_min,
    "ts_sum": _ts_sum,
    "ts_corr": _ts_corr,
    "log": np.log,
    "abs": np.abs,
    "sqrt": np.sqrt,
    "sign": np.sign,
    "clip": np.clip,
    "nan": np.nan,
    "inf": np.inf,
}


def compute_custom_factor(
    expression: str,
    ohlcv: pd.DataFrame,
) -> pd.Series:
    """Evaluate a custom expression against an OHLCV DataFrame.

    The DataFrame must have columns: open, high, low, close, volume
    and be indexed by (instrument, date) or just (date) for a single stock.

    Returns a Series of factor values with the same index.
    """
    _validate_expression(expression)

    # Make column names available as variables
    local_vars = dict(_EVAL_LOCALS)
    for col in ohlcv.columns:
        if isinstance(col, str) and _VALID_IDENTIFIER.match(col):
            local_vars[col] = ohlcv[col]

    try:
        result = pd.eval(expression, local_dict=local_vars, engine="python")
    except Exception as exc:
        raise ValueError(f"Expression evaluation failed: {exc}") from exc

    if isinstance(result, (int, float)):
        return pd.Series(result, index=ohlcv.index, name="factor_value")
    return pd.Series(result, name="factor_value")


# ---------------------------------------------------------------------------
# Qlib built-in factor set computation
# ---------------------------------------------------------------------------


def compute_qlib_factor_set(
    factor_set: str = "Alpha158",
    instruments: str = "csi300",
    start_date: str = "2023-01-01",
    end_date: str = "2024-12-31",
) -> pd.DataFrame:
    """Compute a Qlib built-in factor set (Alpha158 / Alpha360).

    Returns a DataFrame with MultiIndex (instrument, datetime) and factor columns.
    Requires Qlib to be installed and initialized.
    """
    from app.infrastructure.qlib.qlib_config import (
        SUPPORTED_DATASETS,
        ensure_qlib_initialized,
        is_qlib_available,
    )

    if not is_qlib_available():
        raise RuntimeError("Qlib is not installed — cannot compute built-in factor sets")

    if factor_set not in SUPPORTED_DATASETS:
        raise ValueError(f"Unsupported factor set: {factor_set}. Supported: {list(SUPPORTED_DATASETS.keys())}")

    ensure_qlib_initialized()

    from qlib.utils import init_instance_by_config

    handler_class = SUPPORTED_DATASETS[factor_set]
    handler_config = {
        "class": handler_class.split(".")[-1],
        "module_path": handler_class.rsplit(".", 1)[0],
        "kwargs": {
            "instruments": instruments,
            "start_time": start_date,
            "end_time": end_date,
        },
    }

    handler = init_instance_by_config(handler_config)
    df = handler.fetch()

    if df is None or df.empty:
        raise RuntimeError(f"Qlib returned no data for {factor_set} / {instruments} / {start_date}-{end_date}")

    return df


# ---------------------------------------------------------------------------
# Fetch OHLCV for custom factor computation (from tushare)
# ---------------------------------------------------------------------------


def fetch_ohlcv(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    instruments: Optional[list[str]] = None,
) -> pd.DataFrame:
    """Fetch OHLCV data from tushare MySQL for factor computation.

    Returns DataFrame indexed by (instrument, date) with columns:
        open, high, low, close, volume, amount, factor
    """
    params: dict[str, Any] = {}
    where_parts: list[str] = []

    if start_date:
        where_parts.append("d.trade_date >= :start")
        params["start"] = start_date
    if end_date:
        where_parts.append("d.trade_date <= :end")
        params["end"] = end_date
    if instruments:
        where_parts.append("d.ts_code IN :instruments")
        params["instruments"] = tuple(instruments)

    where_sql = (" AND " + " AND ".join(where_parts)) if where_parts else ""

    query = f"""
        SELECT
            d.ts_code AS instrument,
            d.trade_date AS date,
            d.open, d.high, d.low, d.close,
            d.vol AS volume,
            d.amount,
            COALESCE(a.adj_factor, 1.0) AS factor
        FROM stock_daily d
        LEFT JOIN adj_factor a ON d.ts_code = a.ts_code AND d.trade_date = a.trade_date
        WHERE 1=1 {where_sql}
        ORDER BY d.ts_code, d.trade_date
    """

    statement = text(query)
    if instruments:
        statement = statement.bindparams(bindparam("instruments", expanding=True))

    with connection("tushare") as conn:
        df = pd.read_sql(statement, conn, params=params)

    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index(["instrument", "date"]).sort_index()
    return df


# ---------------------------------------------------------------------------
# Persist factor values to DB
# ---------------------------------------------------------------------------


def save_factor_values(
    factor_name: str,
    factor_set: str,
    values: pd.DataFrame | pd.Series,
) -> int:
    """Save computed factor values to `qlib.alpha_factor_values`.

    Args:
        factor_name: Name of the factor (column name for Qlib sets, definition name for custom)
        factor_set: 'Alpha158', 'Alpha360', or 'custom'
        values: Series or single-column DataFrame with MultiIndex (instrument, date)

    Returns:
        Number of rows inserted.
    """
    if isinstance(values, pd.Series):
        df = values.reset_index()
        df.columns = ["instrument", "trade_date", "factor_value"]
    elif isinstance(values, pd.DataFrame):
        if values.shape[1] == 1:
            df = values.reset_index()
            df.columns = ["instrument", "trade_date", "factor_value"]
        else:
            raise ValueError("DataFrame must have exactly one column for single-factor save")
    else:
        raise TypeError(f"Unsupported type: {type(values)}")

    df["factor_set"] = factor_set
    df["factor_name"] = factor_name
    df = df.dropna(subset=["factor_value"])

    if df.empty:
        return 0

    rows_inserted = 0
    batch_size = 5000

    with connection("qlib") as conn:
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i : i + batch_size]
            records = batch[["instrument", "trade_date", "factor_set", "factor_name", "factor_value"]].to_dict("records")

            conn.execute(
                text(
                    "INSERT INTO alpha_factor_values (instrument, trade_date, factor_set, factor_name, factor_value) "
                    "VALUES (:instrument, :trade_date, :factor_set, :factor_name, :factor_value) "
                    "ON DUPLICATE KEY UPDATE factor_value = VALUES(factor_value)"
                ),
                records,
            )
            rows_inserted += len(records)

        conn.commit()

    logger.info("[factor-engine] Saved %d rows for %s/%s", rows_inserted, factor_set, factor_name)
    return rows_inserted


# ---------------------------------------------------------------------------
# IC / ICIR metrics computation
# ---------------------------------------------------------------------------


def compute_factor_metrics(
    factor_values: pd.Series,
    forward_returns: pd.Series,
) -> dict[str, float]:
    """Compute standard factor evaluation metrics.

    Both inputs should have MultiIndex (instrument, date).

    Returns dict with: ic_mean, ic_std, ic_ir, turnover, long_ret, short_ret, long_short_ret
    """
    # Align
    aligned = pd.DataFrame({"factor": factor_values, "return": forward_returns}).dropna()
    if aligned.empty or len(aligned) < 10:
        return {
            "ic_mean": 0.0, "ic_std": 0.0, "ic_ir": 0.0,
            "turnover": 0.0, "long_ret": 0.0, "short_ret": 0.0, "long_short_ret": 0.0,
        }

    # Group by date for cross-sectional IC
    dates = aligned.index.get_level_values("date") if "date" in aligned.index.names else aligned.index.get_level_values(1)
    aligned["_date"] = dates
    ic_series = aligned.groupby("_date").apply(
        lambda g: stats.spearmanr(g["factor"], g["return"]).statistic if len(g) >= 5 else np.nan
    )
    ic_series = ic_series.dropna()

    ic_mean = float(ic_series.mean()) if len(ic_series) > 0 else 0.0
    ic_std = float(ic_series.std()) if len(ic_series) > 1 else 0.0
    ic_ir = float(ic_mean / ic_std) if ic_std > 1e-9 else 0.0

    # Long / short returns: top quintile vs bottom quintile each day
    def _quintile_returns(g: pd.DataFrame) -> dict:
        if len(g) < 10:
            return {"long": np.nan, "short": np.nan}
        sorted_g = g.sort_values("factor")
        n = len(sorted_g)
        q = max(n // 5, 1)
        return {
            "long": sorted_g["return"].iloc[-q:].mean(),
            "short": sorted_g["return"].iloc[:q].mean(),
        }

    quintile_df = aligned.groupby("_date").apply(lambda g: pd.Series(_quintile_returns(g))).dropna()
    long_ret = float(quintile_df["long"].mean()) if not quintile_df.empty else 0.0
    short_ret = float(quintile_df["short"].mean()) if not quintile_df.empty else 0.0
    long_short_ret = long_ret - short_ret

    # Turnover: rank correlation between consecutive days
    factor_by_date = aligned.pivot_table(index="_date", columns=aligned.index.get_level_values(0), values="factor")
    if factor_by_date.shape[0] >= 2:
        rank_today = factor_by_date.rank(axis=1, pct=True)
        rank_shift = rank_today.shift(1)
        turnover_series = (rank_today - rank_shift).abs().mean(axis=1).dropna()
        turnover = float(turnover_series.mean()) if not turnover_series.empty else 0.0
    else:
        turnover = 0.0

    return {
        "ic_mean": round(ic_mean, 6),
        "ic_std": round(ic_std, 6),
        "ic_ir": round(ic_ir, 4),
        "turnover": round(turnover, 4),
        "long_ret": round(long_ret, 6),
        "short_ret": round(short_ret, 6),
        "long_short_ret": round(long_short_ret, 6),
    }


def compute_forward_returns(
    ohlcv: pd.DataFrame,
    periods: int = 1,
) -> pd.Series:
    """Compute N-day forward returns from close prices.

    Input: DataFrame indexed by (instrument, date) with 'close' column.
    Output: Series of forward returns with same index.
    """
    if ohlcv.empty:
        return pd.Series(dtype=float, name="forward_return")

    # Group by instrument, compute forward return
    def _fwd_ret(group: pd.DataFrame) -> pd.Series:
        return group["close"].pct_change(periods).shift(-periods)

    if isinstance(ohlcv.index, pd.MultiIndex):
        result = ohlcv.groupby(level=0).apply(_fwd_ret)
        # Flatten double-grouped index
        if isinstance(result.index, pd.MultiIndex) and result.index.nlevels > 2:
            result = result.droplevel(0)
    else:
        result = _fwd_ret(ohlcv)

    result.name = "forward_return"
    return result
