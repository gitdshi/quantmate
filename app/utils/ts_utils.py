"""Time-series utility functions used by backtests and data services.

Provides lightweight `moving_average` and `pct_change` implementations that
work with pandas Series when available or plain Python iterables otherwise.
"""

from typing import Iterable, List, Union
import math

try:
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover - pandas optional
    pd = None


def moving_average(values: Union[Iterable[float], "pd.Series"], window: int) -> Union[List[float], "pd.Series"]:
    """Compute a simple moving average.

    If `values` is a pandas Series and pandas is available, returns a Series
    using `rolling(window).mean()`. Otherwise returns a list of floats where
    positions with insufficient data are `math.nan`.
    """
    if pd is not None and isinstance(values, pd.Series):
        return values.rolling(window=window, min_periods=window).mean()

    vals = list(values)
    if window <= 0:
        raise ValueError("window must be > 0")

    res: List[float] = []
    s = 0.0
    from collections import deque

    q = deque()
    for v in vals:
        try:
            num = float(v)
        except Exception:
            num = math.nan
        q.append(num)
        s += num if not math.isnan(num) else 0.0
        if len(q) > window:
            old = q.popleft()
            s -= old if not math.isnan(old) else 0.0
        if len(q) < window or any(math.isnan(x) for x in q):
            res.append(math.nan)
        else:
            res.append(s / window)
    return res


def pct_change(values: Union[Iterable[float], "pd.Series"], periods: int = 1) -> Union[List[float], "pd.Series"]:
    """Compute percent change over `periods` periods.

    If `values` is a pandas Series and pandas is available, delegates to
    `Series.pct_change(periods)`. Otherwise returns a list of floats with
    `math.nan` where the percent change cannot be computed.
    """
    if pd is not None and isinstance(values, pd.Series):
        return values.pct_change(periods=periods)

    vals = list(values)
    if periods < 1:
        raise ValueError("periods must be >= 1")

    res: List[float] = []
    for i, v in enumerate(vals):
        try:
            cur = float(v)
        except Exception:
            cur = math.nan
        if i - periods < 0:
            res.append(math.nan)
            continue
        try:
            prev = float(vals[i - periods])
        except Exception:
            prev = math.nan
        if prev == 0 or math.isnan(prev) or math.isnan(cur):
            res.append(math.nan)
        else:
            res.append((cur - prev) / prev)
    return res
