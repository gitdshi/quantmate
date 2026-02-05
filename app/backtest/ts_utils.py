import pandas as pd
import numpy as np


def moving_average(series: pd.Series, window: int, method: str = "SMA", min_periods: int = 1, center: bool = False) -> pd.Series:
    """Return moving average of a price series.

    Args:
        series: pandas Series of numeric values indexed by datetime.
        window: lookback window size (integer).
        method: 'SMA' for simple, 'EMA' for exponential.
        min_periods: min non-NA values in window.
        center: whether to set the labels at the center of the window.

    Returns:
        pd.Series of the same index with the moving average.
    """
    if method.upper() == "EMA":
        return series.ewm(span=window, adjust=False).mean()
    # default to simple moving average
    return series.rolling(window=window, min_periods=min_periods, center=center).mean()


def pct_change(series: pd.Series, periods: int = 1) -> pd.Series:
    """Percent change (simple returns) over given periods."""
    return series.pct_change(periods)


def rolling_cum_return(series: pd.Series, window: int) -> pd.Series:
    """Rolling cumulative return over a window: (1+rt).prod()-1 where rt is pct_change.

    Example: a 5-day rolling return.
    """
    returns = pct_change(series)
    return returns.add(1).rolling(window=window, min_periods=1).apply(lambda x: x.prod() - 1, raw=True)


def log_returns(series: pd.Series, periods: int = 1) -> pd.Series:
    """Log returns ln(p_t / p_{t-periods})."""
    ratio = series / series.shift(periods)
    return ratio.replace([pd.NA, float("nan")], pd.NA).apply(lambda x: pd.NA if pd.isna(x) else float(np.log(x)))

