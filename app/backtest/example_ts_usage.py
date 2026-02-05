from datetime import datetime
import sys
from pathlib import Path
import pandas as pd
import numpy as np

# ensure project root is importable
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.backtest.ts_utils import moving_average, pct_change, rolling_cum_return, log_returns


def demo():
    # build synthetic price series
    dates = pd.date_range(start="2021-01-01", periods=100, freq="D")
    np.random.seed(42)
    prices = pd.Series(100 + np.cumsum(np.random.normal(scale=1.0, size=len(dates))), index=dates)

    sma10 = moving_average(prices, window=10, method="SMA")
    ema10 = moving_average(prices, window=10, method="EMA")
    returns = pct_change(prices)
    rolling5 = rolling_cum_return(prices, window=5)
    logr = log_returns(prices)

    print("Prices head:")
    print(prices.head())
    print("\nSMA(10) head:")
    print(sma10.dropna().head())
    print("\nEMA(10) head:")
    print(ema10.dropna().head())
    print("\nPercent change head:")
    print(returns.dropna().head())
    print("\n5-day rolling cumulative return head:")
    print(rolling5.dropna().head())
    print("\nLog returns head:")
    print(logr.dropna().head())


if __name__ == "__main__":
    demo()
