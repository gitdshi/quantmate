from datetime import datetime
import sys
from pathlib import Path

# Ensure project root is importable
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import app.main  # noqa: F401

from vnpy.trader.database import get_database
from vnpy.trader.constant import Exchange, Interval
from vnpy_ctastrategy.backtesting import BacktestingEngine, BacktestingMode
from app.strategies.triple_ma_strategy import TripleMAStrategy
from app.backtest.ts_utils import moving_average, pct_change


def pick_top_sz_symbols(limit=5, min_count=200):
    db = get_database()
    overviews = db.get_bar_overview()
    sz = [ov for ov in overviews if ov.exchange == Exchange.SZSE and ov.count >= min_count]
    sz.sort(key=lambda o: o.count, reverse=True)
    return [f"{o.symbol}.SZSE" for o in sz[:limit]]


def run_one(vt_symbol, start, end):
    engine = BacktestingEngine()
    engine.set_parameters(
        vt_symbol=vt_symbol,
        interval=Interval.DAILY,
        start=start,
        end=end,
        rate=0.0001,
        slippage=0.0,
        size=1,
        pricetick=0.01,
        capital=100000,
        mode=BacktestingMode.BAR,
    )

    setting = TripleMAStrategy.get_class_parameters()
    engine.add_strategy(TripleMAStrategy, setting)

    print(f"\n=== {vt_symbol} ===")
    engine.load_data()
    print("history_data count:", len(engine.history_data))
    if not engine.history_data:
        print("no history, skip")
        return

    engine.run_backtesting()
    print("trades count:", len(engine.trades))

    # ensure daily_df exists
    df = engine.calculate_result()
    if df is not None and not df.empty and 'close_price' in df.columns:
        mid_w = setting.get('mid_window', 10)
        df['ma_mid'] = moving_average(df['close_price'], mid_w)
        df['daily_return'] = pct_change(df['close_price'])
        print('daily_df sample with indicators:')
        print(df[['close_price','ma_mid','daily_return']].head())

    stats = engine.calculate_statistics()
    print("stats:", stats)

    if engine.daily_df is not None and not engine.daily_df.empty:
        print("daily_df sample:")
        print(engine.daily_df.head())


def main():
    start = datetime(2021, 1, 1)
    end = datetime.now()

    symbols = pick_top_sz_symbols(limit=5, min_count=200)
    if not symbols:
        print("No SZ symbols with enough bars found in vnpy DB")
        return

    for s in symbols:
        try:
            run_one(s, start, end)
        except Exception as e:
            print(f"Error for {s}: {e}")


if __name__ == '__main__':
    main()
