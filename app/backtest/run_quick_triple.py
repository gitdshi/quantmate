from datetime import datetime
import sys
from pathlib import Path

# Ensure project root is importable
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Import app.main to ensure vnpy settings (DB/datafeed) are configured
import app.main  # noqa: F401

from vnpy.trader.constant import Interval
from vnpy_ctastrategy.backtesting import BacktestingEngine, BacktestingMode
from app.strategies.triple_ma_strategy import TripleMAStrategy
from app.backtest.ts_utils import moving_average, pct_change


def run():
    # Configure backtest parameters
    vt_symbol = "000001.SZSE"  # change if needed (use SSE/SZSE exchange codes)
    start = datetime(2020, 1, 1)
    end = datetime(2020, 12, 31)

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

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--fast", type=int, default=5)
    parser.add_argument("--mid", type=int, default=10)
    parser.add_argument("--slow", type=int, default=20)
    parser.add_argument("--size", type=int, default=1)
    args = parser.parse_args()

    setting = TripleMAStrategy.get_class_parameters()
    setting["fast_window"] = args.fast
    setting["mid_window"] = args.mid
    setting["slow_window"] = args.slow
    setting["fixed_size"] = args.size

    engine.add_strategy(TripleMAStrategy, setting)

    print("Loading data...")
    engine.load_data()

    if not engine.history_data:
        print("No history data loaded. Backtest aborted.")
        return

    print("Running backtest...")
    engine.run_backtesting()

    print("Calculating results...")
    df = engine.calculate_result()
    stats = engine.calculate_statistics()

    # add simple pandas-derived indicators if daily df exists
    if df is not None and not df.empty and 'close_price' in df.columns:
        fast = setting.get('fast_window', 5)
        mid = setting.get('mid_window', 10)
        slow = setting.get('slow_window', 20)
        df['ma_fast'] = moving_average(df['close_price'], fast)
        df['ma_mid'] = moving_average(df['close_price'], mid)
        df['ma_slow'] = moving_average(df['close_price'], slow)
        df['daily_return'] = pct_change(df['close_price'])
        print('\nDaily DF sample with indicators:')
        print(df[['close_price', 'ma_fast', 'ma_mid', 'ma_slow', 'daily_return']].head())

    print("Done.")
    print(stats)


if __name__ == "__main__":
    run()
