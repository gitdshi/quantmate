from datetime import datetime
import sys
from pathlib import Path

# Ensure project root is importable
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import app.main  # noqa: F401

from vnpy.trader.constant import Interval
from vnpy_ctastrategy.backtesting import BacktestingEngine, BacktestingMode
from app.strategies.triple_ma_strategy import TripleMAStrategy
from app.backtest.ts_utils import moving_average, pct_change


def inspect():
    vt = "000001.SZSE"
    start = datetime(2021, 1, 1)
    end = datetime.now()

    engine = BacktestingEngine()
    engine.set_parameters(
        vt_symbol=vt,
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

    print("Loading data...")
    engine.load_data()
    print("history_data count:", len(engine.history_data))

    print("Running backtest...")
    engine.run_backtesting()

    print("Trades count:", len(engine.trades))
    trades = list(engine.trades.values())
    for t in trades[:10]:
        print({
            'vt_tradeid': t.vt_tradeid,
            'symbol': t.symbol,
            'direction': str(t.direction),
            'offset': str(t.offset),
            'price': t.price,
            'volume': t.volume,
            'datetime': t.datetime,
        })

    dr_keys = sorted(engine.daily_results.keys())
    print("daily_results entries:", len(dr_keys))
    for d in dr_keys[:10]:
        dr = engine.daily_results[d]
        dd = {k: v for k, v in dr.__dict__.items() if k != 'trades'}
        print(f"{d}: ", dd)
        print("  trades in daily_result:", len(getattr(dr, 'trades', [])))

    # Build daily_df from trades/daily_results first
    df = engine.calculate_result()
    print("daily_df empty:", df.empty if df is not None else 'no-df')

    # compute indicators on daily df
    if df is not None and not df.empty and 'close_price' in df.columns:
        setting = TripleMAStrategy.get_class_parameters()
        df['ma_mid'] = moving_average(df['close_price'], setting.get('mid_window', 10))
        df['daily_return'] = pct_change(df['close_price'])

    stats = engine.calculate_statistics(df)
    print("calculate_statistics returned:", stats)


if __name__ == '__main__':
    inspect()
