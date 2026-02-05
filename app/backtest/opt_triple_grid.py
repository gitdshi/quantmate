from datetime import datetime
import sys
from pathlib import Path

# Ensure project root is importable
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import itertools
from vnpy.trader.constant import Interval
from vnpy_ctastrategy.backtesting import BacktestingEngine, BacktestingMode
from app.strategies.triple_ma_strategy import TripleMAStrategy
from app.backtest.ts_utils import moving_average, pct_change


def run_single(setting, vt_symbol, start, end):
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

    engine.add_strategy(TripleMAStrategy, setting)
    engine.load_data()
    if not engine.history_data:
        return None
    engine.run_backtesting()
    # ensure daily_df is built so downstream code can inspect indicators
    try:
        _ = engine.calculate_result()
    except Exception:
        pass
    stats = engine.calculate_statistics(output=False)
    return stats


def main():
    vt_symbol = "000001.SZSE"
    start = datetime(2020, 1, 1)
    end = datetime(2020, 12, 31)

    # Grid: fast < mid < slow
    fast_opts = [3, 5, 7]
    mid_opts = [8, 10, 13]
    slow_opts = [15, 20, 30]

    results = []

    combos = []
    for f, m, s in itertools.product(fast_opts, mid_opts, slow_opts):
        if f < m < s:
            combos.append((f, m, s))

    print(f"Running {len(combos)} combinations...")

    for f, m, s in combos:
        setting = TripleMAStrategy.get_class_parameters()
        setting["fast_window"] = f
        setting["mid_window"] = m
        setting["slow_window"] = s
        setting["fixed_size"] = 1

        stats = run_single(setting, vt_symbol, start, end)
        if not stats:
            print(f"No/empty stats for {f},{m},{s}")
            continue
        results.append((f, m, s, stats))
        tr = float(stats.get('total_return', 0) or 0)
        eb = float(stats.get('end_balance', 0) or 0)
        print(f"Done {f},{m},{s}: ret={tr:.6f}, end_balance={eb:.2f}")

    # Sort by total_return desc
    results_sorted = sorted(results, key=lambda x: float(x[3].get('total_return', 0)), reverse=True)

    print("\nTop 5 results:")
    for row in results_sorted[:5]:
        f, m, s, st = row
        print(f"fast={f}, mid={m}, slow={s} -> total_return={st.get('total_return'):.6f}, end_balance={st.get('end_balance'):.2f}, trades={int(st.get('total_trade_count',0))}")


if __name__ == '__main__':
    main()
