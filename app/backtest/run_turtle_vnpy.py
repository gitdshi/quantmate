"""
Run vn.py CTA backtest for TurtleTradingStrategy on 000001.SZ using local MySQL data
Usage:
    python3 app/backtest/run_turtle_vnpy.py --symbol 000001.SZ --start 2010-01-01 --end 2025-12-31

Notes:
- This script reads bars from `tushare_stock_daily` table in the project's MySQL (via the engine in `app.services.tushare_ingest`).
- It maps tushare ts_code suffixes (e.g., .SZ -> SZSE) to vnpy Exchange values.
"""

import sys
import importlib.util
import argparse
from datetime import datetime

# Load the ingest module to reuse its SQLAlchemy engine
spec = importlib.util.spec_from_file_location('ti','app/services/tushare_ingest.py')
ti = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ti)
ENGINE = ti.engine

# vn.py imports
from vnpy.trader.object import BarData
from vnpy.trader.constant import Exchange, Interval
from vnpy_ctastrategy.backtesting import BacktestingEngine

# Load strategy class from file
spec2 = importlib.util.spec_from_file_location('turtle','app/strategies/turtle_trading.py')
mod = importlib.util.module_from_spec(spec2)
spec2.loader.exec_module(mod)
TurtleTradingStrategy = None
for name in dir(mod):
    obj = getattr(mod, name)
    if isinstance(obj, type) and name.endswith('TurtleTradingStrategy'):
        TurtleTradingStrategy = obj
        break
if not TurtleTradingStrategy:
    # fallback: check for class named TurtleTradingStrategy
    TurtleTradingStrategy = getattr(mod, 'TurtleTradingStrategy', None)

if not TurtleTradingStrategy:
    print('Could not locate TurtleTradingStrategy class in app/strategies/turtle_trading.py')
    sys.exit(2)


def map_exchange(suffix: str) -> Exchange:
    # tushare uses '.SZ' or '.SH'
    if suffix.upper().startswith('SZ') or suffix.upper().endswith('SZ') or suffix.upper()=='.SZ':
        return Exchange.SZSE
    if suffix.upper().startswith('SH') or suffix.upper().endswith('SH') or suffix.upper()=='.SH':
        return Exchange.SSE
    # fallback
    return Exchange.SZSE


def load_bars_from_db(ts_code: str, start: datetime, end: datetime):
    # Query local MySQL stock_daily table (tushare database)
    q = "SELECT trade_date, open, high, low, close, vol, amount FROM stock_daily WHERE ts_code=:ts AND trade_date BETWEEN :s AND :e ORDER BY trade_date ASC"
    s = start.strftime('%Y-%m-%d')
    e = end.strftime('%Y-%m-%d')
    rows = []
    with ENGINE.connect() as conn:
        res = conn.execute(ti.text(q), {'ts': ts_code, 's': s, 'e': e})
        rows = res.fetchall()

    bars = []
    # Parse ts_code like '000001.SZ'
    parts = ts_code.split('.')
    symbol = parts[0]
    suffix = parts[1] if len(parts) > 1 else 'SZ'
    exchange = map_exchange(suffix)

    for r in rows:
        trade_date = r[0]
        # trade_date may be stored as str or date
        if isinstance(trade_date, str):
            try:
                dt = datetime.strptime(trade_date, '%Y-%m-%d')
            except Exception:
                dt = datetime.strptime(trade_date, '%Y%m%d')
        else:
            # assume datetime.date or datetime
            if hasattr(trade_date, 'hour'):
                dt = trade_date
            else:
                dt = datetime.combine(trade_date, datetime.min.time())

        open_p, high_p, low_p, close_p, vol, amount = r[1], r[2], r[3], r[4], r[5], r[6]
        bar = BarData(
            "BACKTESTING",
            symbol,
            exchange,
            dt,
            Interval.DAILY,
            float(vol) if vol is not None else 0.0,
            float(amount) if amount is not None else 0.0,
            0,
            float(open_p) if open_p is not None else 0.0,
            float(high_p) if high_p is not None else 0.0,
            float(low_p) if low_p is not None else 0.0,
            float(close_p) if close_p is not None else 0.0,
        )
        bars.append(bar)

    return bars


def run_backtest(ts_code: str, start: datetime, end: datetime):
    bars = load_bars_from_db(ts_code, start, end)
    if not bars:
        print('No bars found for', ts_code)
        return

    vt_symbol = f"{ts_code.split('.')[0]}.{ 'SZSE' if ts_code.endswith('.SZ') else 'SSE' }"
    engine = BacktestingEngine()
    # engine expects Interval value string for set_parameters; pass Interval.DAILY
    engine.set_parameters(
        vt_symbol=vt_symbol,
        interval=Interval.DAILY,
        start=start,
        rate=0.0003,
        slippage=0.001,
        size=100,
        pricetick=0.01,
        capital=1000000,
    )

    engine.add_strategy(TurtleTradingStrategy, {})

    engine.history_data = bars

    engine.run_backtesting()

    df = engine.calculate_result()
    # empyrical/other libraries may expect np.NINF which was removed in NumPy 2.0
    try:
        import numpy as np
        if not hasattr(np, 'NINF'):
            setattr(np, 'NINF', -np.inf)
    except Exception:
        pass

    stats = engine.calculate_statistics()

    print('Backtest finished')
    print('Statistics:')
    for k,v in stats.items():
        print(k, v)

    # save daily results CSV
    try:
        df.to_csv(f'backtest_result_{ts_code.replace(".","_")}.csv', index=False)
        print('Saved CSV to backtest_result_{ts_code}.csv')
    except Exception as e:
        print('Could not save CSV:', e)


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--symbol', default='000001.SZ')
    p.add_argument('--start', default='2010-01-01')
    p.add_argument('--end', default=datetime.now().strftime('%Y-%m-%d'))
    args = p.parse_args()

    s = datetime.strptime(args.start, '%Y-%m-%d')
    e = datetime.strptime(args.end, '%Y-%m-%d')
    run_backtest(args.symbol, s, e)
