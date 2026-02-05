from datetime import datetime
import os
import sys
from pathlib import Path
import csv

# Ensure project root is importable
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# avoid importing app.main here to prevent side-effectful app startup (DB connections, watchers)
# import app.main  # noqa: F401

import pymysql
from vnpy.trader.constant import Interval
from vnpy_ctastrategy.backtesting import BacktestingEngine, BacktestingMode
from app.strategies.triple_ma_strategy import TripleMAStrategy
from app.backtest.ts_utils import moving_average, pct_change


def get_sz_symbols(conn):
    cur = conn.cursor()
    # Try common exchange values
    for exch in ("SZ", "SZSE"):
        try:
            cur.execute("SELECT symbol FROM stock_basic WHERE exchange=%s AND list_status='L'", (exch,))
            rows = cur.fetchall()
            if rows:
                return [r[0] for r in rows]
        except Exception:
            pass

    # Fallback: select where exchange LIKE 'SZ%'
    try:
        cur.execute("SELECT symbol FROM stock_basic WHERE exchange LIKE 'SZ%'")
        rows = cur.fetchall()
        return [r[0] for r in rows]
    except Exception:
        return []


def run_backtest_for_symbol(vt_symbol, start, end):
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
    # use default params
    engine.add_strategy(TripleMAStrategy, setting)

    engine.load_data()
    if not engine.history_data:
        return None

    engine.run_backtesting()
    # build daily results (aggregate trades into daily results) before statistics
    try:
        engine.calculate_result()
    except Exception:
        # fallback: ignore if method not present or fails
        pass

    stats = engine.calculate_statistics()
    return stats


def main():
    host = os.getenv("MYSQL_HOST", os.getenv("VN_DATABASE_HOST", "127.0.0.1"))
    port = int(os.getenv("MYSQL_PORT", os.getenv("VN_DATABASE_PORT", "3306")))
    user = os.getenv("MYSQL_USER", os.getenv("VN_DATABASE_USER", "root"))
    password = os.getenv("MYSQL_PASSWORD", os.getenv("VN_DATABASE_PASSWORD", "password"))
    db = os.getenv("TUSHARE_DB", "tushare")

    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=db)

    symbols = get_sz_symbols(conn)
    if not symbols:
        # Fallback: query available symbols from vnpy database overview
        from vnpy.trader.database import get_database
        from vnpy.trader.constant import Exchange

        db = get_database()
        overviews = db.get_bar_overview()
        symbols = [ov.symbol for ov in overviews if ov.exchange == Exchange.SZSE]

    print(f"Found {len(symbols)} SZ symbols")

    start = datetime(2021, 1, 1)
    end = datetime.now()

    results = []
    count = 0
    for sym in symbols:
        count += 1
        vt = f"{sym}.SZSE"
        print(f"({count}/{len(symbols)}) Backtesting {vt} ...")
        try:
            stats = run_backtest_for_symbol(vt, start, end)
        except Exception as e:
            print(f"Error backtesting {vt}: {e}")
            continue

        if not stats:
            print(f"No data for {vt}, skipped")
            continue

        results.append({
            "symbol": vt,
            "total_return": float(stats.get("total_return", 0)),
            "sharpe": float(stats.get("sharpe_ratio", 0)),
            "end_balance": float(stats.get("end_balance", 0)),
            "trades": int(stats.get("total_trade_count", 0)),
        })

    # sort and keep top 10 by total_return
    results.sort(key=lambda x: x["total_return"], reverse=True)
    top10 = results[:10]

    out_csv = ROOT / "backtest_top10_triple_sz.csv"
    with open(out_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["symbol", "total_return", "sharpe", "end_balance", "trades"])
        writer.writeheader()
        for r in top10:
            writer.writerow(r)

    print("Top 10 saved to", out_csv)
    for r in top10:
        print(r)


if __name__ == "__main__":
    main()
