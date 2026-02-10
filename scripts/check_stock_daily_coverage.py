#!/usr/bin/env python3
import sys
sys.path.insert(0,'.')
from app.services.data_sync_daemon import DataSyncDaemon
from datetime import date, timedelta
from sqlalchemy import text
import pandas as pd
import os
try:
    import akshare as ak
    AK_AVAILABLE = True
except Exception:
    AK_AVAILABLE = False

def main(days=60):
    d = DataSyncDaemon()
    end = date.today()-timedelta(days=1)
    start = end - timedelta(days=days-1)
    # Prefer AkShare trade calendar to avoid Tushare permission delays
    if AK_AVAILABLE:
        df = ak.tool_trade_date_hist_sina()
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        mask = (df['trade_date'].dt.date >= start) & (df['trade_date'].dt.date <= end)
        trade_days = df[mask]['trade_date'].dt.date.tolist()
    else:
        trade_days = d.get_trade_days(start,end)
    print('Trade days count:', len(trade_days))
    with d.tushare_engine.connect() as conn:
        res = conn.execute(text('SELECT trade_date, COUNT(1) as cnt FROM stock_daily WHERE trade_date BETWEEN :s AND :e GROUP BY trade_date'), {'s': start, 'e': end})
        rows = { (r[0] if not isinstance(r[0], str) else __import__('datetime').datetime.strptime(r[0], '%Y-%m-%d').date()): r[1] for r in res.fetchall() }
    present_dates = sorted(rows.keys())
    missing = [dt for dt in trade_days if dt not in present_dates]
    print('\nMissing trade dates (in trade calendar but no rows in stock_daily):')
    for m in missing:
        print(m)
    print('\nDates with low row counts (<1000):')
    for td,cnt in sorted(rows.items()):
        if cnt < 1000:
            print(td, cnt)
    print('\nSummary:')
    print('Trade days:', len(trade_days))
    print('Dates with data:', len(present_dates))
    print('Missing dates:', len(missing))

if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('--days', type=int, default=60)
    args = p.parse_args()
    main(args.days)
