#!/usr/bin/env python3
"""Test available Tushare index APIs."""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv()
import tushare as ts

token = os.getenv('TUSHARE_TOKEN')
pro = ts.pro_api(token)

print('Testing Tushare index APIs...\n')

# 1. Try index_daily (requires 120+ points)
print('1. index_daily (399300.SZ):')
try:
    df = pro.index_daily(ts_code='399300.SZ', start_date='20250101', end_date='20250115')
    print(f'   ✅ Success: {len(df) if df is not None else 0} rows')
    if df is not None and not df.empty:
        print(df.head(3))
except Exception as e:
    print(f'   ❌ Error: {str(e)[:80]}')

# 2. Try index_global (全球指数) - may have different permission
print('\n2. index_global:')
try:
    df = pro.index_global(ts_code='SPX', start_date='20250101', end_date='20250115')
    print(f'   ✅ Success: {len(df) if df is not None else 0} rows')
except Exception as e:
    print(f'   ❌ Error: {str(e)[:80]}')

# 3. Check what's in stock_daily for a stock (to use as fallback benchmark)
print('\n3. stock_daily (000001.SZ as reference):')
try:
    df = pro.daily(ts_code='000001.SZ', start_date='20250101', end_date='20250115')
    print(f'   ✅ Success: {len(df) if df is not None else 0} rows')
except Exception as e:
    print(f'   ❌ Error: {str(e)[:80]}')

print('\n' + '='*60)
print('CONCLUSION:')
print('If index_daily fails, you need to either:')
print('1. Upgrade Tushare membership to 120+ points')
print('2. Manually load HS300 data into index_daily table')
print('3. Use a stock (like 000001.SZ) as a proxy benchmark')
