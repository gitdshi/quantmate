#!/usr/bin/env python3
"""Backfill missing data in akshare and tushare databases.

This script checks for missing data across index_daily, stock_basic, and stock_daily
tables and backfills them using the existing ingestion functions.

Run from the tradermate folder with venv activated:
    .venv/bin/python3 scripts/backfill_missing_data.py
"""
import sys
import os
from datetime import datetime, timedelta
from sqlalchemy import text

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.infrastructure.db.connections import (
    get_akshare_connection,
    get_tushare_connection,
    get_tradermate_connection,
)
from app.services import akshare_ingest, tushare_ingest


def check_index_daily():
    """Check and backfill missing index_daily data."""
    print("\n=== Checking index_daily data ===")
    
    ak_conn = get_akshare_connection()
    ts_conn = get_tushare_connection()
    
    try:
        # Get all index codes from both DBs
        ak_codes = set([r[0] for r in ak_conn.execute(text("SELECT DISTINCT index_code FROM index_daily")).fetchall()])
        ts_codes = set([r[0] for r in ts_conn.execute(text("SELECT DISTINCT index_code FROM index_daily")).fetchall()])
        
        all_codes = sorted(ak_codes | ts_codes)
        print(f"Found {len(all_codes)} unique index codes across both DBs")
        
        for code in all_codes:
            # Get date ranges
            ak_dates = set()
            ts_dates = set()
            
            if code in ak_codes:
                rows = ak_conn.execute(text("SELECT DISTINCT trade_date FROM index_daily WHERE index_code = :c"), {"c": code}).fetchall()
                ak_dates = set([r[0].isoformat()[:10] if hasattr(r[0], 'isoformat') else str(r[0]) for r in rows])
            
            if code in ts_codes:
                rows = ts_conn.execute(text("SELECT DISTINCT trade_date FROM index_daily WHERE index_code = :c"), {"c": code}).fetchall()
                ts_dates = set([r[0].isoformat()[:10] if hasattr(r[0], 'isoformat') else str(r[0]) for r in rows])
            
            missing_in_ak = sorted(ts_dates - ak_dates)
            missing_in_ts = sorted(ak_dates - ts_dates)
            
            if missing_in_ak or missing_in_ts:
                print(f"\n{code}:")
                print(f"  AkShare: {len(ak_dates)} dates, missing {len(missing_in_ak)}")
                print(f"  Tushare: {len(ts_dates)} dates, missing {len(missing_in_ts)}")
                
                # Backfill akshare
                if missing_in_ak:
                    reverse_map = {v: k for k, v in akshare_ingest.INDEX_MAPPING.items()}
                    ak_sym = reverse_map.get(code)
                    if ak_sym:
                        print(f"  → Backfilling AkShare for {code} using symbol {ak_sym}...")
                        try:
                            rows = akshare_ingest.ingest_index_daily(symbol=ak_sym)
                            print(f"    ✓ Ingested {rows} rows")
                        except Exception as e:
                            print(f"    ✗ Failed: {e}")
                
                # Backfill tushare (skip if permission denied)
                if missing_in_ts:
                    start_date = missing_in_ts[0].replace('-', '')
                    end_date = missing_in_ts[-1].replace('-', '')
                    print(f"  → Backfilling Tushare for {code} from {start_date} to {end_date}...")
                    try:
                        rows = tushare_ingest.ingest_index_daily(ts_code=code, start_date=start_date, end_date=end_date)
                        print(f"    ✓ Ingested {rows} rows")
                    except Exception as e:
                        err_msg = str(e)
                        if '没有接口访问权限' in err_msg or 'permission' in err_msg.lower():
                            print(f"    ⚠ Skipped (requires higher Tushare permission)")
                        else:
                            print(f"    ✗ Failed: {e}")
    
    finally:
        ak_conn.close()
        ts_conn.close()


def check_stock_basic():
    """Check and backfill missing stock_basic data."""
    print("\n=== Checking stock_basic data ===")
    
    ts_conn = get_tushare_connection()
    vnpy_conn = get_tradermate_connection()
    
    try:
        # Check if tushare stock_basic has data
        ts_count = ts_conn.execute(text("SELECT COUNT(*) FROM stock_basic")).fetchone()[0]
        print(f"Tushare stock_basic: {ts_count} stocks")
        
        if ts_count == 0:
            print("  → Ingesting stock_basic from Tushare...")
            try:
                tushare_ingest.ingest_stock_basic()
                ts_count = ts_conn.execute(text("SELECT COUNT(*) FROM stock_basic")).fetchone()[0]
                print(f"    ✓ Ingested {ts_count} stocks")
            except Exception as e:
                print(f"    ✗ Failed: {e}")
        
        # Check vnpy stock_basic
        vnpy_count = vnpy_conn.execute(text("SELECT COUNT(*) FROM stock_basic")).fetchone()[0]
        print(f"VNPy stock_basic: {vnpy_count} stocks")
        
        if vnpy_count < ts_count:
            print(f"  → Syncing {ts_count - vnpy_count} stocks to VNPy...")
            # Copy from tushare to vnpy
            try:
                vnpy_conn.execute(text("""
                    INSERT IGNORE INTO stock_basic 
                    SELECT * FROM tushare.stock_basic
                """))
                vnpy_conn.commit()
                vnpy_count = vnpy_conn.execute(text("SELECT COUNT(*) FROM stock_basic")).fetchone()[0]
                print(f"    ✓ VNPy now has {vnpy_count} stocks")
            except Exception as e:
                print(f"    ✗ Failed: {e}")
    
    finally:
        ts_conn.close()
        vnpy_conn.close()


def check_stock_daily_coverage():
    """Check stock_daily data coverage (last 30 days)."""
    print("\n=== Checking stock_daily data coverage ===")
    
    ts_conn = get_tushare_connection()
    
    try:
        # Get recent trading dates
        today = datetime.now().date()
        thirty_days_ago = today - timedelta(days=30)
        
        # Check how many stocks have recent data
        result = ts_conn.execute(text("""
            SELECT 
                COUNT(DISTINCT ts_code) as stock_count,
                MAX(trade_date) as latest_date
            FROM stock_daily
            WHERE trade_date >= :start_date
        """), {"start_date": thirty_days_ago}).fetchone()
        
        stock_count = result[0]
        latest_date = result[1]
        
        print(f"Stocks with data in last 30 days: {stock_count}")
        print(f"Latest trade date: {latest_date}")
        
        if latest_date:
            # Check if we're missing recent days
            days_behind = (today - latest_date).days if hasattr(latest_date, '__sub__') else 0
            if days_behind > 5:  # More than 5 days behind
                print(f"  ⚠ Data is {days_behind} days behind!")
                print("  → Consider running: tushare_ingest.ingest_daily(start_date=...)")
            else:
                print(f"  ✓ Data is up to date (last update: {latest_date})")
        else:
            print("  ⚠ No recent data found")
    
    finally:
        ts_conn.close()


def get_recent_trading_dates(limit=10):
    """Get recent trading dates from index_daily as reference."""
    ak_conn = get_akshare_connection()
    try:
        rows = ak_conn.execute(text("""
            SELECT DISTINCT trade_date 
            FROM index_daily 
            WHERE index_code = '399300.SZ'
            ORDER BY trade_date DESC 
            LIMIT :lim
        """), {"lim": limit}).fetchall()
        return [r[0] for r in rows]
    finally:
        ak_conn.close()


def check_data_freshness():
    """Check overall data freshness across all tables."""
    print("\n=== Checking Data Freshness ===")
    
    ak_conn = get_akshare_connection()
    ts_conn = get_tushare_connection()
    vnpy_conn = get_tradermate_connection()
    
    try:
        tables = [
            ('akshare', ak_conn, 'index_daily', 'trade_date'),
            ('tushare', ts_conn, 'index_daily', 'trade_date'),
            ('tushare', ts_conn, 'stock_daily', 'trade_date'),
            ('vnpy', vnpy_conn, 'stock_daily', 'trade_date'),
        ]
        
        for db_name, conn, table, date_col in tables:
            try:
                result = conn.execute(text(f"""
                    SELECT MAX({date_col}) as latest, COUNT(*) as total 
                    FROM {table}
                """)).fetchone()
                latest = result[0]
                total = result[1]
                print(f"{db_name}.{table}: {total:,} rows, latest: {latest}")
            except Exception as e:
                print(f"{db_name}.{table}: Error - {e}")
    
    finally:
        ak_conn.close()
        ts_conn.close()
        vnpy_conn.close()


def main():
    """Main backfill routine."""
    print("=" * 60)
    print("TraderMate Data Backfill Utility")
    print("=" * 60)
    
    try:
        # Check freshness first
        check_data_freshness()
        
        # Check and backfill each data type
        check_index_daily()
        check_stock_basic()
        check_stock_daily_coverage()
        
        print("\n" + "=" * 60)
        print("Backfill complete!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Error during backfill: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
