"""
AkShare Data Ingestion Service

This module provides functions to fetch financial data from AkShare API
and store it in the akshare database. It also supports syncing data to
the tushare database for unified access.

AkShare is a free, open-source financial data library that doesn't require
API tokens or membership levels.

Key APIs used:
- stock_zh_index_daily: Index daily data (HS300, SSE50, etc.)
- stock_zh_a_spot_em: Stock basic info and real-time quotes
- stock_zh_a_hist: Stock historical daily data
"""

import os
import time
import logging
import pandas as pd
import akshare as ak
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from typing import Optional, List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database URLs
AKSHARE_DB_URL = os.getenv('AKSHARE_DATABASE_URL', 'mysql+pymysql://root:password@127.0.0.1:3306/akshare?charset=utf8mb4')
TUSHARE_DB_URL = os.getenv('TUSHARE_DATABASE_URL', 'mysql+pymysql://root:password@127.0.0.1:3306/tushare?charset=utf8mb4')

# Create engines
akshare_engine = create_engine(AKSHARE_DB_URL, pool_pre_ping=True)
tushare_engine = create_engine(TUSHARE_DB_URL, pool_pre_ping=True)

# Rate limiting
CALLS_PER_MIN = int(os.getenv('AKSHARE_CALLS_PER_MIN', '30'))
_MIN_INTERVAL = 60.0 / max(1, CALLS_PER_MIN)
_last_call = 0.0


def rate_limit():
    """Simple rate limiter for API calls."""
    global _last_call
    elapsed = time.time() - _last_call
    if elapsed < _MIN_INTERVAL:
        time.sleep(_MIN_INTERVAL - elapsed)
    _last_call = time.time()


def audit_start(api_name: str, params: dict) -> int:
    """Start an audit record for an ingestion operation."""
    import json
    with akshare_engine.begin() as conn:
        result = conn.execute(text(
            "INSERT INTO ingest_audit (api_name, params, status, fetched_rows) "
            "VALUES (:api, :params, 'running', 0)"
        ), {"api": api_name, "params": json.dumps(params)})
        return result.lastrowid


def audit_finish(audit_id: int, status: str, rows: int):
    """Finish an audit record."""
    with akshare_engine.begin() as conn:
        conn.execute(text(
            "UPDATE ingest_audit SET status=:status, fetched_rows=:rows, finished_at=NOW() WHERE id=:id"
        ), {"status": status, "rows": rows, "id": audit_id})


def sync_log_start(table_name: str, sync_type: str) -> int:
    """Start a sync log record."""
    with akshare_engine.begin() as conn:
        result = conn.execute(text(
            "INSERT INTO sync_log (table_name, sync_type, status) VALUES (:table, :type, 'running')"
        ), {"table": table_name, "type": sync_type})
        return result.lastrowid


def sync_log_finish(log_id: int, source_rows: int, synced_rows: int, status: str, error: str = None):
    """Finish a sync log record."""
    with akshare_engine.begin() as conn:
        conn.execute(text(
            "UPDATE sync_log SET source_rows=:source, synced_rows=:synced, status=:status, "
            "error_message=:error, finished_at=NOW() WHERE id=:id"
        ), {"source": source_rows, "synced": synced_rows, "status": status, "error": error, "id": log_id})


# ============================================================================
# INDEX DATA FUNCTIONS
# ============================================================================

# AkShare index symbol mapping (to tushare-style codes)
INDEX_MAPPING = {
    'sh000300': '399300.SZ',   # HS300 (use SZ code for consistency with your DB)
    'sh000001': '000001.SH',   # SSE Composite
    'sz399001': '399001.SZ',   # SZSE Component
    'sh000016': '000016.SH',   # SSE 50
    'sh000905': '000905.SH',   # CSI 500
    'sh000852': '000852.SH',   # CSI 1000
}


def ingest_index_daily(symbol: str = 'sh000300', start_date: str = None) -> int:
    """
    Fetch index daily data from AkShare and store in akshare.index_daily.
    
    Args:
        symbol: AkShare index symbol (e.g., 'sh000300' for HS300)
        start_date: Start date in YYYY-MM-DD format (optional, fetches all if None)
    
    Returns:
        Number of rows ingested
    """
    params = {'symbol': symbol, 'start_date': start_date}
    audit_id = audit_start('index_daily', params)
    
    try:
        rate_limit()
        logger.info(f"Fetching index daily data for {symbol}...")
        
        # Fetch data from AkShare
        df = ak.stock_zh_index_daily(symbol=symbol)
        
        if df is None or df.empty:
            logger.warning(f"No data returned for {symbol}")
            audit_finish(audit_id, 'success', 0)
            return 0
        
        # Convert to standard format
        index_code = INDEX_MAPPING.get(symbol, symbol.upper())
        
        # Rename columns to match schema
        df = df.rename(columns={
            'date': 'trade_date',
            'volume': 'volume'
        })
        
        # Add index_code column
        df['index_code'] = index_code
        
        # Filter by start_date if provided
        if start_date:
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            df = df[df['trade_date'] >= start_date]
        
        # Convert trade_date to string for SQL
        df['trade_date'] = df['trade_date'].astype(str)
        
        # Upsert to database
        rows = 0
        upsert_sql = text("""
            INSERT INTO index_daily (index_code, trade_date, open, high, low, close, volume, amount)
            VALUES (:index_code, :trade_date, :open, :high, :low, :close, :volume, :amount)
            ON DUPLICATE KEY UPDATE 
                open=VALUES(open), high=VALUES(high), low=VALUES(low), 
                close=VALUES(close), volume=VALUES(volume), amount=VALUES(amount)
        """)
        
        with akshare_engine.begin() as conn:
            for _, row in df.iterrows():
                conn.execute(upsert_sql, {
                    'index_code': index_code,
                    'trade_date': str(row['trade_date'])[:10],
                    'open': float(row['open']) if pd.notna(row['open']) else None,
                    'high': float(row['high']) if pd.notna(row['high']) else None,
                    'low': float(row['low']) if pd.notna(row['low']) else None,
                    'close': float(row['close']) if pd.notna(row['close']) else None,
                    'volume': int(row['volume']) if pd.notna(row['volume']) else None,
                    'amount': None  # AkShare doesn't provide amount for indexes
                })
                rows += 1
        
        logger.info(f"Ingested {rows} rows for index {index_code}")
        audit_finish(audit_id, 'success', rows)
        return rows
        
    except Exception as e:
        logger.exception(f"Error ingesting index {symbol}: {e}")
        audit_finish(audit_id, 'error', 0)
        raise


def ingest_all_indexes() -> dict:
    """Ingest daily data for all major indexes."""
    results = {}
    for ak_symbol, ts_code in INDEX_MAPPING.items():
        try:
            rows = ingest_index_daily(symbol=ak_symbol)
            results[ts_code] = {'status': 'success', 'rows': rows}
        except Exception as e:
            results[ts_code] = {'status': 'error', 'error': str(e)}
        time.sleep(1)  # Be nice to the API
    return results


# ============================================================================
# STOCK DATA FUNCTIONS
# ============================================================================

def ingest_stock_basic() -> int:
    """
    Fetch stock basic info from AkShare and store in akshare.stock_basic.
    Uses real-time quote API which includes basic stock info.
    
    Returns:
        Number of rows ingested
    """
    params = {}
    audit_id = audit_start('stock_basic', params)
    
    try:
        rate_limit()
        logger.info("Fetching stock basic info...")
        
        # Get all A-share stocks info
        df = ak.stock_zh_a_spot_em()
        
        if df is None or df.empty:
            logger.warning("No stock basic data returned")
            audit_finish(audit_id, 'success', 0)
            return 0
        
        # Rename and select columns
        df = df.rename(columns={
            '代码': 'symbol',
            '名称': 'name',
        })
        
        # Create ts_code (symbol.exchange)
        def make_ts_code(symbol):
            if symbol.startswith('6'):
                return f"{symbol}.SH"
            elif symbol.startswith(('0', '3')):
                return f"{symbol}.SZ"
            else:
                return f"{symbol}.SZ"
        
        df['ts_code'] = df['symbol'].apply(make_ts_code)
        
        # Upsert to database
        rows = 0
        upsert_sql = text("""
            INSERT INTO stock_basic (ts_code, symbol, name, is_active, updated_at)
            VALUES (:ts_code, :symbol, :name, TRUE, NOW())
            ON DUPLICATE KEY UPDATE name=VALUES(name), is_active=TRUE, updated_at=NOW()
        """)
        
        with akshare_engine.begin() as conn:
            for _, row in df.iterrows():
                conn.execute(upsert_sql, {
                    'ts_code': row['ts_code'],
                    'symbol': row['symbol'],
                    'name': row['name']
                })
                rows += 1
        
        logger.info(f"Ingested {rows} stock basic records")
        audit_finish(audit_id, 'success', rows)
        return rows
        
    except Exception as e:
        logger.exception(f"Error ingesting stock basic: {e}")
        audit_finish(audit_id, 'error', 0)
        raise


def ingest_stock_daily(symbol: str, start_date: str = None, end_date: str = None) -> int:
    """
    Fetch stock daily data from AkShare and store in akshare.stock_daily.
    
    Args:
        symbol: Stock symbol (e.g., '000001' for 平安银行)
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format
    
    Returns:
        Number of rows ingested
    """
    params = {'symbol': symbol, 'start_date': start_date, 'end_date': end_date}
    audit_id = audit_start('stock_daily', params)
    
    try:
        rate_limit()
        logger.info(f"Fetching stock daily data for {symbol}...")
        
        # Convert date format for AkShare (expects YYYYMMDD)
        kwargs = {'symbol': symbol, 'period': 'daily', 'adjust': 'qfq'}
        if start_date:
            kwargs['start_date'] = start_date.replace('-', '')
        if end_date:
            kwargs['end_date'] = end_date.replace('-', '')
        
        df = ak.stock_zh_a_hist(**kwargs)
        
        if df is None or df.empty:
            logger.warning(f"No data returned for {symbol}")
            audit_finish(audit_id, 'success', 0)
            return 0
        
        # Create ts_code
        if symbol.startswith('6'):
            ts_code = f"{symbol}.SH"
        else:
            ts_code = f"{symbol}.SZ"
        
        # Rename columns
        df = df.rename(columns={
            '日期': 'trade_date',
            '开盘': 'open',
            '最高': 'high',
            '最低': 'low',
            '收盘': 'close',
            '成交量': 'volume',
            '成交额': 'amount',
            '换手率': 'turnover'
        })
        
        # Upsert to database
        rows = 0
        upsert_sql = text("""
            INSERT INTO stock_daily (ts_code, trade_date, open, high, low, close, volume, amount, turnover)
            VALUES (:ts_code, :trade_date, :open, :high, :low, :close, :volume, :amount, :turnover)
            ON DUPLICATE KEY UPDATE 
                open=VALUES(open), high=VALUES(high), low=VALUES(low), close=VALUES(close),
                volume=VALUES(volume), amount=VALUES(amount), turnover=VALUES(turnover)
        """)
        
        with akshare_engine.begin() as conn:
            for _, row in df.iterrows():
                conn.execute(upsert_sql, {
                    'ts_code': ts_code,
                    'trade_date': str(row['trade_date'])[:10],
                    'open': float(row['open']) if pd.notna(row.get('open')) else None,
                    'high': float(row['high']) if pd.notna(row.get('high')) else None,
                    'low': float(row['low']) if pd.notna(row.get('low')) else None,
                    'close': float(row['close']) if pd.notna(row.get('close')) else None,
                    'volume': int(row['volume']) if pd.notna(row.get('volume')) else None,
                    'amount': float(row['amount']) if pd.notna(row.get('amount')) else None,
                    'turnover': float(row['turnover']) if pd.notna(row.get('turnover')) else None
                })
                rows += 1
        
        logger.info(f"Ingested {rows} rows for stock {ts_code}")
        audit_finish(audit_id, 'success', rows)
        return rows
        
    except Exception as e:
        logger.exception(f"Error ingesting stock {symbol}: {e}")
        audit_finish(audit_id, 'error', 0)
        raise


# ============================================================================
# SYNC FUNCTIONS (AkShare -> Tushare DB)
# ============================================================================

def sync_index_daily_to_tushare(index_code: str = None) -> dict:
    """
    Sync index_daily data from akshare DB to tushare DB.
    
    Args:
        index_code: Specific index to sync (e.g., '399300.SZ'), or None for all
    
    Returns:
        Dict with sync results
    """
    log_id = sync_log_start('index_daily', 'incremental' if index_code else 'full')
    
    try:
        # Read from akshare DB
        query = "SELECT index_code, trade_date, open, high, low, close, volume, amount FROM index_daily"
        if index_code:
            query += f" WHERE index_code = '{index_code}'"
        
        with akshare_engine.connect() as conn:
            result = conn.execute(text(query))
            rows = result.fetchall()
        
        if not rows:
            sync_log_finish(log_id, 0, 0, 'success')
            return {'status': 'success', 'source_rows': 0, 'synced_rows': 0}
        
        # Upsert to tushare DB
        synced = 0
        upsert_sql = text("""
            INSERT INTO index_daily (index_code, trade_date, open, high, low, close, vol, amount)
            VALUES (:index_code, :trade_date, :open, :high, :low, :close, :vol, :amount)
            ON DUPLICATE KEY UPDATE 
                open=VALUES(open), high=VALUES(high), low=VALUES(low), 
                close=VALUES(close), vol=VALUES(vol), amount=VALUES(amount)
        """)
        
        with tushare_engine.begin() as conn:
            for row in rows:
                conn.execute(upsert_sql, {
                    'index_code': row[0],
                    'trade_date': row[1],
                    'open': row[2],
                    'high': row[3],
                    'low': row[4],
                    'close': row[5],
                    'vol': row[6],  # tushare schema uses 'vol'
                    'amount': row[7]
                })
                synced += 1
        
        logger.info(f"Synced {synced}/{len(rows)} index_daily rows to tushare DB")
        sync_log_finish(log_id, len(rows), synced, 'success')
        return {'status': 'success', 'source_rows': len(rows), 'synced_rows': synced}
        
    except Exception as e:
        logger.exception(f"Error syncing index_daily: {e}")
        sync_log_finish(log_id, 0, 0, 'error', str(e))
        return {'status': 'error', 'error': str(e)}


def sync_stock_daily_to_tushare(ts_code: str = None, start_date: str = None) -> dict:
    """
    Sync stock_daily data from akshare DB to tushare DB.
    
    Args:
        ts_code: Specific stock to sync, or None for all
        start_date: Only sync data from this date onwards
    
    Returns:
        Dict with sync results
    """
    log_id = sync_log_start('stock_daily', 'incremental')
    
    try:
        # Read from akshare DB
        query = "SELECT ts_code, trade_date, open, high, low, close, volume, amount FROM stock_daily"
        conditions = []
        if ts_code:
            conditions.append(f"ts_code = '{ts_code}'")
        if start_date:
            conditions.append(f"trade_date >= '{start_date}'")
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        with akshare_engine.connect() as conn:
            result = conn.execute(text(query))
            rows = result.fetchall()
        
        if not rows:
            sync_log_finish(log_id, 0, 0, 'success')
            return {'status': 'success', 'source_rows': 0, 'synced_rows': 0}
        
        # Upsert to tushare DB
        synced = 0
        upsert_sql = text("""
            INSERT INTO stock_daily (ts_code, trade_date, open, high, low, close, vol, amount)
            VALUES (:ts_code, :trade_date, :open, :high, :low, :close, :vol, :amount)
            ON DUPLICATE KEY UPDATE 
                open=VALUES(open), high=VALUES(high), low=VALUES(low), 
                close=VALUES(close), vol=VALUES(vol), amount=VALUES(amount)
        """)
        
        with tushare_engine.begin() as conn:
            for row in rows:
                conn.execute(upsert_sql, {
                    'ts_code': row[0],
                    'trade_date': row[1],
                    'open': row[2],
                    'high': row[3],
                    'low': row[4],
                    'close': row[5],
                    'vol': row[6],
                    'amount': row[7]
                })
                synced += 1
        
        logger.info(f"Synced {synced}/{len(rows)} stock_daily rows to tushare DB")
        sync_log_finish(log_id, len(rows), synced, 'success')
        return {'status': 'success', 'source_rows': len(rows), 'synced_rows': synced}
        
    except Exception as e:
        logger.exception(f"Error syncing stock_daily: {e}")
        sync_log_finish(log_id, 0, 0, 'error', str(e))
        return {'status': 'error', 'error': str(e)}


def full_sync_to_tushare() -> dict:
    """
    Perform full sync of all data from akshare to tushare DB.
    
    Returns:
        Dict with results for each table
    """
    results = {}
    
    logger.info("Starting full sync from akshare to tushare...")
    
    # Sync index_daily
    results['index_daily'] = sync_index_daily_to_tushare()
    
    # Sync stock_daily (this could be large, so we do it incrementally)
    results['stock_daily'] = sync_stock_daily_to_tushare()
    
    logger.info(f"Full sync completed: {results}")
    return results


# ============================================================================
# CLI INTERFACE
# ============================================================================

def main():
    """Main CLI entry point."""
    import sys
    
    if len(sys.argv) < 2:
        print("""
AkShare Data Ingestion Tool

Usage:
    python akshare_ingest.py <command> [args]

Commands:
    index <symbol>      Ingest index daily data (default: sh000300 for HS300)
    index_all           Ingest all major indexes
    stock_basic         Ingest stock basic info
    stock <symbol>      Ingest stock daily data
    sync_index [code]   Sync index_daily to tushare DB
    sync_stock [code]   Sync stock_daily to tushare DB
    sync_all            Full sync to tushare DB
    
Examples:
    python akshare_ingest.py index sh000300
    python akshare_ingest.py index_all
    python akshare_ingest.py stock 000001
    python akshare_ingest.py sync_all
        """)
        return
    
    cmd = sys.argv[1]
    
    if cmd == 'index':
        symbol = sys.argv[2] if len(sys.argv) > 2 else 'sh000300'
        rows = ingest_index_daily(symbol=symbol)
        print(f"✅ Ingested {rows} rows for {symbol}")
        
    elif cmd == 'index_all':
        results = ingest_all_indexes()
        for code, res in results.items():
            status = '✅' if res['status'] == 'success' else '❌'
            print(f"{status} {code}: {res}")
            
    elif cmd == 'stock_basic':
        rows = ingest_stock_basic()
        print(f"✅ Ingested {rows} stock basic records")
        
    elif cmd == 'stock':
        if len(sys.argv) < 3:
            print("Error: Please provide stock symbol")
            return
        symbol = sys.argv[2]
        start = sys.argv[3] if len(sys.argv) > 3 else None
        rows = ingest_stock_daily(symbol=symbol, start_date=start)
        print(f"✅ Ingested {rows} rows for {symbol}")
        
    elif cmd == 'sync_index':
        code = sys.argv[2] if len(sys.argv) > 2 else None
        result = sync_index_daily_to_tushare(index_code=code)
        print(f"Sync result: {result}")
        
    elif cmd == 'sync_stock':
        code = sys.argv[2] if len(sys.argv) > 2 else None
        result = sync_stock_daily_to_tushare(ts_code=code)
        print(f"Sync result: {result}")
        
    elif cmd == 'sync_all':
        results = full_sync_to_tushare()
        print(f"Full sync results: {results}")
        
    else:
        print(f"Unknown command: {cmd}")


if __name__ == '__main__':
    main()
