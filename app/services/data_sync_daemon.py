"""
Data Sync Daemon for TraderMate

This daemon handles two main responsibilities:
1. Fetch market data from Tushare API and store in tushare_data database
2. Sync data from tushare_data to vnpy_data database for backtesting

Architecture:
- tushare_data: Raw market data from Tushare (source of truth)
- vnpy_data: Formatted data for vnpy trading platform (derived)

Usage:
    python app/services/data_sync_daemon.py --once          # Run once
    python app/services/data_sync_daemon.py --daemon        # Run as daemon
    python app/services/data_sync_daemon.py --sync-vnpy     # Sync to vnpy only
"""

import os
import sys
import time
import json
import logging
import argparse
import schedule
from datetime import datetime, date, timedelta
from typing import List, Optional, Tuple

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Add project root to path
sys.path.insert(0, str(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

from app.services.tushare_ingest import (
    ingest_daily,
    ingest_stock_basic,
    ingest_daily_basic,
    ingest_adj_factor,
    get_all_ts_codes,
    get_max_trade_date,
    engine as tushare_engine
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database URLs
TUSHARE_DB_URL = os.getenv('TUSHARE_DATABASE_URL', 'mysql+pymysql://root:password@127.0.0.1/tushare?charset=utf8mb4')
VNPY_DB_URL = os.getenv('VNPY_DATABASE_URL', 'mysql+pymysql://root:password@127.0.0.1/vnpy?charset=utf8mb4')

# Sync configuration
SYNC_HOUR = int(os.getenv('SYNC_HOUR', '18'))  # Default: 6 PM after market close
SYNC_MINUTE = int(os.getenv('SYNC_MINUTE', '0'))
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '100'))

# Exchange mapping for vnpy
EXCHANGE_MAP = {
    'SZ': 'SZSE',
    'SH': 'SSE',
}


class DataSyncDaemon:
    """Daemon for syncing data between Tushare and vnpy databases."""
    
    def __init__(self):
        self.tushare_engine: Engine = create_engine(TUSHARE_DB_URL, pool_pre_ping=True)
        self.vnpy_engine: Engine = create_engine(VNPY_DB_URL, pool_pre_ping=True)
        self.running = False
    
    def map_exchange(self, ts_code: str) -> str:
        """Map tushare ts_code suffix to vnpy exchange string."""
        suffix = ts_code.split('.')[-1] if '.' in ts_code else 'SZ'
        return EXCHANGE_MAP.get(suffix, 'SZSE')
    
    def get_symbol(self, ts_code: str) -> str:
        """Extract symbol from ts_code."""
        return ts_code.split('.')[0] if '.' in ts_code else ts_code
    
    # =========================================================================
    # Step 1: Fetch from Tushare API to tushare_data
    # =========================================================================
    
    def fetch_tushare_data(self, ts_codes: Optional[List[str]] = None, 
                           start_date: Optional[str] = None,
                           end_date: Optional[str] = None) -> int:
        """
        Fetch daily data from Tushare API and store in tushare_data database.
        
        Args:
            ts_codes: List of stock codes to fetch (None = all)
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format
            
        Returns:
            Number of records fetched
        """
        if ts_codes is None:
            ts_codes = get_all_ts_codes()
            if not ts_codes:
                # If no stocks in DB, first ingest stock_basic
                logger.info("No stocks in database, fetching stock_basic first...")
                ingest_stock_basic()
                ts_codes = get_all_ts_codes()
        
        total_fetched = 0
        for i, ts_code in enumerate(ts_codes, 1):
            try:
                # Get last trade date for incremental sync
                if start_date is None:
                    last_date = get_max_trade_date(ts_code)
                    if last_date:
                        start = (pd.to_datetime(last_date) + timedelta(days=1)).strftime('%Y%m%d')
                    else:
                        start = None
                else:
                    start = start_date
                
                logger.info(f"[{i}/{len(ts_codes)}] Fetching {ts_code} from {start or 'beginning'}...")
                ingest_daily(ts_code=ts_code, start_date=start, end_date=end_date)
                total_fetched += 1
                
                # Rate limiting
                time.sleep(0.2)
                
            except Exception as e:
                logger.error(f"Error fetching {ts_code}: {e}")
                continue
        
        logger.info(f"Tushare fetch complete: {total_fetched} symbols processed")
        return total_fetched
    
    # =========================================================================
    # Step 2: Sync from tushare_data to vnpy_data
    # =========================================================================
    
    def get_last_sync_date(self, symbol: str, exchange: str, interval: str = 'd') -> Optional[date]:
        """Get the last synced date for a symbol from vnpy_data."""
        with self.vnpy_engine.connect() as conn:
            result = conn.execute(text("""
                SELECT last_sync_date FROM sync_status 
                WHERE symbol = :symbol AND exchange = :exchange AND `interval` = :interval
            """), {'symbol': symbol, 'exchange': exchange, 'interval': interval})
            row = result.fetchone()
            return row[0] if row else None
    
    def update_sync_status(self, symbol: str, exchange: str, interval: str, 
                           sync_date: date, count: int):
        """Update sync status for a symbol."""
        with self.vnpy_engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO sync_status (symbol, exchange, `interval`, last_sync_date, last_sync_count)
                VALUES (:symbol, :exchange, :interval, :sync_date, :count)
                ON DUPLICATE KEY UPDATE 
                    last_sync_date = VALUES(last_sync_date),
                    last_sync_count = VALUES(last_sync_count),
                    updated_at = CURRENT_TIMESTAMP
            """), {
                'symbol': symbol,
                'exchange': exchange,
                'interval': interval,
                'sync_date': sync_date,
                'count': count
            })
    
    def sync_symbol_to_vnpy(self, ts_code: str, start_date: Optional[date] = None) -> int:
        """
        Sync a single symbol's daily data from tushare_data to vnpy_data.
        
        Args:
            ts_code: Tushare stock code (e.g., '000001.SZ')
            start_date: Sync data starting from this date (None = from last sync)
            
        Returns:
            Number of bars synced
        """
        symbol = self.get_symbol(ts_code)
        exchange = self.map_exchange(ts_code)
        interval = 'd'
        
        # Determine start date for incremental sync
        if start_date is None:
            last_sync = self.get_last_sync_date(symbol, exchange, interval)
            if last_sync:
                start_date = last_sync + timedelta(days=1)
        
        # Query data from tushare_data
        query = """
            SELECT trade_date, open, high, low, close, vol, amount
            FROM stock_daily
            WHERE ts_code = :ts_code
        """
        params = {'ts_code': ts_code}
        
        if start_date:
            query += " AND trade_date >= :start_date"
            params['start_date'] = start_date
        
        query += " ORDER BY trade_date ASC"
        
        with self.tushare_engine.connect() as conn:
            result = conn.execute(text(query), params)
            rows = result.fetchall()
        
        if not rows:
            logger.debug(f"No new data to sync for {ts_code}")
            return 0
        
        # Insert into vnpy_data.dbbardata
        insert_sql = text("""
            INSERT INTO dbbardata 
                (symbol, exchange, `datetime`, `interval`, volume, turnover, 
                 open_interest, open_price, high_price, low_price, close_price)
            VALUES 
                (:symbol, :exchange, :datetime, :interval, :volume, :turnover,
                 :open_interest, :open_price, :high_price, :low_price, :close_price)
            ON DUPLICATE KEY UPDATE
                volume = VALUES(volume),
                turnover = VALUES(turnover),
                open_price = VALUES(open_price),
                high_price = VALUES(high_price),
                low_price = VALUES(low_price),
                close_price = VALUES(close_price)
        """)
        
        synced = 0
        last_date = None
        
        with self.vnpy_engine.begin() as conn:
            for row in rows:
                trade_date = row[0]
                if isinstance(trade_date, str):
                    dt = datetime.strptime(trade_date, '%Y-%m-%d')
                else:
                    dt = datetime.combine(trade_date, datetime.min.time())
                
                conn.execute(insert_sql, {
                    'symbol': symbol,
                    'exchange': exchange,
                    'datetime': dt,
                    'interval': interval,
                    'volume': float(row[4]) if row[4] else 0.0,
                    'turnover': float(row[5]) if row[5] else 0.0,
                    'open_interest': 0.0,
                    'open_price': float(row[1]) if row[1] else 0.0,
                    'high_price': float(row[2]) if row[2] else 0.0,
                    'low_price': float(row[3]) if row[3] else 0.0,
                    'close_price': float(row[4]) if row[4] else 0.0,  # Fixed: was row[4] should be close
                })
                synced += 1
                last_date = trade_date
        
        # Update sync status
        if last_date:
            if isinstance(last_date, str):
                last_date = datetime.strptime(last_date, '%Y-%m-%d').date()
            self.update_sync_status(symbol, exchange, interval, last_date, synced)
        
        return synced
    
    def update_bar_overview(self, symbol: str, exchange: str, interval: str = 'd'):
        """Update the bar overview for a symbol after sync."""
        with self.vnpy_engine.begin() as conn:
            # Get bar statistics
            result = conn.execute(text("""
                SELECT COUNT(*), MIN(`datetime`), MAX(`datetime`)
                FROM dbbardata
                WHERE symbol = :symbol AND exchange = :exchange AND `interval` = :interval
            """), {'symbol': symbol, 'exchange': exchange, 'interval': interval})
            row = result.fetchone()
            
            if row and row[0] > 0:
                conn.execute(text("""
                    INSERT INTO dbbaroverview (symbol, exchange, `interval`, count, start, end)
                    VALUES (:symbol, :exchange, :interval, :count, :start, :end)
                    ON DUPLICATE KEY UPDATE
                        count = VALUES(count),
                        start = VALUES(start),
                        end = VALUES(end)
                """), {
                    'symbol': symbol,
                    'exchange': exchange,
                    'interval': interval,
                    'count': row[0],
                    'start': row[1],
                    'end': row[2]
                })
    
    def sync_to_vnpy(self, ts_codes: Optional[List[str]] = None, 
                     full_refresh: bool = False) -> Tuple[int, int]:
        """
        Sync all data from tushare_data to vnpy_data.
        
        Args:
            ts_codes: List of ts_codes to sync (None = all)
            full_refresh: If True, re-sync all data; if False, incremental sync
            
        Returns:
            Tuple of (symbols_synced, total_bars_synced)
        """
        # Get all ts_codes from tushare_data if not specified
        if ts_codes is None:
            with self.tushare_engine.connect() as conn:
                result = conn.execute(text("SELECT DISTINCT ts_code FROM stock_daily ORDER BY ts_code"))
                ts_codes = [row[0] for row in result.fetchall()]
        
        if not ts_codes:
            logger.warning("No symbols found in tushare_data to sync")
            return 0, 0
        
        logger.info(f"Syncing {len(ts_codes)} symbols to vnpy_data...")
        
        total_symbols = 0
        total_bars = 0
        
        for i, ts_code in enumerate(ts_codes, 1):
            try:
                start_date = None if not full_refresh else None  # Always incremental unless full_refresh
                
                if full_refresh:
                    # Clear existing data for this symbol (optional)
                    pass
                
                bars = self.sync_symbol_to_vnpy(ts_code, start_date if full_refresh else None)
                
                if bars > 0:
                    symbol = self.get_symbol(ts_code)
                    exchange = self.map_exchange(ts_code)
                    self.update_bar_overview(symbol, exchange)
                    total_symbols += 1
                    total_bars += bars
                    logger.info(f"[{i}/{len(ts_codes)}] Synced {bars} bars for {ts_code}")
                
            except Exception as e:
                logger.error(f"Error syncing {ts_code}: {e}")
                continue
        
        logger.info(f"Sync complete: {total_symbols} symbols, {total_bars} bars")
        return total_symbols, total_bars
    
    # =========================================================================
    # Step 3: Full sync pipeline
    # =========================================================================
    
    def run_daily_sync(self, ts_codes: Optional[List[str]] = None):
        """
        Run the full daily sync pipeline:
        1. Fetch new data from Tushare API to tushare_data
        2. Sync new data from tushare_data to vnpy_data
        """
        sync_date = date.today()
        logger.info(f"Starting daily sync for {sync_date}...")
        
        # Log sync start
        with self.tushare_engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO sync_log (sync_date, endpoint, status, started_at)
                VALUES (:sync_date, 'daily_sync', 'running', NOW())
                ON DUPLICATE KEY UPDATE status = 'running', started_at = NOW()
            """), {'sync_date': sync_date})
        
        try:
            # Step 1: Fetch from Tushare
            logger.info("Step 1: Fetching data from Tushare API...")
            fetched = self.fetch_tushare_data(ts_codes)
            
            # Step 2: Sync to vnpy
            logger.info("Step 2: Syncing to vnpy_data...")
            symbols, bars = self.sync_to_vnpy(ts_codes)
            
            # Log success
            with self.tushare_engine.begin() as conn:
                conn.execute(text("""
                    UPDATE sync_log 
                    SET status = 'success', rows_synced = :rows, finished_at = NOW()
                    WHERE sync_date = :sync_date AND endpoint = 'daily_sync'
                """), {'sync_date': sync_date, 'rows': bars})
            
            logger.info(f"Daily sync complete: {fetched} symbols fetched, {bars} bars synced")
            
        except Exception as e:
            logger.exception(f"Daily sync failed: {e}")
            with self.tushare_engine.begin() as conn:
                conn.execute(text("""
                    UPDATE sync_log 
                    SET status = 'error', error_message = :error, finished_at = NOW()
                    WHERE sync_date = :sync_date AND endpoint = 'daily_sync'
                """), {'sync_date': sync_date, 'error': str(e)})
            raise
    
    # =========================================================================
    # Daemon mode
    # =========================================================================
    
    def run_daemon(self):
        """Run as a background daemon with scheduled sync."""
        logger.info(f"Starting data sync daemon (scheduled at {SYNC_HOUR:02d}:{SYNC_MINUTE:02d})...")
        
        # Schedule daily sync
        schedule.every().day.at(f"{SYNC_HOUR:02d}:{SYNC_MINUTE:02d}").do(self.run_daily_sync)
        
        self.running = True
        try:
            while self.running:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
        except KeyboardInterrupt:
            logger.info("Daemon stopped by user")
        finally:
            self.running = False
    
    def stop(self):
        """Stop the daemon."""
        self.running = False


def main():
    parser = argparse.ArgumentParser(description="TraderMate Data Sync Daemon")
    parser.add_argument('--once', action='store_true', help='Run sync once and exit')
    parser.add_argument('--daemon', action='store_true', help='Run as background daemon')
    parser.add_argument('--sync-vnpy', action='store_true', help='Only sync tushare_data to vnpy_data')
    parser.add_argument('--fetch-only', action='store_true', help='Only fetch from Tushare API')
    parser.add_argument('--symbol', type=str, help='Sync specific symbol (e.g., 000001.SZ)')
    parser.add_argument('--all-symbols', action='store_true', help='Sync all symbols')
    parser.add_argument('--full-refresh', action='store_true', help='Full refresh (not incremental)')
    args = parser.parse_args()
    
    daemon = DataSyncDaemon()
    
    # Determine which symbols to process
    ts_codes = None
    if args.symbol:
        ts_codes = [args.symbol]
    elif args.all_symbols:
        ts_codes = None  # Will fetch all
    
    if args.daemon:
        daemon.run_daemon()
    elif args.sync_vnpy:
        daemon.sync_to_vnpy(ts_codes, full_refresh=args.full_refresh)
    elif args.fetch_only:
        daemon.fetch_tushare_data(ts_codes)
    elif args.once:
        daemon.run_daily_sync(ts_codes)
    else:
        # Default: run once
        daemon.run_daily_sync(ts_codes)


if __name__ == '__main__':
    main()
