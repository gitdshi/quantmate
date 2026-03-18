"""
VNPy Data Ingestion Module

Syncs stock daily data from tushare database to vnpy database.
Converts tushare stock_daily format to vnpy dbbardata format.
"""

import logging
from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple

from app.domains.extdata.dao.tushare_dao import (
    fetch_stock_daily_rows,
    get_all_ts_codes,
)
from app.domains.extdata.dao.data_sync_status_dao import (
    get_stock_daily_ts_codes_for_date,
)
from app.domains.extdata.dao.vnpy_dao import (
    bulk_upsert_dbbardata,
    get_last_sync_date,
    update_sync_status,
    get_bar_stats,
    upsert_dbbaroverview,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Exchange mapping
EXCHANGE_MAP = {
    "SH": "SSE",
    "SZ": "SZSE",
    "BJ": "BSE",
}


def get_symbol(ts_code: str) -> str:
    """Extract symbol from ts_code (e.g., '000001.SZ' -> '000001')."""
    return ts_code.split(".")[0] if "." in ts_code else ts_code


def map_exchange(ts_code: str) -> str:
    """Map tushare ts_code suffix to vnpy exchange string."""
    suffix = ts_code.split(".")[-1] if "." in ts_code else "SZ"
    return EXCHANGE_MAP.get(suffix, "SZSE")


def sync_symbol_to_vnpy(ts_code: str, start_date: Optional[date] = None) -> int:
    """Sync a single symbol's daily data from tushare_data to vnpy_data.

    Args:
        ts_code: Tushare stock code (e.g., '000001.SZ')
        start_date: Sync data starting from this date (None = from last sync)

    Returns:
        Number of bars synced
    """
    symbol = get_symbol(ts_code)
    exchange = map_exchange(ts_code)
    interval = "d"

    # Determine start date for incremental sync
    if start_date is None:
        last_sync = get_last_sync_date(symbol, exchange, interval)
        if last_sync:
            start_date = last_sync + timedelta(days=1)

    # Fetch rows from tushare database via DAO
    rows = fetch_stock_daily_rows(ts_code, start_date)

    if not rows:
        logger.debug(f"No new data to sync for {ts_code}")
        return 0

    # Prepare rows for bulk insert into vnpy dbbardata
    to_insert = []
    last_date = None

    for row in rows:
        trade_date = row[0]
        if isinstance(trade_date, str):
            dt = datetime.strptime(trade_date, "%Y-%m-%d")
        else:
            dt = datetime.combine(trade_date, datetime.min.time())

        to_insert.append(
            {
                "symbol": symbol,
                "exchange": exchange,
                "datetime": dt,
                "interval": interval,
                "volume": float(row[5]) if row[5] else 0.0,
                "turnover": float(row[6]) if row[6] else 0.0,
                "open_interest": 0.0,
                "open_price": float(row[1]) if row[1] else 0.0,
                "high_price": float(row[2]) if row[2] else 0.0,
                "low_price": float(row[3]) if row[3] else 0.0,
                "close_price": float(row[4]) if row[4] else 0.0,
            }
        )
        last_date = trade_date

    # Bulk upsert into vnpy dbbardata via DAO
    synced = bulk_upsert_dbbardata(to_insert)

    # Update sync status
    if last_date:
        if isinstance(last_date, str):
            last_date = datetime.strptime(last_date, "%Y-%m-%d").date()
        update_sync_status(symbol, exchange, interval, last_date, synced)

    return synced


def update_bar_overview(symbol: str, exchange: str, interval: str = "d"):
    """Update the bar overview for a symbol after sync."""
    cnt, start_dt, end_dt = get_bar_stats(symbol, exchange, interval)
    if cnt and cnt > 0:
        upsert_dbbaroverview(symbol, exchange, interval, cnt, start_dt, end_dt)


def sync_date_to_vnpy(sync_date: date) -> Tuple[int, int]:
    """Sync specific date's Tushare data into vnpy database.

    More efficient than syncing all data, as it only processes one date.

    Args:
        sync_date: The date to sync

    Returns:
        Tuple of (symbols_synced, total_bars_synced)
    """
    logger.info("[VNPy %s] Starting date-specific sync...", sync_date)

    # Get all ts_codes from tushare stock_daily for this date
    ts_codes = get_stock_daily_ts_codes_for_date(sync_date)

    if not ts_codes:
        logger.warning("[VNPy %s] No data found in stock_daily for this date", sync_date)
        return 0, 0

    total_bars = 0
    total_symbols = 0

    for ts_code in ts_codes:
        try:
            bars = sync_symbol_to_vnpy(ts_code, start_date=sync_date)
            if bars > 0:
                symbol = get_symbol(ts_code)
                exchange = map_exchange(ts_code)
                update_bar_overview(symbol, exchange)
                total_bars += bars
                total_symbols += 1
        except Exception as exc:
            logger.warning("[VNPy %s] Failed to sync %s: %s", sync_date, ts_code, exc)
            continue

    logger.info("[VNPy %s] Synced %d symbols, %d bars", sync_date, total_symbols, total_bars)
    return total_symbols, total_bars


def sync_all_to_vnpy(ts_codes: Optional[List[str]] = None, full_refresh: bool = False) -> Tuple[int, int]:
    """Sync all data from tushare_data to vnpy_data.

    Args:
        ts_codes: List of ts_codes to sync (None = all)
        full_refresh: If True, re-sync all data; if False, incremental sync

    Returns:
        Tuple of (symbols_synced, total_bars_synced)
    """
    # Get all ts_codes from tushare_data if not specified
    if ts_codes is None:
        ts_codes = get_all_ts_codes()

    if not ts_codes:
        logger.warning("No symbols found in tushare_data to sync")
        return 0, 0

    logger.info(f"Syncing {len(ts_codes)} symbols to vnpy_data...")

    total_symbols = 0
    total_bars = 0

    for i, ts_code in enumerate(ts_codes, 1):
        try:
            start_date = None if not full_refresh else None
            bars = sync_symbol_to_vnpy(ts_code, start_date)

            if bars > 0:
                symbol = get_symbol(ts_code)
                exchange = map_exchange(ts_code)
                update_bar_overview(symbol, exchange)
                total_symbols += 1
                total_bars += bars
                logger.info(f"[{i}/{len(ts_codes)}] Synced {bars} bars for {ts_code}")

        except Exception as e:
            logger.error(f"Error syncing {ts_code}: {e}")
            continue

    logger.info(f"Sync complete: {total_symbols} symbols, {total_bars} bars")
    return total_symbols, total_bars
