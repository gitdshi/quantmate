"""Legacy data sync daemon helpers for QuantMate.

Operational CLI entrypoints now delegate to ``app.datasync.scheduler`` so the
DB-driven registry remains the source of truth for enabled interfaces. The
older helper functions in this module stay importable for backward
compatibility and focused unit tests.

Usage:
    python -m app.datasync.service.data_sync_daemon --daemon        # Run scheduler daemon
    python -m app.datasync.service.data_sync_daemon --daily         # Run daily sync once
    python -m app.datasync.service.data_sync_daemon --backfill      # Run backfill once
    python -m app.datasync.service.data_sync_daemon --init          # Reconcile sync status once
"""

import os
import sys
import time
import logging
import argparse
from datetime import datetime, date, timedelta
from typing import List, Optional, Tuple, Dict
from enum import Enum

import pandas as pd

# Add project root to path
sys.path.insert(0, str(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

# Import ingest modules
from app.infrastructure.logging.logging_setup import configure_logging
from app.infrastructure.config import get_runtime_bool, get_runtime_int, get_runtime_str
from app.datasync.service.tushare_ingest import (
    ingest_daily,
    ingest_stock_basic,
    ingest_adj_factor,
    ingest_dividend,
    ingest_top10_holders,
    ingest_daily_basic,
    ingest_repo,
    ingest_all_other_data,
    ingest_all_daily,
    ingest_dividend_by_date_range,
    ingest_top10_holders_by_date_range,
    ingest_adj_factor_by_date_range,
    ingest_weekly,
    ingest_monthly,
    ingest_index_daily,
    ingest_index_weekly,
    get_all_ts_codes,
    call_pro,
    upsert_daily,
)
from app.datasync.service.akshare_ingest import (
    ingest_index_daily as ak_ingest_index_daily,
    INDEX_MAPPING,
)
from app.datasync.service.vnpy_ingest import (
    sync_date_to_vnpy,
)

# Import DAOs
from app.domains.extdata.dao.data_sync_status_dao import (
    ensure_tables as _ensure_tables,
    write_step_status,
    get_step_status,
    get_failed_steps,
    get_cached_trade_dates,
    upsert_trade_dates,
    get_stock_basic_count,
    get_adj_factor_count_for_date,
    truncate_trade_cal,
    get_stock_daily_counts,
    get_bak_daily_counts,
    get_moneyflow_counts,
    get_suspend_d_counts,
    get_suspend_counts,
    get_adj_factor_counts,
    get_stock_weekly_counts,
    get_stock_monthly_counts,
    get_vnpy_counts,
    bulk_upsert_status,
    acquire_backfill_lock,
    release_backfill_lock,
    is_backfill_locked,
)

# Backward-compatible module export used by legacy tests and monkeypatch-based callers.
ensure_tables = _ensure_tables

from app.domains.extdata.dao.tushare_dao import (
    upsert_dividend_df,
)

# Legacy sync log DAO (used by Tushare-specific helpers)
from app.domains.extdata.dao.sync_log_dao import (
    write_tushare_stock_sync_log as dao_write_tushare_stock_sync_log,
    get_last_success_tushare_sync_date as dao_get_last_success_tushare_sync_date,
)

# Tushare daemon compatibility flags
DRY_RUN = get_runtime_bool(env_keys="DRY_RUN", db_key="datasync.dry_run", default=False)
LOOKBACK_DAYS = get_runtime_int(
    env_keys="LOOKBACK_DAYS",
    db_key="datasync.legacy_backfill_lookback_days",
    default=30,
)
LOOKBACK_YEARS = get_runtime_int(
    env_keys="LOOKBACK_YEARS",
    db_key="datasync.legacy_reconcile_lookback_years",
    default=15,
)


# Import AkShare for trade calendar
try:
    import akshare as ak

    AKSHARE_AVAILABLE = True
except ImportError:
    AKSHARE_AVAILABLE = False
    ak = None

LOG_LEVEL = getattr(
    logging,
    get_runtime_str(env_keys="LOG_LEVEL", db_key="logging.level", default="INFO").upper(),
    logging.INFO,
)
configure_logging(LOG_LEVEL)
logger = logging.getLogger(__name__)

# Configuration
SYNC_HOUR = get_runtime_int(env_keys="SYNC_HOUR", db_key="datasync.sync_hour", default=2)
SYNC_MINUTE = get_runtime_int(env_keys="SYNC_MINUTE", db_key="datasync.sync_minute", default=0)
BACKFILL_INTERVAL_HOURS = get_runtime_int(
    env_keys="BACKFILL_INTERVAL_HOURS",
    db_key="datasync.backfill_interval_hours",
    default=6,
)
BATCH_SIZE = get_runtime_int(env_keys="BATCH_SIZE", db_key="datasync.batch_size", default=100)
MAX_RETRIES = get_runtime_int(env_keys="MAX_RETRIES", db_key="datasync.max_retries", default=3)
TIMEZONE = get_runtime_str(env_keys="DATASYNC_TIMEZONE", db_key="datasync.timezone", default="Asia/Shanghai")

# Endpoints that must be synced (used by SyncStatusService)
REQUIRED_ENDPOINTS = [
    "akshare_index",
    "tushare_stock_basic",
    "tushare_stock_daily",
    "tushare_moneyflow",
    "tushare_adj_factor",
    "tushare_dividend",
    "tushare_top10_holders",
]


class SyncStep(str, Enum):
    """Sync step identifiers matching DB enum"""

    AKSHARE_INDEX = "akshare_index"
    TUSHARE_STOCK_BASIC = "tushare_stock_basic"
    TUSHARE_STOCK_DAILY = "tushare_stock_daily"
    TUSHARE_BAK_DAILY = "tushare_bak_daily"
    TUSHARE_MONEYFLOW = "tushare_moneyflow"
    TUSHARE_SUSPEND_D = "tushare_suspend_d"
    TUSHARE_SUSPEND = "tushare_suspend"
    TUSHARE_ADJ_FACTOR = "tushare_adj_factor"
    TUSHARE_DIVIDEND = "tushare_dividend"
    TUSHARE_TOP10_HOLDERS = "tushare_top10_holders"
    VNPY_SYNC = "vnpy_sync"
    TUSHARE_STOCK_WEEKLY = "tushare_stock_weekly"
    TUSHARE_STOCK_MONTHLY = "tushare_stock_monthly"
    TUSHARE_INDEX_DAILY = "tushare_index_daily"
    TUSHARE_INDEX_WEEKLY = "tushare_index_weekly"


class SyncStatus(str, Enum):
    """Sync status enum matching DB"""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    PARTIAL = "partial"
    ERROR = "error"


# =============================================================================
# Trade Calendar Management
# =============================================================================


def get_trade_calendar(start_date: date, end_date: date) -> List[date]:
    """Get trade dates from cached calendar or fetch from AkShare.

    First tries akshare.trade_cal table (cached), then fetches from AkShare API
    and caches the result for future use.
    """
    # Try cached calendar first (via DAO)
    try:
        dates = get_cached_trade_dates(start_date, end_date)
        if dates:
            logger.debug("Using cached trade calendar: %d dates", len(dates))
            return dates
    except Exception as e:
        logger.debug("trade_cal table not available or empty: %s", e)

    # Fetch from AkShare and cache
    if AKSHARE_AVAILABLE:
        try:
            df = ak.tool_trade_date_hist_sina()
            if df is not None and not df.empty:
                df["trade_date"] = pd.to_datetime(df["trade_date"])
                # Cache all dates to DB
                upsert_trade_dates([td.date() for td in df["trade_date"]])
                logger.info("Cached %d trade dates to akshare.trade_cal", len(df))

                # Filter requested range
                mask = (df["trade_date"].dt.date >= start_date) & (df["trade_date"].dt.date <= end_date)
                dates = df[mask]["trade_date"].dt.date.tolist()
                logger.debug("Fetched trade calendar from AkShare: %d dates", len(dates))
                return dates
        except Exception as e:
            logger.warning("AkShare trade calendar failed: %s", e)

    # Fallback to weekdays
    logger.debug("Using weekday fallback for trade calendar")
    days = []
    cur = start_date
    while cur <= end_date:
        if cur.weekday() < 5:  # Monday=0, Friday=4
            days.append(cur)
        cur += timedelta(days=1)
    return days


def get_previous_trade_date(offset: int = 1) -> date:
    """Get the Nth previous trade date."""
    end = date.today()
    start = end - timedelta(days=30)
    trade_days = get_trade_calendar(start, end)
    if trade_days:
        return trade_days[-offset] if len(trade_days) >= offset else trade_days[0]
    return date.today() - timedelta(days=offset)


def refresh_trade_calendar():
    """Refresh cached trade calendar from AkShare (call monthly)."""
    if not AKSHARE_AVAILABLE:
        logger.warning("AkShare not available, cannot refresh trade calendar")
        return

    try:
        df = ak.tool_trade_date_hist_sina()
        if df is None or df.empty:
            logger.warning("AkShare returned no trade dates")
            return

        df["trade_date"] = pd.to_datetime(df["trade_date"])
        # Truncate and re-insert
        truncate_trade_cal()
        upsert_trade_dates([td.date() for td in df["trade_date"]])
        logger.info("Refreshed trade calendar: %d dates cached", len(df))
    except Exception as e:
        logger.exception("Failed to refresh trade calendar: %s", e)


# =========================================================================
# Tushare-sync compatibility helpers (merged from tushare_sync_daemon.py)
# =========================================================================

ENDPOINTS = {
    "daily": lambda dt: (
        ingest_all_daily(start_date=None, sleep_between=0.02) if "ingest_all_daily" in globals() else None
    ),
    "daily_by_date": None,
    "daily_basic": lambda dt: ingest_daily_basic() if "ingest_daily_basic" in globals() else None,
    "adj_factor": lambda dt: ingest_all_other_data() if "ingest_all_other_data" in globals() else None,
    "moneyflow": lambda dt: ingest_all_other_data() if "ingest_all_other_data" in globals() else None,
    "dividend": lambda dt: ingest_all_other_data() if "ingest_all_other_data" in globals() else None,
    "top10_holders": lambda dt: ingest_all_other_data() if "ingest_all_other_data" in globals() else None,
    "margin": lambda dt: ingest_all_other_data() if "ingest_all_other_data" in globals() else None,
    "block_trade": lambda dt: ingest_all_other_data() if "ingest_all_other_data" in globals() else None,
    "repo": lambda dt: ingest_repo(repo_date=dt.strftime("%Y-%m-%d")) if "ingest_repo" in globals() else None,
}


def get_trade_days(start_d: date, end_d: date) -> List[str]:
    s = start_d.strftime("%Y%m%d")
    e = end_d.strftime("%Y%m%d")
    try:
        df = call_pro("trade_cal", exchange="SSE", start_date=s, end_date=e)
        if df is None:
            raise Exception("trade_cal returned None")
        df = df[df["is_open"] == 1]
        col = "calendar_date" if "calendar_date" in df.columns else ("cal_date" if "cal_date" in df.columns else None)
        dates = [str(pd.to_datetime(d).date()) for d in df[col]] if col else []
        return dates
    except Exception as exc:
        logger.warning("Could not use trade_cal (fallback to weekdays): %s", exc)
        days = []
        cur = start_d
        while cur <= end_d:
            if cur.weekday() < 5:
                days.append(str(cur))
            cur = cur + timedelta(days=1)
        return days


def write_sync_log(sync_date: date, endpoint: str, status: str, rows: int = 0, err: Optional[str] = None):
    if DRY_RUN:
        logger.info("DRY RUN - skip writing sync log: %s %s %s", sync_date, endpoint, status)
        return
    dao_write_tushare_stock_sync_log(sync_date, endpoint, status, rows, err)


def get_last_success_date(endpoint: str):
    return dao_get_last_success_tushare_sync_date(endpoint)


def sync_daily_for_date(d: date):
    logger.info("Starting daily sync for %s", d)
    ts_codes = get_all_ts_codes()
    total = len(ts_codes)
    rows_total = 0
    failures = 0
    for i, ts_code in enumerate(ts_codes, start=1):
        try:
            ingest_daily(ts_code=ts_code, start_date=d.strftime("%Y%m%d"), end_date=d.strftime("%Y%m%d"))
        except Exception as e:
            failures += 1
            logger.warning("Failed daily for %s on %s: %s", ts_code, d, e)
        time.sleep(0.02)
        if i % 500 == 0:
            logger.info("Daily sync progress: %d/%d", i, total)
    status = "success" if failures == 0 else "partial" if failures < total else "error"
    write_sync_log(d, "daily", status, rows_total, f"failures={failures}" if failures else None)
    logger.info("Daily sync finished for %s: status=%s failures=%d", d, status, failures)


def run_sync_for_date(d: date, allowed_endpoints: list):
    logger.info("Running sync for date %s, endpoints: %s", d, allowed_endpoints)
    for ep in allowed_endpoints:
        try:
            if ep == "daily":
                sync_daily_for_date(d)
            elif ep == "repo":
                try:
                    if not DRY_RUN:
                        ingest_repo(repo_date=d.strftime("%Y-%m-%d"))
                        write_sync_log(d, "repo", "success", 0, None)
                except Exception as e:
                    write_sync_log(d, "repo", "error", 0, str(e))
            else:
                try:
                    if ep == "daily_basic":
                        ingest_daily_basic()
                    if ep in (
                        "daily_basic",
                        "adj_factor",
                        "moneyflow",
                        "dividend",
                        "top10_holders",
                        "margin",
                        "block_trade",
                    ):
                        ingest_all_other_data()
                        write_sync_log(d, ep, "success", 0, None)
                except Exception as e:
                    write_sync_log(d, ep, "error", 0, str(e))
        except Exception as e:
            logger.exception("Error syncing endpoint %s for %s: %s", ep, d, e)
            write_sync_log(d, ep, "error", 0, str(e))


# =============================================================================
# Step Implementations
# =============================================================================


def run_akshare_index_step(sync_date: date) -> Tuple[SyncStatus, int, Optional[str]]:
    """Step 1: Ingest AkShare index daily data."""
    total_success = 0
    failures = []
    target_date = sync_date.strftime("%Y-%m-%d")

    for symbol in INDEX_MAPPING.keys():
        try:
            ak_ingest_index_daily(symbol=symbol, start_date=target_date)
            total_success += 1
        except Exception as e:
            logger.warning("AkShare index %s failed: %s", symbol, e)
            failures.append(symbol)

    if failures:
        err_msg = f"Failed symbols: {','.join(failures)}"
        if total_success > 0:
            return SyncStatus.PARTIAL, total_success, err_msg
        return SyncStatus.ERROR, 0, err_msg
    return SyncStatus.SUCCESS, total_success, None


def run_tushare_stock_basic_step(sync_date: date) -> Tuple[SyncStatus, int, Optional[str]]:
    """Step 2a: Refresh Tushare stock_basic metadata."""
    try:
        ingest_stock_basic()
        count = get_stock_basic_count()
        return SyncStatus.SUCCESS, count, None
    except Exception as e:
        logger.exception("stock_basic failed: %s", e)
        return SyncStatus.ERROR, 0, str(e)


def run_tushare_stock_daily_step(sync_date: date) -> Tuple[SyncStatus, int, Optional[str]]:
    """Step 2b: Ingest Tushare stock_daily using batch API."""
    target = sync_date.strftime("%Y%m%d")
    try:
        df = call_pro("daily", trade_date=target)
        if df is None or df.empty:
            return SyncStatus.SUCCESS, 0, "No trading data (non-trading day?)"
        rows = upsert_daily(df)
        return SyncStatus.SUCCESS, rows, None
    except Exception as e:
        logger.exception("stock_daily failed: %s", e)
        return SyncStatus.ERROR, 0, str(e)


def run_tushare_adj_factor_step(sync_date: date) -> Tuple[SyncStatus, int, Optional[str]]:
    """Step 2c: Ingest Tushare adj_factor using batch API."""
    target = sync_date.strftime("%Y%m%d")
    try:
        ingest_adj_factor(trade_date=target)
        count = get_adj_factor_count_for_date(sync_date)
        return SyncStatus.SUCCESS, count, None
    except Exception as e:
        logger.exception("adj_factor failed: %s", e)
        return SyncStatus.ERROR, 0, str(e)


def run_tushare_dividend_step(sync_date: date, use_batch: bool = False) -> Tuple[SyncStatus, int, Optional[str]]:
    """Step 2d: Ingest Tushare dividend data.

    For daily mode: Sample 100 stocks per-symbol
    For backfill mode: Use batch API by ann_date
    """
    if use_batch:
        try:
            target = sync_date.strftime("%Y%m%d")
            df = call_pro("dividend", ann_date=target)
            if df is None or df.empty:
                return SyncStatus.SUCCESS, 0, None

            rows = upsert_dividend_df(df)
            return SyncStatus.SUCCESS, rows, None
        except Exception as e:
            err_msg = str(e)
            if "没有接口访问权限" in err_msg or "permission" in err_msg.lower():
                return SyncStatus.PARTIAL, 0, "Permission denied"
            logger.exception("dividend batch failed: %s", e)
            return SyncStatus.ERROR, 0, str(e)
    else:
        # Daily mode: sample approach
        ts_codes = get_all_ts_codes()
        total = min(100, len(ts_codes))
        success = 0
        for code in ts_codes[:total]:
            try:
                ingest_dividend(ts_code=code)
                success += 1
            except Exception:
                pass
        if success > 0:
            return SyncStatus.SUCCESS, success, f"Sampled {success}/{total} stocks"
        return SyncStatus.PARTIAL, 0, "No dividends fetched"


def run_tushare_top10_holders_step(sync_date: date) -> Tuple[SyncStatus, int, Optional[str]]:
    """Step 2e: Ingest Tushare top10_holders (sample mode for daily)."""
    ts_codes = get_all_ts_codes()
    total = min(50, len(ts_codes))
    success = 0
    for code in ts_codes[:total]:
        try:
            ingest_top10_holders(ts_code=code)
            success += 1
        except Exception:
            pass
    if success > 0:
        return SyncStatus.SUCCESS, success, f"Sampled {success}/{total} stocks"
    return SyncStatus.PARTIAL, 0, "No holder data fetched"


def run_vnpy_sync_step(sync_date: date) -> Tuple[SyncStatus, int, Optional[str]]:
    """Step 3: Sync data to VNPy database."""
    try:
        total_symbols, total_bars = sync_date_to_vnpy(sync_date)
        if total_symbols > 0:
            return SyncStatus.SUCCESS, total_bars, None
        return SyncStatus.PARTIAL, 0, "No symbols synced"
    except Exception as e:
        logger.exception("VNPy sync failed: %s", e)
        return SyncStatus.ERROR, 0, str(e)


def run_tushare_stock_weekly_step(sync_date: date) -> Tuple[SyncStatus, int, Optional[str]]:
    """Ingest Tushare stock_weekly using batch API (trade_date)."""
    target = sync_date.strftime("%Y%m%d")
    try:
        rows = ingest_weekly(trade_date=target)
        return SyncStatus.SUCCESS, rows or 0, None
    except Exception as e:
        logger.exception("stock_weekly failed: %s", e)
        return SyncStatus.ERROR, 0, str(e)


def run_tushare_stock_monthly_step(sync_date: date) -> Tuple[SyncStatus, int, Optional[str]]:
    """Ingest Tushare stock_monthly using batch API (trade_date)."""
    target = sync_date.strftime("%Y%m%d")
    try:
        rows = ingest_monthly(trade_date=target)
        return SyncStatus.SUCCESS, rows or 0, None
    except Exception as e:
        logger.exception("stock_monthly failed: %s", e)
        return SyncStatus.ERROR, 0, str(e)


def run_tushare_index_daily_step(sync_date: date) -> Tuple[SyncStatus, int, Optional[str]]:
    """Ingest Tushare index_daily for major indices."""
    target = sync_date.strftime("%Y%m%d")
    index_codes = ["000001.SH", "399001.SZ", "399006.SZ", "000300.SH", "000905.SH"]
    total_rows = 0
    failures = []
    for code in index_codes:
        try:
            rows = ingest_index_daily(ts_code=code, start_date=target, end_date=target)
            total_rows += rows or 0
        except Exception as e:
            logger.warning("index_daily %s failed: %s", code, e)
            failures.append(code)
    if failures and total_rows == 0:
        return SyncStatus.ERROR, 0, f"Failed: {','.join(failures)}"
    if failures:
        return SyncStatus.PARTIAL, total_rows, f"Failed: {','.join(failures)}"
    return SyncStatus.SUCCESS, total_rows, None


def run_tushare_index_weekly_step(sync_date: date) -> Tuple[SyncStatus, int, Optional[str]]:
    """Ingest Tushare index_weekly for major indices."""
    target = sync_date.strftime("%Y%m%d")
    index_codes = ["000001.SH", "399001.SZ", "399006.SZ", "000300.SH", "000905.SH"]
    total_rows = 0
    failures = []
    for code in index_codes:
        try:
            rows = ingest_index_weekly(ts_code=code, start_date=target, end_date=target)
            total_rows += rows or 0
        except Exception as e:
            logger.warning("index_weekly %s failed: %s", code, e)
            failures.append(code)
    if failures and total_rows == 0:
        return SyncStatus.ERROR, 0, f"Failed: {','.join(failures)}"
    if failures:
        return SyncStatus.PARTIAL, total_rows, f"Failed: {','.join(failures)}"
    return SyncStatus.SUCCESS, total_rows, None


# =============================================================================
# Main Functions
# =============================================================================


def daily_ingest(target_date: Optional[date] = None, continue_on_error: bool = True) -> Dict[str, Dict]:
    """Run incremental daily sync for a specific date.

    Steps:
    1. AkShare index daily
    2. Tushare stock_basic + stock_daily + adj_factor + dividend + top10_holders
    3. VNPy sync

    Args:
        target_date: Date to sync (None = previous trade date)
        continue_on_error: If True, continue with other steps even if one fails

    Returns:
        Dict with step results
    """
    if target_date is None:
        target_date = get_previous_trade_date()

    logger.info("=" * 80)
    logger.info("Daily ingest starting for %s", target_date)
    logger.info("=" * 80)

    results = {}

    # Step 1: AkShare index
    logger.info("[Step 1/7] AkShare Index Daily")
    # Defensive check: skip if already successfully synced
    existing_status = get_step_status(target_date, SyncStep.AKSHARE_INDEX.value)
    status_val = None
    rows_proc = 0
    if isinstance(existing_status, str):
        status_val = existing_status
    elif isinstance(existing_status, dict):
        status_val = existing_status.get("status")
        rows_proc = existing_status.get("rows_processed", 0)
    if status_val == SyncStatus.SUCCESS.value:
        logger.info("[Step 1/7] akshare_index already synced (status=success), skipping")
        results["akshare_index"] = {"status": "success", "rows": rows_proc, "error": None, "skipped": True}
    else:
        write_step_status(target_date, SyncStep.AKSHARE_INDEX.value, SyncStatus.RUNNING.value)
        try:
            status, rows, err = run_akshare_index_step(target_date)
            write_step_status(target_date, SyncStep.AKSHARE_INDEX.value, status.value, rows, err)
            results["akshare_index"] = {"status": status.value, "rows": rows, "error": err}
            logger.info("[Step 1/7] akshare_index: %s (%d rows)", status.value, rows)
        except Exception as e:
            logger.exception("[Step 1/7] akshare_index failed: %s", e)
            write_step_status(target_date, SyncStep.AKSHARE_INDEX.value, SyncStatus.ERROR.value, 0, str(e))
            results["akshare_index"] = {"status": "error", "rows": 0, "error": str(e)}
            if not continue_on_error:
                return results

    # Step 2a: Stock basic
    logger.info("[Step 2a/7] Tushare Stock Basic")
    # Defensive check: skip if already successfully synced
    existing_status = get_step_status(target_date, SyncStep.TUSHARE_STOCK_BASIC.value)
    status_val = None
    rows_proc = 0
    if isinstance(existing_status, str):
        status_val = existing_status
    elif isinstance(existing_status, dict):
        status_val = existing_status.get("status")
        rows_proc = existing_status.get("rows_processed", 0)
    if status_val == SyncStatus.SUCCESS.value:
        logger.info("[Step 2a/7] tushare_stock_basic already synced (status=success), skipping")
        results["tushare_stock_basic"] = {"status": "success", "rows": rows_proc, "error": None, "skipped": True}
    else:
        write_step_status(target_date, SyncStep.TUSHARE_STOCK_BASIC.value, SyncStatus.RUNNING.value)
        try:
            status, rows, err = run_tushare_stock_basic_step(target_date)
            write_step_status(target_date, SyncStep.TUSHARE_STOCK_BASIC.value, status.value, rows, err)
            results["tushare_stock_basic"] = {"status": status.value, "rows": rows, "error": err}
            logger.info("[Step 2a/7] tushare_stock_basic: %s (%d rows)", status.value, rows)
        except Exception as e:
            logger.exception("[Step 2a/7] tushare_stock_basic failed: %s", e)
            write_step_status(target_date, SyncStep.TUSHARE_STOCK_BASIC.value, SyncStatus.ERROR.value, 0, str(e))
            results["tushare_stock_basic"] = {"status": "error", "rows": 0, "error": str(e)}
            if not continue_on_error:
                return results

    # Step 2b: Stock daily
    logger.info("[Step 2b/7] Tushare Stock Daily")
    # Defensive check: skip if already successfully synced
    existing_status = get_step_status(target_date, SyncStep.TUSHARE_STOCK_DAILY.value)
    status_val = None
    rows_proc = 0
    if isinstance(existing_status, str):
        status_val = existing_status
    elif isinstance(existing_status, dict):
        status_val = existing_status.get("status")
        rows_proc = existing_status.get("rows_processed", 0)
    if status_val == SyncStatus.SUCCESS.value:
        logger.info("[Step 2b/7] tushare_stock_daily already synced (status=success), skipping")
        results["tushare_stock_daily"] = {"status": "success", "rows": rows_proc, "error": None, "skipped": True}
    else:
        write_step_status(target_date, SyncStep.TUSHARE_STOCK_DAILY.value, SyncStatus.RUNNING.value)
        try:
            status, rows, err = run_tushare_stock_daily_step(target_date)
            write_step_status(target_date, SyncStep.TUSHARE_STOCK_DAILY.value, status.value, rows, err)
            results["tushare_stock_daily"] = {"status": status.value, "rows": rows, "error": err}
            logger.info("[Step 2b/7] tushare_stock_daily: %s (%d rows)", status.value, rows)
        except Exception as e:
            logger.exception("[Step 2b/7] tushare_stock_daily failed: %s", e)
            write_step_status(target_date, SyncStep.TUSHARE_STOCK_DAILY.value, SyncStatus.ERROR.value, 0, str(e))
            results["tushare_stock_daily"] = {"status": "error", "rows": 0, "error": str(e)}
            if not continue_on_error:
                return results

    # Step 2c: Adj factor
    logger.info("[Step 2c/7] Tushare Adj Factor")
    # Defensive check: skip if already successfully synced
    existing_status = get_step_status(target_date, SyncStep.TUSHARE_ADJ_FACTOR.value)
    status_val = None
    rows_proc = 0
    if isinstance(existing_status, str):
        status_val = existing_status
    elif isinstance(existing_status, dict):
        status_val = existing_status.get("status")
        rows_proc = existing_status.get("rows_processed", 0)
    if status_val == SyncStatus.SUCCESS.value:
        logger.info("[Step 2c/7] tushare_adj_factor already synced (status=success), skipping")
        results["tushare_adj_factor"] = {"status": "success", "rows": rows_proc, "error": None, "skipped": True}
    else:
        write_step_status(target_date, SyncStep.TUSHARE_ADJ_FACTOR.value, SyncStatus.RUNNING.value)
        try:
            status, rows, err = run_tushare_adj_factor_step(target_date)
            write_step_status(target_date, SyncStep.TUSHARE_ADJ_FACTOR.value, status.value, rows, err)
            results["tushare_adj_factor"] = {"status": status.value, "rows": rows, "error": err}
            logger.info("[Step 2c/7] tushare_adj_factor: %s (%d rows)", status.value, rows)
        except Exception as e:
            logger.exception("[Step 2c/7] tushare_adj_factor failed: %s", e)
            write_step_status(target_date, SyncStep.TUSHARE_ADJ_FACTOR.value, SyncStatus.ERROR.value, 0, str(e))
            results["tushare_adj_factor"] = {"status": "error", "rows": 0, "error": str(e)}
            if not continue_on_error:
                return results

    # Step 2d: Dividend (daily mode - sampled)
    logger.info("[Step 2d/7] Tushare Dividend")
    # Defensive check: skip if already successfully synced
    existing_status = get_step_status(target_date, SyncStep.TUSHARE_DIVIDEND.value)
    status_val = None
    rows_proc = 0
    if isinstance(existing_status, str):
        status_val = existing_status
    elif isinstance(existing_status, dict):
        status_val = existing_status.get("status")
        rows_proc = existing_status.get("rows_processed", 0)
    if status_val == SyncStatus.SUCCESS.value:
        logger.info("[Step 2d/7] tushare_dividend already synced (status=success), skipping")
        results["tushare_dividend"] = {"status": "success", "rows": rows_proc, "error": None, "skipped": True}
    else:
        write_step_status(target_date, SyncStep.TUSHARE_DIVIDEND.value, SyncStatus.RUNNING.value)
        try:
            status, rows, err = run_tushare_dividend_step(target_date, use_batch=False)
            write_step_status(target_date, SyncStep.TUSHARE_DIVIDEND.value, status.value, rows, err)
            results["tushare_dividend"] = {"status": status.value, "rows": rows, "error": err}
            logger.info("[Step 2d/7] tushare_dividend: %s (%d rows)", status.value, rows)
        except Exception as e:
            logger.exception("[Step 2d/7] tushare_dividend failed: %s", e)
            write_step_status(target_date, SyncStep.TUSHARE_DIVIDEND.value, SyncStatus.ERROR.value, 0, str(e))
            results["tushare_dividend"] = {"status": "error", "rows": 0, "error": str(e)}
            if not continue_on_error:
                return results

    # Step 2e: Top10 holders
    logger.info("[Step 2e/7] Tushare Top10 Holders")
    # Defensive check: skip if already successfully synced
    existing_status = get_step_status(target_date, SyncStep.TUSHARE_TOP10_HOLDERS.value)
    status_val = None
    rows_proc = 0
    if isinstance(existing_status, str):
        status_val = existing_status
    elif isinstance(existing_status, dict):
        status_val = existing_status.get("status")
        rows_proc = existing_status.get("rows_processed", 0)
    if status_val == SyncStatus.SUCCESS.value:
        logger.info("[Step 2e/7] tushare_top10_holders already synced (status=success), skipping")
        results["tushare_top10_holders"] = {"status": "success", "rows": rows_proc, "error": None, "skipped": True}
    else:
        write_step_status(target_date, SyncStep.TUSHARE_TOP10_HOLDERS.value, SyncStatus.RUNNING.value)
        try:
            status, rows, err = run_tushare_top10_holders_step(target_date)
            write_step_status(target_date, SyncStep.TUSHARE_TOP10_HOLDERS.value, status.value, rows, err)
            results["tushare_top10_holders"] = {"status": status.value, "rows": rows, "error": err}
            logger.info("[Step 2e/7] tushare_top10_holders: %s (%d rows)", status.value, rows)
        except Exception as e:
            logger.exception("[Step 2e/7] tushare_top10_holders failed: %s", e)
            write_step_status(target_date, SyncStep.TUSHARE_TOP10_HOLDERS.value, SyncStatus.ERROR.value, 0, str(e))
            results["tushare_top10_holders"] = {"status": "error", "rows": 0, "error": str(e)}
            if not continue_on_error:
                return results

    # Step 2f: Stock Weekly
    logger.info("[Step 2f/11] Tushare Stock Weekly")
    existing_status = get_step_status(target_date, SyncStep.TUSHARE_STOCK_WEEKLY.value)
    status_val = None
    rows_proc = 0
    if isinstance(existing_status, str):
        status_val = existing_status
    elif isinstance(existing_status, dict):
        status_val = existing_status.get("status")
        rows_proc = existing_status.get("rows_processed", 0)
    if status_val == SyncStatus.SUCCESS.value:
        logger.info("[Step 2f/11] tushare_stock_weekly already synced, skipping")
        results["tushare_stock_weekly"] = {"status": "success", "rows": rows_proc, "error": None, "skipped": True}
    else:
        write_step_status(target_date, SyncStep.TUSHARE_STOCK_WEEKLY.value, SyncStatus.RUNNING.value)
        try:
            status, rows, err = run_tushare_stock_weekly_step(target_date)
            write_step_status(target_date, SyncStep.TUSHARE_STOCK_WEEKLY.value, status.value, rows, err)
            results["tushare_stock_weekly"] = {"status": status.value, "rows": rows, "error": err}
            logger.info("[Step 2f/11] tushare_stock_weekly: %s (%d rows)", status.value, rows)
        except Exception as e:
            logger.exception("[Step 2f/11] tushare_stock_weekly failed: %s", e)
            write_step_status(target_date, SyncStep.TUSHARE_STOCK_WEEKLY.value, SyncStatus.ERROR.value, 0, str(e))
            results["tushare_stock_weekly"] = {"status": "error", "rows": 0, "error": str(e)}
            if not continue_on_error:
                return results

    # Step 2g: Stock Monthly
    logger.info("[Step 2g/11] Tushare Stock Monthly")
    existing_status = get_step_status(target_date, SyncStep.TUSHARE_STOCK_MONTHLY.value)
    status_val = None
    rows_proc = 0
    if isinstance(existing_status, str):
        status_val = existing_status
    elif isinstance(existing_status, dict):
        status_val = existing_status.get("status")
        rows_proc = existing_status.get("rows_processed", 0)
    if status_val == SyncStatus.SUCCESS.value:
        logger.info("[Step 2g/11] tushare_stock_monthly already synced, skipping")
        results["tushare_stock_monthly"] = {"status": "success", "rows": rows_proc, "error": None, "skipped": True}
    else:
        write_step_status(target_date, SyncStep.TUSHARE_STOCK_MONTHLY.value, SyncStatus.RUNNING.value)
        try:
            status, rows, err = run_tushare_stock_monthly_step(target_date)
            write_step_status(target_date, SyncStep.TUSHARE_STOCK_MONTHLY.value, status.value, rows, err)
            results["tushare_stock_monthly"] = {"status": status.value, "rows": rows, "error": err}
            logger.info("[Step 2g/11] tushare_stock_monthly: %s (%d rows)", status.value, rows)
        except Exception as e:
            logger.exception("[Step 2g/11] tushare_stock_monthly failed: %s", e)
            write_step_status(target_date, SyncStep.TUSHARE_STOCK_MONTHLY.value, SyncStatus.ERROR.value, 0, str(e))
            results["tushare_stock_monthly"] = {"status": "error", "rows": 0, "error": str(e)}
            if not continue_on_error:
                return results

    # Step 2h: Index Daily (Tushare)
    logger.info("[Step 2h/11] Tushare Index Daily")
    existing_status = get_step_status(target_date, SyncStep.TUSHARE_INDEX_DAILY.value)
    status_val = None
    rows_proc = 0
    if isinstance(existing_status, str):
        status_val = existing_status
    elif isinstance(existing_status, dict):
        status_val = existing_status.get("status")
        rows_proc = existing_status.get("rows_processed", 0)
    if status_val == SyncStatus.SUCCESS.value:
        logger.info("[Step 2h/11] tushare_index_daily already synced, skipping")
        results["tushare_index_daily"] = {"status": "success", "rows": rows_proc, "error": None, "skipped": True}
    else:
        write_step_status(target_date, SyncStep.TUSHARE_INDEX_DAILY.value, SyncStatus.RUNNING.value)
        try:
            status, rows, err = run_tushare_index_daily_step(target_date)
            write_step_status(target_date, SyncStep.TUSHARE_INDEX_DAILY.value, status.value, rows, err)
            results["tushare_index_daily"] = {"status": status.value, "rows": rows, "error": err}
            logger.info("[Step 2h/11] tushare_index_daily: %s (%d rows)", status.value, rows)
        except Exception as e:
            logger.exception("[Step 2h/11] tushare_index_daily failed: %s", e)
            write_step_status(target_date, SyncStep.TUSHARE_INDEX_DAILY.value, SyncStatus.ERROR.value, 0, str(e))
            results["tushare_index_daily"] = {"status": "error", "rows": 0, "error": str(e)}
            if not continue_on_error:
                return results

    # Step 2i: Index Weekly
    logger.info("[Step 2i/11] Tushare Index Weekly")
    existing_status = get_step_status(target_date, SyncStep.TUSHARE_INDEX_WEEKLY.value)
    status_val = None
    rows_proc = 0
    if isinstance(existing_status, str):
        status_val = existing_status
    elif isinstance(existing_status, dict):
        status_val = existing_status.get("status")
        rows_proc = existing_status.get("rows_processed", 0)
    if status_val == SyncStatus.SUCCESS.value:
        logger.info("[Step 2i/11] tushare_index_weekly already synced, skipping")
        results["tushare_index_weekly"] = {"status": "success", "rows": rows_proc, "error": None, "skipped": True}
    else:
        write_step_status(target_date, SyncStep.TUSHARE_INDEX_WEEKLY.value, SyncStatus.RUNNING.value)
        try:
            status, rows, err = run_tushare_index_weekly_step(target_date)
            write_step_status(target_date, SyncStep.TUSHARE_INDEX_WEEKLY.value, status.value, rows, err)
            results["tushare_index_weekly"] = {"status": status.value, "rows": rows, "error": err}
            logger.info("[Step 2i/11] tushare_index_weekly: %s (%d rows)", status.value, rows)
        except Exception as e:
            logger.exception("[Step 2i/11] tushare_index_weekly failed: %s", e)
            write_step_status(target_date, SyncStep.TUSHARE_INDEX_WEEKLY.value, SyncStatus.ERROR.value, 0, str(e))
            results["tushare_index_weekly"] = {"status": "error", "rows": 0, "error": str(e)}
            if not continue_on_error:
                return results

    # Step 3: VNPy sync
    logger.info("[Step 3/11] VNPy Sync")
    # Defensive check: skip if already successfully synced
    existing_status = get_step_status(target_date, SyncStep.VNPY_SYNC.value)
    status_val = None
    rows_proc = 0
    if isinstance(existing_status, str):
        status_val = existing_status
    elif isinstance(existing_status, dict):
        status_val = existing_status.get("status")
        rows_proc = existing_status.get("rows_processed", 0)
    if status_val == SyncStatus.SUCCESS.value:
        logger.info("[Step 3/11] vnpy_sync already synced (status=success), skipping")
        results["vnpy_sync"] = {"status": "success", "rows": rows_proc, "error": None, "skipped": True}
    else:
        write_step_status(target_date, SyncStep.VNPY_SYNC.value, SyncStatus.RUNNING.value)
        try:
            status, rows, err = run_vnpy_sync_step(target_date)
            write_step_status(target_date, SyncStep.VNPY_SYNC.value, status.value, rows, err)
            results["vnpy_sync"] = {"status": status.value, "rows": rows, "error": err}
            logger.info("[Step 3/11] vnpy_sync: %s (%d rows)", status.value, rows)
        except Exception as e:
            logger.exception("[Step 3/11] vnpy_sync failed: %s", e)
            write_step_status(target_date, SyncStep.VNPY_SYNC.value, SyncStatus.ERROR.value, 0, str(e))
            results["vnpy_sync"] = {"status": "error", "rows": 0, "error": str(e)}

    logger.info("=" * 80)
    logger.info("Daily ingest finished for %s", target_date)
    logger.info("Results: %s", results)
    logger.info("=" * 80)

    return results


def missing_data_backfill(**_compat):
    """Scan for failed/pending steps and backfill using batch APIs.

    Uses DB lock to prevent concurrent backfill jobs.
    """
    from app.datasync.service.init_service import get_coverage_window

    lookback_days = _compat.get("lookback_days")
    coverage_window = None
    window_start = None
    window_end = None
    if lookback_days is None:
        coverage_window = get_coverage_window()
        window_start = coverage_window["start_date"]
        window_end = coverage_window["end_date"]

    # Check DB lock
    if is_backfill_locked():
        logger.warning("Backfill already running (DB locked), skipping this run")
        return

    # Acquire lock
    try:
        acquire_backfill_lock()
        logger.info("Acquired backfill lock")
    except Exception as e:
        logger.warning("Failed to acquire backfill lock: %s", e)
        return

    try:
        if lookback_days is not None:
            logger.info("Starting missing data backfill (lookback_days=%s)", lookback_days)
        else:
            logger.info("Starting missing data backfill (start=%s end=%s)", window_start, window_end)

        # Get failed steps
        if lookback_days is not None:
            failed = get_failed_steps(lookback_days=lookback_days)
        else:
            try:
                failed = get_failed_steps(window_start, window_end)
            except TypeError:
                failed = get_failed_steps(lookback_days=LOOKBACK_DAYS)
        if not failed:
            logger.info("No failed steps to backfill")
            return

        logger.info("Found %d failed step entries", len(failed))

        # Group by step name and find contiguous date ranges
        by_step: Dict[str, List[date]] = {}
        for sync_date, step_name in failed:
            if step_name not in by_step:
                by_step[step_name] = []
            by_step[step_name].append(sync_date)

        for step_name, dates in by_step.items():
            dates_sorted = sorted(set(dates))
            logger.info("Backfilling %s: %d dates", step_name, len(dates_sorted))

            if step_name == SyncStep.TUSHARE_DIVIDEND.value:
                # Group into month ranges
                month_ranges = group_dates_by_month(dates_sorted)
                for start, end in month_ranges:
                    logger.info("  Backfilling dividend %s -> %s", start, end)
                    try:
                        ingest_dividend_by_date_range(
                            start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"), batch_size=BATCH_SIZE
                        )
                        # Mark all dates in range as success
                        for d in dates_sorted:
                            if start <= d <= end:
                                write_step_status(d, SyncStep.TUSHARE_DIVIDEND.value, SyncStatus.SUCCESS.value)
                    except Exception as e:
                        logger.exception("Dividend backfill failed for %s->%s: %s", start, end, e)
                        for d in dates_sorted:
                            if start <= d <= end:
                                write_step_status(
                                    d, SyncStep.TUSHARE_DIVIDEND.value, SyncStatus.ERROR.value, error_message=str(e)
                                )

            elif step_name == SyncStep.TUSHARE_TOP10_HOLDERS.value:
                month_ranges = group_dates_by_month(dates_sorted)
                for start, end in month_ranges:
                    logger.info("  Backfilling top10_holders %s -> %s", start, end)
                    try:
                        ingest_top10_holders_by_date_range(
                            start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"), batch_size=BATCH_SIZE
                        )
                        for d in dates_sorted:
                            if start <= d <= end:
                                write_step_status(d, SyncStep.TUSHARE_TOP10_HOLDERS.value, SyncStatus.SUCCESS.value)
                    except Exception as e:
                        logger.exception("Top10 backfill failed for %s->%s: %s", start, end, e)
                        for d in dates_sorted:
                            if start <= d <= end:
                                write_step_status(
                                    d,
                                    SyncStep.TUSHARE_TOP10_HOLDERS.value,
                                    SyncStatus.ERROR.value,
                                    error_message=str(e),
                                )

            elif step_name == SyncStep.TUSHARE_ADJ_FACTOR.value:
                month_ranges = group_dates_by_month(dates_sorted)
                for start, end in month_ranges:
                    logger.info("  Backfilling adj_factor %s -> %s", start, end)
                    try:
                        ingest_adj_factor_by_date_range(
                            start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"), batch_size=BATCH_SIZE
                        )
                        for d in dates_sorted:
                            if start <= d <= end:
                                write_step_status(d, SyncStep.TUSHARE_ADJ_FACTOR.value, SyncStatus.SUCCESS.value)
                    except Exception as e:
                        logger.exception("Adj factor backfill failed for %s->%s: %s", start, end, e)
                        for d in dates_sorted:
                            if start <= d <= end:
                                write_step_status(
                                    d, SyncStep.TUSHARE_ADJ_FACTOR.value, SyncStatus.ERROR.value, error_message=str(e)
                                )

            else:
                # For other steps, retry one by one with daily APIs
                for d in dates_sorted:
                    logger.info("  Retrying %s for %s", step_name, d)
                    try:
                        daily_ingest(target_date=d, continue_on_error=True)
                    except Exception as e:
                        logger.exception("Daily retry failed for %s on %s: %s", step_name, d, e)

        logger.info("Backfill complete")

    finally:
        # Release lock
        try:
            release_backfill_lock()
            logger.info("Released backfill lock")
        except Exception as e:
            logger.warning("Failed to release backfill lock: %s", e)


def group_dates_by_month(dates: List[date]) -> List[Tuple[date, date]]:
    """Group dates into contiguous month ranges."""
    if not dates:
        return []

    ranges = []
    current_start = dates[0]
    current_end = dates[0]

    for d in dates[1:]:
        # Check if within same month or contiguous
        if (d.year == current_end.year and d.month == current_end.month) or (d - current_end).days <= 31:
            current_end = d
        else:
            ranges.append((current_start, current_end))
            current_start = d
            current_end = d

    ranges.append((current_start, current_end))
    return ranges


def _select_period_end_trade_dates(trade_days: List[date], period: str) -> List[date]:
    """Return trade dates that close a trading week or month."""
    if period not in {"weekly", "monthly"}:
        raise ValueError(f"Unsupported period: {period}")

    selected: List[date] = []
    for index, trade_day in enumerate(trade_days):
        next_day = trade_days[index + 1] if index + 1 < len(trade_days) else None
        if next_day is None:
            selected.append(trade_day)
            continue

        if period == "weekly":
            current_week = trade_day.isocalendar()[:2]
            next_week = next_day.isocalendar()[:2]
            if current_week != next_week:
                selected.append(trade_day)
            continue

        if (trade_day.year, trade_day.month) != (next_day.year, next_day.month):
            selected.append(trade_day)

    return selected


def initialize_sync_status_table(**_compat):
    """Initialize data_sync_status table by scanning existing data."""
    from app.datasync.service.init_service import get_coverage_window

    coverage_window = get_coverage_window()
    start = coverage_window["start_date"]
    end = coverage_window["end_date"]

    logger.info(
        "Initializing data_sync_status table (env=%s start=%s end=%s)",
        coverage_window["env"],
        start,
        end,
    )

    # Get trade calendar
    trade_days = get_trade_calendar(start, end)
    logger.info("Processing %d trade dates from %s to %s", len(trade_days), start, end)

    if not trade_days:
        logger.info("No trade days found for range, exiting")
        return

    s = trade_days[0]
    e = trade_days[-1]

    # Get aggregated counts via DAO
    logger.info("Querying aggregated counts via DAO")
    stock_daily_counts = get_stock_daily_counts(s, e)
    bak_daily_counts = get_bak_daily_counts(s, e)
    moneyflow_counts = get_moneyflow_counts(s, e)
    suspend_d_counts = get_suspend_d_counts(s, e)
    suspend_counts = get_suspend_counts(s, e)
    adj_factor_counts = get_adj_factor_counts(s, e)
    stock_weekly_counts = get_stock_weekly_counts(s, e)
    stock_monthly_counts = get_stock_monthly_counts(s, e)
    vnpy_counts = get_vnpy_counts(s, e)
    weekly_trade_days = set(_select_period_end_trade_dates(trade_days, period="weekly"))
    monthly_trade_days = set(_select_period_end_trade_dates(trade_days, period="monthly"))

    # Build rows list
    rows_to_insert = []
    for td in trade_days:
        daily_count = stock_daily_counts.get(td, 0)
        status_daily = SyncStatus.SUCCESS if daily_count > 0 else SyncStatus.PENDING
        rows_to_insert.append(
            (td, SyncStep.TUSHARE_STOCK_DAILY.value, status_daily.value, daily_count, None, None, None)
        )

        bak_count = bak_daily_counts.get(td, 0)
        status_bak = SyncStatus.SUCCESS if bak_count > 0 else SyncStatus.PENDING
        rows_to_insert.append((td, SyncStep.TUSHARE_BAK_DAILY.value, status_bak.value, bak_count, None, None, None))

        moneyflow_count = moneyflow_counts.get(td, 0)
        status_moneyflow = SyncStatus.SUCCESS if moneyflow_count > 0 else SyncStatus.PENDING
        rows_to_insert.append(
            (td, SyncStep.TUSHARE_MONEYFLOW.value, status_moneyflow.value, moneyflow_count, None, None, None)
        )

        suspend_d_count = suspend_d_counts.get(td, 0)
        status_suspend_d = SyncStatus.SUCCESS if suspend_d_count > 0 else SyncStatus.PENDING
        rows_to_insert.append(
            (td, SyncStep.TUSHARE_SUSPEND_D.value, status_suspend_d.value, suspend_d_count, None, None, None)
        )

        suspend_count = suspend_counts.get(td, 0)
        status_suspend = SyncStatus.SUCCESS if suspend_count > 0 else SyncStatus.PENDING
        rows_to_insert.append(
            (td, SyncStep.TUSHARE_SUSPEND.value, status_suspend.value, suspend_count, None, None, None)
        )

        adj_count = adj_factor_counts.get(td, 0)
        status_adj = SyncStatus.SUCCESS if adj_count > 0 else SyncStatus.PENDING
        rows_to_insert.append((td, SyncStep.TUSHARE_ADJ_FACTOR.value, status_adj.value, adj_count, None, None, None))

        vnpy_count = vnpy_counts.get(td, 0)
        status_vnpy = SyncStatus.SUCCESS if vnpy_count > 0 else SyncStatus.PENDING
        rows_to_insert.append((td, SyncStep.VNPY_SYNC.value, status_vnpy.value, vnpy_count, None, None, None))

        if td in weekly_trade_days:
            weekly_count = stock_weekly_counts.get(td, 0)
            status_weekly = SyncStatus.SUCCESS if weekly_count > 0 else SyncStatus.PENDING
            rows_to_insert.append(
                (td, SyncStep.TUSHARE_STOCK_WEEKLY.value, status_weekly.value, weekly_count, None, None, None)
            )

        if td in monthly_trade_days:
            monthly_count = stock_monthly_counts.get(td, 0)
            status_monthly = SyncStatus.SUCCESS if monthly_count > 0 else SyncStatus.PENDING
            rows_to_insert.append(
                (td, SyncStep.TUSHARE_STOCK_MONTHLY.value, status_monthly.value, monthly_count, None, None, None)
            )

        for step in (SyncStep.AKSHARE_INDEX, SyncStep.TUSHARE_DIVIDEND, SyncStep.TUSHARE_TOP10_HOLDERS):
            rows_to_insert.append((td, step.value, SyncStatus.PENDING.value, 0, None, None, None))

    processed = bulk_upsert_status(rows_to_insert)
    logger.info("Initialization complete: %d step statuses inserted for %d dates", processed, len(trade_days))


def _get_dynamic_scheduler():
    """Load the dynamic scheduler lazily to avoid import cycles at module import time."""
    from app.datasync import scheduler as scheduler_module

    return scheduler_module


def _run_dynamic_reconcile(target_date: Optional[date], lookback_years: int | None = None):
    """Delegate legacy init CLI usage to the dynamic reconciliation flow."""
    scheduler_module = _get_dynamic_scheduler()
    if lookback_years is not None:
        logger.info("Ignoring legacy lookback_years=%s for dynamic reconcile", lookback_years)
    return scheduler_module.run_reconcile(target_end_date=target_date)


# =============================================================================
# Scheduler
# =============================================================================


def run_daily_job():
    """Job: Daily ingest at 2:00 AM Shanghai time."""
    logger.info("=" * 80)
    logger.info("Scheduled daily job triggered")
    logger.info("=" * 80)
    try:
        daily_ingest(continue_on_error=True)
    except Exception as e:
        logger.exception("Daily job failed: %s", e)


def run_backfill_job():
    """Job: Backfill every 6 hours with DB lock."""
    logger.info("=" * 80)
    logger.info("Scheduled backfill job triggered")
    logger.info("=" * 80)
    try:
        missing_data_backfill()
    except Exception as e:
        logger.exception("Backfill job failed: %s", e)


def run_daemon():
    """Run the modern scheduler daemon via the legacy entrypoint."""
    logger.warning(
        "Legacy data_sync_daemon.run_daemon() is deprecated; delegating to app.datasync.scheduler.daemon_loop()"
    )
    return _get_dynamic_scheduler().daemon_loop()


# =============================================================================
# CLI
# =============================================================================


def main():
    parser = argparse.ArgumentParser(description="QuantMate Data Sync Daemon")
    parser.add_argument("--daemon", action="store_true", help="Run as daemon with scheduler")
    parser.add_argument("--daily", action="store_true", help="Run daily ingest once")
    parser.add_argument("--backfill", action="store_true", help="Run backfill once")
    parser.add_argument("--init", action="store_true", help="Initialize sync status table")
    parser.add_argument("--date", type=lambda raw: datetime.strptime(raw, "%Y-%m-%d").date(), help="Target date (YYYY-MM-DD)")
    parser.add_argument("--lookback-days", type=int, default=LOOKBACK_DAYS, help="Legacy backfill lookback window")
    parser.add_argument("--lookback-years", type=int, default=LOOKBACK_YEARS, help="Legacy reconcile lookback window")
    parser.add_argument("--refresh-calendar", action="store_true", help="Refresh trade calendar")

    args = parser.parse_args()

    if args.init:
        logger.warning(
            "Legacy --init entrypoint is deprecated; delegating to dynamic sync-status reconciliation"
        )
        result = _run_dynamic_reconcile(target_date=args.date, lookback_years=args.lookback_years)
        logger.info("Reconcile result: %s", result)
    elif args.refresh_calendar:
        refresh_trade_calendar()
    elif args.daily:
        logger.warning("Legacy --daily entrypoint is deprecated; delegating to dynamic scheduler daily sync")
        results = _get_dynamic_scheduler().run_daily_sync(target_date=args.date)
        logger.info("Daily sync results: %s", results)
    elif args.backfill:
        logger.warning("Legacy --backfill entrypoint is deprecated; delegating to dynamic scheduler backfill")
        results = _get_dynamic_scheduler().run_backfill(lookback_days=args.lookback_days)
        logger.info("Backfill results: %s", results)
    elif args.daemon:
        run_daemon()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()


# =============================================================================
# DataSyncDaemon Class (added to satisfy SyncStatusService dependency)
# =============================================================================


class DataSyncDaemon:
    """Minimal implementation to allow SyncStatusService to function.

    This is a temporary stub until full daemon class is implemented.
    """

    @staticmethod
    def find_missing_trade_dates(
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        **_compat,
    ) -> List[date]:
        """
        Return missing trade dates for backfill.

        TODO: Implement actual missing date detection by querying sync logs.
        For now, return empty list (assumes no missing dates).
        """
        # Placeholder: in a real implementation, this would:
        # 1. Get list of expected trade dates for configured coverage window
        # 2. Compare with successfully synced dates from sync_log table
        # 3. Return dates that are missing
        _ = (start_date, end_date)
        return []
