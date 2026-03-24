"""New datasync scheduler — delegates to sync_engine + vnpy_sync.

Replaces the scheduling logic from data_sync_daemon.py while
keeping the old daemon importable for backward compatibility.

Usage:
    python -m app.datasync.scheduler --daemon       # Run as daemon
    python -m app.datasync.scheduler --daily         # Run daily sync once
    python -m app.datasync.scheduler --backfill      # Run backfill retry once
    python -m app.datasync.scheduler --vnpy          # Run VNPy sync once
    python -m app.datasync.scheduler --init          # Run initialization
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import date
from zoneinfo import ZoneInfo

import schedule

# Ensure project root is importable
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SYNC_HOUR = int(os.getenv("SYNC_HOUR", "2"))
SYNC_MINUTE = int(os.getenv("SYNC_MINUTE", "0"))
BACKFILL_INTERVAL_HOURS = int(os.getenv("BACKFILL_INTERVAL_HOURS", "6"))
TIMEZONE = "Asia/Shanghai"


def _build_registry():
    from app.datasync.registry import build_default_registry

    return build_default_registry()


def run_daily_sync(target_date: date | None = None):
    """Run daily sync with the plugin registry."""
    from app.datasync.service.sync_engine import daily_sync

    registry = _build_registry()
    results = daily_sync(registry, target_date=target_date)

    # Run VNPy sync after daily sync
    from app.datasync.service.vnpy_sync import run_vnpy_sync_job

    vnpy_result = run_vnpy_sync_job(target_date)
    results["vnpy/vnpy_sync"] = {
        "status": vnpy_result.status.value,
        "rows": vnpy_result.rows_synced,
        "error": vnpy_result.error_message,
    }

    return results


def run_backfill():
    """Run backfill retry."""
    from app.datasync.service.sync_engine import backfill_retry

    registry = _build_registry()
    return backfill_retry(registry)


def run_vnpy():
    """Run VNPy sync only."""
    from app.datasync.service.vnpy_sync import run_vnpy_sync_job

    return run_vnpy_sync_job()


def run_init(run_backfill_flag: bool = False):
    """Run initialization."""
    from app.datasync.service.init_service import initialize

    registry = _build_registry()
    return initialize(registry, run_backfill=run_backfill_flag)


def _scheduled_daily():
    """Called by the scheduler at 02:00 Shanghai time."""
    logger.info("Scheduled daily sync triggered")
    try:
        run_daily_sync()
    except Exception:
        logger.exception("Scheduled daily sync failed")


def _scheduled_backfill():
    """Called by the scheduler every 6 hours."""
    logger.info("Scheduled backfill triggered")
    try:
        run_backfill()
    except Exception:
        logger.exception("Scheduled backfill failed")


def daemon_loop():
    """Run the scheduler daemon."""
    logger.info("DataSync scheduler starting (daily at %02d:%02d, backfill every %dh)", SYNC_HOUR, SYNC_MINUTE, BACKFILL_INTERVAL_HOURS)

    # Init metrics
    try:
        from app.datasync.metrics import init_metrics

        init_metrics()
    except Exception:
        logger.warning("Metrics initialization failed (non-fatal)")

    # Ensure tables exist
    try:
        from app.domains.extdata.dao.data_sync_status_dao import ensure_tables, ensure_backfill_lock_table

        ensure_tables()
        ensure_backfill_lock_table()
    except Exception:
        logger.exception("Failed to ensure tables")

    # Run initial sync
    logger.info("Running initial daily sync...")
    try:
        run_daily_sync()
    except Exception:
        logger.exception("Initial daily sync failed")

    # Run initial backfill
    logger.info("Running initial backfill...")
    try:
        run_backfill()
    except Exception:
        logger.exception("Initial backfill failed")

    # Schedule recurring jobs
    schedule.every().day.at(f"{SYNC_HOUR:02d}:{SYNC_MINUTE:02d}").do(_scheduled_daily)
    schedule.every(BACKFILL_INTERVAL_HOURS).hours.do(_scheduled_backfill)

    logger.info("Scheduler loop started")
    while True:
        schedule.run_pending()
        time.sleep(30)


def main():
    parser = argparse.ArgumentParser(description="DataSync Scheduler")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--daemon", action="store_true", help="Run as daemon")
    group.add_argument("--daily", action="store_true", help="Run daily sync once")
    group.add_argument("--backfill", action="store_true", help="Run backfill once")
    group.add_argument("--vnpy", action="store_true", help="Run VNPy sync once")
    group.add_argument("--init", action="store_true", help="Run initialization")
    parser.add_argument("--date", type=str, help="Target date (YYYY-MM-DD)")
    parser.add_argument("--with-backfill", action="store_true", help="Run backfill during init")

    args = parser.parse_args()

    target_date = None
    if args.date:
        from datetime import datetime

        target_date = datetime.strptime(args.date, "%Y-%m-%d").date()

    if args.daemon:
        daemon_loop()
    elif args.daily:
        results = run_daily_sync(target_date)
        logger.info("Daily sync results: %s", results)
    elif args.backfill:
        results = run_backfill()
        logger.info("Backfill results: %s", results)
    elif args.vnpy:
        result = run_vnpy()
        logger.info("VNPy sync result: %s", result)
    elif args.init:
        result = run_init(run_backfill_flag=args.with_backfill)
        logger.info("Init result: %s", result)


if __name__ == "__main__":
    main()
