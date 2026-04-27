"""New datasync scheduler — delegates to sync_engine + vnpy_sync.

Replaces the scheduling logic from data_sync_daemon.py while
keeping the old daemon importable for backward compatibility.

Usage:
    python -m app.datasync.scheduler --daemon       # Run as daemon
    python -m app.datasync.scheduler --daily         # Run daily sync once
    python -m app.datasync.scheduler --backfill      # Run backfill retry once
    python -m app.datasync.scheduler --backfill-loop # Run dedicated backfill loop
    python -m app.datasync.scheduler --vnpy          # Run VNPy sync once
    python -m app.datasync.scheduler --init          # Run initialization
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date

import schedule

# Ensure project root is importable
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

from app.infrastructure.config import get_runtime_bool, get_runtime_int, get_runtime_str

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def _sync_hour() -> int:
    return get_runtime_int(env_keys="SYNC_HOUR", db_key="datasync.sync_hour", default=2)


def _sync_minute() -> int:
    return get_runtime_int(env_keys="SYNC_MINUTE", db_key="datasync.sync_minute", default=0)


def _backfill_idle_interval_hours() -> int:
    return get_runtime_int(
        env_keys="BACKFILL_IDLE_INTERVAL_HOURS",
        db_key="datasync.backfill_idle_interval_hours",
        default=4,
    )


def _backfill_lock_retry_seconds() -> int:
    return get_runtime_int(
        env_keys="BACKFILL_LOCK_RETRY_SECONDS",
        db_key="datasync.backfill_lock_retry_seconds",
        default=60,
    )


def _datasync_timezone() -> str:
    return get_runtime_str(env_keys="DATASYNC_TIMEZONE", db_key="datasync.timezone", default="Asia/Shanghai")


def _scheduler_signature() -> tuple[int, int, str]:
    return (_sync_hour(), _sync_minute(), _datasync_timezone())


def _env_flag(name: str) -> bool:
    return get_runtime_bool(env_keys=name, default=False)


def _build_registry():
    from app.datasync.registry import build_default_registry

    return build_default_registry()


def run_daily_sync(target_date: date | None = None, registry=None):
    """Run daily sync with the plugin registry."""
    from app.datasync.service.sync_engine import daily_sync

    if registry is None:
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


def run_backfill(registry=None, **_compat):
    """Run backfill retry."""
    from app.datasync.service.sync_engine import backfill_retry

    if registry is None:
        registry = _build_registry()
    return backfill_retry(registry)


def run_backfill_loop(idle_hours: int | None = None, registry=None):
    """Continuously drain backfill work, then sleep until the next cycle."""
    from app.domains.extdata.dao.data_sync_status_dao import is_backfill_locked

    if idle_hours is None:
        idle_hours = _backfill_idle_interval_hours()
    idle_hours = max(1, idle_hours)
    if registry is None:
        registry = _build_registry()

    logger.info("Backfill loop starting (idle every %dh)", idle_hours)
    while True:
        if is_backfill_locked():
            retry_seconds = _backfill_lock_retry_seconds()
            logger.info(
                "Backfill loop detected active lock; retrying in %ds",
                retry_seconds,
            )
            time.sleep(retry_seconds)
            continue

        results = run_backfill(registry=registry)
        if results:
            logger.info("Backfill loop pass complete: drained=%d retryable rows", len(results))
            continue

        if is_backfill_locked():
            retry_seconds = _backfill_lock_retry_seconds()
            logger.info(
                "Backfill loop pass ended while lock is active; retrying in %ds",
                retry_seconds,
            )
            time.sleep(retry_seconds)
            continue

        logger.info("Backfill loop idle: no retryable rows; sleeping %dh", idle_hours)
        time.sleep(idle_hours * 3600)


def run_vnpy():
    """Run VNPy sync only."""
    from app.datasync.service.vnpy_sync import run_vnpy_sync_job

    return run_vnpy_sync_job()


def run_init(run_backfill_flag: bool = False, registry=None):
    """Run initialization."""
    from app.datasync.service.init_service import initialize

    if registry is None:
        registry = _build_registry()
    return initialize(registry, run_backfill=run_backfill_flag)


def run_reconcile(target_end_date: date | None = None, registry=None):
    """Seed/normalize runtime state and ensure coverage through target date."""
    from app.datasync.service.init_service import reconcile_runtime_state

    if registry is None:
        registry = _build_registry()
    return reconcile_runtime_state(registry, target_end_date=target_end_date)


def _scheduled_daily():
    """Called by the scheduler at 02:00 Shanghai time."""
    logger.info("Scheduled daily sync triggered")
    try:
        run_reconcile()
        run_daily_sync()
    except Exception:
        logger.exception("Scheduled daily sync failed")


def _scheduled_backfill():
    """Backward-compatible scheduled backfill hook for tests and legacy callers."""
    logger.info("Scheduled backfill triggered")
    try:
        run_backfill()
    except Exception:
        logger.exception("Scheduled backfill failed")


def _run_startup_sequence(registry) -> None:
    if _env_flag("DATASYNC_SKIP_INITIAL_RECONCILE"):
        logger.info("Skipping initial datasync init due to DATASYNC_SKIP_INITIAL_RECONCILE")
    else:
        logger.info("Running initial datasync init...")
        try:
            run_init(run_backfill_flag=False, registry=registry)
        except Exception:
            logger.exception("Initial datasync init failed")

    if _env_flag("DATASYNC_SKIP_INITIAL_DAILY"):
        logger.info("Skipping initial daily sync due to DATASYNC_SKIP_INITIAL_DAILY")
    else:
        logger.info("Running initial daily sync...")
        try:
            run_daily_sync(registry=registry)
        except Exception:
            logger.exception("Initial daily sync failed")


def daemon_loop():
    """Run the scheduler daemon."""
    sync_hour, sync_minute, timezone_name = _scheduler_signature()
    logger.info(
        "DataSync scheduler starting (daily at %02d:%02d, timezone=%s)",
        sync_hour,
        sync_minute,
        timezone_name,
    )

    registry = _build_registry()

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

    _run_startup_sequence(registry)

    last_schedule_signature: tuple[int, int, str] | None = None

    logger.info("Scheduler loop started")
    while True:
        schedule_signature = _scheduler_signature()
        if schedule_signature != last_schedule_signature:
            last_schedule_signature = schedule_signature
            schedule.clear()
            schedule.every().day.at(f"{schedule_signature[0]:02d}:{schedule_signature[1]:02d}").do(_scheduled_daily)
            logger.info(
                "Scheduler daily trigger updated to %02d:%02d (%s)",
                schedule_signature[0],
                schedule_signature[1],
                schedule_signature[2],
            )
        schedule.run_pending()
        time.sleep(30)


def main():
    parser = argparse.ArgumentParser(description="DataSync Scheduler")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--daemon", action="store_true", help="Run as daemon")
    group.add_argument("--daily", action="store_true", help="Run daily sync once")
    group.add_argument("--backfill", action="store_true", help="Run backfill once")
    group.add_argument("--backfill-loop", action="store_true", help="Run dedicated backfill loop")
    group.add_argument("--vnpy", action="store_true", help="Run VNPy sync once")
    group.add_argument("--init", action="store_true", help="Run initialization")
    group.add_argument("--reconcile", action="store_true", help="Run runtime reconciliation once")
    parser.add_argument("--date", type=str, help="Target date (YYYY-MM-DD)")
    parser.add_argument("--idle-hours", type=int, help="Backfill loop idle sleep hours")
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
    elif args.backfill_loop:
        run_backfill_loop(idle_hours=args.idle_hours)
    elif args.vnpy:
        result = run_vnpy()
        logger.info("VNPy sync result: %s", result)
    elif args.init:
        result = run_init(run_backfill_flag=args.with_backfill)
        logger.info("Init result: %s", result)
    elif args.reconcile:
        result = run_reconcile(target_end_date=target_date)
        logger.info("Reconcile result: %s", result)


if __name__ == "__main__":
    main()
