"""Sync engine — dynamic, plugin-driven replacement for data_sync_daemon.py.

Reads enabled interfaces from DB, dispatches to registered plugins,
tracks per-interface per-trading-day status.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import date, timedelta
from typing import Optional

from sqlalchemy import text

from app.datasync.base import SyncResult, SyncStatus
from app.datasync.registry import DataSourceRegistry
from app.datasync.table_manager import ensure_table
from app.infrastructure.db.connections import get_quantmate_engine

logger = logging.getLogger(__name__)

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "60"))


# ---------------------------------------------------------------------------
# DAO helpers (operate on the new data_sync_status schema)
# ---------------------------------------------------------------------------

def _write_status(
    sync_date: date,
    source: str,
    interface_key: str,
    status: str,
    rows_synced: int = 0,
    error_message: Optional[str] = None,
    retry_count: int = 0,
) -> None:
    engine = get_quantmate_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO data_sync_status "
                "(sync_date, source, interface_key, status, rows_synced, error_message, retry_count, started_at, finished_at) "
                "VALUES (:sd, :src, :ik, :st, :rows, :err, :rc, NOW(), NOW()) "
                "ON DUPLICATE KEY UPDATE "
                "status=VALUES(status), rows_synced=VALUES(rows_synced), error_message=VALUES(error_message), "
                "retry_count=VALUES(retry_count), finished_at=NOW(), updated_at=CURRENT_TIMESTAMP"
            ),
            {
                "sd": sync_date,
                "src": source,
                "ik": interface_key,
                "st": status,
                "rows": rows_synced,
                "err": error_message,
                "rc": retry_count,
            },
        )


def _get_status(sync_date: date, source: str, interface_key: str) -> Optional[str]:
    engine = get_quantmate_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT status FROM data_sync_status "
                "WHERE sync_date = :sd AND source = :src AND interface_key = :ik"
            ),
            {"sd": sync_date, "src": source, "ik": interface_key},
        ).fetchone()
        return row[0] if row else None


def _get_failed_records(lookback_days: int = 60) -> list[tuple[date, str, str, int]]:
    """Return (sync_date, source, interface_key, retry_count) where status is not success."""
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=lookback_days)
    engine = get_quantmate_engine()
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT sync_date, source, interface_key, retry_count "
                "FROM data_sync_status "
                "WHERE sync_date BETWEEN :s AND :e AND status IN ('error', 'partial', 'pending') "
                "ORDER BY sync_date ASC, source, interface_key"
            ),
            {"s": start, "e": end},
        ).fetchall()
        return [(r[0], r[1], r[2], r[3]) for r in rows]


def _get_enabled_items() -> list[dict]:
    """Return enabled data_source_items ordered by sync_priority."""
    engine = get_quantmate_engine()
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT dsi.source, dsi.item_key, dsi.target_database, dsi.target_table, "
                "dsi.table_created, dsi.sync_priority "
                "FROM data_source_items dsi "
                "JOIN data_source_configs dsc ON dsi.source = dsc.source_key AND dsc.enabled = 1 "
                "WHERE dsi.enabled = 1 "
                "ORDER BY dsi.sync_priority ASC"
            )
        ).fetchall()
        return [
            {
                "source": r[0],
                "item_key": r[1],
                "target_database": r[2],
                "target_table": r[3],
                "table_created": r[4],
                "sync_priority": r[5],
            }
            for r in rows
        ]


# ---------------------------------------------------------------------------
# Trade calendar
# ---------------------------------------------------------------------------

def get_trade_calendar(start_date: date, end_date: date) -> list[date]:
    """Re-use existing trade calendar logic from the old daemon."""
    from app.datasync.service.data_sync_daemon import get_trade_calendar as _get_cal

    return _get_cal(start_date, end_date)


def get_previous_trade_date(offset: int = 1) -> date:
    from app.datasync.service.data_sync_daemon import get_previous_trade_date as _get_prev

    return _get_prev(offset)


# ---------------------------------------------------------------------------
# Core sync functions
# ---------------------------------------------------------------------------

def daily_sync(
    registry: DataSourceRegistry,
    target_date: Optional[date] = None,
    continue_on_error: bool = True,
) -> dict[str, dict]:
    """Run daily sync for all enabled interfaces on a given trading day.

    1. Read enabled items from DB
    2. For each item, find the plugin interface
    3. Skip if already success
    4. Ensure table exists
    5. Run sync_date()
    6. Record status
    """
    if target_date is None:
        target_date = get_previous_trade_date()

    logger.info("=" * 80)
    logger.info("Daily sync starting for %s", target_date)
    logger.info("=" * 80)

    enabled_items = _get_enabled_items()
    results: dict[str, dict] = {}

    for idx, item in enumerate(enabled_items, 1):
        source = item["source"]
        item_key = item["item_key"]
        label = f"{source}/{item_key}"

        logger.info("[%d/%d] %s", idx, len(enabled_items), label)

        # Find interface in registry
        iface = registry.get_interface(source, item_key)
        if iface is None:
            logger.warning("No interface registered for %s, skipping", label)
            results[label] = {"status": "skipped", "reason": "no plugin"}
            continue

        # Skip if already success
        existing = _get_status(target_date, source, item_key)
        if existing == SyncStatus.SUCCESS.value:
            logger.info("[%d/%d] %s already synced, skipping", idx, len(enabled_items), label)
            results[label] = {"status": "success", "skipped": True}
            continue

        # Ensure table exists
        if not item["table_created"]:
            try:
                ensure_table(item["target_database"], item["target_table"], iface.get_ddl())
            except Exception as e:
                logger.exception("Failed to create table for %s: %s", label, e)
                _write_status(target_date, source, item_key, SyncStatus.ERROR.value, 0, f"DDL failed: {e}")
                results[label] = {"status": "error", "error": f"DDL failed: {e}"}
                if not continue_on_error:
                    return results
                continue

        # Mark running
        _write_status(target_date, source, item_key, SyncStatus.RUNNING.value)

        # Execute sync
        try:
            result: SyncResult = iface.sync_date(target_date)
            _write_status(
                target_date, source, item_key,
                result.status.value, result.rows_synced, result.error_message,
            )
            results[label] = {
                "status": result.status.value,
                "rows": result.rows_synced,
                "error": result.error_message,
            }
            logger.info("[%d/%d] %s: %s (%d rows)", idx, len(enabled_items), label, result.status.value, result.rows_synced)
        except Exception as e:
            logger.exception("[%d/%d] %s failed: %s", idx, len(enabled_items), label, e)
            _write_status(target_date, source, item_key, SyncStatus.ERROR.value, 0, str(e))
            results[label] = {"status": "error", "rows": 0, "error": str(e)}
            if not continue_on_error:
                return results

    logger.info("=" * 80)
    logger.info("Daily sync finished for %s", target_date)
    logger.info("=" * 80)

    return results


def backfill_retry(
    registry: DataSourceRegistry,
    lookback_days: int = None,
) -> dict[str, dict]:
    """Retry failed/pending syncs within the lookback window.

    Uses DB lock from the old daemon to prevent concurrent runs.
    """
    if lookback_days is None:
        lookback_days = LOOKBACK_DAYS

    from app.domains.extdata.dao.data_sync_status_dao import (
        acquire_backfill_lock,
        release_backfill_lock,
        is_backfill_locked,
    )

    if is_backfill_locked():
        logger.warning("Backfill already running (DB locked), skipping")
        return {}

    try:
        acquire_backfill_lock()
    except Exception as e:
        logger.warning("Failed to acquire backfill lock: %s", e)
        return {}

    results: dict[str, dict] = {}
    try:
        failed = _get_failed_records(lookback_days)
        logger.info("Backfill: %d records to retry", len(failed))

        for sync_date, source, iface_key, retry_count in failed:
            if retry_count >= MAX_RETRIES:
                logger.debug("Skipping %s/%s on %s: max retries reached", source, iface_key, sync_date)
                continue

            iface = registry.get_interface(source, iface_key)
            if iface is None:
                continue

            label = f"{source}/{iface_key}@{sync_date}"
            _write_status(sync_date, source, iface_key, SyncStatus.RUNNING.value, retry_count=retry_count + 1)

            try:
                result = iface.sync_date(sync_date)
                _write_status(
                    sync_date, source, iface_key,
                    result.status.value, result.rows_synced, result.error_message,
                    retry_count=retry_count + 1,
                )
                results[label] = {"status": result.status.value, "rows": result.rows_synced}
            except Exception as e:
                logger.exception("Backfill %s failed: %s", label, e)
                _write_status(
                    sync_date, source, iface_key,
                    SyncStatus.ERROR.value, 0, str(e),
                    retry_count=retry_count + 1,
                )
                results[label] = {"status": "error", "error": str(e)}
    finally:
        try:
            release_backfill_lock()
        except Exception:
            logger.exception("Failed to release backfill lock")

    return results
results
