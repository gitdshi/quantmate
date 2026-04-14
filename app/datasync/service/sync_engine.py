"""Sync engine — dynamic, plugin-driven replacement for data_sync_daemon.py.

Reads enabled interfaces from DB, dispatches to registered plugins,
tracks per-interface per-trading-day status.

Supports parallel execution via ThreadPoolExecutor, with a per-source
semaphore to respect rate limits (e.g. Tushare max 3 concurrent).
"""

from __future__ import annotations

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Semaphore
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
PARALLEL_WORKERS = int(os.getenv("SYNC_PARALLEL_WORKERS", "4"))

# Maximum concurrent API calls per data source (respects Tushare rate limits)
SOURCE_CONCURRENCY: dict[str, int] = {
    "tushare": int(os.getenv("TUSHARE_CONCURRENCY", "3")),
}

_source_semaphores: dict[str, Semaphore] = {}


def _get_source_semaphore(source: str) -> Semaphore | None:
    """Return a semaphore for the given source, or None for unlimited."""
    limit = SOURCE_CONCURRENCY.get(source)
    if limit is None:
        return None
    if source not in _source_semaphores:
        _source_semaphores[source] = Semaphore(limit)
    return _source_semaphores[source]


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
            text("SELECT status FROM data_sync_status WHERE sync_date = :sd AND source = :src AND interface_key = :ik"),
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


def _sync_one_item(
    registry: DataSourceRegistry,
    item: dict,
    target_date: date,
    idx: int,
    total: int,
) -> tuple[str, dict]:
    """Sync a single item. Called from daily_sync threads."""
    source = item["source"]
    item_key = item["item_key"]
    label = f"{source}/{item_key}"

    sem = _get_source_semaphore(source)

    try:
        if sem:
            sem.acquire()

        iface = registry.get_interface(source, item_key)
        if iface is None:
            logger.warning("No interface registered for %s, skipping", label)
            return label, {"status": "skipped", "reason": "no plugin"}

        existing = _get_status(target_date, source, item_key)
        if existing == SyncStatus.SUCCESS.value:
            logger.info("[%d/%d] %s already synced, skipping", idx, total, label)
            return label, {"status": "success", "skipped": True}

        if not item["table_created"]:
            try:
                ensure_table(item["target_database"], item["target_table"], iface.get_ddl())
            except Exception as e:
                logger.exception("Failed to create table for %s: %s", label, e)
                _write_status(target_date, source, item_key, SyncStatus.ERROR.value, 0, f"DDL failed: {e}")
                return label, {"status": "error", "error": f"DDL failed: {e}"}

        _write_status(target_date, source, item_key, SyncStatus.RUNNING.value)

        try:
            result: SyncResult = iface.sync_date(target_date)
            _write_status(
                target_date, source, item_key,
                result.status.value, result.rows_synced, result.error_message,
            )
            logger.info("[%d/%d] %s: %s (%d rows)", idx, total, label, result.status.value, result.rows_synced)
            return label, {"status": result.status.value, "rows": result.rows_synced, "error": result.error_message}
        except Exception as e:
            logger.exception("[%d/%d] %s failed: %s", idx, total, label, e)
            _write_status(target_date, source, item_key, SyncStatus.ERROR.value, 0, str(e))
            return label, {"status": "error", "rows": 0, "error": str(e)}
    finally:
        if sem:
            sem.release()


def daily_sync(
    registry: DataSourceRegistry,
    target_date: Optional[date] = None,
    continue_on_error: bool = True,
    max_workers: int | None = None,
) -> dict[str, dict]:
    """Run daily sync for all enabled interfaces on a given trading day.

    Uses a thread pool with per-source semaphores to respect rate limits
    while maximizing throughput across sources.
    """
    if target_date is None:
        target_date = get_previous_trade_date()

    if max_workers is None:
        max_workers = PARALLEL_WORKERS

    logger.info("=" * 80)
    logger.info("Daily sync starting for %s (workers=%d)", target_date, max_workers)
    logger.info("=" * 80)

    enabled_items = _get_enabled_items()
    results: dict[str, dict] = {}
    total = len(enabled_items)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_sync_one_item, registry, item, target_date, idx, total): f"{item['source']}/{item['item_key']}"
            for idx, item in enumerate(enabled_items, 1)
        }
        for future in as_completed(futures):
            label = futures[future]
            try:
                lbl, res = future.result()
                results[lbl] = res
            except Exception as e:
                logger.exception("Unexpected error in sync thread for %s: %s", label, e)
                results[label] = {"status": "error", "error": str(e)}
                if not continue_on_error:
                    # Cancel remaining futures
                    for f in futures:
                        f.cancel()
                    break

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
                    sync_date,
                    source,
                    iface_key,
                    result.status.value,
                    result.rows_synced,
                    result.error_message,
                    retry_count=retry_count + 1,
                )
                results[label] = {"status": result.status.value, "rows": result.rows_synced}
            except Exception as e:
                logger.exception("Backfill %s failed: %s", label, e)
                _write_status(
                    sync_date,
                    source,
                    iface_key,
                    SyncStatus.ERROR.value,
                    0,
                    str(e),
                    retry_count=retry_count + 1,
                )
                results[label] = {"status": "error", "error": str(e)}
    finally:
        try:
            release_backfill_lock()
        except Exception:
            logger.exception("Failed to release backfill lock")

    return results
