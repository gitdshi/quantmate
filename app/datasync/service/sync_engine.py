"""Sync engine — dynamic, plugin-driven replacement for data_sync_daemon.py.

Reads enabled interfaces from DB, dispatches to registered plugins,
tracks per-interface per-trading-day status.

Supports parallel execution via ThreadPoolExecutor, with separate
per-source semaphores for daily sync and backfill so they can be tuned
independently per provider.
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
BACKFILL_WORKERS = int(os.getenv("BACKFILL_WORKERS", "10"))

# Maximum concurrent API calls per data source (respects Tushare rate limits)
SOURCE_CONCURRENCY: dict[str, int] = {
    "tushare": int(os.getenv("TUSHARE_CONCURRENCY", "3")),
}

_source_semaphores: dict[str, Semaphore] = {}
_backfill_source_semaphores: dict[str, Semaphore] = {}


def _get_source_env_key(source: str) -> str:
    return "".join(char if char.isalnum() else "_" for char in source.upper())


def _get_source_concurrency_limit(source: str) -> int | None:
    return SOURCE_CONCURRENCY.get(source)


def _get_backfill_source_concurrency_limit(source: str) -> int | None:
    override = os.getenv(f"BACKFILL_{_get_source_env_key(source)}_CONCURRENCY")
    if override is not None:
        return max(1, int(override))
    return _get_source_concurrency_limit(source)


def _get_semaphore(cache: dict[str, Semaphore], source: str, limit: int | None) -> Semaphore | None:
    if limit is None:
        return None
    if source not in cache:
        cache[source] = Semaphore(limit)
    return cache[source]


def _get_source_semaphore(source: str) -> Semaphore | None:
    """Return the daily sync semaphore for the given source, or None for unlimited."""
    return _get_semaphore(_source_semaphores, source, _get_source_concurrency_limit(source))


def _get_backfill_source_semaphore(source: str) -> Semaphore | None:
    """Return the backfill semaphore for the given source, or None for unlimited."""
    return _get_semaphore(
        _backfill_source_semaphores,
        source,
        _get_backfill_source_concurrency_limit(source),
    )


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
    try:
        stale_hours = int(os.getenv("SYNC_STATUS_RUNNING_STALE_HOURS", os.getenv("BACKFILL_LOCK_STALE_HOURS", "6")))
    except Exception:
        stale_hours = 6
    stale_seconds = max(stale_hours, 0) * 3600
    engine = get_quantmate_engine()
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT sync_date, source, interface_key, retry_count "
                "FROM data_sync_status "
                "WHERE sync_date BETWEEN :s AND :e "
                "AND ("
                "status IN ('error', 'partial', 'pending') "
                "OR ("
                "status = 'running' "
                "AND TIMESTAMPDIFF(SECOND, COALESCE(updated_at, started_at, created_at), CURRENT_TIMESTAMP) >= :stale_seconds"
                ")"
                ") "
                "ORDER BY sync_date ASC, source, interface_key"
            ),
            {"s": start, "e": end, "stale_seconds": stale_seconds},
        ).fetchall()
        return [(r[0], r[1], r[2], r[3]) for r in rows]


def _group_backfill_records_by_date(
    records: list[tuple[date, str, str, int]],
) -> list[tuple[date, list[tuple[date, str, str, int]]]]:
    grouped: list[tuple[date, list[tuple[date, str, str, int]]]] = []
    current_date: date | None = None
    current_group: list[tuple[date, str, str, int]] = []

    for record in records:
        sync_date = record[0]
        if current_date != sync_date:
            if current_group:
                grouped.append((current_date, current_group))
            current_date = sync_date
            current_group = [record]
        else:
            current_group.append(record)

    if current_group:
        grouped.append((current_date, current_group))

    return grouped


def _supports_backfill(iface, source: str, iface_key: str) -> bool:
    supports_backfill = True
    method = getattr(iface, "supports_backfill", None)
    if callable(method):
        try:
            supports_backfill = bool(method())
        except Exception:
            logger.exception("Failed to inspect backfill support for %s/%s", source, iface_key)
    return supports_backfill


def _normalize_log_value(value) -> str:
    if value is None:
        return "-"
    if isinstance(value, (list, tuple, set)):
        return ",".join(str(item) for item in value) if value else "-"
    if isinstance(value, dict):
        if not value:
            return "-"
        return ",".join(f"{key}={value[key]}" for key in sorted(value))
    text = str(value).strip()
    return text if text else "-"


def _build_backfill_log_context(
    sync_date: date,
    source: str,
    iface_key: str,
    result: SyncResult,
    iface=None,
) -> dict[str, str]:
    details = result.details or {}
    symbols = details.get("symbols")
    if symbols is None and iface is not None:
        symbols = getattr(iface, "SYMBOLS", None)
    if symbols is None and iface is not None:
        symbols = getattr(iface, "ETF_SYMBOLS", None)

    failed_symbols = details.get("failed_symbols")
    if failed_symbols is None and result.error_message and result.error_message.startswith("Failed:"):
        failed_symbols = result.error_message.split(":", 1)[1].strip()

    context = {
        "date": sync_date.isoformat(),
        "interface": f"{source}/{iface_key}",
        "status": result.status.value,
        "rows": str(result.rows_synced),
        "symbols": _normalize_log_value(symbols),
        "failed_symbols": _normalize_log_value(failed_symbols),
        "message": _normalize_log_value(result.error_message),
    }

    for key, value in sorted(details.items()):
        if key in {"symbols", "failed_symbols"}:
            continue
        context[key] = _normalize_log_value(value)

    return context


def _log_backfill_result(
    sync_date: date,
    source: str,
    iface_key: str,
    result: SyncResult,
    iface=None,
) -> None:
    context = _build_backfill_log_context(sync_date, source, iface_key, result, iface)
    extra_fields = " ".join(
        f"{key}={value}"
        for key, value in context.items()
        if key not in {"date", "interface", "status", "rows", "symbols", "failed_symbols", "message"}
    )
    logger.info(
        "Backfill result date=%s interface=%s status=%s rows=%s symbols=%s failed_symbols=%s message=%s%s",
        context["date"],
        context["interface"],
        context["status"],
        context["rows"],
        context["symbols"],
        context["failed_symbols"],
        context["message"],
        f" {extra_fields}" if extra_fields else "",
    )


def _execute_backfill_task(iface, source: str, sync_date: date) -> SyncResult:
    sem = _get_backfill_source_semaphore(source)
    try:
        if sem:
            sem.acquire()
        return iface.sync_date(sync_date)
    finally:
        if sem:
            sem.release()


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
    max_workers: int | None = None,
) -> dict[str, dict]:
    """Retry failed/pending syncs within the lookback window.

    Uses DB lock from the old daemon to prevent concurrent runs.
    """
    if lookback_days is None:
        lookback_days = LOOKBACK_DAYS
    if max_workers is None:
        max_workers = BACKFILL_WORKERS
    max_workers = max(1, max_workers)

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
        grouped_records = _group_backfill_records_by_date(failed)
        logger.info(
            "Backfill starting: records=%d dates=%d workers=%d lookback_days=%d",
            len(failed),
            len(grouped_records),
            max_workers,
            lookback_days,
        )

        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="datasync-backfill") as executor:
            for sync_date, date_records in grouped_records:
                logger.info(
                    "Backfill dispatch date=%s tasks=%d workers=%d",
                    sync_date,
                    len(date_records),
                    max_workers,
                )
                future_map = {}

                for _, source, iface_key, retry_count in date_records:
                    label = f"{source}/{iface_key}@{sync_date}"

                    if retry_count >= MAX_RETRIES:
                        logger.debug("Skipping %s: max retries reached", label)
                        continue

                    iface = registry.get_interface(source, iface_key)
                    if iface is None:
                        logger.warning("Backfill skip date=%s interface=%s: no plugin registered", sync_date, f"{source}/{iface_key}")
                        results[label] = {"status": "skipped", "reason": "no plugin"}
                        continue

                    if not _supports_backfill(iface, source, iface_key):
                        logger.info("Skipping historical backfill for non-historical interface %s", label)
                        _write_status(
                            sync_date,
                            source,
                            iface_key,
                            SyncStatus.SUCCESS.value,
                            0,
                            "Skipped historical backfill for non-historical interface",
                            retry_count=retry_count,
                        )
                        skipped_result = SyncResult(
                            SyncStatus.SUCCESS,
                            0,
                            "Skipped historical backfill for non-historical interface",
                        )
                        _log_backfill_result(sync_date, source, iface_key, skipped_result, iface)
                        results[label] = {"status": SyncStatus.SUCCESS.value, "rows": 0, "skipped": True}
                        continue

                    attempt_retry_count = retry_count + 1
                    _write_status(
                        sync_date,
                        source,
                        iface_key,
                        SyncStatus.RUNNING.value,
                        retry_count=attempt_retry_count,
                    )
                    logger.info(
                        "Backfill submit date=%s interface=%s retry=%d",
                        sync_date,
                        f"{source}/{iface_key}",
                        attempt_retry_count,
                    )
                    future = executor.submit(_execute_backfill_task, iface, source, sync_date)
                    future_map[future] = (sync_date, source, iface_key, attempt_retry_count, iface)

                for future in as_completed(future_map):
                    task_date, source, iface_key, retry_count, iface = future_map[future]
                    label = f"{source}/{iface_key}@{task_date}"
                    try:
                        result = future.result()
                        _write_status(
                            task_date,
                            source,
                            iface_key,
                            result.status.value,
                            result.rows_synced,
                            result.error_message,
                            retry_count=retry_count,
                        )
                        _log_backfill_result(task_date, source, iface_key, result, iface)
                        results[label] = {
                            "status": result.status.value,
                            "rows": result.rows_synced,
                            "error": result.error_message,
                        }
                    except Exception as e:
                        logger.exception("Backfill %s failed: %s", label, e)
                        _write_status(
                            task_date,
                            source,
                            iface_key,
                            SyncStatus.ERROR.value,
                            0,
                            str(e),
                            retry_count=retry_count,
                        )
                        error_result = SyncResult(SyncStatus.ERROR, 0, str(e))
                        _log_backfill_result(task_date, source, iface_key, error_result, iface)
                        results[label] = {"status": SyncStatus.ERROR.value, "rows": 0, "error": str(e)}
    finally:
        try:
            release_backfill_lock()
        except Exception:
            logger.exception("Failed to release backfill lock")

    return results
