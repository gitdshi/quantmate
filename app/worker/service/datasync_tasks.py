"""Datasync background tasks for RQ workers.

Provides backfill and per-item sync jobs enqueued from the settings API
when items are enabled for the first time.
"""

from __future__ import annotations

import logging
import sys
from datetime import date
from pathlib import Path

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def run_backfill_task(source: str, item_key: str, batch_size: int = 30) -> dict:
    """Backfill pending sync_status rows for *source/item_key*.

    Processes up to ``batch_size`` retryable dates per invocation, prioritizing
    pending -> partial -> error and newest dates first within each status.
    Returns a summary dict with counts.
    """
    from rq import get_current_job

    from app.datasync.registry import build_default_registry
    from app.datasync.service.sync_engine import (
        _BackfillTask,
        _backfill_status_priority,
        _final_retry_count_for_result,
        _get_backfill_source_semaphore,
        _get_backfill_rows_by_date,
        _group_range_backfill_dates,
        _is_quota_pause_result,
        _max_retries,
        _write_status,
    )
    from app.datasync.base import SyncStatus, SyncResult
    from app.datasync.sync_mode import (
        backfill_mode_uses_trade_calendar,
        infer_backfill_mode_from_interface,
        infer_sync_mode_from_interface,
        normalize_backfill_mode,
        normalize_sync_mode,
        sync_mode_supports_backfill,
    )
    from app.datasync.table_manager import ensure_table
    from app.infrastructure.db.connections import get_quantmate_engine
    from sqlalchemy import text

    job = get_current_job()
    if job:
        job.meta["source"] = source
        job.meta["item_key"] = item_key
        job.save_meta()

    registry = build_default_registry()
    max_retries = _max_retries()

    iface = registry.get_interface(source, item_key)
    if iface is None:
        msg = f"No interface registered for {source}/{item_key}"
        logger.warning(msg)
        return {"status": "skipped", "reason": msg}

    # Static-schema interfaces can precreate tables before sync.
    engine = get_quantmate_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT target_database, target_table, table_created, sync_mode, backfill_mode "
                "FROM data_source_items WHERE source = :s AND item_key = :k"
            ),
            {"s": source, "k": item_key},
        ).fetchone()
    sync_mode = normalize_sync_mode(
        row[3] if row is not None and len(row) > 3 else None,
        default=infer_sync_mode_from_interface(iface),
    )
    backfill_mode = normalize_backfill_mode(
        row[4] if row is not None and len(row) > 4 else None,
        default=infer_backfill_mode_from_interface(iface),
    )
    if row:
        try:
            if iface.should_ensure_table_before_sync():
                ensure_table(row[0], row[1], iface.get_ddl())
        except Exception as e:
            logger.exception("DDL failed for %s/%s: %s", source, item_key, e)
            return {"status": "error", "error": f"DDL failed: {e}"}

    anchor_target_date = None
    use_trade_calendar = sync_mode_supports_backfill(sync_mode) and backfill_mode_uses_trade_calendar(backfill_mode)
    if not use_trade_calendar:
        with engine.begin() as conn:
            latest_row = conn.execute(
                text(
                    "SELECT MAX(sync_date) FROM data_sync_status "
                    "WHERE source = :s AND interface_key = :k "
                    "AND status IN ('pending', 'error', 'partial') "
                    "AND retry_count < :max_retries"
                ),
                {"s": source, "k": item_key, "max_retries": max_retries},
            ).fetchone()
            anchor_target_date = latest_row[0] if latest_row else None
            if anchor_target_date is not None:
                skip_message = (
                    "Skipped historical backfill for latest-only interface"
                    if not sync_mode_supports_backfill(sync_mode)
                    else "Skipped historical backfill for anchor-only interface"
                )
                conn.execute(
                    text(
                        "UPDATE data_sync_status SET status = 'success', rows_synced = 0, "
                        "error_message = :msg, updated_at = CURRENT_TIMESTAMP "
                        "WHERE source = :s AND interface_key = :k "
                        "AND status IN ('pending', 'error', 'partial') "
                        "AND retry_count < :max_retries "
                        "AND sync_date < :latest_sync_date"
                    ),
                    {
                        "s": source,
                        "k": item_key,
                        "max_retries": max_retries,
                        "latest_sync_date": anchor_target_date,
                        "msg": skip_message,
                    },
                )

    # Fetch retryable dates
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT sync_date, status, retry_count FROM data_sync_status "
                "WHERE source = :s AND interface_key = :k "
                "AND status IN ('pending', 'error', 'partial') "
                "AND retry_count < :max_retries "
                "AND (:latest_sync_date IS NULL OR sync_date = :latest_sync_date) "
                "ORDER BY sync_date DESC LIMIT :limit"
            ),
            {
                "s": source,
                "k": item_key,
                "limit": batch_size,
                "max_retries": max_retries,
                "latest_sync_date": anchor_target_date,
            },
        ).fetchall()

    pending_records = [(r[0], r[1], int(r[2] or 0)) for r in rows]
    pending_records.sort(
        key=lambda record: (
            _backfill_status_priority(record[1]),
            -record[0].toordinal(),
        )
    )
    if not pending_records:
        with engine.connect() as conn:
            exhausted_row = conn.execute(
                text(
                    "SELECT COUNT(*) FROM data_sync_status "
                    "WHERE source = :s AND interface_key = :k "
                    "AND status IN ('error', 'partial') AND retry_count >= :max_retries"
                ),
                {"s": source, "k": item_key, "max_retries": max_retries},
            ).fetchone()

        exhausted = exhausted_row[0] if exhausted_row else 0
        status = "failed" if exhausted else "complete"
        logger.info("No retryable dates for %s/%s (exhausted=%d)", source, item_key, exhausted)
        return {"status": status, "synced": 0, "remaining": 0, "exhausted": exhausted}

    logger.info("Backfill %s/%s: %d retryable dates", source, item_key, len(pending_records))

    synced = 0
    errors = 0
    quota_paused = False
    sem = _get_backfill_source_semaphore(source)

    def _run_range_backfill(start_date: date, end_date: date) -> SyncResult:
        if sem:
            sem.acquire()
        try:
            return iface.sync_range(start_date, end_date)
        finally:
            if sem:
                sem.release()

    def _run_single_backfill(sync_date: date) -> SyncResult:
        if sem:
            sem.acquire()
        try:
            if backfill_mode == "code":
                return iface.sync_code(sync_date)
            if backfill_mode == "code_date":
                return iface.sync_code_date(sync_date)
            if backfill_mode == "other":
                return iface.sync_other(sync_date)
            return iface.sync_date(sync_date)
        finally:
            if sem:
                sem.release()

    if backfill_mode == "range":
        retry_by_date = {sync_date: retry_count + 1 for sync_date, _, retry_count in pending_records}
        priority_by_date = {sync_date: _backfill_status_priority(status) for sync_date, status, _ in pending_records}
        grouped_dates = _group_range_backfill_dates([sync_date for sync_date, _, _ in pending_records])
        grouped_dates.sort(
            key=lambda dates: (
                min(priority_by_date[sync_date] for sync_date in dates),
                -dates[-1].toordinal(),
                -dates[0].toordinal(),
            )
        )

        for grouped in grouped_dates:
            for sync_date in grouped:
                _write_status(
                    sync_date,
                    source,
                    item_key,
                    SyncStatus.RUNNING.value,
                    retry_count=retry_by_date[sync_date],
                )

            try:
                result = _run_range_backfill(grouped[0], grouped[-1])
                task = _BackfillTask(
                    source=source,
                    iface_key=item_key,
                    iface=iface,
                    dates=grouped,
                    retry_counts=retry_by_date,
                    mode="range",
                    status_priority=min(priority_by_date[sync_date] for sync_date in grouped),
                )
                rows_by_date = _get_backfill_rows_by_date(task, result)

                for sync_date in grouped:
                    per_date_result = SyncResult(
                        result.status,
                        rows_by_date.get(sync_date, 0),
                        result.error_message,
                        details=result.details,
                    )
                    _write_status(
                        sync_date,
                        source,
                        item_key,
                        per_date_result.status.value,
                        per_date_result.rows_synced,
                        per_date_result.error_message,
                        retry_count=_final_retry_count_for_result(per_date_result, retry_by_date[sync_date]),
                    )
                    if per_date_result.status == SyncStatus.SUCCESS:
                        synced += 1
                    elif _is_quota_pause_result(per_date_result):
                        quota_paused = True
                    else:
                        errors += 1
                if quota_paused:
                    logger.warning(
                        "Backfill %s/%s paused on %s -> %s due to quota: %s",
                        source,
                        item_key,
                        grouped[0],
                        grouped[-1],
                        result.error_message,
                    )
                    break
            except Exception as e:
                logger.exception("Backfill %s/%s on %s -> %s failed: %s", source, item_key, grouped[0], grouped[-1], e)
                for sync_date in grouped:
                    _write_status(
                        sync_date,
                        source,
                        item_key,
                        SyncStatus.ERROR.value,
                        0,
                        str(e),
                        retry_count=retry_by_date[sync_date],
                    )
                    errors += 1

            if job:
                job.meta["progress"] = f"{synced + errors}/{len(pending_records)}"
                job.save_meta()
    else:
        for d, current_status, retry_count in pending_records:
            attempt_retry_count = retry_count + 1
            try:
                _write_status(d, source, item_key, SyncStatus.RUNNING.value, retry_count=attempt_retry_count)

                result = _run_single_backfill(d)

                _write_status(
                    d,
                    source,
                    item_key,
                    result.status.value,
                    result.rows_synced,
                    result.error_message,
                    retry_count=_final_retry_count_for_result(result, attempt_retry_count),
                )
                if result.status == SyncStatus.SUCCESS:
                    synced += 1
                elif _is_quota_pause_result(result):
                    quota_paused = True
                    logger.warning("Backfill %s/%s paused on %s due to quota: %s", source, item_key, d, result.error_message)
                    break
                else:
                    errors += 1
            except Exception as e:
                logger.exception("Backfill %s/%s on %s failed: %s", source, item_key, d, e)
                _write_status(
                    d,
                    source,
                    item_key,
                    SyncStatus.ERROR.value,
                    0,
                    str(e),
                    retry_count=attempt_retry_count,
                )
                errors += 1

            if job:
                job.meta["progress"] = f"{synced + errors}/{len(pending_records)}"
                job.save_meta()

    # Check if more retryable dates remain or if any records exhausted retries
    with engine.connect() as conn:
        remaining_row = conn.execute(
            text(
                "SELECT COUNT(*) FROM data_sync_status "
                "WHERE source = :s AND interface_key = :k "
                "AND status IN ('pending', 'error', 'partial') AND retry_count < :max_retries"
            ),
            {"s": source, "k": item_key, "max_retries": max_retries},
        ).fetchone()
        exhausted_row = conn.execute(
            text(
                "SELECT COUNT(*) FROM data_sync_status "
                "WHERE source = :s AND interface_key = :k "
                "AND status IN ('error', 'partial') AND retry_count >= :max_retries"
            ),
            {"s": source, "k": item_key, "max_retries": max_retries},
        ).fetchone()
    remaining = remaining_row[0] if remaining_row else 0
    exhausted = exhausted_row[0] if exhausted_row else 0

    summary = {
        "status": "complete" if remaining == 0 and exhausted == 0 else "partial",
        "synced": synced,
        "errors": errors,
        "remaining": remaining,
        "exhausted": exhausted,
    }

    if quota_paused:
        summary["paused"] = True

    if remaining == 0 and exhausted > 0:
        summary["status"] = "failed"

    # Auto-enqueue next batch if more remain
    if remaining > 0 and not quota_paused:
        try:
            from app.worker.service.config import get_queue
            from app.infrastructure.config import get_runtime_int

            queue = get_queue("low")
            queue.enqueue(
                run_backfill_task,
                source,
                item_key,
                batch_size,
                job_timeout=get_runtime_int(
                    env_keys="DATASYNC_BACKFILL_JOB_TIMEOUT_SECONDS",
                    db_key="datasync.backfill_job_timeout_seconds",
                    default=3600,
                ),
            )
            summary["next_job_enqueued"] = True
        except Exception:
            logger.warning("Failed to enqueue next backfill batch for %s/%s", source, item_key, exc_info=True)

    logger.info("Backfill %s/%s done: %s", source, item_key, summary)
    return summary
