"""Datasync background tasks for RQ workers.

Provides backfill and per-item sync jobs enqueued from the settings API
when items are enabled for the first time.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def run_backfill_task(source: str, item_key: str, batch_size: int = 30) -> dict:
    """Backfill pending sync_status rows for *source/item_key*.

    Processes up to ``batch_size`` pending dates per invocation (oldest first).
    Returns a summary dict with counts.
    """
    from rq import get_current_job

    from app.datasync.registry import build_default_registry
    from app.datasync.service.sync_engine import MAX_RETRIES, _get_backfill_source_semaphore, _write_status
    from app.datasync.base import SyncStatus, SyncResult
    from app.datasync.table_manager import ensure_table
    from app.infrastructure.db.connections import get_quantmate_engine
    from sqlalchemy import text

    job = get_current_job()
    if job:
        job.meta["source"] = source
        job.meta["item_key"] = item_key
        job.save_meta()

    registry = build_default_registry()

    iface = registry.get_interface(source, item_key)
    if iface is None:
        msg = f"No interface registered for {source}/{item_key}"
        logger.warning(msg)
        return {"status": "skipped", "reason": msg}

    # Ensure target table exists
    engine = get_quantmate_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT target_database, target_table, table_created "
                "FROM data_source_items WHERE source = :s AND item_key = :k"
            ),
            {"s": source, "k": item_key},
        ).fetchone()
    if row and not row[2]:
        try:
            ensure_table(row[0], row[1], iface.get_ddl())
        except Exception as e:
            logger.exception("DDL failed for %s/%s: %s", source, item_key, e)
            return {"status": "error", "error": f"DDL failed: {e}"}

    # Fetch retryable dates
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT sync_date, status, retry_count FROM data_sync_status "
                "WHERE source = :s AND interface_key = :k "
                "AND status IN ('pending', 'error', 'partial') "
                "AND retry_count < :max_retries "
                "ORDER BY sync_date ASC LIMIT :limit"
            ),
            {"s": source, "k": item_key, "limit": batch_size, "max_retries": MAX_RETRIES},
        ).fetchall()

    pending_records = [(r[0], r[1], int(r[2] or 0)) for r in rows]
    if not pending_records:
        with engine.connect() as conn:
            exhausted_row = conn.execute(
                text(
                    "SELECT COUNT(*) FROM data_sync_status "
                    "WHERE source = :s AND interface_key = :k "
                    "AND status IN ('error', 'partial') AND retry_count >= :max_retries"
                ),
                {"s": source, "k": item_key, "max_retries": MAX_RETRIES},
            ).fetchone()

        exhausted = exhausted_row[0] if exhausted_row else 0
        status = "failed" if exhausted else "complete"
        logger.info("No retryable dates for %s/%s (exhausted=%d)", source, item_key, exhausted)
        return {"status": status, "synced": 0, "remaining": 0, "exhausted": exhausted}

    logger.info("Backfill %s/%s: %d retryable dates", source, item_key, len(pending_records))

    synced = 0
    errors = 0
    sem = _get_backfill_source_semaphore(source)

    for d, current_status, retry_count in pending_records:
        attempt_retry_count = retry_count + 1
        try:
            _write_status(d, source, item_key, SyncStatus.RUNNING.value, retry_count=attempt_retry_count)

            if sem:
                sem.acquire()
            try:
                result: SyncResult = iface.sync_date(d)
            finally:
                if sem:
                    sem.release()

            _write_status(
                d,
                source,
                item_key,
                result.status.value,
                result.rows_synced,
                result.error_message,
                retry_count=attempt_retry_count,
            )
            if result.status == SyncStatus.SUCCESS:
                synced += 1
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

        # Update job progress
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
            {"s": source, "k": item_key, "max_retries": MAX_RETRIES},
        ).fetchone()
        exhausted_row = conn.execute(
            text(
                "SELECT COUNT(*) FROM data_sync_status "
                "WHERE source = :s AND interface_key = :k "
                "AND status IN ('error', 'partial') AND retry_count >= :max_retries"
            ),
            {"s": source, "k": item_key, "max_retries": MAX_RETRIES},
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

    if remaining == 0 and exhausted > 0:
        summary["status"] = "failed"

    # Auto-enqueue next batch if more remain
    if remaining > 0:
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
