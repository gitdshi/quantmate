"""Data synchronization status API routes.

Provides endpoints for monitoring sync status, summaries, and triggering manual syncs.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel

from app.api.dependencies.permissions import require_permission
from app.api.services.auth_service import get_current_user
from app.api.models.user import TokenData
from app.api.errors import ErrorCode
from app.api.exception_handlers import APIError
from app.infrastructure.config import get_runtime_int, get_settings

router = APIRouter(prefix="/datasync", tags=["DataSync"])


class ManualSyncRequest(BaseModel):
    target_date: Optional[str] = None


# ---------------------------------------------------------------------------
# Sync Status
# ---------------------------------------------------------------------------


@router.get("/status", dependencies=[require_permission("system", "read")])
async def get_sync_status(
    sync_date: Optional[str] = Query(None, description="Filter by date (YYYY-MM-DD)"),
    source: Optional[str] = Query(None, description="Filter by source key"),
    status: Optional[str] = Query(None, description="Filter by status: pending, running, success, error, partial"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    current_user: TokenData = Depends(get_current_user),
):
    """Get paginated sync status records."""
    from sqlalchemy import text
    from app.infrastructure.db.connections import get_quantmate_engine

    engine = get_quantmate_engine()

    conditions = []
    params: dict = {}
    if sync_date:
        conditions.append("sync_date = :sd")
        params["sd"] = sync_date
    if source:
        conditions.append("source = :src")
        params["src"] = source
    if status:
        conditions.append("status = :st")
        params["st"] = status

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    with engine.connect() as conn:
        # Total count
        count_row = conn.execute(text(f"SELECT COUNT(*) FROM data_sync_status {where}"), params).fetchone()
        total = count_row[0] if count_row else 0

        # Data
        rows = conn.execute(
            text(
                f"SELECT * FROM data_sync_status {where} "
                f"ORDER BY sync_date DESC, source, interface_key "
                f"LIMIT :lim OFFSET :off"
            ),
            {**params, "lim": limit, "off": offset},
        ).fetchall()

        data = [dict(r._mapping) for r in rows]

    return {"data": data, "total": total, "limit": limit, "offset": offset}


@router.get("/status/summary", dependencies=[require_permission("system", "read")])
async def get_sync_summary(
    days: int = Query(7, ge=1, le=90, description="Number of recent days to summarize"),
    current_user: TokenData = Depends(get_current_user),
):
    """Get a summary of sync status grouped by date and source."""
    from sqlalchemy import text
    from app.infrastructure.db.connections import get_quantmate_engine

    engine = get_quantmate_engine()
    cutoff = (date.today() - timedelta(days=days)).isoformat()

    with engine.connect() as conn:
        # Per-date per-source status counts
        rows = conn.execute(
            text(
                "SELECT sync_date, source, status, COUNT(*) as cnt "
                "FROM data_sync_status WHERE sync_date >= :cutoff "
                "GROUP BY sync_date, source, status "
                "ORDER BY sync_date DESC, source, status"
            ),
            {"cutoff": cutoff},
        ).fetchall()

        summary: dict = {}
        for r in rows:
            d = r[0].isoformat() if hasattr(r[0], "isoformat") else str(r[0])
            if d not in summary:
                summary[d] = {}
            src = r[1]
            if src not in summary[d]:
                summary[d][src] = {"success": 0, "error": 0, "pending": 0, "running": 0, "partial": 0}
            summary[d][src][r[2]] = r[3]

        # Overall counts
        overall = conn.execute(
            text("SELECT status, COUNT(*) FROM data_sync_status WHERE sync_date >= :cutoff GROUP BY status"),
            {"cutoff": cutoff},
        ).fetchall()
        overall_map = {r[0]: r[1] for r in overall}

    return {
        "days": days,
        "overall": overall_map,
        "by_date": summary,
    }


@router.get("/status/latest", dependencies=[require_permission("system", "read")])
async def get_latest_sync_status(
    current_user: TokenData = Depends(get_current_user),
):
    """Get the most recent sync date and its status."""
    from sqlalchemy import text
    from app.infrastructure.db.connections import get_quantmate_engine

    engine = get_quantmate_engine()

    with engine.connect() as conn:
        row = conn.execute(text("SELECT MAX(sync_date) FROM data_sync_status")).fetchone()
        if not row or row[0] is None:
            return {"latest_date": None, "items": []}

        latest_date = row[0]
        items = conn.execute(
            text(
                "SELECT source, interface_key, status, rows_synced, error_message, "
                "retry_count, started_at, finished_at "
                "FROM data_sync_status WHERE sync_date = :sd "
                "ORDER BY source, interface_key"
            ),
            {"sd": latest_date},
        ).fetchall()

        return {
            "latest_date": latest_date.isoformat() if hasattr(latest_date, "isoformat") else str(latest_date),
            "items": [dict(r._mapping) for r in items],
        }


@router.get("/status/initialization", dependencies=[require_permission("system", "read")])
async def get_sync_initialization_status(
    current_user: TokenData = Depends(get_current_user),
):
    """Return whether all enabled sync-supported interfaces are initialized for the current coverage window."""
    from app.datasync.service.init_service import get_initialization_state

    return get_initialization_state()


# ---------------------------------------------------------------------------
# Manual sync trigger
# ---------------------------------------------------------------------------


@router.post("/trigger", dependencies=[require_permission("system", "manage")])
async def trigger_manual_sync(
    body: ManualSyncRequest,
    current_user: TokenData = Depends(get_current_user),
):
    """Dispatch a manual daily sync to an RQ worker (non-blocking)."""
    from app.worker.service.config import get_queue

    queue = get_queue("default")
    job = queue.enqueue(
        "app.worker.service.tasks.run_datasync_task",
        body.target_date,
        job_timeout=get_runtime_int(
            env_keys="MANUAL_DATASYNC_JOB_TIMEOUT_SECONDS",
            db_key="api.manual_datasync_job_timeout_seconds",
            default=1800,
        ),
    )
    return {"status": "queued", "job_id": job.id}


@router.get("/job/{job_id}", dependencies=[require_permission("system", "read")])
async def get_datasync_job_status(
    job_id: str,
    current_user: TokenData = Depends(get_current_user),
):
    """Poll RQ job status for a previously triggered datasync."""
    from redis import Redis
    from rq.job import Job, NoSuchJobError

    redis_url = get_settings().redis_url
    conn = Redis.from_url(redis_url)
    try:
        job = Job.fetch(job_id, connection=conn)
    except NoSuchJobError:
        raise APIError(status_code=404, code=ErrorCode.INTERNAL_ERROR, message=f"Job not found: {job_id}")

    result = None
    error = None
    if job.is_finished:
        result = job.result
    elif job.is_failed:
        error = str(job.exc_info) if job.exc_info else "Unknown error"

    return {
        "job_id": job_id,
        "status": job.get_status(),
        "result": result,
        "error": error,
    }
