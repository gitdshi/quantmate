"""Data synchronization status API routes.

Provides endpoints for monitoring sync status, summaries, and triggering manual syncs.
"""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from starlette.concurrency import run_in_threadpool

from app.api.dependencies.permissions import require_permission
from app.api.services.auth_service import get_current_user
from app.api.models.user import TokenData
from app.api.errors import ErrorCode
from app.api.exception_handlers import APIError
from app.infrastructure.config import get_runtime_int, get_settings

router = APIRouter(prefix="/datasync", tags=["DataSync"])


def _get_sync_status_response(
    sync_date: Optional[str],
    source: Optional[str],
    status: Optional[str],
    limit: int,
    offset: int,
):
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
        count_row = conn.execute(text(f"SELECT COUNT(*) FROM data_sync_status {where}"), params).fetchone()
        total = count_row[0] if count_row else 0
        rows = conn.execute(
            text(
                f"SELECT * FROM data_sync_status {where} "
                f"ORDER BY sync_date DESC, source, interface_key "
                f"LIMIT :lim OFFSET :off"
            ),
            {**params, "lim": limit, "off": offset},
        ).fetchall()

    return {"data": [dict(row._mapping) for row in rows], "total": total, "limit": limit, "offset": offset}


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
    return await run_in_threadpool(_get_sync_status_response, sync_date, source, status, limit, offset)


@router.get("/status/summary", dependencies=[require_permission("system", "read")])
async def get_sync_summary(
    days: int = Query(7, ge=1, le=90, description="Number of recent days to summarize"),
    current_user: TokenData = Depends(get_current_user),
):
    """Get a summary of sync status grouped by date and source."""
    from app.domains.extdata.service import DataSyncDashboardService

    service = DataSyncDashboardService()
    return await run_in_threadpool(service.get_summary, days=days)


@router.get("/status/latest", dependencies=[require_permission("system", "read")])
async def get_latest_sync_status(
    current_user: TokenData = Depends(get_current_user),
):
    """Get the most recent sync date and its status."""
    from app.domains.extdata.service import DataSyncDashboardService

    service = DataSyncDashboardService()
    return await run_in_threadpool(service.get_latest)


@router.get("/status/initialization", dependencies=[require_permission("system", "read")])
async def get_sync_initialization_status(
    current_user: TokenData = Depends(get_current_user),
):
    """Return whether all enabled sync-supported interfaces are initialized for the current coverage window."""
    from app.domains.extdata.service import DataSyncDashboardService

    service = DataSyncDashboardService()
    return await run_in_threadpool(service.get_initialization)


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
