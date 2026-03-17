"""Audit log query/export API routes (Issue #2).

Admin-only access to query, filter, and export audit logs.
"""
from __future__ import annotations

import csv
import io
import json
from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse

from app.api.services.auth_service import get_current_user
from app.api.models.user import TokenData
from app.api.errors import ErrorCode
from app.api.exception_handlers import APIError
from app.api.pagination import PaginationParams, paginate
from app.domains.audit.dao.audit_log_dao import AuditLogDao

router = APIRouter(prefix="/audit", tags=["Audit"])


def _require_admin(current_user: TokenData = Depends(get_current_user)) -> TokenData:
    """Dependency: only admin users can access audit logs."""
    if current_user.username != "admin":
        raise APIError(
            status_code=403,
            code=ErrorCode.FORBIDDEN,
            message="Only admin can access audit logs",
        )
    return current_user


@router.get("/logs")
async def query_audit_logs(
    user_id: Optional[int] = Query(None, description="Filter by user ID"),
    operation_type: Optional[str] = Query(None, description="Filter by operation type"),
    resource_type: Optional[str] = Query(None, description="Filter by resource type"),
    start_date: Optional[date] = Query(None, description="Filter from date"),
    end_date: Optional[date] = Query(None, description="Filter to date"),
    pagination: PaginationParams = Depends(),
    admin: TokenData = Depends(_require_admin),
):
    """Query audit logs with filtering and pagination."""
    dao = AuditLogDao()
    total = dao.count(
        user_id=user_id,
        operation_type=operation_type,
        resource_type=resource_type,
        start_date=start_date,
        end_date=end_date,
    )
    rows = dao.query(
        user_id=user_id,
        operation_type=operation_type,
        resource_type=resource_type,
        start_date=start_date,
        end_date=end_date,
        limit=pagination.limit,
        offset=pagination.offset,
    )
    # Serialize datetime objects
    items = []
    for row in rows:
        item = dict(row)
        if item.get("timestamp"):
            item["timestamp"] = item["timestamp"].isoformat()
        if isinstance(item.get("details"), str):
            try:
                item["details"] = json.loads(item["details"])
            except (json.JSONDecodeError, TypeError):
                pass
        items.append(item)

    return paginate(items, total, pagination)


@router.get("/logs/export")
async def export_audit_logs(
    format: str = Query("json", description="Export format: json or csv"),
    user_id: Optional[int] = Query(None),
    operation_type: Optional[str] = Query(None),
    resource_type: Optional[str] = Query(None),
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    limit: int = Query(10000, le=50000, description="Max rows to export"),
    admin: TokenData = Depends(_require_admin),
):
    """Export audit logs as JSON or CSV."""
    dao = AuditLogDao()
    rows = dao.query(
        user_id=user_id,
        operation_type=operation_type,
        resource_type=resource_type,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        offset=0,
    )

    items = []
    for row in rows:
        item = dict(row)
        if item.get("timestamp"):
            item["timestamp"] = item["timestamp"].isoformat()
        if isinstance(item.get("details"), str):
            try:
                item["details"] = json.loads(item["details"])
            except (json.JSONDecodeError, TypeError):
                pass
        items.append(item)

    if format == "csv":
        output = io.StringIO()
        if items:
            writer = csv.DictWriter(output, fieldnames=items[0].keys())
            writer.writeheader()
            for item in items:
                # Flatten details for CSV
                row_copy = dict(item)
                if isinstance(row_copy.get("details"), dict):
                    row_copy["details"] = json.dumps(row_copy["details"])
                writer.writerow(row_copy)
        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=audit_logs.csv"},
        )

    # Default: JSON
    return StreamingResponse(
        iter([json.dumps(items, default=str, ensure_ascii=False)]),
        media_type="application/json",
        headers={"Content-Disposition": "attachment; filename=audit_logs.json"},
    )
