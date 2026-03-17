"""Report generation and management routes (P2 Issue: Reports Auto-gen, Performance Attribution)."""
from typing import Optional

from fastapi import APIRouter, Depends, Query, status
from pydantic import BaseModel

from app.api.services.auth_service import get_current_user
from app.api.errors import ErrorCode
from app.api.exception_handlers import APIError
from app.domains.monitoring.dao.report_dao import ReportDao

router = APIRouter(prefix="/reports", tags=["Reports"])


class ReportGenerateRequest(BaseModel):
    report_type: str  # daily / weekly / monthly / custom
    title: Optional[str] = None
    content_json: Optional[dict] = None


@router.get("")
async def list_reports(
    report_type: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    current_user: dict = Depends(get_current_user),
):
    """List reports for the current user."""
    dao = ReportDao()
    reports, total = dao.list_by_user(current_user["id"], report_type=report_type, page=page, page_size=page_size)
    return {
        "data": reports,
        "meta": {"page": page, "page_size": page_size, "total": total},
    }


@router.get("/{report_id}")
async def get_report(report_id: int, current_user: dict = Depends(get_current_user)):
    """Get report detail."""
    dao = ReportDao()
    report = dao.get_by_id(report_id, current_user["id"])
    if not report:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Report not found")
    return report


@router.post("", status_code=status.HTTP_201_CREATED)
async def generate_report(req: ReportGenerateRequest, current_user: dict = Depends(get_current_user)):
    """Generate a new report."""
    valid_types = ("daily", "weekly", "monthly", "custom")
    if req.report_type not in valid_types:
        raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message="Invalid report type")
    dao = ReportDao()
    report_id = dao.create(
        user_id=current_user["id"],
        report_type=req.report_type,
        title=req.title or f"{req.report_type.capitalize()} Report",
        content_json=req.content_json or {},
    )
    return {"id": report_id, "message": "Report generated"}
