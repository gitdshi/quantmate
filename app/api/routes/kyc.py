"""KYC verification API routes (Issue #9).

- POST /kyc/submit — Submit KYC application
- GET  /kyc/status — Get current user's KYC status
- GET  /kyc/pending — Admin: list pending reviews
- POST /kyc/{id}/review — Admin: approve/reject
"""
from __future__ import annotations

import re
from typing import Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field

from app.api.services.auth_service import get_current_user
from app.api.models.user import TokenData
from app.api.errors import ErrorCode
from app.api.exception_handlers import APIError
from app.api.pagination import PaginationParams, paginate

router = APIRouter(prefix="/kyc", tags=["KYC"])

VALID_ID_TYPES = {"mainland_id", "passport", "hk_pass"}


# --- Request/Response models ---

class KycSubmitRequest(BaseModel):
    real_name: str = Field(..., min_length=2, max_length=100)
    id_number: str = Field(..., min_length=6, max_length=30)
    id_type: str = Field("mainland_id")
    id_front_path: str = Field(..., min_length=1, max_length=500)
    id_back_path: str = Field(..., min_length=1, max_length=500)


class KycReviewRequest(BaseModel):
    action: str = Field(..., pattern=r"^(approved|rejected)$")
    review_notes: Optional[str] = Field(None, max_length=500)


class KycStatusResponse(BaseModel):
    status: str  # "not_submitted" | "pending" | "approved" | "rejected"
    real_name_masked: Optional[str] = None
    id_type: Optional[str] = None
    submitted_at: Optional[str] = None
    reviewed_at: Optional[str] = None
    review_notes: Optional[str] = None


def _mask_name(name: str) -> str:
    """Mask real name: 张三 → 张* , 张三丰 → 张*丰"""
    if len(name) <= 1:
        return name
    if len(name) == 2:
        return name[0] + "*"
    return name[0] + "*" * (len(name) - 2) + name[-1]


def _require_admin(current_user: TokenData = Depends(get_current_user)) -> TokenData:
    if current_user.username != "admin":
        raise APIError(status_code=403, code=ErrorCode.FORBIDDEN, message="Admin only")
    return current_user


# --- Endpoints ---

@router.post("/submit")
async def submit_kyc(
    body: KycSubmitRequest,
    current_user: TokenData = Depends(get_current_user),
):
    """Submit a KYC application."""
    if body.id_type not in VALID_ID_TYPES:
        raise APIError(
            status_code=400,
            code=ErrorCode.VALIDATION_ERROR,
            message=f"Invalid id_type. Allowed: {', '.join(VALID_ID_TYPES)}",
        )

    from app.domains.auth.dao.kyc_dao import KycDao
    dao = KycDao()

    # Check if already approved
    existing = dao.get_latest(current_user.user_id)
    if existing and existing["status"] == "approved":
        raise APIError(
            status_code=400,
            code=ErrorCode.VALIDATION_ERROR,
            message="KYC already approved",
        )
    # Don't allow re-submit while pending
    if existing and existing["status"] == "pending":
        raise APIError(
            status_code=400,
            code=ErrorCode.VALIDATION_ERROR,
            message="KYC submission already pending review",
        )

    submission_id = dao.insert(
        current_user.user_id,
        real_name=body.real_name,
        id_number=body.id_number,
        id_type=body.id_type,
        id_front_path=body.id_front_path,
        id_back_path=body.id_back_path,
    )
    return {"id": submission_id, "status": "pending"}


@router.get("/status", response_model=KycStatusResponse)
async def get_kyc_status(current_user: TokenData = Depends(get_current_user)):
    """Get the current user's KYC status."""
    from app.domains.auth.dao.kyc_dao import KycDao
    dao = KycDao()
    sub = dao.get_latest(current_user.user_id)
    if not sub:
        return KycStatusResponse(status="not_submitted")

    return KycStatusResponse(
        status=sub["status"],
        real_name_masked=_mask_name(sub["real_name"]),
        id_type=sub["id_type"],
        submitted_at=sub["created_at"].isoformat() if sub.get("created_at") else None,
        reviewed_at=sub["reviewed_at"].isoformat() if sub.get("reviewed_at") else None,
        review_notes=sub.get("review_notes"),
    )


@router.get("/pending")
async def list_pending(
    pagination: PaginationParams = Depends(),
    admin: TokenData = Depends(_require_admin),
):
    """Admin: list pending KYC submissions."""
    from app.domains.auth.dao.kyc_dao import KycDao
    dao = KycDao()
    total = dao.count_pending()
    rows = dao.list_pending(limit=pagination.limit, offset=pagination.offset)
    return paginate(rows, total, pagination)


@router.post("/{submission_id}/review")
async def review_kyc(
    submission_id: int,
    body: KycReviewRequest,
    admin: TokenData = Depends(_require_admin),
):
    """Admin: approve or reject a KYC submission."""
    from app.domains.auth.dao.kyc_dao import KycDao
    dao = KycDao()
    dao.update_status(
        submission_id=submission_id,
        status=body.action,
        reviewer_id=admin.user_id,
        review_notes=body.review_notes,
    )
    return {"id": submission_id, "status": body.action}
