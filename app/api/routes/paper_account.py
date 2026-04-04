"""Paper Account routes — virtual capital account management for paper trading.

Provides endpoints for:
- Creating / listing / closing paper accounts
- Querying account details and equity curves
"""

from typing import Optional

from fastapi import APIRouter, Depends, Query, status
from pydantic import BaseModel, Field

from app.api.dependencies.permissions import require_permission
from app.api.services.auth_service import get_current_user
from app.api.models.user import TokenData
from app.api.errors import ErrorCode
from app.api.exception_handlers import APIError
from app.domains.trading.paper_account_service import PaperAccountService
from app.domains.trading.paper_analytics_service import PaperAnalyticsService

router = APIRouter(prefix="/paper-account", tags=["Paper Account"])


# ── Request models ──────────────────────────────────────────


class CreateAccountRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    initial_capital: float = Field(default=1_000_000, gt=0)
    market: str = Field(default="CN", pattern="^(CN|HK|US)$")


# ── Endpoints ───────────────────────────────────────────────


@router.post("", status_code=status.HTTP_201_CREATED, dependencies=[require_permission("trading", "write")])
async def create_account(req: CreateAccountRequest, current_user: TokenData = Depends(get_current_user)):
    """Create a new paper trading account."""
    svc = PaperAccountService()
    result = svc.create_account(
        user_id=current_user.user_id,
        name=req.name,
        initial_capital=req.initial_capital,
        market=req.market,
    )
    if not result.get("success"):
        raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message=result.get("error", "Create failed"))
    return result


@router.get("", dependencies=[require_permission("trading", "read")])
async def list_accounts(
    status_filter: Optional[str] = Query(None, alias="status"),
    current_user: TokenData = Depends(get_current_user),
):
    """List all paper accounts for the current user."""
    svc = PaperAccountService()
    accounts = svc.list_accounts(user_id=current_user.user_id, status=status_filter)
    return {"accounts": accounts}


@router.get("/{account_id}", dependencies=[require_permission("trading", "read")])
async def get_account(account_id: int, current_user: TokenData = Depends(get_current_user)):
    """Get detailed info for a specific paper account."""
    svc = PaperAccountService()
    account = svc.get_account(account_id=account_id, user_id=current_user.user_id)
    if not account:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Paper account not found")
    return account


@router.get("/{account_id}/equity-curve", dependencies=[require_permission("trading", "read")])
async def get_equity_curve(account_id: int, current_user: TokenData = Depends(get_current_user)):
    """Get daily equity snapshots for a paper account (for charting)."""
    svc = PaperAccountService()
    curve = svc.get_equity_curve(account_id=account_id, user_id=current_user.user_id)
    return {"curve": curve}


@router.get("/{account_id}/analytics", dependencies=[require_permission("trading", "read")])
async def get_analytics(account_id: int, current_user: TokenData = Depends(get_current_user)):
    """Get comprehensive performance analytics for a paper account."""
    svc = PaperAnalyticsService()
    result = svc.get_analytics(account_id=account_id, user_id=current_user.user_id)
    if "error" in result:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message=result["error"])
    return result


@router.delete("/{account_id}", dependencies=[require_permission("trading", "write")])
async def close_account(account_id: int, current_user: TokenData = Depends(get_current_user)):
    """Close (soft-delete) a paper account."""
    svc = PaperAccountService()
    ok = svc.close_account(account_id=account_id, user_id=current_user.user_id)
    if not ok:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Account not found or already closed")
    return {"message": "Paper account closed"}
