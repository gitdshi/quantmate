"""Portfolio API routes (Issue: Portfolio Backend).

Endpoints the frontend already calls:
- GET  /portfolio/positions
- POST /portfolio/close
- GET  /portfolio/{id}/transactions
- GET  /portfolio/{id}/snapshots
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from app.api.services.auth_service import get_current_user
from app.api.models.user import TokenData
from app.api.errors import ErrorCode
from app.api.exception_handlers import APIError
from app.api.pagination import PaginationParams, paginate

router = APIRouter(prefix="/portfolio", tags=["Portfolio"])


class ClosePositionRequest(BaseModel):
    symbol: str = Field(..., min_length=1)
    quantity: int = Field(..., gt=0)
    price: float = Field(..., gt=0)


@router.get("/positions")
async def get_positions(current_user: TokenData = Depends(get_current_user)):
    """Get all open positions for the user's default portfolio."""
    from app.domains.portfolio.dao.portfolio_dao import PortfolioDao
    dao = PortfolioDao()
    portfolio = dao.get_or_create(current_user.user_id)
    positions = dao.list_positions(portfolio["id"])
    return {
        "portfolio_id": portfolio["id"],
        "cash": float(portfolio["cash"]),
        "positions": positions,
    }


@router.post("/close")
async def close_position(
    body: ClosePositionRequest,
    current_user: TokenData = Depends(get_current_user),
):
    """Close (sell) a position."""
    from app.domains.portfolio.dao.portfolio_dao import PortfolioDao
    dao = PortfolioDao()
    portfolio = dao.get_or_create(current_user.user_id)
    pid = portfolio["id"]

    pos = dao.get_position(pid, body.symbol)
    if not pos or pos["quantity"] < body.quantity:
        raise APIError(
            status_code=400,
            code=ErrorCode.PORTFOLIO_INSUFFICIENT_POSITION,
            message=f"Insufficient position for {body.symbol}",
        )

    # Calculate proceeds
    proceeds = body.quantity * body.price
    fee = round(proceeds * 0.001, 4)  # 0.1% commission estimate
    new_qty = pos["quantity"] - body.quantity
    new_cash = float(portfolio["cash"]) + proceeds - fee

    dao.upsert_position(pid, body.symbol, new_qty, float(pos["avg_cost"]))
    dao.update_cash(pid, new_cash)
    dao.insert_transaction(
        pid,
        symbol=body.symbol,
        direction="sell",
        quantity=body.quantity,
        price=body.price,
        fee=fee,
    )

    return {"symbol": body.symbol, "sold": body.quantity, "proceeds": proceeds, "fee": fee, "cash": new_cash}


@router.get("/{portfolio_id}/transactions")
async def get_transactions(
    portfolio_id: int,
    pagination: PaginationParams = Depends(),
    current_user: TokenData = Depends(get_current_user),
):
    """Get transaction history."""
    from app.domains.portfolio.dao.portfolio_dao import PortfolioDao
    dao = PortfolioDao()
    total = dao.count_transactions(portfolio_id)
    rows = dao.list_transactions(portfolio_id, limit=pagination.limit, offset=pagination.offset)
    return paginate(rows, total, pagination)


@router.get("/{portfolio_id}/snapshots")
async def get_snapshots(
    portfolio_id: int,
    current_user: TokenData = Depends(get_current_user),
):
    """Get daily NAV snapshots."""
    from app.domains.portfolio.dao.portfolio_dao import PortfolioDao
    dao = PortfolioDao()
    rows = dao.list_snapshots(portfolio_id)
    return {"data": rows}


# ── Position Sizing ──────────────────────────────────────────────────────

class PositionSizingRequest(BaseModel):
    method: str  # fixed_amount / fixed_pct / kelly / equal_risk / risk_parity
    total_capital: float
    params: dict = {}
    max_position_pct: float = 20.0
    max_total_pct: float = 80.0


@router.post("/position-sizing")
async def calculate_position_size(
    req: PositionSizingRequest,
    current_user: TokenData = Depends(get_current_user),
):
    """Calculate position size using the specified method."""
    from app.domains.portfolio.position_sizing_service import PositionSizingService
    svc = PositionSizingService()
    try:
        return svc.calculate(
            method=req.method,
            total_capital=req.total_capital,
            params=req.params,
            max_position_pct=req.max_position_pct,
            max_total_pct=req.max_total_pct,
        )
    except ValueError as e:
        from app.api.exception_handlers import APIError
        from app.api.errors import ErrorCode
        raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message=str(e))


# ── Performance Attribution ──────────────────────────────────────────────

class AttributionRequest(BaseModel):
    portfolio_weights: dict[str, float]
    benchmark_weights: dict[str, float]
    portfolio_returns: dict[str, float]
    benchmark_returns: dict[str, float]


@router.post("/attribution")
async def performance_attribution(
    req: AttributionRequest,
    current_user: TokenData = Depends(get_current_user),
):
    """Run Brinson performance attribution analysis."""
    from app.domains.portfolio.attribution_service import PerformanceAttributionService
    svc = PerformanceAttributionService()
    return svc.brinson_attribution(
        portfolio_weights=req.portfolio_weights,
        benchmark_weights=req.benchmark_weights,
        portfolio_returns=req.portfolio_returns,
        benchmark_returns=req.benchmark_returns,
    )
