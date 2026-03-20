"""Paper Trading routes — independent simulation environment.

Provides endpoints for:
- Deploying strategies to paper trading
- Manual paper order submission
- Viewing paper positions (aggregated from fills)
- Paper trading performance metrics
"""

from typing import Optional

from fastapi import APIRouter, Depends, Query, status
from pydantic import BaseModel

from app.api.services.auth_service import get_current_user
from app.api.models.user import TokenData
from app.api.errors import ErrorCode
from app.api.exception_handlers import APIError
from app.domains.trading.dao.order_dao import OrderDao
from app.domains.trading.paper_trading_service import PaperTradingService

router = APIRouter(prefix="/paper-trade", tags=["Paper Trading"])


# ── Request Models ──────────────────────────────────────────

class DeployRequest(BaseModel):
    strategy_id: int
    vt_symbol: str
    parameters: dict = {}


class PaperOrderRequest(BaseModel):
    symbol: str
    direction: str  # buy/sell
    order_type: str = "market"
    quantity: int
    price: Optional[float] = None


# ── Deploy Endpoints ────────────────────────────────────────

@router.post("/deploy", status_code=status.HTTP_201_CREATED)
async def deploy_strategy(req: DeployRequest, current_user: TokenData = Depends(get_current_user)):
    """Deploy a strategy to paper trading simulation."""
    svc = PaperTradingService()
    result = svc.deploy(
        user_id=current_user.user_id,
        strategy_id=req.strategy_id,
        vt_symbol=req.vt_symbol,
        parameters=req.parameters,
    )
    if not result.get("success"):
        raise APIError(
            status_code=400, code=ErrorCode.VALIDATION_ERROR,
            message=result.get("error", "Deploy failed"),
        )
    return result


@router.get("/deployments")
async def list_deployments(current_user: TokenData = Depends(get_current_user)):
    """List all paper trading deployments for the current user."""
    svc = PaperTradingService()
    deployments = svc.list_deployments(user_id=current_user.user_id)
    return {"deployments": deployments}


@router.post("/deployments/{deployment_id}/stop")
async def stop_deployment(deployment_id: int, current_user: TokenData = Depends(get_current_user)):
    """Stop a running paper deployment."""
    svc = PaperTradingService()
    ok = svc.stop_deployment(deployment_id=deployment_id, user_id=current_user.user_id)
    if not ok:
        raise APIError(
            status_code=404, code=ErrorCode.NOT_FOUND,
            message="Deployment not found or already stopped",
        )
    return {"message": "Deployment stopped"}


# ── Paper Order Endpoints ───────────────────────────────────

@router.post("/orders", status_code=status.HTTP_201_CREATED)
async def create_paper_order(req: PaperOrderRequest, current_user: TokenData = Depends(get_current_user)):
    """Submit a paper order. Market orders are auto-filled with simulated pricing."""
    if req.direction not in ("buy", "sell"):
        raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message="Invalid direction")
    if req.order_type not in ("market", "limit"):
        raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message="Invalid order type for paper trading")
    if req.quantity <= 0:
        raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message="Quantity must be positive")

    dao = OrderDao()
    order_id = dao.create(
        user_id=current_user.user_id,
        symbol=req.symbol,
        direction=req.direction,
        order_type=req.order_type,
        quantity=req.quantity,
        price=req.price,
        mode="paper",
    )

    # Paper market orders: immediate simulated fill
    if req.order_type == "market":
        fill_price = req.price or 0
        fee = round(fill_price * req.quantity * 0.0003, 4)
        dao.update_status(order_id, "filled", filled_quantity=req.quantity, avg_fill_price=fill_price, fee=fee)
        dao.insert_trade(order_id, req.quantity, fill_price, fee)

    order = dao.get_by_id(order_id, current_user.user_id)
    return order


@router.get("/orders")
async def list_paper_orders(
    status_filter: Optional[str] = Query(None, alias="status"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    current_user: TokenData = Depends(get_current_user),
):
    """List paper orders for the current user."""
    dao = OrderDao()
    orders, total = dao.list_by_user(
        user_id=current_user.user_id,
        status=status_filter,
        mode="paper",
        page=page,
        page_size=page_size,
    )
    return {"orders": orders, "meta": {"total": total, "page": page, "page_size": page_size}}


@router.post("/orders/{order_id}/cancel")
async def cancel_paper_order(order_id: int, current_user: TokenData = Depends(get_current_user)):
    """Cancel a pending paper order."""
    dao = OrderDao()
    ok = dao.cancel(order_id, current_user.user_id)
    if not ok:
        raise APIError(
            status_code=400, code=ErrorCode.VALIDATION_ERROR,
            message="Order not found or cannot be cancelled",
        )
    return {"message": "Paper order cancelled"}


# ── Positions & Performance ─────────────────────────────────

@router.get("/positions")
async def get_paper_positions(current_user: TokenData = Depends(get_current_user)):
    """Get aggregated paper positions computed from filled paper orders."""
    svc = PaperTradingService()
    positions = svc.get_positions(user_id=current_user.user_id)
    return {"positions": positions}


@router.get("/performance")
async def get_paper_performance(current_user: TokenData = Depends(get_current_user)):
    """Get paper trading performance metrics."""
    svc = PaperTradingService()
    perf = svc.get_performance(user_id=current_user.user_id)
    return perf
