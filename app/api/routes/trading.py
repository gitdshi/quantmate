"""Trading routes (P2 Issue: Order Management, Paper Trading)."""

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query, status
from pydantic import BaseModel

from app.api.services.auth_service import get_current_user
from app.api.errors import ErrorCode
from app.api.exception_handlers import APIError
from app.domains.trading.dao.order_dao import OrderDao

router = APIRouter(prefix="/trade", tags=["Trading"])


class OrderCreateRequest(BaseModel):
    symbol: str
    direction: str  # buy/sell
    order_type: str = "market"
    quantity: int
    price: Optional[float] = None
    stop_price: Optional[float] = None
    strategy_id: Optional[int] = None
    portfolio_id: Optional[int] = None
    mode: str = "paper"


@router.post("/orders", status_code=status.HTTP_201_CREATED)
async def create_order(req: OrderCreateRequest, current_user: dict = Depends(get_current_user)):
    """Submit a new order."""
    if req.direction not in ("buy", "sell"):
        raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message="Invalid direction")
    if req.order_type not in ("market", "limit", "stop", "stop_limit"):
        raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message="Invalid order type")
    if req.quantity <= 0:
        raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message="Quantity must be positive")

    dao = OrderDao()
    order_id = dao.create(
        user_id=current_user["id"],
        symbol=req.symbol,
        direction=req.direction,
        order_type=req.order_type,
        quantity=req.quantity,
        price=req.price,
        stop_price=req.stop_price,
        strategy_id=req.strategy_id,
        portfolio_id=req.portfolio_id,
        mode=req.mode,
    )

    # For paper trading market orders, simulate immediate fill
    if req.mode == "paper" and req.order_type == "market":
        fill_price = req.price or 0
        fee = round(fill_price * req.quantity * 0.0003, 4)
        dao.update_status(order_id, "filled", filled_quantity=req.quantity, avg_fill_price=fill_price, fee=fee)
        dao.insert_trade(order_id, req.quantity, fill_price, fee)

    order = dao.get_by_id(order_id, current_user["id"])
    return order


@router.get("/orders")
async def list_orders(
    status_filter: Optional[str] = Query(None, alias="status"),
    mode: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    current_user: dict = Depends(get_current_user),
):
    """List orders for the current user."""
    dao = OrderDao()
    orders, total = dao.list_by_user(
        current_user["id"], status=status_filter, mode=mode, page=page, page_size=page_size
    )
    return {
        "data": orders,
        "meta": {"page": page, "page_size": page_size, "total": total},
    }


@router.get("/orders/{order_id}")
async def get_order(order_id: int, current_user: dict = Depends(get_current_user)):
    """Get a specific order."""
    dao = OrderDao()
    order = dao.get_by_id(order_id, current_user["id"])
    if not order:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Order not found")
    return order


@router.post("/orders/{order_id}/cancel")
async def cancel_order(order_id: int, current_user: dict = Depends(get_current_user)):
    """Cancel a pending order."""
    dao = OrderDao()
    if not dao.cancel(order_id, current_user["id"]):
        raise APIError(status_code=400, code=ErrorCode.BAD_REQUEST, message="Cannot cancel this order")
    return {"message": "Order cancelled"}


# ── Algo execution endpoints ─────────────────────────────────────────


class TWAPRequest(BaseModel):
    total_quantity: int
    num_slices: int
    start_time: datetime
    end_time: datetime
    price_limit: float | None = None


class VWAPRequest(BaseModel):
    total_quantity: int
    volume_profile: list[float]
    start_time: datetime
    interval_minutes: int = 30
    price_limit: float | None = None


class IcebergRequest(BaseModel):
    total_quantity: int
    display_quantity: int
    price_limit: float


@router.post("/algo/twap")
async def algo_twap(req: TWAPRequest, current_user: dict = Depends(get_current_user)):
    """Generate TWAP order slices."""
    from app.domains.trading.algo_execution_service import AlgoExecutionService

    svc = AlgoExecutionService()
    return {"slices": svc.twap(req.total_quantity, req.num_slices, req.start_time, req.end_time, req.price_limit)}


@router.post("/algo/vwap")
async def algo_vwap(req: VWAPRequest, current_user: dict = Depends(get_current_user)):
    """Generate VWAP order slices."""
    from app.domains.trading.algo_execution_service import AlgoExecutionService

    svc = AlgoExecutionService()
    return {
        "slices": svc.vwap(
            req.total_quantity, req.volume_profile, req.start_time, req.interval_minutes, req.price_limit
        )
    }


@router.post("/algo/iceberg")
async def algo_iceberg(req: IcebergRequest, current_user: dict = Depends(get_current_user)):
    """Generate Iceberg order slices."""
    from app.domains.trading.algo_execution_service import AlgoExecutionService

    svc = AlgoExecutionService()
    return {"slices": svc.iceberg(req.total_quantity, req.display_quantity, req.price_limit)}
