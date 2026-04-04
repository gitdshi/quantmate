"""Trading routes — Live trading via VNPy gateways (paper trading moved to /paper-trade/*)."""

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query, status
from pydantic import BaseModel

from app.api.dependencies.permissions import require_permission
from app.api.services.auth_service import get_current_user
from app.api.models.user import TokenData
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
    gateway_name: Optional[str] = None  # for live mode: which gateway to use


@router.post("/orders", status_code=status.HTTP_201_CREATED, dependencies=[require_permission("trading", "write")])
async def create_order(req: OrderCreateRequest, current_user: TokenData = Depends(get_current_user)):
    """Submit a live order through vnpy gateway. Paper orders should use /paper-trade/orders."""
    if req.mode == "paper":
        raise APIError(
            status_code=400,
            code=ErrorCode.VALIDATION_ERROR,
            message="Paper orders have moved to /api/v1/paper-trade/orders",
        )
    if req.direction not in ("buy", "sell"):
        raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message="Invalid direction")
    if req.order_type not in ("market", "limit", "stop", "stop_limit"):
        raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message="Invalid order type")
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
        stop_price=req.stop_price,
        strategy_id=req.strategy_id,
        portfolio_id=req.portfolio_id,
        mode="live",
    )

    # Route through vnpy gateway
    from app.domains.trading.vnpy_trading_service import VnpyTradingService

    svc = VnpyTradingService()
    vt_orderid = svc.send_order(
        symbol=req.symbol,
        direction=req.direction,
        order_type=req.order_type,
        quantity=req.quantity,
        price=req.price or 0,
        gateway_name=req.gateway_name,
    )
    if vt_orderid is None:
        dao.update_status(order_id, "rejected")
        raise APIError(
            status_code=502,
            code=ErrorCode.BAD_REQUEST,
            message="Failed to submit order to gateway — check gateway connection",
        )
    dao.update_status(order_id, "submitted")

    order = dao.get_by_id(order_id, current_user.user_id)
    return order


@router.get("/orders", dependencies=[require_permission("trading", "read")])
async def list_orders(
    status_filter: Optional[str] = Query(None, alias="status"),
    mode: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    current_user: TokenData = Depends(get_current_user),
):
    """List orders for the current user."""
    dao = OrderDao()
    orders, total = dao.list_by_user(
        current_user.user_id, status=status_filter, mode=mode, page=page, page_size=page_size
    )
    return {
        "data": orders,
        "meta": {"page": page, "page_size": page_size, "total": total},
    }


@router.get("/orders/{order_id}", dependencies=[require_permission("trading", "read")])
async def get_order(order_id: int, current_user: TokenData = Depends(get_current_user)):
    """Get a specific order."""
    dao = OrderDao()
    order = dao.get_by_id(order_id, current_user.user_id)
    if not order:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Order not found")
    return order


@router.post("/orders/{order_id}/cancel", dependencies=[require_permission("trading", "write")])
async def cancel_order(order_id: int, current_user: TokenData = Depends(get_current_user)):
    """Cancel a pending order."""
    dao = OrderDao()
    if not dao.cancel(order_id, current_user.user_id):
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


@router.post("/algo/twap", dependencies=[require_permission("trading", "write")])
async def algo_twap(req: TWAPRequest, current_user: TokenData = Depends(get_current_user)):
    """Generate TWAP order slices."""
    from app.domains.trading.algo_execution_service import AlgoExecutionService

    svc = AlgoExecutionService()
    return {"slices": svc.twap(req.total_quantity, req.num_slices, req.start_time, req.end_time, req.price_limit)}


@router.post("/algo/vwap", dependencies=[require_permission("trading", "write")])
async def algo_vwap(req: VWAPRequest, current_user: TokenData = Depends(get_current_user)):
    """Generate VWAP order slices."""
    from app.domains.trading.algo_execution_service import AlgoExecutionService

    svc = AlgoExecutionService()
    return {
        "slices": svc.vwap(
            req.total_quantity, req.volume_profile, req.start_time, req.interval_minutes, req.price_limit
        )
    }


@router.post("/algo/iceberg", dependencies=[require_permission("trading", "write")])
async def algo_iceberg(req: IcebergRequest, current_user: TokenData = Depends(get_current_user)):
    """Generate Iceberg order slices."""
    from app.domains.trading.algo_execution_service import AlgoExecutionService

    svc = AlgoExecutionService()
    return {"slices": svc.iceberg(req.total_quantity, req.display_quantity, req.price_limit)}


# ── VNPy gateway management ──────────────────────────────────────────


class GatewayConnectRequest(BaseModel):
    gateway_type: str  # ctp / xtp / sim
    config: dict
    gateway_name: Optional[str] = None


@router.post("/gateway/connect", dependencies=[require_permission("trading", "manage")])
async def connect_gateway(req: GatewayConnectRequest, current_user: TokenData = Depends(get_current_user)):
    """Connect to a vnpy broker gateway for live trading."""
    from app.domains.trading.vnpy_trading_service import VnpyTradingService, GatewayType

    try:
        gw_type = GatewayType(req.gateway_type)
    except ValueError:
        raise APIError(
            status_code=400, code=ErrorCode.VALIDATION_ERROR, message=f"Unknown gateway type: {req.gateway_type}"
        )

    svc = VnpyTradingService()
    ok = svc.connect_gateway(gw_type, req.config, req.gateway_name)
    if not ok:
        raise APIError(status_code=502, code=ErrorCode.BAD_REQUEST, message="Failed to connect gateway")
    return {"message": f"Gateway '{req.gateway_name or req.gateway_type}' connected"}


@router.post("/gateway/disconnect", dependencies=[require_permission("trading", "manage")])
async def disconnect_gateway(gateway_name: str = Query(...), current_user: TokenData = Depends(get_current_user)):
    """Disconnect a vnpy gateway."""
    from app.domains.trading.vnpy_trading_service import VnpyTradingService

    svc = VnpyTradingService()
    if not svc.disconnect_gateway(gateway_name):
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Gateway not found")
    return {"message": f"Gateway '{gateway_name}' disconnected"}


@router.get("/gateways", dependencies=[require_permission("trading", "read")])
async def list_gateways(current_user: TokenData = Depends(get_current_user)):
    """List connected vnpy gateways."""
    from app.domains.trading.vnpy_trading_service import VnpyTradingService

    svc = VnpyTradingService()
    return {"gateways": svc.list_gateways()}


@router.get("/gateway/positions", dependencies=[require_permission("trading", "read")])
async def gateway_positions(gateway_name: Optional[str] = None, current_user: TokenData = Depends(get_current_user)):
    """Query live positions from vnpy gateway."""
    from app.domains.trading.vnpy_trading_service import VnpyTradingService
    import dataclasses

    svc = VnpyTradingService()
    positions = svc.query_positions(gateway_name)
    return {"positions": [dataclasses.asdict(p) for p in positions]}


@router.get("/gateway/account", dependencies=[require_permission("trading", "read")])
async def gateway_account(gateway_name: Optional[str] = None, current_user: TokenData = Depends(get_current_user)):
    """Query account info from vnpy gateway."""
    from app.domains.trading.vnpy_trading_service import VnpyTradingService
    import dataclasses

    svc = VnpyTradingService()
    account = svc.query_account(gateway_name)
    if account is None:
        return {"account": None}
    return {"account": dataclasses.asdict(account)}


# ── Automated CTA strategy execution ─────────────────────────────────


class AutoStrategyStartRequest(BaseModel):
    strategy_class_name: str
    strategy_code: Optional[str] = None
    strategy_id: Optional[int] = None
    vt_symbol: str
    parameters: Optional[dict] = None
    gateway_name: Optional[str] = None


class AutoStrategyStopRequest(BaseModel):
    strategy_name: str


@router.post("/auto-strategy/start")
async def auto_strategy_start(req: AutoStrategyStartRequest, current_user: TokenData = Depends(get_current_user)):
    """Start an automated CTA strategy on a live vnpy gateway."""
    from app.domains.trading.cta_strategy_runner import CtaStrategyRunner

    runner = CtaStrategyRunner()
    result = runner.start_strategy(
        strategy_class_name=req.strategy_class_name,
        strategy_code=req.strategy_code,
        strategy_id=req.strategy_id,
        user_id=current_user.user_id,
        vt_symbol=req.vt_symbol,
        parameters=req.parameters or {},
        gateway_name=req.gateway_name,
    )
    if not result["success"]:
        raise APIError(
            status_code=400, code=ErrorCode.BAD_REQUEST, message=result.get("error", "Failed to start strategy")
        )
    return result


@router.post("/auto-strategy/stop")
async def auto_strategy_stop(req: AutoStrategyStopRequest, current_user: TokenData = Depends(get_current_user)):
    """Stop a running CTA strategy."""
    from app.domains.trading.cta_strategy_runner import CtaStrategyRunner

    runner = CtaStrategyRunner()
    ok = runner.stop_strategy(req.strategy_name)
    if not ok:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Strategy not running")
    return {"message": f"Strategy '{req.strategy_name}' stopped"}


@router.get("/auto-strategy/status")
async def auto_strategy_status(current_user: TokenData = Depends(get_current_user)):
    """List all running CTA strategies and their status."""
    from app.domains.trading.cta_strategy_runner import CtaStrategyRunner

    runner = CtaStrategyRunner()
    return {"strategies": runner.list_strategies()}
