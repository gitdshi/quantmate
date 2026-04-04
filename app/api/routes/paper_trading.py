"""Paper Trading routes — independent simulation environment.

Provides endpoints for:
- Deploying strategies to paper trading
- Manual paper order submission with matching engine + market rules
- Viewing paper positions (aggregated from fills)
- Paper trading performance metrics
"""

from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, Query, status
from pydantic import BaseModel

from app.api.dependencies.permissions import require_permission
from app.api.services.auth_service import get_current_user
from app.api.models.user import TokenData
from app.api.errors import ErrorCode
from app.api.exception_handlers import APIError
from app.domains.trading.dao.order_dao import OrderDao
from app.domains.trading.paper_trading_service import PaperTradingService
from app.domains.trading.paper_account_service import PaperAccountService
from app.domains.trading.matching_engine import try_fill_market_order
from app.domains.trading.market_rules import validate_order
from app.domains.market.realtime_quote_service import RealtimeQuoteService
from app.infrastructure.db.connections import connection
from sqlalchemy import text

router = APIRouter(prefix="/paper-trade", tags=["Paper Trading"])


# ── Request Models ──────────────────────────────────────────


class DeployRequest(BaseModel):
    strategy_id: int
    vt_symbol: str
    parameters: dict = {}
    paper_account_id: Optional[int] = None
    execution_mode: str = "auto"  # auto/semi_auto


class PaperOrderRequest(BaseModel):
    paper_account_id: Optional[int] = None
    symbol: str
    direction: str  # buy/sell
    order_type: str = "market"  # market/limit
    quantity: int
    price: Optional[float] = None
    stop_price: Optional[float] = None


# ── Deploy Endpoints ────────────────────────────────────────


@router.post("/deploy", status_code=status.HTTP_201_CREATED, dependencies=[require_permission("trading", "manage")])
async def deploy_strategy(req: DeployRequest, current_user: TokenData = Depends(get_current_user)):
    """Deploy a strategy to paper trading simulation."""
    if req.execution_mode not in ("auto", "semi_auto"):
        raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message="execution_mode must be 'auto' or 'semi_auto'")

    # Validate paper account if provided
    if req.paper_account_id:
        acct_svc = PaperAccountService()
        account = acct_svc.get_account(req.paper_account_id, current_user.user_id)
        if not account:
            raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Paper account not found")
        if account["status"] != "active":
            raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message="Paper account is not active")

    svc = PaperTradingService()
    result = svc.deploy(
        user_id=current_user.user_id,
        strategy_id=req.strategy_id,
        vt_symbol=req.vt_symbol,
        parameters=req.parameters,
        paper_account_id=req.paper_account_id,
        execution_mode=req.execution_mode,
    )
    if not result.get("success"):
        raise APIError(
            status_code=400,
            code=ErrorCode.VALIDATION_ERROR,
            message=result.get("error", "Deploy failed"),
        )

    # If paper_account_id provided, start the strategy executor thread
    if req.paper_account_id:
        from app.domains.trading.paper_strategy_executor import PaperStrategyExecutor
        executor = PaperStrategyExecutor()
        exec_result = executor.start_deployment(
            deployment_id=result["deployment_id"],
            paper_account_id=req.paper_account_id,
            user_id=current_user.user_id,
            strategy_class_name=result.get("strategy_name", ""),
            vt_symbol=req.vt_symbol,
            parameters=req.parameters,
            execution_mode=req.execution_mode,
            strategy_id=req.strategy_id,
        )
        if not exec_result.get("success"):
            result["executor_warning"] = exec_result.get("error", "Executor failed to start")

    return result


@router.get("/deployments", dependencies=[require_permission("trading", "read")])
async def list_deployments(current_user: TokenData = Depends(get_current_user)):
    """List all paper trading deployments for the current user."""
    svc = PaperTradingService()
    deployments = svc.list_deployments(user_id=current_user.user_id)
    return {"deployments": deployments}


@router.post("/deployments/{deployment_id}/stop", dependencies=[require_permission("trading", "manage")])
async def stop_deployment(deployment_id: int, current_user: TokenData = Depends(get_current_user)):
    """Stop a running paper deployment."""
    # Stop the executor thread if running
    from app.domains.trading.paper_strategy_executor import PaperStrategyExecutor
    executor = PaperStrategyExecutor()
    executor.stop_deployment(deployment_id)

    svc = PaperTradingService()
    ok = svc.stop_deployment(deployment_id=deployment_id, user_id=current_user.user_id)
    if not ok:
        raise APIError(
            status_code=404,
            code=ErrorCode.NOT_FOUND,
            message="Deployment not found or already stopped",
        )
    return {"message": "Deployment stopped"}


# ── Paper Order Endpoints ───────────────────────────────────


@router.post("/orders", status_code=status.HTTP_201_CREATED, dependencies=[require_permission("trading", "write")])
async def create_paper_order(req: PaperOrderRequest, current_user: TokenData = Depends(get_current_user)):
    """Submit a paper order with market-rules validation & matching engine."""
    if req.direction not in ("buy", "sell"):
        raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message="Invalid direction")
    if req.order_type not in ("market", "limit"):
        raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message="Invalid order type for paper trading")
    if req.quantity <= 0:
        raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message="Quantity must be positive")

    # ── Resolve paper account & market ──────────────────────
    acct_svc = PaperAccountService()
    account = None
    market = "CN"
    if req.paper_account_id is not None:
        account = acct_svc.get_account(req.paper_account_id, current_user.user_id)
        if not account:
            raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Paper account not found")
        if account["status"] != "active":
            raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message="Paper account is not active")
        market = account["market"]

    # ── Fetch realtime quote ────────────────────────────────
    quote_svc = RealtimeQuoteService()
    try:
        quote = quote_svc.get_quote(req.symbol, market)
    except Exception:
        quote = {}

    last_price = quote.get("last_price") or quote.get("price") or quote.get("current") or req.price or 0.0
    prev_close = quote.get("prev_close") or quote.get("pre_close") or last_price

    # Market orders should validate against the executable market price, not
    # an optional client-supplied placeholder price that may drift from live bands.
    validation_price = req.price
    if req.order_type == "market":
        validation_price = last_price if last_price > 0 else req.price

    # ── Market rules validation ─────────────────────────────
    vr = validate_order(
        market=market,
        symbol=req.symbol,
        direction=req.direction,
        quantity=req.quantity,
        price=validation_price,
        order_type=req.order_type,
        prev_close=prev_close if prev_close else None,
        available_balance=account["balance"] if account and req.direction == "buy" else None,
    )
    if not vr.valid:
        raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message=vr.error or "Order validation failed")

    dao = OrderDao()
    today_str = date.today().isoformat()

    # ── Market order → immediate fill via matching engine ───
    if req.order_type == "market":
        if last_price <= 0:
            raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message="No valid market price available for symbol")

        fill = try_fill_market_order(
            direction=req.direction,
            quantity=req.quantity,
            market=market,
            last_price=last_price,
        )
        if not fill.filled:
            raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message=fill.reason or "Market order fill failed")

        # Freeze funds for buy, then settle immediately
        if account and req.direction == "buy":
            total_cost = fill.fill_price * fill.fill_quantity + fill.fee.total
            ok = acct_svc.freeze_funds(req.paper_account_id, total_cost)
            if not ok:
                raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message="Insufficient funds in paper account")
            acct_svc.settle_buy(req.paper_account_id, total_cost)

        elif account and req.direction == "sell":
            proceeds = fill.fill_price * fill.fill_quantity - fill.fee.total
            acct_svc.settle_sell(req.paper_account_id, proceeds)

        order_id = dao.create(
            user_id=current_user.user_id,
            symbol=req.symbol,
            direction=req.direction,
            order_type=req.order_type,
            quantity=req.quantity,
            price=fill.fill_price,
            mode="paper",
            paper_account_id=req.paper_account_id,
            buy_date=today_str if req.direction == "buy" else None,
        )
        dao.update_status(order_id, "filled", filled_quantity=fill.fill_quantity, avg_fill_price=fill.fill_price, fee=fill.fee.total)
        dao.insert_trade(order_id, fill.fill_quantity, fill.fill_price, fill.fee.total)

    # ── Limit order → pending, worker handles matching ─
    else:
        est_price = req.price or last_price or 0.0
        if account and req.direction == "buy" and est_price > 0:
            est_cost = est_price * req.quantity * 1.003  # small buffer for fees
            ok = acct_svc.freeze_funds(req.paper_account_id, est_cost)
            if not ok:
                raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message="Insufficient funds in paper account")

        order_id = dao.create(
            user_id=current_user.user_id,
            symbol=req.symbol,
            direction=req.direction,
            order_type=req.order_type,
            quantity=req.quantity,
            price=req.price,
            stop_price=req.stop_price,
            mode="paper",
            paper_account_id=req.paper_account_id,
            buy_date=today_str if req.direction == "buy" else None,
        )
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
    """Cancel a pending paper order and release frozen funds."""
    dao = OrderDao()
    order = dao.get_by_id(order_id, current_user.user_id)
    if not order:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Order not found")

    ok = dao.cancel(order_id, current_user.user_id)
    if not ok:
        raise APIError(
            status_code=400,
            code=ErrorCode.VALIDATION_ERROR,
            message="Order not found or cannot be cancelled",
        )

    # Release frozen funds for unfilled buy orders
    paper_account_id = order.get("paper_account_id")
    if paper_account_id and order["direction"] == "buy":
        est_price = order.get("price") or 0.0
        if est_price > 0:
            est_cost = est_price * order["quantity"] * 1.003
            acct_svc = PaperAccountService()
            acct_svc.release_funds(paper_account_id, est_cost)

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


# ── Signal Endpoints (Semi-auto mode) ──────────────────────


@router.get("/signals")
async def list_signals(
    status_filter: Optional[str] = Query(None, alias="status"),
    paper_account_id: Optional[int] = Query(None),
    current_user: TokenData = Depends(get_current_user),
):
    """List strategy signals for the current user."""
    with connection("quantmate") as conn:
        conditions = ["user_id = :uid"]
        params: dict = {"uid": current_user.user_id}
        if status_filter:
            conditions.append("status = :status")
            params["status"] = status_filter
        if paper_account_id:
            conditions.append("paper_account_id = :paid")
            params["paid"] = paper_account_id

        where = " AND ".join(conditions)
        rows = conn.execute(
            text(f"""
                SELECT id, user_id, paper_account_id, deployment_id, symbol, direction,
                       quantity, suggested_price, reason, status, created_at, confirmed_at
                FROM paper_signals
                WHERE {where}
                ORDER BY created_at DESC LIMIT 100
            """),
            params,
        ).fetchall()

    signals = [
        {
            "id": r.id,
            "paper_account_id": r.paper_account_id,
            "deployment_id": r.deployment_id,
            "symbol": r.symbol,
            "direction": r.direction,
            "quantity": r.quantity,
            "suggested_price": float(r.suggested_price) if r.suggested_price else None,
            "reason": r.reason,
            "status": r.status,
            "created_at": str(r.created_at) if r.created_at else None,
            "confirmed_at": str(r.confirmed_at) if r.confirmed_at else None,
        }
        for r in rows
    ]
    return {"signals": signals}


@router.post("/signals/{signal_id}/confirm")
async def confirm_signal(signal_id: int, current_user: TokenData = Depends(get_current_user)):
    """Confirm a pending signal — execute the trade on the paper account."""
    with connection("quantmate") as conn:
        row = conn.execute(
            text("""
                SELECT id, user_id, paper_account_id, symbol, direction, quantity, suggested_price
                FROM paper_signals
                WHERE id = :sid AND user_id = :uid AND status = 'pending'
            """),
            {"sid": signal_id, "uid": current_user.user_id},
        ).fetchone()

    if not row:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Signal not found or not pending")

    # Execute the trade via matching engine
    acct_svc = PaperAccountService()
    account = acct_svc.get_account(row.paper_account_id, current_user.user_id)
    if not account or account["status"] != "active":
        raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message="Paper account not active")

    market = account["market"]
    quote_svc = RealtimeQuoteService()
    try:
        quote = quote_svc.get_quote(row.symbol, market)
    except Exception:
        quote = {}
    last_price = quote.get("last_price") or quote.get("price") or quote.get("current") or 0.0

    if last_price <= 0:
        raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message="No market price available")

    fill = try_fill_market_order(
        direction=row.direction,
        quantity=row.quantity,
        market=market,
        last_price=last_price,
    )
    if not fill.filled:
        raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message=fill.reason or "Fill failed")

    # Settle account
    if row.direction == "buy":
        total_cost = fill.fill_price * fill.fill_quantity + fill.fee.total
        ok = acct_svc.freeze_funds(row.paper_account_id, total_cost)
        if not ok:
            raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message="Insufficient funds")
        acct_svc.settle_buy(row.paper_account_id, total_cost)
    else:
        proceeds = fill.fill_price * fill.fill_quantity - fill.fee.total
        acct_svc.settle_sell(row.paper_account_id, proceeds)

    # Create order record
    dao = OrderDao()
    today_str = date.today().isoformat()
    order_id = dao.create(
        user_id=current_user.user_id,
        symbol=row.symbol,
        direction=row.direction,
        order_type="market",
        quantity=row.quantity,
        price=fill.fill_price,
        mode="paper",
        paper_account_id=row.paper_account_id,
        buy_date=today_str if row.direction == "buy" else None,
    )
    dao.update_status(order_id, "filled", filled_quantity=fill.fill_quantity, avg_fill_price=fill.fill_price, fee=fill.fee.total)
    dao.insert_trade(order_id, fill.fill_quantity, fill.fill_price, fill.fee.total)

    # Mark signal as confirmed
    with connection("quantmate") as conn:
        conn.execute(
            text("UPDATE paper_signals SET status='confirmed', confirmed_at=NOW() WHERE id=:sid"),
            {"sid": signal_id},
        )
        conn.commit()

    return {"message": "Signal confirmed and order filled", "order_id": order_id}


@router.post("/signals/{signal_id}/reject")
async def reject_signal(signal_id: int, current_user: TokenData = Depends(get_current_user)):
    """Reject a pending signal."""
    with connection("quantmate") as conn:
        result = conn.execute(
            text("UPDATE paper_signals SET status='rejected' WHERE id=:sid AND user_id=:uid AND status='pending'"),
            {"sid": signal_id, "uid": current_user.user_id},
        )
        conn.commit()
    if result.rowcount == 0:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Signal not found or not pending")
    return {"message": "Signal rejected"}
