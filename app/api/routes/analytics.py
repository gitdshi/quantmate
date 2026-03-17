"""Analytics dashboard API routes (Issue: Analytics Dashboard Backend).

Frontend already calls:
- GET /api/analytics/dashboard
- GET /api/analytics/risk-metrics
"""
from __future__ import annotations

from fastapi import APIRouter, Depends

from app.api.services.auth_service import get_current_user
from app.api.models.user import TokenData

router = APIRouter(prefix="/analytics", tags=["Analytics"])


@router.get("/dashboard")
async def get_dashboard(current_user: TokenData = Depends(get_current_user)):
    """Return aggregated portfolio analytics dashboard data."""
    from app.domains.portfolio.dao.portfolio_dao import PortfolioDao
    dao = PortfolioDao()
    portfolio = dao.get_or_create(current_user.user_id)
    positions = dao.list_positions(portfolio["id"])

    total_market_value = sum(
        float(p.get("quantity", 0)) * float(p.get("avg_cost", 0)) for p in positions
    )
    total_value = float(portfolio["cash"]) + total_market_value

    return {
        "total_value": total_value,
        "cash": float(portfolio["cash"]),
        "market_value": total_market_value,
        "positions_count": len(positions),
        "daily_pnl": 0.0,  # TODO: real-time price integration
        "daily_return": 0.0,
        "positions": [
            {
                "symbol": p["symbol"],
                "quantity": p["quantity"],
                "avg_cost": float(p["avg_cost"]),
                "market_value": float(p["quantity"]) * float(p["avg_cost"]),
            }
            for p in positions
        ],
    }


@router.get("/risk-metrics")
async def get_risk_metrics(current_user: TokenData = Depends(get_current_user)):
    """Return risk metrics calculated from portfolio snapshots."""
    from app.domains.portfolio.dao.portfolio_dao import PortfolioDao
    dao = PortfolioDao()
    portfolio = dao.get_or_create(current_user.user_id)
    snapshots = dao.list_snapshots(portfolio["id"], limit=252)  # ~1 year

    if not snapshots:
        return {
            "volatility": 0.0,
            "sharpe_ratio": 0.0,
            "max_drawdown": 0.0,
            "returns_1d": 0.0,
            "returns_5d": 0.0,
            "returns_20d": 0.0,
            "returns_ytd": 0.0,
        }

    latest = snapshots[0]
    return {
        "volatility": 0.0,  # TODO: calculate from daily returns
        "sharpe_ratio": 0.0,
        "max_drawdown": 0.0,
        "returns_1d": float(latest.get("returns_1d") or 0),
        "returns_5d": float(latest.get("returns_5d") or 0),
        "returns_20d": float(latest.get("returns_20d") or 0),
        "returns_ytd": float(latest.get("returns_ytd") or 0),
    }


@router.get("/live-pnl")
async def get_live_pnl(current_user: TokenData = Depends(get_current_user)):
    """Compute real-time portfolio P&L from positions and last known prices."""
    from app.domains.portfolio.dao.portfolio_dao import PortfolioDao
    from app.domains.monitoring.pnl_monitor_service import PnLMonitorService

    dao = PortfolioDao()
    portfolio = dao.get_or_create(current_user.user_id)
    positions = dao.list_positions(portfolio["id"])

    # Use avg_cost as price proxy when real-time feed unavailable
    current_prices = {
        p["symbol"]: float(p.get("last_price") or p.get("avg_cost", 0))
        for p in positions
    }

    svc = PnLMonitorService()
    return svc.calculate_live_pnl(
        positions=positions,
        current_prices=current_prices,
        cash=float(portfolio["cash"]),
    )


@router.get("/anomalies")
async def detect_portfolio_anomalies(current_user: TokenData = Depends(get_current_user)):
    """Run anomaly detection rules against current portfolio."""
    from app.domains.portfolio.dao.portfolio_dao import PortfolioDao
    from app.domains.monitoring.pnl_monitor_service import PnLMonitorService

    dao = PortfolioDao()
    portfolio = dao.get_or_create(current_user.user_id)
    positions = dao.list_positions(portfolio["id"])
    snapshots = dao.list_snapshots(portfolio["id"], limit=30)

    daily_returns = []
    for s in reversed(snapshots):
        ret = float(s.get("returns_1d") or 0)
        daily_returns.append(ret)

    # Compute position market values
    total_value = float(portfolio["cash"])
    pos_with_mv = []
    for p in positions:
        price = float(p.get("last_price") or p.get("avg_cost", 0))
        mv = float(p["quantity"]) * price
        total_value += mv
        pos_with_mv.append({**p, "market_value": mv})

    svc = PnLMonitorService()
    alerts = svc.detect_anomalies(daily_returns, pos_with_mv, total_value)
    return {"alerts": alerts, "rules": svc.get_rules()}
