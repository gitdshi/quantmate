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
    """Return aggregated portfolio analytics dashboard data.

    Returns the nested structure expected by the frontend AnalyticsDashboard component.
    """
    from app.domains.portfolio.dao.portfolio_dao import PortfolioDao

    dao = PortfolioDao()
    portfolio = dao.get_or_create(current_user.user_id)
    positions = dao.list_positions(portfolio["id"])

    total_market_value = sum(float(p.get("quantity", 0)) * float(p.get("avg_cost", 0)) for p in positions)
    total_value = float(portfolio["cash"]) + total_market_value

    return {
        "portfolio_stats": {
            "total_value": total_value,
            "total_pnl": 0.0,  # TODO: calculate from cost basis
            "total_pnl_pct": 0.0,
            "daily_pnl": 0.0,  # TODO: real-time price integration
            "daily_pnl_pct": 0.0,
            "positions_count": len(positions),
        },
        "performance_history": [],  # TODO: build from portfolio snapshots
        "strategy_performance": [],  # TODO: aggregate from backtest results
        "sector_allocation": [],  # TODO: derive from position sectors
        "risk_metrics": {
            "volatility": 0.0,
            "max_drawdown": 0.0,
            "var_95": 0.0,
            "beta": 0.0,
            "alpha": 0.0,
        },
    }


@router.get("/risk-metrics")
async def get_risk_metrics(current_user: TokenData = Depends(get_current_user)):
    """Return risk metrics in the nested structure expected by the frontend RiskMetrics component."""
    from app.domains.portfolio.dao.portfolio_dao import PortfolioDao

    dao = PortfolioDao()
    portfolio = dao.get_or_create(current_user.user_id)
    positions = dao.list_positions(portfolio["id"])

    # Calculate cash ratio from portfolio data
    total_market_value = sum(float(p.get("quantity", 0)) * float(p.get("avg_cost", 0)) for p in positions)
    total_value = float(portfolio["cash"]) + total_market_value
    cash_ratio = float(portfolio["cash"]) / total_value if total_value > 0 else 1.0

    return {
        "volatility": {
            "daily": 0.0,   # TODO: calculate from daily return series
            "monthly": 0.0,
            "annual": 0.0,
        },
        "value_at_risk": {
            "var_95": 0.0,
            "var_99": 0.0,
            "cvar_95": 0.0,
        },
        "drawdown": {
            "current": 0.0,
            "max": 0.0,
            "max_duration": 0,
            "recovery_time": None,
        },
        "beta": {
            "beta": 0.0,
            "alpha": 0.0,
            "r_squared": 0.0,
        },
        "concentration": {
            "top_position_pct": 0.0,
            "top_3_positions_pct": 0.0,
            "top_5_positions_pct": 0.0,
            "herfindahl_index": 0.0,
        },
        "liquidity": {
            "cash_ratio": round(cash_ratio, 4),
            "current_ratio": round(cash_ratio, 4),  # simplified
            "quick_ratio": round(cash_ratio, 4),     # simplified
        },
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
    current_prices = {p["symbol"]: float(p.get("last_price") or p.get("avg_cost", 0)) for p in positions}

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
