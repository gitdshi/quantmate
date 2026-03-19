"""Multi-market data routes (HK / US)."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from app.api.services.auth_service import get_current_user
from app.api.models.user import TokenData

router = APIRouter(prefix="/market", tags=["MultiMarket"])


@router.get("/exchanges")
async def list_exchanges(current_user: TokenData = Depends(get_current_user)):
    """List supported exchanges."""
    from app.domains.market.multi_market_dao import MultiMarketDao

    return MultiMarketDao().list_exchanges()


@router.get("/hk/stocks")
async def list_hk_stocks(
    limit: int = Query(500, le=2000),
    current_user: TokenData = Depends(get_current_user),
):
    """List Hong Kong listed stocks."""
    from app.domains.market.multi_market_dao import MultiMarketDao

    return MultiMarketDao().list_hk_stocks(limit=limit)


@router.get("/hk/daily")
async def get_hk_daily(
    ts_code: str = Query(...),
    start_date: str = Query(...),
    end_date: str = Query(...),
    current_user: TokenData = Depends(get_current_user),
):
    """Get HK stock daily OHLCV data."""
    from app.domains.market.multi_market_dao import MultiMarketDao

    return MultiMarketDao().get_hk_daily(ts_code, start_date, end_date)


@router.get("/us/stocks")
async def list_us_stocks(
    limit: int = Query(500, le=2000),
    current_user: TokenData = Depends(get_current_user),
):
    """List US listed stocks."""
    from app.domains.market.multi_market_dao import MultiMarketDao

    return MultiMarketDao().list_us_stocks(limit=limit)


@router.get("/us/daily")
async def get_us_daily(
    ts_code: str = Query(...),
    start_date: str = Query(...),
    end_date: str = Query(...),
    current_user: TokenData = Depends(get_current_user),
):
    """Get US stock daily OHLCV data."""
    from app.domains.market.multi_market_dao import MultiMarketDao

    return MultiMarketDao().get_us_daily(ts_code, start_date, end_date)
