"""Calendar API routes — trade days + financial events."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from typing import Optional

from app.api.services.auth_service import get_current_user_optional
from app.api.models.user import TokenData

router = APIRouter(prefix="/calendar", tags=["Calendar"])


@router.get("/trade-days")
async def get_trade_days(
    exchange: str = Query("SSE", max_length=10),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    current_user: Optional[TokenData] = Depends(get_current_user_optional),
):
    """Get trade-day calendar for an exchange."""
    from datetime import datetime
    from app.domains.market.calendar_service import CalendarService

    svc = CalendarService()
    s = datetime.strptime(start_date, "%Y-%m-%d").date() if start_date else None
    e = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else None
    return svc.get_trade_days(exchange=exchange, start_date=s, end_date=e)


@router.get("/events")
async def get_events(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    event_type: Optional[str] = Query(None, description="macro, ipo, dividend"),
    current_user: Optional[TokenData] = Depends(get_current_user_optional),
):
    """Get upcoming financial events (macro calendar, IPO, dividends)."""
    from datetime import datetime
    from app.domains.market.calendar_service import CalendarService

    svc = CalendarService()
    s = datetime.strptime(start_date, "%Y-%m-%d").date() if start_date else None
    e = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else None
    return svc.get_events(start_date=s, end_date=e, event_type=event_type)
