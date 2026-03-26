"""Market sentiment API routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from typing import Optional

from app.api.services.auth_service import get_current_user_optional
from app.api.models.user import TokenData

router = APIRouter(prefix="/sentiment", tags=["Sentiment"])


@router.get("/overview")
async def get_sentiment_overview(
    current_user: Optional[TokenData] = Depends(get_current_user_optional),
):
    """Get market sentiment overview — advance/decline, volume, index momentum."""
    from app.domains.market.sentiment_service import SentimentService

    return SentimentService().get_overview()


@router.get("/fear-greed")
async def get_fear_greed(
    current_user: Optional[TokenData] = Depends(get_current_user_optional),
):
    """Get composite fear & greed index (0-100)."""
    from app.domains.market.sentiment_service import SentimentService

    return SentimentService().get_fear_greed()
