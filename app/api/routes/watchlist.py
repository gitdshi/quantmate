"""Watchlist management API routes (Issue #6)."""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from app.api.services.auth_service import get_current_user
from app.api.models.user import TokenData
from app.api.errors import ErrorCode
from app.api.exception_handlers import APIError

router = APIRouter(prefix="/data/watchlists", tags=["Watchlists"])


class WatchlistCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    description: Optional[str] = Field(None, max_length=500)


class WatchlistUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = Field(None, max_length=500)
    sort_order: Optional[int] = None


class WatchlistItemAdd(BaseModel):
    symbol: str = Field(..., min_length=1, max_length=20)
    notes: Optional[str] = Field(None, max_length=500)


def _ensure_owner(watchlist: dict, user_id: int) -> None:
    if watchlist["user_id"] != user_id:
        raise APIError(status_code=403, code=ErrorCode.FORBIDDEN, message="Not your watchlist")


# --- Watchlist CRUD ---

@router.get("")
async def list_watchlists(current_user: TokenData = Depends(get_current_user)):
    """List all watchlists for the current user."""
    from app.domains.market.dao.watchlist_dao import WatchlistDao
    dao = WatchlistDao()
    return {"data": dao.list_for_user(current_user.user_id)}


@router.post("")
async def create_watchlist(
    body: WatchlistCreate,
    current_user: TokenData = Depends(get_current_user),
):
    from app.domains.market.dao.watchlist_dao import WatchlistDao
    dao = WatchlistDao()
    wid = dao.create(current_user.user_id, body.name, body.description)
    return {"id": wid, "name": body.name}


@router.put("/{watchlist_id}")
async def update_watchlist(
    watchlist_id: int,
    body: WatchlistUpdate,
    current_user: TokenData = Depends(get_current_user),
):
    from app.domains.market.dao.watchlist_dao import WatchlistDao
    dao = WatchlistDao()
    wl = dao.get(watchlist_id)
    if not wl:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Watchlist not found")
    _ensure_owner(wl, current_user.user_id)
    dao.update(watchlist_id, **body.model_dump(exclude_unset=True))
    return {"id": watchlist_id, "updated": True}


@router.delete("/{watchlist_id}")
async def delete_watchlist(
    watchlist_id: int,
    current_user: TokenData = Depends(get_current_user),
):
    from app.domains.market.dao.watchlist_dao import WatchlistDao
    dao = WatchlistDao()
    wl = dao.get(watchlist_id)
    if not wl:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Watchlist not found")
    _ensure_owner(wl, current_user.user_id)
    dao.delete(watchlist_id)
    return {"id": watchlist_id, "deleted": True}


# --- Watchlist items ---

@router.post("/{watchlist_id}/items")
async def add_item(
    watchlist_id: int,
    body: WatchlistItemAdd,
    current_user: TokenData = Depends(get_current_user),
):
    from app.domains.market.dao.watchlist_dao import WatchlistDao
    dao = WatchlistDao()
    wl = dao.get(watchlist_id)
    if not wl:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Watchlist not found")
    _ensure_owner(wl, current_user.user_id)
    item_id = dao.add_item(watchlist_id, body.symbol, body.notes)
    return {"id": item_id, "symbol": body.symbol}


@router.delete("/{watchlist_id}/items/{symbol}")
async def remove_item(
    watchlist_id: int,
    symbol: str,
    current_user: TokenData = Depends(get_current_user),
):
    from app.domains.market.dao.watchlist_dao import WatchlistDao
    dao = WatchlistDao()
    wl = dao.get(watchlist_id)
    if not wl:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Watchlist not found")
    _ensure_owner(wl, current_user.user_id)
    removed = dao.remove_item(watchlist_id, symbol)
    if not removed:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message=f"Symbol {symbol} not in watchlist")
    return {"watchlist_id": watchlist_id, "symbol": symbol, "removed": True}
