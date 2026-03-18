"""Indicator library routes (P2 Issue: Indicator Library Extension)."""

from typing import Optional

from fastapi import APIRouter, Depends, status
from pydantic import BaseModel

from app.api.services.auth_service import get_current_user
from app.api.errors import ErrorCode
from app.api.exception_handlers import APIError
from app.domains.system.dao.indicator_dao import IndicatorConfigDao

router = APIRouter(prefix="/indicators", tags=["Indicator Library"])


class IndicatorCreateRequest(BaseModel):
    name: str
    display_name: str
    category: str
    description: Optional[str] = None
    default_params: Optional[dict] = None
    formula: Optional[str] = None


class IndicatorUpdateRequest(BaseModel):
    display_name: Optional[str] = None
    description: Optional[str] = None
    default_params: Optional[dict] = None
    formula: Optional[str] = None
    is_active: Optional[bool] = None


@router.get("")
async def list_indicators(
    category: Optional[str] = None,
    current_user: dict = Depends(get_current_user),
):
    """List all available indicators."""
    dao = IndicatorConfigDao()
    indicators = dao.list_all(category=category)
    return {"indicators": indicators}


@router.get("/{indicator_id}")
async def get_indicator(indicator_id: int, current_user: dict = Depends(get_current_user)):
    """Get indicator detail."""
    dao = IndicatorConfigDao()
    indicator = dao.get_by_id(indicator_id)
    if not indicator:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Indicator not found")
    return indicator


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_indicator(req: IndicatorCreateRequest, current_user: dict = Depends(get_current_user)):
    """Create a custom indicator."""
    valid_categories = ("trend", "oscillator", "volume", "volatility", "custom")
    if req.category not in valid_categories:
        raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message="Invalid indicator category")
    dao = IndicatorConfigDao()
    indicator_id = dao.create(
        name=req.name,
        display_name=req.display_name,
        category=req.category,
        description=req.description,
        default_params=req.default_params,
        formula=req.formula,
    )
    return {"id": indicator_id, "message": "Indicator created"}


@router.put("/{indicator_id}")
async def update_indicator(
    indicator_id: int, req: IndicatorUpdateRequest, current_user: dict = Depends(get_current_user)
):
    """Update an indicator."""
    updates = {k: v for k, v in req.model_dump().items() if v is not None}
    if not updates:
        raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message="No fields to update")
    dao = IndicatorConfigDao()
    if not dao.update(indicator_id, **updates):
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Indicator not found")
    return {"message": "Indicator updated"}


@router.delete("/{indicator_id}")
async def delete_indicator(indicator_id: int, current_user: dict = Depends(get_current_user)):
    """Delete a custom indicator (cannot delete built-in)."""
    dao = IndicatorConfigDao()
    if not dao.delete(indicator_id):
        raise APIError(
            status_code=400, code=ErrorCode.VALIDATION_ERROR, message="Cannot delete built-in indicator or not found"
        )
    return {"message": "Indicator deleted"}
