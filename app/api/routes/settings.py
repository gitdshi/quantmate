"""Data source item management API routes (Issue #5)."""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field

from app.api.services.auth_service import get_current_user
from app.api.models.user import TokenData
from app.api.errors import ErrorCode
from app.api.exception_handlers import APIError

router = APIRouter(prefix="/settings", tags=["Settings"])


class DataSourceItemUpdate(BaseModel):
    enabled: bool


class DataSourceBatchUpdate(BaseModel):
    items: list[dict] = Field(..., min_length=1)


@router.get("/datasource-items")
async def list_datasource_items(
    source: Optional[str] = Query(None, description="Filter by source: tushare or akshare"),
    current_user: TokenData = Depends(get_current_user),
):
    """List all data source items and their enabled status."""
    from app.domains.market.dao.data_source_item_dao import DataSourceItemDao

    dao = DataSourceItemDao()
    items = dao.list_all(source=source)
    return {"data": items}


@router.put("/datasource-items/batch")
async def batch_update_datasource_items(
    body: DataSourceBatchUpdate,
    current_user: TokenData = Depends(get_current_user),
):
    """Batch enable/disable data source items."""
    from app.domains.market.dao.data_source_item_dao import DataSourceItemDao

    dao = DataSourceItemDao()
    updated = dao.batch_update(body.items)
    return {"updated": updated}


@router.put("/datasource-items/{item_key}")
async def update_datasource_item(
    item_key: str,
    body: DataSourceItemUpdate,
    source: str = Query(..., description="Data source: tushare or akshare"),
    current_user: TokenData = Depends(get_current_user),
):
    """Enable or disable a single data source item."""
    from app.domains.market.dao.data_source_item_dao import DataSourceItemDao

    dao = DataSourceItemDao()
    existing = dao.get_by_key(source, item_key)
    if not existing:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message=f"Item {source}/{item_key} not found")
    dao.update_enabled(source, item_key, body.enabled)
    return {"item_key": item_key, "source": source, "enabled": body.enabled}


@router.post("/datasource-items/test/{source}")
async def test_datasource_connection(
    source: str,
    current_user: TokenData = Depends(get_current_user),
):
    """Test connectivity to a data source (tushare or akshare)."""
    source_lower = source.lower()
    if source_lower == "tushare":
        try:
            from app.infrastructure.config import get_settings

            settings = get_settings()
            import tushare as ts

            pro = ts.pro_api(settings.tushare_token)
            pro.trade_cal(exchange="SSE", start_date="20250101", end_date="20250102")
            return {"source": source, "status": "ok", "message": "Tushare connection successful"}
        except Exception as e:
            return {"source": source, "status": "error", "message": str(e)}
    elif source_lower == "akshare":
        try:
            import akshare as ak

            ak.stock_zh_index_spot_em()
            return {"source": source, "status": "ok", "message": "AKShare connection successful"}
        except Exception as e:
            return {"source": source, "status": "error", "message": str(e)}
    else:
        raise APIError(
            status_code=400,
            code=ErrorCode.VALIDATION_ERROR,
            message=f"Unknown data source: {source}. Supported: tushare, akshare",
        )
