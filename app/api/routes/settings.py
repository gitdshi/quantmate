"""Data source item management API routes (Issue #5)."""

from __future__ import annotations

import json
from typing import Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field

from app.api.dependencies.permissions import require_permission
from app.api.services.auth_service import get_current_user
from app.api.models.user import TokenData
from app.api.errors import ErrorCode
from app.api.exception_handlers import APIError

router = APIRouter(prefix="/settings", tags=["Settings"])


class DataSourceItemUpdate(BaseModel):
    enabled: bool


class DataSourceBatchUpdate(BaseModel):
    items: list[dict] = Field(..., min_length=1)


class DataSourceConfigUpdate(BaseModel):
    enabled: Optional[bool] = None
    config_json: Optional[dict] = None


# ---------------------------------------------------------------------------
# Data Source Items
# ---------------------------------------------------------------------------


@router.get("/datasource-items", dependencies=[require_permission("system", "read")])
async def list_datasource_items(
    source: Optional[str] = Query(None, description="Filter by source: tushare or akshare"),
    current_user: TokenData = Depends(get_current_user),
):
    """List all data source items and their enabled status."""
    from app.domains.market.dao.data_source_item_dao import DataSourceItemDao

    dao = DataSourceItemDao()
    items = dao.list_all(source=source)
    return {"data": items}


@router.put("/datasource-items/batch", dependencies=[require_permission("system", "manage")])
async def batch_update_datasource_items(
    body: DataSourceBatchUpdate,
    current_user: TokenData = Depends(get_current_user),
):
    """Batch enable/disable data source items.

    When an item is enabled for the first time, its target table is created automatically.
    """
    from app.domains.market.dao.data_source_item_dao import DataSourceItemDao

    dao = DataSourceItemDao()
    updated = dao.batch_update(body.items)

    # Trigger table creation for newly enabled items
    for item in body.items:
        if item.get("enabled"):
            _ensure_table_for_item(item["source"], item["item_key"])

    return {"updated": updated}


@router.put("/datasource-items/{item_key}", dependencies=[require_permission("system", "manage")])
async def update_datasource_item(
    item_key: str,
    body: DataSourceItemUpdate,
    source: str = Query(..., description="Data source: tushare or akshare"),
    current_user: TokenData = Depends(get_current_user),
):
    """Enable or disable a single data source item.

    When enabled, the interface's target table is created if it doesn't exist.
    """
    from app.domains.market.dao.data_source_item_dao import DataSourceItemDao

    dao = DataSourceItemDao()
    existing = dao.get_by_key(source, item_key)
    if not existing:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message=f"Item {source}/{item_key} not found")
    dao.update_enabled(source, item_key, body.enabled)

    # Trigger table creation when enabling
    if body.enabled:
        _ensure_table_for_item(source, item_key)

    return {"item_key": item_key, "source": source, "enabled": body.enabled}


# ---------------------------------------------------------------------------
# Data Source Configs
# ---------------------------------------------------------------------------


@router.get("/datasource-configs", dependencies=[require_permission("system", "read")])
async def list_datasource_configs(
    current_user: TokenData = Depends(get_current_user),
):
    """List all data source configurations."""
    from app.domains.market.dao.data_source_item_dao import DataSourceConfigDao

    dao = DataSourceConfigDao()
    configs = dao.list_all()
    return {"data": configs}


@router.put("/datasource-configs/{source_key}", dependencies=[require_permission("system", "manage")])
async def update_datasource_config(
    source_key: str,
    body: DataSourceConfigUpdate,
    current_user: TokenData = Depends(get_current_user),
):
    """Update a data source configuration (enable/disable, update config)."""
    from app.domains.market.dao.data_source_item_dao import DataSourceConfigDao

    dao = DataSourceConfigDao()
    existing = dao.get_by_key(source_key)
    if not existing:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message=f"Config {source_key} not found")

    config_str = json.dumps(body.config_json) if body.config_json is not None else None
    dao.update_config(source_key, config_json=config_str, enabled=body.enabled)
    return {"source_key": source_key, "updated": True}


# ---------------------------------------------------------------------------
# Connection test — now delegates to registry plugins
# ---------------------------------------------------------------------------


@router.post("/datasource-items/test/{source}", dependencies=[require_permission("system", "manage")])
async def test_datasource_connection(
    source: str,
    current_user: TokenData = Depends(get_current_user),
):
    """Test connectivity to a data source using the plugin registry."""
    from app.datasync.registry import build_default_registry

    registry = build_default_registry()
    ds = registry.get_source(source.lower())
    if ds is None:
        raise APIError(
            status_code=400,
            code=ErrorCode.VALIDATION_ERROR,
            message=f"Unknown data source: {source}. Available: {[s.source_key for s in registry.all_sources()]}",
        )
    ok, msg = ds.test_connection()
    return {"source": source, "status": "ok" if ok else "error", "message": msg}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ensure_table_for_item(source: str, item_key: str) -> None:
    """Best-effort table creation when an item is enabled."""
    import logging

    logger = logging.getLogger(__name__)
    try:
        from app.domains.market.dao.data_source_item_dao import DataSourceItemDao
        from app.datasync.registry import build_default_registry
        from app.datasync.table_manager import ensure_table

        dao = DataSourceItemDao()
        item = dao.get_by_key(source, item_key)
        if item is None or item.get("table_created"):
            return  # already exists or no item

        registry = build_default_registry()
        iface = registry.get_interface(source, item_key)
        if iface is None:
            return  # no plugin for this item

        target_db = item.get("target_database")
        target_tbl = item.get("target_table")
        if not target_db or not target_tbl:
            return

        ensure_table(target_db, target_tbl, iface.get_ddl())
    except Exception:
        logger.warning("Table creation for %s/%s failed (non-fatal)", source, item_key, exc_info=True)
