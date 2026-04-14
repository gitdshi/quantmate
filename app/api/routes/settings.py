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


class DataSourceBatchByPermission(BaseModel):
    permission_points: str
    enabled: bool


class DataSourceConfigUpdate(BaseModel):
    enabled: Optional[bool] = None
    config_json: Optional[dict] = None


# ---------------------------------------------------------------------------
# Data Source Items
# ---------------------------------------------------------------------------


@router.get("/datasource-items", dependencies=[require_permission("system", "read")])
async def list_datasource_items(
    source: Optional[str] = Query(None, description="Filter by source: tushare or akshare"),
    category: Optional[str] = Query(None, description="Filter by category: 股票数据, 指数数据, etc."),
    current_user: TokenData = Depends(get_current_user),
):
    """List all data source items with categories, permission info, and sync support."""
    items = _list_items_with_sync_support(source=source, category=category)
    return {"data": items}


@router.get("/datasource-items/permissions", dependencies=[require_permission("system", "read")])
async def list_permission_levels(
    source: str = Query("tushare", description="Data source key"),
    current_user: TokenData = Depends(get_current_user),
):
    """Return distinct permission_points values for sync-supported batch operations."""
    items = _list_items_with_sync_support(source=source)
    permissions = sorted(
        {
            str(item["permission_points"])
            for item in items
            if item.get("permission_points")
            and not _requires_paid_access(item.get("requires_permission"))
            and item.get("sync_supported")
        },
        key=_permission_sort_key,
    )
    return {"data": permissions}


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
    support_map = _get_sync_support_map()
    updates: list[dict] = []
    newly_enabled: list[tuple[str, str]] = []
    skipped_unsupported: list[dict[str, str]] = []

    for item in body.items:
        source = item.get("source")
        item_key = item.get("item_key")
        if not source or not item_key or "enabled" not in item:
            continue

        existing = dao.get_by_key(source, item_key)
        if not existing:
            continue

        enabled = bool(item.get("enabled"))
        was_enabled = bool(existing.get("enabled"))

        if enabled and not _is_item_sync_supported(support_map, source, item_key):
            skipped_unsupported.append({"source": source, "item_key": item_key})
            continue

        if enabled == was_enabled:
            continue

        updates.append({"source": source, "item_key": item_key, "enabled": enabled})
        if enabled:
            newly_enabled.append((source, item_key))

    updated = dao.batch_update(updates) if updates else 0

    for source, item_key in newly_enabled:
        _ensure_table_for_item(source, item_key)
        _trigger_sync_init(source, item_key)

    return {"updated": updated, "skipped_unsupported": skipped_unsupported}


@router.put("/datasource-items/batch-by-permission", dependencies=[require_permission("system", "manage")])
async def batch_update_by_permission(
    body: DataSourceBatchByPermission,
    source: str = Query("tushare", description="Data source key"),
    current_user: TokenData = Depends(get_current_user),
):
    """Enable/disable all items matching a specific permission_points value.

    Items requiring paid access are excluded.
    """
    from app.domains.market.dao.data_source_item_dao import DataSourceItemDao

    dao = DataSourceItemDao()
    items = _list_items_with_sync_support(source=source)
    updates: list[dict] = []
    newly_enabled: list[tuple[str, str]] = []
    skipped_unsupported: list[dict[str, str]] = []

    for item in items:
        if item.get("permission_points") != body.permission_points:
            continue
        if _requires_paid_access(item.get("requires_permission")):
            continue

        item_key = item["item_key"]
        was_enabled = bool(item.get("enabled"))

        if body.enabled and not item.get("sync_supported"):
            skipped_unsupported.append({"source": source, "item_key": item_key})
            continue

        if was_enabled == body.enabled:
            continue

        updates.append({"source": source, "item_key": item_key, "enabled": body.enabled})
        if body.enabled:
            newly_enabled.append((source, item_key))

    updated = dao.batch_update(updates) if updates else 0

    for target_source, item_key in newly_enabled:
        _ensure_table_for_item(target_source, item_key)
        _trigger_sync_init(target_source, item_key)

    return {
        "updated": updated,
        "permission_points": body.permission_points,
        "enabled": body.enabled,
        "skipped_unsupported": skipped_unsupported,
    }


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

    support_map = _get_sync_support_map(source)
    if body.enabled and not _is_item_sync_supported(support_map, source, item_key):
        raise APIError(
            status_code=400,
            code=ErrorCode.BAD_REQUEST,
            message=f"Sync is not implemented for {source}/{item_key} yet",
        )

    was_enabled = bool(existing.get("enabled"))
    dao.update_enabled(source, item_key, body.enabled)

    result: dict = {"item_key": item_key, "source": source, "enabled": body.enabled}

    # Trigger table creation + sync init when enabling
    if body.enabled:
        _ensure_table_for_item(source, item_key)
        if not was_enabled:
            job_id = _trigger_sync_init(source, item_key)
            if job_id:
                result["backfill_job_id"] = job_id

    return result


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


def _trigger_sync_init(source: str, item_key: str) -> Optional[str]:
    """Initialize sync status rows for a newly enabled item and enqueue backfill."""
    import logging

    logger = logging.getLogger(__name__)
    try:
        from app.datasync.service.sync_init_service import initialize_sync_status

        initialize_sync_status(source, item_key)

        from app.worker.service.config import get_queue
        from app.worker.service.datasync_tasks import run_backfill_task

        queue = get_queue("low")
        job = queue.enqueue(run_backfill_task, source, item_key, job_timeout=3600)
        return job.id
    except Exception:
        logger.warning("Sync init for %s/%s failed (non-fatal)", source, item_key, exc_info=True)
        return None


def _list_items_with_sync_support(
    source: Optional[str] = None,
    category: Optional[str] = None,
) -> list[dict]:
    from app.domains.market.dao.data_source_item_dao import DataSourceItemDao

    dao = DataSourceItemDao()
    if category or source:
        items = dao.list_with_categories(source=source, category=category)
    else:
        items = dao.list_all(source=source)

    support_map = _get_sync_support_map(source)
    for item in items:
        item["sync_supported"] = _is_item_sync_supported(support_map, item["source"], item["item_key"])
    return items


def _get_sync_support_map(source: Optional[str] = None) -> set[tuple[str, str]]:
    from app.datasync.registry import build_default_registry

    registry = build_default_registry()
    supported = {(iface.info.source_key, iface.info.interface_key) for iface in registry.all_interfaces()}
    if source is None:
        return supported
    return {item for item in supported if item[0] == source}


def _is_item_sync_supported(support_map: set[tuple[str, str]], source: str, item_key: str) -> bool:
    return (source, item_key) in support_map


def _requires_paid_access(value: object) -> bool:
    if value is None:
        return False
    normalized = str(value).strip().lower()
    return normalized in {"1", "true", "yes", "paid"}


def _permission_sort_key(permission_points: str) -> tuple[int, int, str]:
    import re

    match = re.search(r"(\d+)", permission_points)
    if match:
        return (0, int(match.group(1)), permission_points)
    if "单独" in permission_points:
        return (1, 10**9, permission_points)
    return (0, 10**9, permission_points)
