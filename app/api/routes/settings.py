"""Data source item management API routes (Issue #5)."""

from __future__ import annotations

import json
import logging
from typing import Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool

from app.api.dependencies.permissions import require_permission
from app.api.services.auth_service import get_current_user
from app.api.models.user import TokenData
from app.api.errors import ErrorCode
from app.api.exception_handlers import APIError
from app.infrastructure.config import get_runtime_int

router = APIRouter(prefix="/settings", tags=["Settings"])
logger = logging.getLogger(__name__)


class DataSourceItemUpdate(BaseModel):
    enabled: bool


class DataSourceBatchUpdate(BaseModel):
    items: list[dict] = Field(..., min_length=1)


class DataSourceBatchByPermission(BaseModel):
    permission_points: int
    enabled: bool


class DataSourceConfigUpdate(BaseModel):
    enabled: Optional[bool] = None
    config_json: Optional[dict] = None


class SyncCoverageRepairItem(BaseModel):
    source: str
    item_key: str


class SyncCoverageRepairRequest(BaseModel):
    source: Optional[str] = None
    items: list[SyncCoverageRepairItem] = Field(default_factory=list)
    only_missing: bool = True


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
            int(item["permission_points"])
            for item in items
            if _parse_permission_points(item.get("permission_points")) > 0
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

    Static-schema items are created immediately; sample-inferred items are created on first successful fetch.
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
        if _parse_permission_points(item.get("permission_points")) != body.permission_points:
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

    Static-schema items are created immediately; sample-inferred items are created on first successful fetch.
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
            message=f"Sync is not available for {source}/{item_key} with the current registry and token capability",
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


@router.get("/datasource-items/sync-coverage", dependencies=[require_permission("system", "read")])
async def list_datasource_sync_coverage(
    source: Optional[str] = Query(None, description="Optional source filter"),
    current_user: TokenData = Depends(get_current_user),
):
    """Return per-interface sync coverage and status counts for enabled items."""
    from app.domains.extdata.service import DataSyncDashboardService

    service = DataSyncDashboardService()
    return await run_in_threadpool(lambda: service.get_interface_coverage(source=source))


@router.post("/datasource-items/sync-coverage/repair", dependencies=[require_permission("system", "manage")])
async def repair_datasource_sync_coverage(
    body: SyncCoverageRepairRequest,
    current_user: TokenData = Depends(get_current_user),
):
    """Repair missing data_sync_status rows for selected or missing interfaces."""
    selected_items = [(item.source, item.item_key) for item in body.items]
    return _repair_sync_status_items(
        source=body.source,
        items=selected_items or None,
        only_missing=body.only_missing,
    )


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

    was_enabled = bool(existing.get("enabled"))
    config_str = json.dumps(body.config_json) if body.config_json is not None else None
    dao.update_config(source_key, config_json=config_str, enabled=body.enabled)
    if body.enabled and not was_enabled:
        _reconcile_source_sync(source_key)
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
    try:
        from app.domains.market.dao.data_source_item_dao import DataSourceItemDao
        from app.datasync.registry import build_default_registry
        from app.datasync.table_manager import ensure_table

        dao = DataSourceItemDao()
        item = dao.get_by_key(source, item_key)
        if item is None:
            return

        registry = build_default_registry()
        iface = registry.get_interface(source, item_key)
        if iface is None:
            return  # no plugin for this item

        target_db = item.get("target_database")
        target_tbl = item.get("target_table")
        if not target_db or not target_tbl:
            return
        if not iface.should_ensure_table_before_sync():
            return

        ensure_table(target_db, target_tbl, iface.get_ddl())
    except Exception:
        logger.warning("Table creation for %s/%s failed (non-fatal)", source, item_key, exc_info=True)


def _trigger_sync_init(source: str, item_key: str) -> Optional[str]:
    """Initialize sync status rows for a newly enabled item and enqueue backfill."""
    try:
        from app.datasync.registry import build_default_registry
        from app.datasync.service.sync_init_service import reconcile_enabled_sync_status

        registry = build_default_registry()
        result = reconcile_enabled_sync_status(registry, source=source, item_key=item_key)
        item_result = next(
            (
                row
                for row in result.get("item_results", [])
                if row.get("source") == source and row.get("item_key") == item_key
            ),
            None,
        )
        if not item_result or int(item_result.get("pending_records", 0) or 0) <= 0:
            return None
        return _enqueue_backfill_task(source, item_key)
    except Exception:
        logger.warning("Sync init for %s/%s failed (non-fatal)", source, item_key, exc_info=True)
        return None


def _reconcile_source_sync(source: str) -> None:
    """Best-effort reconciliation when a whole source is enabled again."""
    try:
        _repair_sync_status_items(source=source)
    except Exception:
        logger.warning("Source sync reconciliation for %s failed (non-fatal)", source, exc_info=True)


def _enqueue_backfill_task(source: str, item_key: str) -> Optional[str]:
    from app.worker.service.config import get_queue
    from app.worker.service.datasync_tasks import run_backfill_task

    queue = get_queue("low")
    job = queue.enqueue(
        run_backfill_task,
        source,
        item_key,
        job_timeout=get_runtime_int(
            env_keys="DATASYNC_BACKFILL_JOB_TIMEOUT_SECONDS",
            db_key="datasync.backfill_job_timeout_seconds",
            default=3600,
        ),
    )
    return job.id


def _repair_sync_status_items(
    source: Optional[str] = None,
    items: Optional[list[tuple[str, str]]] = None,
    *,
    only_missing: bool = False,
) -> dict[str, object]:
    from app.datasync.registry import build_default_registry
    from app.datasync.service.sync_init_service import reconcile_enabled_sync_status, reconcile_sync_status_item
    from app.domains.extdata.service import DataSyncDashboardService, clear_datasync_dashboard_cache

    registry = build_default_registry()
    selected_items: list[tuple[str, str]] | None = None

    if items:
        deduped: list[tuple[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for item_source, item_key in items:
            pair = (item_source, item_key)
            if not item_source or not item_key or pair in seen:
                continue
            if source is not None and item_source != source:
                continue
            seen.add(pair)
            deduped.append(pair)
        selected_items = deduped
    elif only_missing:
        coverage = DataSyncDashboardService().get_interface_coverage(source=source)
        selected_items = [
            (str(item["source"]), str(item["item_key"]))
            for item in coverage.get("items", [])
            if int(item.get("missing_sync_dates", 0) or 0) > 0
        ]

    if selected_items is None:
        result = reconcile_enabled_sync_status(registry, source=source)
    else:
        pending_records = 0
        item_results: list[dict[str, object]] = []
        skipped_unsupported: list[dict[str, str]] = []
        for item_source, item_key in selected_items:
            item_result = reconcile_sync_status_item(registry, item_source, item_key)
            if item_result is None:
                skipped_unsupported.append({"source": item_source, "item_key": item_key})
                continue
            pending_records += int(item_result.get("pending_records", 0) or 0)
            item_results.append(item_result)

        result = {
            "pending_records": pending_records,
            "items_reconciled": len(item_results),
            "item_results": item_results,
            "skipped_unsupported": skipped_unsupported,
        }

    backfill_jobs: list[dict[str, str]] = []
    for item in result.get("item_results", []):
        pending_records = int(item.get("pending_records", 0) or 0)
        if pending_records <= 0:
            continue

        job_id = _enqueue_backfill_task(str(item["source"]), str(item["item_key"]))
        if job_id:
            backfill_jobs.append(
                {
                    "source": str(item["source"]),
                    "item_key": str(item["item_key"]),
                    "job_id": job_id,
                }
            )

    clear_datasync_dashboard_cache()
    return {
        "source": source,
        "items_requested": len(selected_items) if selected_items is not None else None,
        "pending_records": result.get("pending_records", 0),
        "items_reconciled": result.get("items_reconciled", 0),
        "skipped_unsupported": result.get("skipped_unsupported", []),
        "item_results": result.get("item_results", []),
        "backfill_jobs": backfill_jobs,
    }


def _list_items_with_sync_support(
    source: Optional[str] = None,
    category: Optional[str] = None,
) -> list[dict]:
    from app.datasync.capabilities import get_item_support_state, load_source_config_map
    from app.datasync.registry import build_default_registry
    from app.domains.market.dao.data_source_item_dao import DataSourceItemDao

    dao = DataSourceItemDao()
    if category or source:
        items = dao.list_with_categories(source=source, category=category)
    else:
        items = dao.list_all(source=source)

    registry = build_default_registry()
    source_configs = load_source_config_map(source)
    for item in items:
        support_state = get_item_support_state(registry, item, source_configs=source_configs)
        item["capability_supported"] = support_state["capability_supported"]
        item["auto_sync_supported"] = support_state["auto_sync_supported"]
        item["sync_supported"] = support_state["sync_supported"]
    return items


def _get_sync_support_map(source: Optional[str] = None) -> set[tuple[str, str]]:
    from app.datasync.capabilities import build_supported_item_keys, load_source_config_map
    from app.datasync.registry import build_default_registry
    from app.domains.market.dao.data_source_item_dao import DataSourceItemDao

    registry = build_default_registry()
    items = DataSourceItemDao().list_all(source=source)
    return build_supported_item_keys(
        registry,
        items,
        source_configs=load_source_config_map(source),
    )


def _is_item_sync_supported(support_map: set[tuple[str, str]], source: str, item_key: str) -> bool:
    return (source, item_key) in support_map


def _requires_paid_access(value: object) -> bool:
    if value is None:
        return False
    normalized = str(value).strip().lower()
    return normalized in {"1", "true", "yes", "paid"}


def _parse_permission_points(value: object) -> int:
    if value in {None, ""}:
        return 0
    try:
        return max(0, int(value))
    except Exception:
        return 0


def _permission_sort_key(permission_points: object) -> int:
    return _parse_permission_points(permission_points)
