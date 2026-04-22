"""System configuration routes (P2 Issue: System Config Backend)."""

from typing import Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from app.api.dependencies.permissions import require_permission
from app.api.errors import ErrorCode
from app.api.services.auth_service import get_current_user
from app.api.models.user import TokenData
from app.api.exception_handlers import APIError
from app.domains.system.dao.system_config_dao import SystemConfigDao, DataSourceConfigDao
from app.infrastructure.config import clear_runtime_config_cache, resolve_runtime_config_value
from app.infrastructure.config.system_config_registry import list_db_system_config_definitions

router = APIRouter(prefix="/system", tags=["System Configuration"])


def _require_admin(current_user: TokenData = require_permission("system", "manage")) -> TokenData:
    """Restrict mutating endpoints to RBAC system managers."""
    return current_user


class ConfigUpsertRequest(BaseModel):
    config_key: str
    config_value: str
    category: str = "general"
    description: Optional[str] = None
    user_overridable: bool = False


class DataSourceConfigRequest(BaseModel):
    source_name: str
    is_enabled: bool = True
    rate_limit_per_min: Optional[int] = None
    priority: int = 0


# ── System Configs ───────────────────────────────────────────────────


@router.get("/configs")
async def list_system_configs(
    category: Optional[str] = None,
    current_user: TokenData = Depends(get_current_user),
):
    """List system configurations."""
    dao = SystemConfigDao()
    configs = dao.list_all(category=category)
    return {"configs": configs}


@router.get("/configs/catalog")
async def list_runtime_system_config_catalog(current_user: TokenData = Depends(get_current_user)):
    """List DB-managed runtime system configurations with effective values."""
    dao = SystemConfigDao()
    stored_configs = {row["config_key"]: row for row in dao.list_all()}
    items: list[dict] = []

    for definition in list_db_system_config_definitions():
        stored_row = stored_configs.get(definition.key)
        effective_value, value_source = resolve_runtime_config_value(
            env_keys=definition.legacy_env_keys,
            db_key=definition.key,
            default=definition.default_value,
        )
        items.append(
            {
                **definition.to_dict(),
                "current_value": effective_value,
                "stored_value": stored_row["config_value"] if stored_row else None,
                "is_overridden": stored_row is not None,
                "value_source": value_source,
                "updated_at": stored_row.get("updated_at") if stored_row else None,
            }
        )

    return {"configs": items}


@router.get("/configs/{key}")
async def get_system_config(key: str, current_user: TokenData = Depends(get_current_user)):
    """Get a single system config."""
    dao = SystemConfigDao()
    config = dao.get(key)
    if not config:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Config not found")
    return config


@router.put("/configs")
async def upsert_system_config(req: ConfigUpsertRequest, current_user: TokenData = Depends(_require_admin)):
    """Upsert a system config."""
    dao = SystemConfigDao()
    dao.upsert(
        key=req.config_key,
        value=req.config_value,
        category=req.category,
        description=req.description,
        user_overridable=req.user_overridable,
    )
    clear_runtime_config_cache()
    return {"message": "Config saved"}


@router.delete("/configs/{key}")
async def delete_system_config(key: str, current_user: TokenData = Depends(_require_admin)):
    """Delete a system config."""
    dao = SystemConfigDao()
    if not dao.delete(key):
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Config not found")
    clear_runtime_config_cache()
    return {"message": "Config deleted"}


# ── Data Source Configs ──────────────────────────────────────────────


@router.get("/data-sources")
async def list_data_sources(current_user: TokenData = Depends(get_current_user)):
    """List data source configurations."""
    dao = DataSourceConfigDao()
    return {"sources": dao.list_all()}


@router.put("/data-sources")
async def upsert_data_source(req: DataSourceConfigRequest, current_user: TokenData = Depends(_require_admin)):
    """Upsert a data source configuration."""
    dao = DataSourceConfigDao()
    dao.upsert(
        source_name=req.source_name,
        is_enabled=req.is_enabled,
        rate_limit_per_min=req.rate_limit_per_min,
        priority=req.priority,
    )
    return {"message": "Data source config saved"}
