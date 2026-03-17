"""System configuration routes (P2 Issue: System Config Backend)."""
from typing import Optional

from fastapi import APIRouter, Depends, status
from pydantic import BaseModel

from app.api.services.auth_service import get_current_user
from app.api.errors import ErrorCode
from app.api.exception_handlers import APIError
from app.domains.system.dao.system_config_dao import SystemConfigDao, DataSourceConfigDao

router = APIRouter(prefix="/system", tags=["System Configuration"])


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
    current_user: dict = Depends(get_current_user),
):
    """List system configurations."""
    dao = SystemConfigDao()
    configs = dao.list_all(category=category)
    return {"configs": configs}


@router.get("/configs/{key}")
async def get_system_config(key: str, current_user: dict = Depends(get_current_user)):
    """Get a single system config."""
    dao = SystemConfigDao()
    config = dao.get(key)
    if not config:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Config not found")
    return config


@router.put("/configs")
async def upsert_system_config(req: ConfigUpsertRequest, current_user: dict = Depends(get_current_user)):
    """Upsert a system config."""
    dao = SystemConfigDao()
    dao.upsert(
        key=req.config_key, value=req.config_value,
        category=req.category, description=req.description,
        user_overridable=req.user_overridable,
    )
    return {"message": "Config saved"}


@router.delete("/configs/{key}")
async def delete_system_config(key: str, current_user: dict = Depends(get_current_user)):
    """Delete a system config."""
    dao = SystemConfigDao()
    if not dao.delete(key):
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Config not found")
    return {"message": "Config deleted"}


# ── Data Source Configs ──────────────────────────────────────────────

@router.get("/data-sources")
async def list_data_sources(current_user: dict = Depends(get_current_user)):
    """List data source configurations."""
    dao = DataSourceConfigDao()
    return {"sources": dao.list_all()}


@router.put("/data-sources")
async def upsert_data_source(req: DataSourceConfigRequest, current_user: dict = Depends(get_current_user)):
    """Upsert a data source configuration."""
    dao = DataSourceConfigDao()
    dao.upsert(
        source_name=req.source_name,
        is_enabled=req.is_enabled,
        rate_limit_per_min=req.rate_limit_per_min,
        priority=req.priority,
    )
    return {"message": "Data source config saved"}
