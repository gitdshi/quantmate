"""System status routes."""

import os
from datetime import datetime, timezone
from typing import Dict, Any

from fastapi import APIRouter, Depends

from app.api.services.auth_service import get_current_user
from app.api.models.user import TokenData
from app.infrastructure.config import get_settings

from app.domains.extdata.service import SyncStatusService

router = APIRouter(prefix="/system", tags=["system"])
settings = get_settings()
DEFAULT_BUILD_TIME = datetime.now(timezone.utc).isoformat()


@router.get("/sync-status")
async def get_sync_status(current_user: TokenData = Depends(get_current_user)) -> Dict[str, Any]:
    return SyncStatusService().get_sync_status()


@router.get("/version")
async def get_version_info() -> Dict[str, str]:
    return {
        "name": settings.app_name,
        "version": settings.app_version,
        "build_time": os.getenv("APP_BUILD_TIME", DEFAULT_BUILD_TIME),
        "environment": settings.environment,
    }
