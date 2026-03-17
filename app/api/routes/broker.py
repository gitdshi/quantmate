"""Broker configuration routes (P2 Issue: Broker Config Management)."""
from typing import Optional

from fastapi import APIRouter, Depends, status
from pydantic import BaseModel

from app.api.services.auth_service import get_current_user
from app.api.errors import ErrorCode
from app.api.exception_handlers import APIError
from app.domains.trading.dao.broker_config_dao import BrokerConfigDao

router = APIRouter(prefix="/broker", tags=["Broker Configuration"])


class BrokerConfigCreateRequest(BaseModel):
    broker_name: str
    config: dict
    is_paper: bool = True


class BrokerConfigUpdateRequest(BaseModel):
    broker_name: Optional[str] = None
    config: Optional[dict] = None
    is_paper: Optional[bool] = None
    is_active: Optional[bool] = None


@router.get("/configs")
async def list_broker_configs(current_user: dict = Depends(get_current_user)):
    """List broker configurations for the current user."""
    dao = BrokerConfigDao()
    configs = dao.list_by_user(current_user["id"])
    # Strip sensitive fields before returning
    for c in configs:
        if "config" in c and isinstance(c["config"], dict):
            c["config"] = {k: "***" if "secret" in k.lower() or "password" in k.lower() else v
                           for k, v in c["config"].items()}
    return {"configs": configs}


@router.post("/configs", status_code=status.HTTP_201_CREATED)
async def create_broker_config(req: BrokerConfigCreateRequest, current_user: dict = Depends(get_current_user)):
    """Create a new broker configuration."""
    dao = BrokerConfigDao()
    config_id = dao.create(
        user_id=current_user["id"],
        broker_name=req.broker_name,
        config=req.config,
        is_paper=req.is_paper,
    )
    return {"id": config_id, "message": "Broker config created"}


@router.put("/configs/{config_id}")
async def update_broker_config(config_id: int, req: BrokerConfigUpdateRequest, current_user: dict = Depends(get_current_user)):
    """Update a broker configuration."""
    updates = {k: v for k, v in req.model_dump().items() if v is not None}
    if not updates:
        raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message="No fields to update")
    dao = BrokerConfigDao()
    if not dao.update(config_id, current_user["id"], **updates):
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Broker config not found")
    return {"message": "Broker config updated"}


@router.delete("/configs/{config_id}")
async def delete_broker_config(config_id: int, current_user: dict = Depends(get_current_user)):
    """Delete a broker configuration."""
    dao = BrokerConfigDao()
    if not dao.delete(config_id, current_user["id"]):
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Broker config not found")
    return {"message": "Broker config deleted"}
