"""Alert management routes (P2 Issue: Alert Engine, Notifications, Drawdown Monitoring)."""

from typing import Optional

from fastapi import APIRouter, Depends, Query, status
from pydantic import BaseModel

from app.api.services.auth_service import get_current_user
from app.api.models.user import TokenData
from app.api.errors import ErrorCode
from app.api.exception_handlers import APIError
from app.domains.monitoring.dao.alert_dao import AlertRuleDao, AlertHistoryDao, NotificationChannelDao

router = APIRouter(prefix="/alerts", tags=["Alerts & Monitoring"])


class AlertRuleCreateRequest(BaseModel):
    name: str
    metric: str
    comparator: str
    threshold: float
    level: str = "warning"
    time_window: Optional[int] = None


class AlertRuleUpdateRequest(BaseModel):
    name: Optional[str] = None
    metric: Optional[str] = None
    comparator: Optional[str] = None
    threshold: Optional[float] = None
    level: Optional[str] = None
    time_window: Optional[int] = None
    is_active: Optional[bool] = None


class NotificationChannelCreateRequest(BaseModel):
    channel_type: str
    config: dict


# ── Alert Rules ──────────────────────────────────────────────────────


@router.get("/rules")
async def list_alert_rules(current_user: TokenData = Depends(get_current_user)):
    """List all alert rules for the current user."""
    dao = AlertRuleDao()
    return {"rules": dao.list_by_user(current_user.user_id)}


@router.post("/rules", status_code=status.HTTP_201_CREATED)
async def create_alert_rule(req: AlertRuleCreateRequest, current_user: TokenData = Depends(get_current_user)):
    """Create a new alert rule."""
    valid_comparators = ("gt", "gte", "lt", "lte", "eq", "neq")
    if req.comparator not in valid_comparators:
        raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message="Invalid comparator")
    valid_levels = ("info", "warning", "severe")
    if req.level not in valid_levels:
        raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message="Invalid level")

    dao = AlertRuleDao()
    rule_id = dao.create(
        user_id=current_user.user_id,
        name=req.name,
        metric=req.metric,
        comparator=req.comparator,
        threshold=req.threshold,
        level=req.level,
        time_window=req.time_window,
    )
    return {"id": rule_id, "message": "Alert rule created"}


@router.put("/rules/{rule_id}")
async def update_alert_rule(
    rule_id: int, req: AlertRuleUpdateRequest, current_user: TokenData = Depends(get_current_user)
):
    """Update an alert rule."""
    dao = AlertRuleDao()
    updates = {k: v for k, v in req.model_dump().items() if v is not None}
    if not updates:
        raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message="No fields to update")
    if not dao.update(rule_id, current_user.user_id, **updates):
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Alert rule not found")
    return {"message": "Alert rule updated"}


@router.delete("/rules/{rule_id}")
async def delete_alert_rule(rule_id: int, current_user: TokenData = Depends(get_current_user)):
    """Delete an alert rule."""
    dao = AlertRuleDao()
    if not dao.delete(rule_id, current_user.user_id):
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Alert rule not found")
    return {"message": "Alert rule deleted"}


# ── Alert History ────────────────────────────────────────────────────


@router.get("/history")
async def list_alert_history(
    level: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    current_user: TokenData = Depends(get_current_user),
):
    """List alert history for the current user."""
    dao = AlertHistoryDao()
    alerts, total = dao.list_by_user(current_user.user_id, level=level, page=page, page_size=page_size)
    return {
        "data": alerts,
        "meta": {"page": page, "page_size": page_size, "total": total},
    }


@router.post("/history/{alert_id}/acknowledge")
async def acknowledge_alert(alert_id: int, current_user: TokenData = Depends(get_current_user)):
    """Acknowledge an alert."""
    dao = AlertHistoryDao()
    if not dao.acknowledge(alert_id, current_user.user_id):
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Alert not found")
    return {"message": "Alert acknowledged"}


# ── Notification Channels ────────────────────────────────────────────


@router.get("/channels")
async def list_channels(current_user: TokenData = Depends(get_current_user)):
    """List notification channels."""
    dao = NotificationChannelDao()
    return {"channels": dao.list_by_user(current_user.user_id)}


@router.post("/channels", status_code=status.HTTP_201_CREATED)
async def create_channel(req: NotificationChannelCreateRequest, current_user: TokenData = Depends(get_current_user)):
    """Create a notification channel."""
    valid_types = ("email", "wechat", "dingtalk", "telegram", "slack", "webhook")
    if req.channel_type not in valid_types:
        raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message="Invalid channel type")
    dao = NotificationChannelDao()
    channel_id = dao.create(current_user.user_id, req.channel_type, req.config)
    return {"id": channel_id, "message": "Channel created"}


@router.delete("/channels/{channel_id}")
async def delete_channel(channel_id: int, current_user: TokenData = Depends(get_current_user)):
    """Delete a notification channel."""
    dao = NotificationChannelDao()
    if not dao.delete(channel_id, current_user.user_id):
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Channel not found")
    return {"message": "Channel deleted"}
