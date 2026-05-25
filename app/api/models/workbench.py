"""Workbench workflow models."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


class WorkbenchStage(str, Enum):
    """Guided workflow stage."""

    FACTOR = "factor"
    STRATEGY = "strategy"
    BACKTEST = "backtest"
    PAPER_TRADE = "paper_trade"


class WorkbenchSessionStatus(str, Enum):
    """High-level workflow status."""

    DRAFT = "draft"
    RUNNING_BACKTEST = "running_backtest"
    PAPER_ACTIVE = "paper_active"
    ARCHIVED = "archived"


class WorkbenchSessionBase(BaseModel):
    """Fields shared across create/update requests."""

    name: Optional[str] = Field(None, min_length=1, max_length=100)
    current_stage: WorkbenchStage = WorkbenchStage.FACTOR
    status: WorkbenchSessionStatus = WorkbenchSessionStatus.DRAFT
    state_json: dict[str, Any] = Field(default_factory=dict)


class WorkbenchSessionCreate(WorkbenchSessionBase):
    """Create a new workbench workflow session."""


class WorkbenchSessionUpdate(BaseModel):
    """Update an existing workbench workflow session."""

    name: Optional[str] = Field(None, min_length=1, max_length=100)
    current_stage: Optional[WorkbenchStage] = None
    status: Optional[WorkbenchSessionStatus] = None
    state_json: Optional[dict[str, Any]] = None


class WorkbenchSessionListItem(BaseModel):
    """Compact session list item."""

    id: int
    name: str
    current_stage: WorkbenchStage
    status: WorkbenchSessionStatus
    updated_at: datetime
    created_at: datetime


class WorkbenchSession(WorkbenchSessionListItem):
    """Full session response."""

    user_id: int
    state_json: dict[str, Any] = Field(default_factory=dict)
    last_backtest_job_id: Optional[str] = None
    last_deployment_id: Optional[int] = None


class WorkbenchTransitionRequest(BaseModel):
    """Advance or roll back the workflow stage."""

    target_stage: WorkbenchStage


class WorkbenchSessionEvent(BaseModel):
    """Auditable workflow event record."""

    id: int
    session_id: int
    event_type: str
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime