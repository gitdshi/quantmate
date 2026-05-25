"""Backtest AI report models."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field


class BacktestAIReport(BaseModel):
    """Structured interpretation artifact for one backtest job."""

    job_id: str
    status: str
    report_json: dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


class BacktestAIReportCreateResponse(BaseModel):
    """Response after report generation."""

    job_id: str
    status: str
    message: str