"""Composite strategy models.

Defines the three-layer component system (Universe → Trading → Risk)
and composite strategy composition models.
"""

from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any, List

from pydantic import BaseModel, Field


# ── Enums ────────────────────────────────────────────────────────────────

class ComponentLayer(str, Enum):
    """Strategy component layer."""

    UNIVERSE = "universe"
    TRADING = "trading"
    RISK = "risk"


class ExecutionMode(str, Enum):
    """Composite strategy execution mode."""

    BACKTEST = "backtest"
    PAPER = "paper"
    LIVE = "live"


# ── Strategy Component ───────────────────────────────────────────────────

class StrategyComponentBase(BaseModel):
    """Base fields for a strategy component."""

    name: str = Field(..., min_length=1, max_length=100)
    layer: ComponentLayer
    sub_type: str = Field(..., min_length=1, max_length=50)
    description: Optional[str] = None
    code: Optional[str] = None
    config: Optional[Dict[str, Any]] = None
    parameters: Optional[Dict[str, Any]] = None


class StrategyComponentCreate(StrategyComponentBase):
    """Create a new strategy component."""
    pass


class StrategyComponentUpdate(BaseModel):
    """Partial update for a strategy component."""

    name: Optional[str] = Field(None, min_length=1, max_length=100)
    sub_type: Optional[str] = Field(None, min_length=1, max_length=50)
    description: Optional[str] = None
    code: Optional[str] = None
    config: Optional[Dict[str, Any]] = None
    parameters: Optional[Dict[str, Any]] = None
    is_active: Optional[bool] = None


class StrategyComponent(StrategyComponentBase):
    """Full strategy component response."""

    id: int
    user_id: int
    version: int = 1
    is_active: bool = True
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class StrategyComponentListItem(BaseModel):
    """Strategy component list item (without code/config)."""

    id: int
    name: str
    layer: ComponentLayer
    sub_type: str
    description: Optional[str] = None
    version: int = 1
    is_active: bool = True
    created_at: datetime
    updated_at: datetime


# ── Component Binding ────────────────────────────────────────────────────

class ComponentBindingBase(BaseModel):
    """A binding between a composite strategy and a component."""

    component_id: int
    layer: ComponentLayer
    ordinal: int = 0
    weight: float = Field(1.0, ge=0.0, le=1.0)
    config_override: Optional[Dict[str, Any]] = None


class ComponentBindingCreate(ComponentBindingBase):
    """Create a new binding."""
    pass


class ComponentBinding(ComponentBindingBase):
    """Full binding response (includes component details)."""

    id: int
    component_name: Optional[str] = None
    component_sub_type: Optional[str] = None


# ── Composite Strategy ───────────────────────────────────────────────────

class CompositeStrategyBase(BaseModel):
    """Base fields for a composite strategy."""

    name: str = Field(..., min_length=1, max_length=100)
    description: Optional[str] = None
    portfolio_config: Optional[Dict[str, Any]] = None
    market_constraints: Optional[Dict[str, Any]] = None
    execution_mode: ExecutionMode = ExecutionMode.BACKTEST


class CompositeStrategyCreate(CompositeStrategyBase):
    """Create a composite strategy with initial bindings."""

    bindings: List[ComponentBindingCreate] = Field(default_factory=list)


class CompositeStrategyUpdate(BaseModel):
    """Partial update for a composite strategy."""

    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = None
    portfolio_config: Optional[Dict[str, Any]] = None
    market_constraints: Optional[Dict[str, Any]] = None
    execution_mode: Optional[ExecutionMode] = None
    is_active: Optional[bool] = None


class CompositeStrategy(CompositeStrategyBase):
    """Full composite strategy response."""

    id: int
    user_id: int
    is_active: bool = True
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class CompositeStrategyDetail(CompositeStrategy):
    """Composite strategy with its component bindings."""

    bindings: List[ComponentBinding] = Field(default_factory=list)


class CompositeStrategyListItem(BaseModel):
    """Composite strategy list item."""

    id: int
    name: str
    description: Optional[str] = None
    execution_mode: ExecutionMode
    is_active: bool = True
    created_at: datetime
    updated_at: datetime
    component_count: int = 0


# ── Composite Backtest ───────────────────────────────────────────────────

class CompositeBacktestStatus(str, Enum):
    """Composite backtest job status."""

    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class CompositeBacktestSubmit(BaseModel):
    """Submit a composite backtest."""

    composite_strategy_id: int
    start_date: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}$")
    end_date: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}$")
    initial_capital: float = 1_000_000.0
    benchmark: str = "000300.SH"


class CompositeBacktestListItem(BaseModel):
    """Backtest list item (without full result)."""

    id: int
    job_id: str
    composite_strategy_id: int
    start_date: str
    end_date: str
    initial_capital: float
    benchmark: str
    status: CompositeBacktestStatus
    error_message: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    created_at: datetime


class CompositeBacktestResult(BaseModel):
    """Full backtest result."""

    id: int
    job_id: str
    composite_strategy_id: int
    start_date: str
    end_date: str
    initial_capital: float
    benchmark: str
    status: CompositeBacktestStatus
    result: Optional[Dict[str, Any]] = None
    attribution: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    created_at: datetime
