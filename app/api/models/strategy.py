"""Strategy models."""
from datetime import datetime
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field


class StrategyBase(BaseModel):
    """Base strategy model."""
    name: str = Field(..., min_length=1, max_length=100)
    description: Optional[str] = None
    class_name: str = Field(..., description="Python class name of the strategy")
    parameters: Dict[str, Any] = Field(default_factory=dict)


class StrategyCreate(StrategyBase):
    """Strategy creation model."""
    code: Optional[str] = Field(None, description="Python source code of the strategy")


class StrategyUpdate(BaseModel):
    """Strategy update model."""
    name: Optional[str] = None
    class_name: Optional[str] = None
    description: Optional[str] = None
    parameters: Optional[Dict[str, Any]] = None
    code: Optional[str] = None
    is_active: Optional[bool] = None


class Strategy(StrategyBase):
    """Strategy response model."""
    id: int
    code: str
    user_id: int
    version: int = 1
    is_active: bool = True
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True


class StrategyInDB(Strategy):
    """Strategy in database."""
    file_path: Optional[str] = None


class StrategyListItem(BaseModel):
    """Strategy list item (without code)."""
    id: int
    name: str
    class_name: str
    description: Optional[str] = None
    version: int = 1
    is_active: bool
    created_at: datetime
    updated_at: datetime


class StrategyValidation(BaseModel):
    """Strategy validation result."""
    valid: bool
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
