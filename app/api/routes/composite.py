"""Composite strategy CRUD routes.

Covers:
  /api/v1/strategy-components  — CRUD for Universe / Trading / Risk components
  /api/v1/composite-strategies — CRUD for composite strategies + bindings
"""

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query, status

from app.api.models.user import TokenData
from app.api.models.composite import (
    ComponentLayer,
    StrategyComponent,
    StrategyComponentCreate,
    StrategyComponentUpdate,
    StrategyComponentListItem,
    CompositeStrategy,
    CompositeStrategyCreate,
    CompositeStrategyUpdate,
    CompositeStrategyDetail,
    CompositeStrategyListItem,
    ComponentBinding,
    ComponentBindingCreate,
    CompositeBacktestSubmit,
    CompositeBacktestListItem,
    CompositeBacktestResult,
)
from app.api.services.auth_service import get_current_user
from app.api.errors import ErrorCode
from app.api.exception_handlers import APIError
from app.api.pagination import PaginationParams, paginate

from app.domains.composite.service import CompositeStrategyService


# ── Strategy Components ──────────────────────────────────────────────────

comp_router = APIRouter(prefix="/strategy-components", tags=["Strategy Components"])


@comp_router.get("")
async def list_components(
    layer: Optional[ComponentLayer] = Query(None, description="Filter by layer"),
    pagination: PaginationParams = Depends(),
    current_user: TokenData = Depends(get_current_user),
):
    """List strategy components for current user."""
    service = CompositeStrategyService()
    layer_val = layer.value if layer else None
    total = service.count_components(current_user.user_id, layer=layer_val)
    rows = service.list_components_paginated(
        current_user.user_id, pagination.limit, pagination.offset, layer=layer_val
    )
    items = [
        StrategyComponentListItem(
            id=r["id"],
            name=r["name"],
            layer=r["layer"],
            sub_type=r["sub_type"],
            description=r.get("description"),
            version=r.get("version", 1),
            is_active=bool(r.get("is_active", True)),
            created_at=r.get("created_at"),
            updated_at=r.get("updated_at"),
        )
        for r in rows
    ]
    return paginate(items, total, pagination)


@comp_router.post("", response_model=StrategyComponent, status_code=status.HTTP_201_CREATED)
async def create_component(
    data: StrategyComponentCreate,
    current_user: TokenData = Depends(get_current_user),
):
    """Create a new strategy component."""
    service = CompositeStrategyService()
    try:
        row = service.create_component(
            user_id=current_user.user_id,
            name=data.name,
            layer=data.layer.value,
            sub_type=data.sub_type,
            description=data.description,
            code=data.code,
            config=data.config,
            parameters=data.parameters,
        )
    except ValueError as e:
        raise APIError(
            status_code=status.HTTP_400_BAD_REQUEST,
            code=ErrorCode.COMPOSITE_VALIDATION_FAILED,
            message=str(e),
        )
    return _component_from_row(row)


@comp_router.get("/{component_id}", response_model=StrategyComponent)
async def get_component(
    component_id: int,
    current_user: TokenData = Depends(get_current_user),
):
    """Get a strategy component by ID."""
    service = CompositeStrategyService()
    try:
        row = service.get_component(current_user.user_id, component_id)
    except KeyError:
        raise APIError(
            status_code=status.HTTP_404_NOT_FOUND,
            code=ErrorCode.COMPOSITE_COMPONENT_NOT_FOUND,
            message="Strategy component not found",
        )
    return _component_from_row(row)


@comp_router.put("/{component_id}", response_model=StrategyComponent)
async def update_component(
    component_id: int,
    data: StrategyComponentUpdate,
    current_user: TokenData = Depends(get_current_user),
):
    """Update a strategy component."""
    service = CompositeStrategyService()
    try:
        row = service.update_component(
            current_user.user_id,
            component_id,
            name=data.name,
            sub_type=data.sub_type,
            description=data.description,
            code=data.code,
            config=data.config,
            parameters=data.parameters,
            is_active=data.is_active,
        )
    except KeyError:
        raise APIError(
            status_code=status.HTTP_404_NOT_FOUND,
            code=ErrorCode.COMPOSITE_COMPONENT_NOT_FOUND,
            message="Strategy component not found",
        )
    except ValueError as e:
        raise APIError(
            status_code=status.HTTP_400_BAD_REQUEST,
            code=ErrorCode.COMPOSITE_VALIDATION_FAILED,
            message=str(e),
        )
    return _component_from_row(row)


@comp_router.delete("/{component_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_component(
    component_id: int,
    current_user: TokenData = Depends(get_current_user),
):
    """Delete a strategy component."""
    service = CompositeStrategyService()
    try:
        service.delete_component(current_user.user_id, component_id)
    except KeyError:
        raise APIError(
            status_code=status.HTTP_404_NOT_FOUND,
            code=ErrorCode.COMPOSITE_COMPONENT_NOT_FOUND,
            message="Strategy component not found",
        )


# ── Composite Strategies ─────────────────────────────────────────────────

composite_router = APIRouter(prefix="/composite-strategies", tags=["Composite Strategies"])


@composite_router.get("")
async def list_composites(
    pagination: PaginationParams = Depends(),
    current_user: TokenData = Depends(get_current_user),
):
    """List composite strategies for current user."""
    service = CompositeStrategyService()
    total = service.count_composites(current_user.user_id)
    rows = service.list_composites_paginated(
        current_user.user_id, pagination.limit, pagination.offset
    )
    items = [
        CompositeStrategyListItem(
            id=r["id"],
            name=r["name"],
            description=r.get("description"),
            execution_mode=r.get("execution_mode", "backtest"),
            is_active=bool(r.get("is_active", True)),
            created_at=r.get("created_at"),
            updated_at=r.get("updated_at"),
            component_count=r.get("component_count", 0),
        )
        for r in rows
    ]
    return paginate(items, total, pagination)


@composite_router.post("", response_model=CompositeStrategyDetail, status_code=status.HTTP_201_CREATED)
async def create_composite(
    data: CompositeStrategyCreate,
    current_user: TokenData = Depends(get_current_user),
):
    """Create a new composite strategy with optional bindings."""
    service = CompositeStrategyService()
    bindings = [b.model_dump() for b in data.bindings] if data.bindings else []
    try:
        row = service.create_composite(
            user_id=current_user.user_id,
            name=data.name,
            description=data.description,
            portfolio_config=data.portfolio_config,
            market_constraints=data.market_constraints,
            execution_mode=data.execution_mode.value,
            bindings=bindings,
        )
    except ValueError as e:
        raise APIError(
            status_code=status.HTTP_400_BAD_REQUEST,
            code=ErrorCode.COMPOSITE_VALIDATION_FAILED,
            message=str(e),
        )
    return _composite_detail_from_row(row)


@composite_router.get("/{composite_id}", response_model=CompositeStrategyDetail)
async def get_composite(
    composite_id: int,
    current_user: TokenData = Depends(get_current_user),
):
    """Get a composite strategy with its bindings."""
    service = CompositeStrategyService()
    try:
        row = service.get_composite_detail(current_user.user_id, composite_id)
    except KeyError:
        raise APIError(
            status_code=status.HTTP_404_NOT_FOUND,
            code=ErrorCode.COMPOSITE_NOT_FOUND,
            message="Composite strategy not found",
        )
    return _composite_detail_from_row(row)


@composite_router.put("/{composite_id}", response_model=CompositeStrategyDetail)
async def update_composite(
    composite_id: int,
    data: CompositeStrategyUpdate,
    current_user: TokenData = Depends(get_current_user),
):
    """Update a composite strategy (metadata only, not bindings)."""
    service = CompositeStrategyService()
    try:
        row = service.update_composite(
            current_user.user_id,
            composite_id,
            name=data.name,
            description=data.description,
            portfolio_config=data.portfolio_config,
            market_constraints=data.market_constraints,
            execution_mode=data.execution_mode.value if data.execution_mode else None,
            is_active=data.is_active,
        )
    except KeyError:
        raise APIError(
            status_code=status.HTTP_404_NOT_FOUND,
            code=ErrorCode.COMPOSITE_NOT_FOUND,
            message="Composite strategy not found",
        )
    except ValueError as e:
        raise APIError(
            status_code=status.HTTP_400_BAD_REQUEST,
            code=ErrorCode.COMPOSITE_VALIDATION_FAILED,
            message=str(e),
        )
    return _composite_detail_from_row(row)


@composite_router.delete("/{composite_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_composite(
    composite_id: int,
    current_user: TokenData = Depends(get_current_user),
):
    """Delete a composite strategy and its bindings."""
    service = CompositeStrategyService()
    try:
        service.delete_composite(current_user.user_id, composite_id)
    except KeyError:
        raise APIError(
            status_code=status.HTTP_404_NOT_FOUND,
            code=ErrorCode.COMPOSITE_NOT_FOUND,
            message="Composite strategy not found",
        )


# ── Bindings sub-resource ────────────────────────────────────────────────

@composite_router.put("/{composite_id}/bindings")
async def replace_bindings(
    composite_id: int,
    bindings: list[ComponentBindingCreate],
    current_user: TokenData = Depends(get_current_user),
):
    """Replace all component bindings for a composite strategy."""
    service = CompositeStrategyService()
    binding_dicts = [b.model_dump() for b in bindings]
    try:
        result = service.replace_bindings(
            current_user.user_id, composite_id, binding_dicts
        )
    except KeyError:
        raise APIError(
            status_code=status.HTTP_404_NOT_FOUND,
            code=ErrorCode.COMPOSITE_NOT_FOUND,
            message="Composite strategy not found",
        )
    except ValueError as e:
        raise APIError(
            status_code=status.HTTP_400_BAD_REQUEST,
            code=ErrorCode.COMPOSITE_VALIDATION_FAILED,
            message=str(e),
        )
    return [
        ComponentBinding(
            id=b["id"],
            component_id=b["component_id"],
            layer=b["layer"],
            ordinal=b.get("ordinal", 0),
            weight=float(b.get("weight", 1.0)),
            config_override=b.get("config_override"),
            component_name=b.get("component_name"),
            component_sub_type=b.get("component_sub_type"),
        )
        for b in result
    ]


# ── Composite Backtests ──────────────────────────────────────────────────

backtest_router = APIRouter(prefix="/composite-backtests", tags=["Composite Backtests"])


@backtest_router.post("", response_model=CompositeBacktestListItem, status_code=status.HTTP_202_ACCEPTED)
async def submit_backtest(
    data: CompositeBacktestSubmit,
    current_user: TokenData = Depends(get_current_user),
):
    """Submit a composite strategy backtest job."""
    service = CompositeStrategyService()
    try:
        row = service.submit_backtest(
            user_id=current_user.user_id,
            composite_strategy_id=data.composite_strategy_id,
            start_date=data.start_date,
            end_date=data.end_date,
            initial_capital=data.initial_capital,
            benchmark=data.benchmark,
        )
    except KeyError:
        raise APIError(
            status_code=status.HTTP_404_NOT_FOUND,
            code=ErrorCode.COMPOSITE_NOT_FOUND,
            message="Composite strategy not found",
        )
    except ValueError as e:
        raise APIError(
            status_code=status.HTTP_400_BAD_REQUEST,
            code=ErrorCode.COMPOSITE_VALIDATION_FAILED,
            message=str(e),
        )
    return _backtest_list_item(row)


@backtest_router.get("")
async def list_backtests(
    composite_strategy_id: Optional[int] = Query(None),
    current_user: TokenData = Depends(get_current_user),
):
    """List composite backtests for current user."""
    service = CompositeStrategyService()
    rows = service.list_backtests(
        current_user.user_id,
        composite_strategy_id=composite_strategy_id,
    )
    return [_backtest_list_item(r) for r in rows]


@backtest_router.get("/{job_id}", response_model=CompositeBacktestResult)
async def get_backtest(
    job_id: str,
    current_user: TokenData = Depends(get_current_user),
):
    """Get full backtest result by job ID."""
    service = CompositeStrategyService()
    try:
        row = service.get_backtest(current_user.user_id, job_id)
    except KeyError:
        raise APIError(
            status_code=status.HTTP_404_NOT_FOUND,
            code=ErrorCode.COMPOSITE_BACKTEST_NOT_FOUND,
            message="Composite backtest not found",
        )
    return CompositeBacktestResult(
        id=row["id"],
        job_id=row["job_id"],
        composite_strategy_id=row["composite_strategy_id"],
        start_date=str(row["start_date"]),
        end_date=str(row["end_date"]),
        initial_capital=float(row.get("initial_capital", 0)),
        benchmark=row.get("benchmark", ""),
        status=row.get("status", "queued"),
        result=row.get("result"),
        attribution=row.get("attribution"),
        error_message=row.get("error_message"),
        started_at=row.get("started_at"),
        completed_at=row.get("completed_at"),
        created_at=row.get("created_at"),
    )


@backtest_router.delete("/{job_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_backtest(
    job_id: str,
    current_user: TokenData = Depends(get_current_user),
):
    """Delete a composite backtest record."""
    service = CompositeStrategyService()
    try:
        service.delete_backtest(current_user.user_id, job_id)
    except KeyError:
        raise APIError(
            status_code=status.HTTP_404_NOT_FOUND,
            code=ErrorCode.COMPOSITE_BACKTEST_NOT_FOUND,
            message="Composite backtest not found",
        )


# ── Helpers ──────────────────────────────────────────────────────────────

def _component_from_row(row: dict) -> StrategyComponent:
    return StrategyComponent(
        id=row["id"],
        user_id=row["user_id"],
        name=row["name"],
        layer=row["layer"],
        sub_type=row["sub_type"],
        description=row.get("description"),
        code=row.get("code"),
        config=row.get("config"),
        parameters=row.get("parameters"),
        version=row.get("version", 1),
        is_active=bool(row.get("is_active", True)),
        created_at=row.get("created_at") or datetime.utcnow(),
        updated_at=row.get("updated_at") or datetime.utcnow(),
    )


def _composite_detail_from_row(row: dict) -> CompositeStrategyDetail:
    bindings = [
        ComponentBinding(
            id=b["id"],
            component_id=b["component_id"],
            layer=b["layer"],
            ordinal=b.get("ordinal", 0),
            weight=float(b.get("weight", 1.0)),
            config_override=b.get("config_override"),
            component_name=b.get("component_name"),
            component_sub_type=b.get("component_sub_type"),
        )
        for b in row.get("bindings", [])
    ]
    return CompositeStrategyDetail(
        id=row["id"],
        user_id=row["user_id"],
        name=row["name"],
        description=row.get("description"),
        portfolio_config=row.get("portfolio_config"),
        market_constraints=row.get("market_constraints"),
        execution_mode=row.get("execution_mode", "backtest"),
        is_active=bool(row.get("is_active", True)),
        created_at=row.get("created_at") or datetime.utcnow(),
        updated_at=row.get("updated_at") or datetime.utcnow(),
        bindings=bindings,
    )


def _backtest_list_item(row: dict) -> CompositeBacktestListItem:
    return CompositeBacktestListItem(
        id=row["id"],
        job_id=row["job_id"],
        composite_strategy_id=row["composite_strategy_id"],
        start_date=str(row["start_date"]),
        end_date=str(row["end_date"]),
        initial_capital=float(row.get("initial_capital", 0)),
        benchmark=row.get("benchmark", ""),
        status=row.get("status", "queued"),
        error_message=row.get("error_message"),
        started_at=row.get("started_at"),
        completed_at=row.get("completed_at"),
        created_at=row.get("created_at"),
    )
