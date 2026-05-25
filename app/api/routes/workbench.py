"""Workbench workflow routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query, status

from app.api.errors import ErrorCode
from app.api.exception_handlers import APIError
from app.api.models.user import TokenData
from app.api.models.workbench import (
    WorkbenchSession,
    WorkbenchSessionCreate,
    WorkbenchSessionEvent,
    WorkbenchSessionListItem,
    WorkbenchSessionUpdate,
    WorkbenchTransitionRequest,
)
from app.api.services.auth_service import get_current_user
from app.domains.workbench.service import WorkbenchService

router = APIRouter(prefix="/workbench/sessions", tags=["Workbench"])


@router.get("", response_model=list[WorkbenchSessionListItem])
async def list_workbench_sessions(
    limit: int = Query(10, ge=1, le=50),
    current_user: TokenData = Depends(get_current_user),
):
    """List recent workflow sessions for the current user."""
    service = WorkbenchService()
    rows = service.list_sessions(current_user.user_id, limit=limit)
    return [WorkbenchSessionListItem(**row) for row in rows]


@router.post("", response_model=WorkbenchSession, status_code=status.HTTP_201_CREATED)
async def create_workbench_session(
    data: WorkbenchSessionCreate,
    current_user: TokenData = Depends(get_current_user),
):
    """Create a workflow session."""
    service = WorkbenchService()
    session = service.create_session(
        current_user.user_id,
        data.name,
        data.current_stage.value,
        data.status.value,
        data.state_json,
    )
    return WorkbenchSession(**session)


@router.get("/{session_id}", response_model=WorkbenchSession)
async def get_workbench_session(
    session_id: int,
    current_user: TokenData = Depends(get_current_user),
):
    """Get one workflow session."""
    service = WorkbenchService()
    try:
        session = service.get_session(current_user.user_id, session_id)
    except KeyError:
        raise APIError(
            status_code=status.HTTP_404_NOT_FOUND,
            code=ErrorCode.NOT_FOUND,
            message="Workbench session not found",
        )
    return WorkbenchSession(**session)


@router.put("/{session_id}", response_model=WorkbenchSession)
async def update_workbench_session(
    session_id: int,
    data: WorkbenchSessionUpdate,
    current_user: TokenData = Depends(get_current_user),
):
    """Persist workflow session state."""
    service = WorkbenchService()
    try:
        session = service.update_session(
            current_user.user_id,
            session_id,
            name=data.name,
            current_stage=data.current_stage.value if data.current_stage else None,
            status=data.status.value if data.status else None,
            state_json=data.state_json,
        )
    except KeyError:
        raise APIError(
            status_code=status.HTTP_404_NOT_FOUND,
            code=ErrorCode.NOT_FOUND,
            message="Workbench session not found",
        )
    except ValueError as exc:
        raise APIError(
            status_code=status.HTTP_400_BAD_REQUEST,
            code=ErrorCode.BAD_REQUEST,
            message=str(exc),
        )
    return WorkbenchSession(**session)


@router.post("/{session_id}/transition", response_model=WorkbenchSession)
async def transition_workbench_session(
    session_id: int,
    data: WorkbenchTransitionRequest,
    current_user: TokenData = Depends(get_current_user),
):
    """Validate and change the workflow stage."""
    service = WorkbenchService()
    try:
        session = service.transition_session(current_user.user_id, session_id, data.target_stage.value)
    except KeyError:
        raise APIError(
            status_code=status.HTTP_404_NOT_FOUND,
            code=ErrorCode.NOT_FOUND,
            message="Workbench session not found",
        )
    except ValueError as exc:
        raise APIError(
            status_code=status.HTTP_400_BAD_REQUEST,
            code=ErrorCode.BAD_REQUEST,
            message=str(exc),
        )
    return WorkbenchSession(**session)


@router.get("/{session_id}/events", response_model=list[WorkbenchSessionEvent])
async def list_workbench_events(
    session_id: int,
    current_user: TokenData = Depends(get_current_user),
):
    """List session events for debugging and audit."""
    service = WorkbenchService()
    try:
        service.get_session(current_user.user_id, session_id)
    except KeyError:
        raise APIError(
            status_code=status.HTTP_404_NOT_FOUND,
            code=ErrorCode.NOT_FOUND,
            message="Workbench session not found",
        )
    return [WorkbenchSessionEvent(**row) for row in service.list_events(session_id)]