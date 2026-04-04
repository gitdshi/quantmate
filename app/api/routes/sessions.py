"""Session management routes (P2 Issue: Session Management)."""

from fastapi import APIRouter, Depends, status

from app.api.dependencies.permissions import require_permission
from app.api.services.auth_service import get_current_user
from app.api.models.user import TokenData
from app.api.errors import ErrorCode
from app.api.exception_handlers import APIError
from app.domains.auth.dao.session_dao import SessionDao

router = APIRouter(prefix="/auth/sessions", tags=["Sessions"])


@router.get("/", dependencies=[require_permission("account", "read", scope="self")])
async def list_sessions(current_user: TokenData = Depends(get_current_user)):
    """List active sessions for the current user."""
    dao = SessionDao()
    sessions = dao.list_by_user(current_user.user_id)
    return {"sessions": sessions}


@router.delete("/all", dependencies=[require_permission("account", "write", scope="self")])
async def revoke_all_sessions(current_user: TokenData = Depends(get_current_user)):
    """Force logout all sessions for the current user."""
    dao = SessionDao()
    count = dao.delete_all_for_user(current_user.user_id)
    return {"message": f"{count} sessions revoked"}


@router.delete("/{session_id}", dependencies=[require_permission("account", "write", scope="self")])
async def revoke_session(session_id: int, current_user: TokenData = Depends(get_current_user)):
    """Force logout a specific session."""
    dao = SessionDao()
    if not dao.delete(session_id, current_user.user_id):
        raise APIError(
            status_code=status.HTTP_404_NOT_FOUND,
            code=ErrorCode.NOT_FOUND,
            message="Session not found",
        )
    return {"message": "Session revoked"}
