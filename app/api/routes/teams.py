"""Team collaboration routes — workspaces, members, strategy sharing."""

from typing import Optional

from fastapi import APIRouter, Depends, status
from pydantic import BaseModel

from app.api.services.auth_service import get_current_user
from app.api.models.user import TokenData
from app.api.errors import ErrorCode
from app.api.exception_handlers import APIError
from app.domains.collaboration.service import CollaborationService

router = APIRouter(prefix="/teams", tags=["Team Collaboration"])


class WorkspaceCreate(BaseModel):
    name: str
    description: Optional[str] = None


class WorkspaceUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    max_members: Optional[int] = None


class MemberAdd(BaseModel):
    user_id: int
    role: str = "member"


class ShareCreate(BaseModel):
    strategy_id: int
    shared_with_user_id: Optional[int] = None
    shared_with_team_id: Optional[int] = None
    permission: str = "view"


# --- Workspace endpoints ---


@router.get("/workspaces")
async def list_workspaces(current_user: TokenData = Depends(get_current_user)):
    service = CollaborationService()
    return service.list_workspaces(current_user.user_id)


@router.post("/workspaces", status_code=status.HTTP_201_CREATED)
async def create_workspace(req: WorkspaceCreate, current_user: TokenData = Depends(get_current_user)):
    service = CollaborationService()
    return service.create_workspace(current_user.user_id, req.name, req.description)


@router.get("/workspaces/{workspace_id}")
async def get_workspace(workspace_id: int, current_user: TokenData = Depends(get_current_user)):
    service = CollaborationService()
    try:
        return service.get_workspace(workspace_id, current_user.user_id)
    except KeyError:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Workspace not found")
    except PermissionError:
        raise APIError(status_code=403, code=ErrorCode.FORBIDDEN, message="Not a member of this workspace")


@router.put("/workspaces/{workspace_id}")
async def update_workspace(
    workspace_id: int, req: WorkspaceUpdate, current_user: TokenData = Depends(get_current_user)
):
    service = CollaborationService()
    try:
        return service.update_workspace(current_user.user_id, workspace_id, **req.model_dump(exclude_none=True))
    except KeyError:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Workspace not found or not owner")


@router.delete("/workspaces/{workspace_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_workspace(workspace_id: int, current_user: TokenData = Depends(get_current_user)):
    service = CollaborationService()
    try:
        service.delete_workspace(current_user.user_id, workspace_id)
    except KeyError:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Workspace not found or not owner")


# --- Member endpoints ---


@router.get("/workspaces/{workspace_id}/members")
async def list_members(workspace_id: int, current_user: TokenData = Depends(get_current_user)):
    service = CollaborationService()
    try:
        return service.list_members(workspace_id, current_user.user_id)
    except (KeyError, PermissionError):
        raise APIError(status_code=403, code=ErrorCode.FORBIDDEN, message="Access denied")


@router.post("/workspaces/{workspace_id}/members", status_code=status.HTTP_201_CREATED)
async def add_member(workspace_id: int, req: MemberAdd, current_user: TokenData = Depends(get_current_user)):
    service = CollaborationService()
    try:
        service.add_member(workspace_id, current_user.user_id, req.user_id, req.role)
        return {"message": "Member added"}
    except KeyError:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Workspace not found")
    except PermissionError:
        raise APIError(status_code=403, code=ErrorCode.FORBIDDEN, message="Insufficient permissions")
    except ValueError as e:
        raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message=str(e))


@router.delete("/workspaces/{workspace_id}/members/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_member(workspace_id: int, user_id: int, current_user: TokenData = Depends(get_current_user)):
    service = CollaborationService()
    try:
        service.remove_member(workspace_id, current_user.user_id, user_id)
    except KeyError:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Member not found")
    except PermissionError:
        raise APIError(status_code=403, code=ErrorCode.FORBIDDEN, message="Insufficient permissions")


# --- Strategy sharing ---


@router.get("/shares/received")
async def list_shared_with_me(current_user: TokenData = Depends(get_current_user)):
    service = CollaborationService()
    return service.list_shared_with_me(current_user.user_id)


@router.post("/shares", status_code=status.HTTP_201_CREATED)
async def share_strategy(req: ShareCreate, current_user: TokenData = Depends(get_current_user)):
    service = CollaborationService()
    share_id = service.share_strategy(
        req.strategy_id,
        current_user.user_id,
        shared_with_user_id=req.shared_with_user_id,
        shared_with_team_id=req.shared_with_team_id,
        permission=req.permission,
    )
    return {"id": share_id, "message": "Strategy shared"}


@router.delete("/shares/{share_id}", status_code=status.HTTP_204_NO_CONTENT)
async def revoke_share(share_id: int, current_user: TokenData = Depends(get_current_user)):
    service = CollaborationService()
    try:
        service.revoke_share(share_id, current_user.user_id)
    except KeyError:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Share not found")
