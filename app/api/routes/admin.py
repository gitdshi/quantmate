"""Admin RBAC management routes."""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from app.api.dependencies.permissions import require_permission
from app.api.errors import ErrorCode
from app.api.exception_handlers import APIError
from app.api.models.user import TokenData
from app.api.services.auth_service import get_current_user
from app.domains.auth.dao.user_dao import UserDao
from app.domains.rbac.dao.rbac_dao import PermissionDao, RoleDao, UserRoleDao

router = APIRouter(prefix="/admin", tags=["Admin"])


class RoleCreateRequest(BaseModel):
    name: str = Field(..., min_length=2, max_length=50)
    description: Optional[str] = None


class RoleUpdateRequest(BaseModel):
    description: Optional[str] = None


class RolePermissionUpdateRequest(BaseModel):
    permission_ids: list[int] = Field(default_factory=list)


class UserRoleUpdateRequest(BaseModel):
    role_ids: list[int] = Field(default_factory=list)


class UserStatusUpdateRequest(BaseModel):
    is_active: bool


@router.get("/roles", dependencies=[require_permission("account", "manage")])
async def list_roles(current_user: TokenData = Depends(get_current_user)):
    role_dao = RoleDao()
    roles = role_dao.list_all()
    for role in roles:
        role["permissions"] = role_dao.list_role_permissions(role["id"])
    return {"roles": roles}


@router.post("/roles", dependencies=[require_permission("account", "manage")])
async def create_role(req: RoleCreateRequest, current_user: TokenData = Depends(get_current_user)):
    role_dao = RoleDao()
    if role_dao.get_by_name(req.name):
        raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message="Role already exists")
    role_id = role_dao.create(req.name, req.description, is_system=False)
    return {"id": role_id, "message": "Role created"}


@router.put("/roles/{role_id}", dependencies=[require_permission("account", "manage")])
async def update_role(role_id: int, req: RoleUpdateRequest, current_user: TokenData = Depends(get_current_user)):
    role_dao = RoleDao()
    role = role_dao.get(role_id)
    if not role:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Role not found")
    if not role_dao.update(role_id, req.description):
        raise APIError(status_code=400, code=ErrorCode.BAD_REQUEST, message="Role update failed")
    return {"message": "Role updated"}


@router.delete("/roles/{role_id}", dependencies=[require_permission("account", "manage")])
async def delete_role(role_id: int, current_user: TokenData = Depends(get_current_user)):
    role_dao = RoleDao()
    role = role_dao.get(role_id)
    if not role:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Role not found")
    if role["is_system"]:
        raise APIError(status_code=400, code=ErrorCode.BAD_REQUEST, message="System roles cannot be deleted")
    if not role_dao.delete(role_id):
        raise APIError(status_code=400, code=ErrorCode.BAD_REQUEST, message="Role delete failed")
    return {"message": "Role deleted"}


@router.get("/permissions", dependencies=[require_permission("account", "manage")])
async def list_permissions(current_user: TokenData = Depends(get_current_user)):
    return {"permissions": PermissionDao().list_all()}


@router.put("/roles/{role_id}/permissions", dependencies=[require_permission("account", "manage")])
async def update_role_permissions(
    role_id: int,
    req: RolePermissionUpdateRequest,
    current_user: TokenData = Depends(get_current_user),
):
    role_dao = RoleDao()
    if not role_dao.get(role_id):
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Role not found")
    role_dao.set_permissions(role_id, req.permission_ids)
    return {"message": "Role permissions updated"}


@router.get("/users", dependencies=[require_permission("account", "manage")])
async def list_users(current_user: TokenData = Depends(get_current_user)):
    return {"users": UserRoleDao().list_users_with_roles()}


@router.put("/users/{user_id}/roles", dependencies=[require_permission("account", "manage")])
async def update_user_roles(
    user_id: int,
    req: UserRoleUpdateRequest,
    current_user: TokenData = Depends(get_current_user),
):
    user = UserDao().get_user_by_id(user_id)
    if not user:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="User not found")
    UserRoleDao().set_user_roles(user_id, req.role_ids, current_user.user_id)
    return {"message": "User roles updated"}


@router.put("/users/{user_id}/status", dependencies=[require_permission("account", "manage")])
async def update_user_status(
    user_id: int,
    req: UserStatusUpdateRequest,
    current_user: TokenData = Depends(get_current_user),
):
    if not UserDao().update_user_status(user_id, req.is_active):
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="User not found")
    return {"message": "User status updated"}
