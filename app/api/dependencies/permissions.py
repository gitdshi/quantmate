"""Permission dependencies backed by RBAC."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from fastapi import Depends, Request

from app.api.errors import ErrorCode
from app.api.exception_handlers import APIError
from app.api.models.user import TokenData
from app.api.services.auth_service import get_current_user
from app.domains.rbac.service.rbac_service import RbacService

OwnerResolver = Callable[[Request, TokenData], bool]


def _deny_details(resource: str, action: str, scope: str | None = None) -> dict[str, Any]:
    details: dict[str, Any] = {"resource": resource, "action": action}
    if scope:
        details["scope"] = scope
    return details


def _log_permission_denied(
    current_user: TokenData,
    request: Request,
    resource: str,
    action: str,
    scope: str | None = None,
) -> None:
    try:
        from app.domains.audit.dao.audit_log_dao import AuditLogDao

        AuditLogDao().insert(
            user_id=current_user.user_id,
            username=current_user.username,
            operation_type="AUTH_PERMISSION_DENIED",
            resource_type=resource,
            resource_id=None,
            details=_deny_details(resource, action, scope),
            ip_address=request.client.host if request.client else None,
            user_agent=request.headers.get("user-agent"),
            http_method=request.method,
            http_path=request.url.path,
            http_status=403,
        )
    except Exception:
        return


def _raise_forbidden(
    current_user: TokenData,
    request: Request,
    resource: str,
    action: str,
    scope: str | None = None,
) -> None:
    _log_permission_denied(current_user, request, resource, action, scope)
    raise APIError(
        status_code=403,
        code=ErrorCode.FORBIDDEN,
        message="Permission denied",
        detail=json.dumps(_deny_details(resource, action, scope)),
    )


def require_permission(
    resource: str,
    action: str,
    scope: str | None = None,
    owner_resolver: OwnerResolver | None = None,
):
    async def _check(request: Request, current_user: TokenData = Depends(get_current_user)) -> TokenData:
        rbac_service = RbacService()
        if not rbac_service.check_permission(current_user.user_id, resource, action, current_user.username):
            _raise_forbidden(current_user, request, resource, action, scope)

        if scope == "self":
            target_user_id = request.path_params.get("user_id", current_user.user_id)
            try:
                target_user_id = int(target_user_id)
            except (TypeError, ValueError):
                target_user_id = current_user.user_id
            if target_user_id != current_user.user_id:
                _raise_forbidden(current_user, request, resource, action, scope)
        elif scope == "own" and owner_resolver is not None:
            if not owner_resolver(request, current_user):
                _raise_forbidden(current_user, request, resource, action, scope)

        return current_user

    return Depends(_check)
