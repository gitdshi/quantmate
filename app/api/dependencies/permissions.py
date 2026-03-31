"""Permission dependencies backed by RBAC."""

from __future__ import annotations

import json

from fastapi import Depends

from app.api.errors import ErrorCode
from app.api.exception_handlers import APIError
from app.api.models.user import TokenData
from app.api.services.auth_service import get_current_user
from app.domains.rbac.service.rbac_service import RbacService


def _log_permission_denied(current_user: TokenData, resource: str, action: str) -> None:
    try:
        from app.domains.audit.dao.audit_log_dao import AuditLogDao

        AuditLogDao().insert(
            user_id=current_user.user_id,
            username=current_user.username,
            operation_type="AUTH_PERMISSION_DENIED",
            resource_type=resource,
            resource_id=None,
            details={"resource": resource, "action": action},
            ip_address=None,
            user_agent="rbac",
            http_method="DEPENDENCY",
            http_path=f"{resource}.{action}",
            http_status=403,
        )
    except Exception:
        return


def require_permission(resource: str, action: str):
    async def _check(current_user: TokenData = Depends(get_current_user)) -> TokenData:
        rbac_service = RbacService()
        if not rbac_service.check_permission(current_user.user_id, resource, action, current_user.username):
            _log_permission_denied(current_user, resource, action)
            raise APIError(
                status_code=403,
                code=ErrorCode.FORBIDDEN,
                message="Permission denied",
                detail=json.dumps({"resource": resource, "action": action}),
            )
        return current_user

    return Depends(_check)
