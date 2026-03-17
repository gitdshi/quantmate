"""Audit logging middleware (Issue #2).

Automatically logs all API requests to the audit_logs table.
Sensitive operations (auth, strategy CRUD) include extra detail.
"""
from __future__ import annotations

import time
from typing import Optional

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware

from app.infrastructure.logging import get_logger

logger = get_logger(__name__)

# Operation type mapping based on path patterns
_OPERATION_MAP: list[tuple[str, str, str, Optional[str]]] = [
    # (path_prefix, method, operation_type, resource_type)
    ("/auth/login", "POST", "AUTH_LOGIN", "user"),
    ("/auth/register", "POST", "AUTH_REGISTER", "user"),
    ("/auth/refresh", "POST", "AUTH_REFRESH", "user"),
    ("/auth/change-password", "POST", "AUTH_CHANGE_PASSWORD", "user"),
    ("/auth/me", "GET", "AUTH_PROFILE_VIEW", "user"),
    ("/strategies", "POST", "STRATEGY_CREATE", "strategy"),
    ("/strategies", "PUT", "STRATEGY_UPDATE", "strategy"),
    ("/strategies", "DELETE", "STRATEGY_DELETE", "strategy"),
    ("/strategies", "GET", "STRATEGY_VIEW", "strategy"),
    ("/backtest", "POST", "BACKTEST_SUBMIT", "backtest"),
    ("/backtest", "GET", "BACKTEST_VIEW", "backtest"),
    ("/queue", "POST", "JOB_SUBMIT", "job"),
    ("/queue", "DELETE", "JOB_DELETE", "job"),
    ("/data/", "GET", "DATA_ACCESS", "data"),
]

# Paths to skip logging
_SKIP_PATHS = {"/health", "/docs", "/redoc", "/openapi.json", "/", "/metrics"}


def _classify_request(path: str, method: str) -> tuple[str, Optional[str]]:
    """Determine operation_type and resource_type from path + method."""
    method_upper = method.upper()
    for prefix, m, op_type, res_type in _OPERATION_MAP:
        if prefix in path and (m == method_upper or m == "*"):
            return op_type, res_type
    return f"API_{method_upper}", None


def _extract_user_info(request: Request) -> tuple[Optional[int], Optional[str]]:
    """Try to extract user_id and username from the request's auth token."""
    auth = request.headers.get("authorization", "")
    if not auth.startswith("Bearer "):
        return None, None
    try:
        from app.api.services.auth_service import decode_token
        data = decode_token(auth[7:])
        if data:
            return data.user_id, data.username
    except Exception:
        pass
    return None, None


def _extract_resource_id(path: str) -> Optional[str]:
    """Extract resource ID from path segments like /strategies/123."""
    parts = path.rstrip("/").split("/")
    # Look for numeric IDs or UUID-like segments
    for part in reversed(parts):
        if part.isdigit() or (len(part) >= 8 and "-" in part):
            return part
    return None


class AuditMiddleware(BaseHTTPMiddleware):
    """Records all API requests to the audit log."""

    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        # Skip non-API and excluded paths
        if path in _SKIP_PATHS or not path.startswith("/api/"):
            return await call_next(request)

        start_time = time.time()
        response = await call_next(request)
        duration_ms = int((time.time() - start_time) * 1000)

        # Fire-and-forget audit log (don't block the response)
        try:
            user_id, username = _extract_user_info(request)
            operation_type, resource_type = _classify_request(path, request.method)
            resource_id = _extract_resource_id(path)

            forwarded = request.headers.get("x-forwarded-for")
            ip_address = forwarded.split(",")[0].strip() if forwarded else (
                request.client.host if request.client else None
            )
            user_agent = request.headers.get("user-agent", "")[:500]

            from app.domains.audit.dao.audit_log_dao import AuditLogDao
            dao = AuditLogDao()
            dao.insert(
                user_id=user_id,
                username=username,
                operation_type=operation_type,
                resource_type=resource_type,
                resource_id=resource_id,
                details={"duration_ms": duration_ms},
                ip_address=ip_address,
                user_agent=user_agent,
                http_method=request.method,
                http_path=path,
                http_status=response.status_code,
            )
        except Exception as e:
            # Never let audit logging break the request
            logger.warning(f"Audit log failed: {e}")

        return response
