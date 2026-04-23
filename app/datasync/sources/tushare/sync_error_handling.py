"""Helpers for converting expected Tushare sync failures into structured results."""

from __future__ import annotations

import logging

from app.datasync.base import SyncResult, SyncStatus


def is_permission_error(error_message: str | None) -> bool:
    raw_message = str(error_message or "")
    normalized = raw_message.lower()
    return (
        "没有接口访问权限" in raw_message
        or ("没有接口" in raw_message and "访问权限" in raw_message)
        or "permission" in normalized
    )


def is_quota_pause_result(result: SyncResult) -> bool:
    details = result.details or {}
    return result.status == SyncStatus.PENDING and bool(details.get("quota_exceeded"))


def final_retry_count_for_result(result: SyncResult, attempt_retry_count: int) -> int:
    if is_quota_pause_result(result):
        return max(0, attempt_retry_count - 1)
    return attempt_retry_count


def build_quota_pending_result(exc: Exception) -> SyncResult:
    details = {"quota_exceeded": True}

    api_name = getattr(exc, "api_name", None)
    if api_name:
        details["quota_api_name"] = str(api_name)

    scope = getattr(exc, "scope", None)
    if scope:
        details["quota_scope"] = str(scope)

    retry_after = getattr(exc, "retry_after", None)
    if retry_after is not None:
        details["quota_retry_after"] = str(retry_after)

    return SyncResult(SyncStatus.PENDING, 0, str(exc), details=details)


def handle_tushare_sync_exception(
    logger: logging.Logger,
    context: str,
    exc: Exception,
    *,
    allow_permission_partial: bool = False,
) -> SyncResult:
    from app.datasync.service.tushare_ingest import TushareQuotaExceededError

    if isinstance(exc, TushareQuotaExceededError):
        logger.warning("%s paused by quota: %s", context, exc)
        return build_quota_pending_result(exc)

    error_message = str(exc)
    if allow_permission_partial and is_permission_error(error_message):
        logger.info("%s skipped due to permission: %s", context, error_message)
        return SyncResult(
            SyncStatus.PARTIAL,
            0,
            "Permission denied",
            details={"permission_denied": True},
        )

    logger.exception("%s failed: %s", context, exc)
    return SyncResult(SyncStatus.ERROR, 0, error_message)