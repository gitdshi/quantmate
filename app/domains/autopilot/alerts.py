"""Autopilot stoppage alerts (SPEC-OPS-005).

Persists operational alerts into the shared ``alert_history`` table (visible
through the existing /alerts API) and logs them at ERROR/WARNING level.
Deduplication prevents periodic events (daemon ticks) from flooding history.
"""

from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)

_DEDUPE_TTL_SECONDS = 3600

# alert_history.level is ENUM('info','warning','severe'); map internal
# operational levels onto it (log level keeps the original severity).
_LEVEL_MAP = {"error": "severe", "critical": "severe", "warning": "warning", "warn": "warning", "info": "info"}


def _redis_seen(dedupe_key: str) -> bool:
    try:
        from app.worker.service.config import redis_conn

        return not bool(redis_conn.set(f"autopilot:alert:{dedupe_key}", "1", nx=True, ex=_DEDUPE_TTL_SECONDS))
    except Exception:
        # Redis unavailable: fall back to always emitting (never block ops flow).
        return False


def emit_autopilot_alert(
    message: str,
    level: str = "error",
    *,
    user_id: Optional[int] = None,
    dedupe_key: Optional[str] = None,
) -> None:
    """Record an autopilot operational alert.

    Best-effort: failures in alerting must never interrupt the orchestrator.
    """
    if dedupe_key and _redis_seen(dedupe_key):
        return

    log = logger.error if level in ("error", "severe", "critical") else logger.warning
    log("[autopilot-alert] %s", message)

    if user_id is None:
        try:
            from app.domains.autopilot.policies import Policies
            from app.domains.autopilot.resolvers import resolve_user_id

            user_id = resolve_user_id(Policies.load())
        except Exception:
            user_id = 0

    try:
        from app.domains.monitoring.dao.alert_dao import AlertHistoryDao

        db_level = _LEVEL_MAP.get(level, "warning")
        AlertHistoryDao().insert(rule_id=None, user_id=user_id or 0, level=db_level, message=f"[autopilot] {message}")
    except Exception:
        logger.debug("[autopilot-alert] failed to persist alert", exc_info=True)
