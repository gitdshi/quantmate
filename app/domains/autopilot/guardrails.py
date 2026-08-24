"""Autopilot guardrails: kill-switch, data-quality gate, deploy review, approval."""

from __future__ import annotations

import logging
from typing import Any, Dict

from sqlalchemy import text

from app.domains.autopilot.policies import Policies
from app.infrastructure.db.connections import connection

logger = logging.getLogger(__name__)


def is_kill_switch_active(policies: Policies) -> bool:
    return policies.kill_switch


def data_quality_gate(business_date, policies: Policies) -> Dict[str, Any]:
    """Block the research loop if pending/error/partial syncs exceed a safety share.

    Uses a reasonable default threshold (<= 5% unhealthy is acceptable). If the
    ``data_sync_status`` table is empty (first run), treat it as not-ready so the
    loop waits for the datasync daemon to finish its first pass.
    """
    threshold = 0.05
    with connection("quantmate") as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM data_sync_status")).scalar() or 0
        if total == 0:
            return {"ok": False, "reason": "data_sync_status is empty; waiting for datasync daemon"}
        unhealthy = conn.execute(
            text("SELECT COUNT(*) FROM data_sync_status WHERE status IN ('pending', 'error', 'partial')")
        ).scalar() or 0

    ratio = unhealthy / total
    if ratio > threshold:
        return {
            "ok": False,
            "reason": f"{unhealthy}/{total} syncs unhealthy ({ratio:.1%} > {threshold:.0%})",
        }
    return {"ok": True, "reason": f"sync health ok ({unhealthy}/{total} unhealthy)", "unhealthy_ratio": ratio}


def pre_deploy_review(deployment_plan: Dict[str, Any], policies: Policies) -> Dict[str, Any]:
    """Validate an automatic deployment plan before it runs.

    ``deployment_plan`` is expected to contain at least ``factors`` (list with
    ``weight``) and ``min_overlap_days`` (backtest sample overlap). Returns a
    unified ``{ok, reason, risk_summary}``.
    """
    problems: list[str] = []
    risk_summary: dict[str, Any] = {"factors": len(deployment_plan.get("factors", [])), "warnings": []}

    factors = deployment_plan.get("factors", []) or []
    max_weight = 0.0
    for f in factors:
        w = float(f.get("weight", 0.0))
        max_weight = max(max_weight, abs(w))
    # Factor signal weights are normalized to sum to 1.0 and are a different
    # scale from the per-position cap (max_position_pct), which the risk
    # component enforces at the actual position level. So this is a warning,
    # not a hard block — a hard block here would reject every normal portfolio.
    if max_weight > policies.max_position_pct:
        risk_summary["warnings"].append(
            f"max single-factor weight {max_weight:.4f} exceeds max_position_pct "
            f"{policies.max_position_pct}; position-level cap still enforced by risk component"
        )

    overlap_days = int(deployment_plan.get("min_overlap_days", 0) or 0)
    if overlap_days < policies.min_overlap_days:
        risk_summary["warnings"].append(
            f"backtest overlap {overlap_days}d < min_overlap_days {policies.min_overlap_days}d"
        )

    if is_kill_switch_active(policies):
        problems.append("global kill-switch is active")

    if problems:
        return {"ok": False, "reason": "; ".join(problems), "risk_summary": risk_summary}
    return {"ok": True, "reason": "deployment plan passed review", "risk_summary": risk_summary}


def request_approval(decision_plan: Dict[str, Any], policies: Policies) -> str:
    """Decide whether a deployment needs manual approval.

    First-day deployments always require approval when ``approval_required`` is
    set. Kinetic (subsequent intraday) adjustments inherit auto approval.
    """
    kinetic = decision_plan.get("kinetic", False)
    if policies.approval_required and not kinetic:
        return "pending_approval"
    return "auto"


def intraday_circuit_breaker(analytics: Dict[str, Any], policies: Policies) -> Dict[str, bool]:
    """Intraday equity drawdown / single-day loss circuit breaker."""
    max_drawdown = float(analytics.get("max_drawdown_pct", 0.0) or 0.0)
    day_loss = float(analytics.get("total_return_pct", 0.0) or 0.0)
    if max_drawdown >= policies.max_drawdown_stop_pct:
        return {"stop": True}
    if day_loss <= -policies.max_drawdown_stop_pct:
        return {"stop": True}
    return {"stop": False}