"""DAO for autopilot_runs / autopilot_stages / autopilot_decisions.

All SQL against the ``quantmate`` database lives here, following the existing
domain DAO convention of ``connection("quantmate")`` + ``sqlalchemy.text``.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import date, datetime
from typing import Any, Optional

from sqlalchemy import text

from app.domains.autopilot.state import RunStatus, StageStatus
from app.infrastructure.db.connections import connection

logger = logging.getLogger(__name__)


def _now() -> datetime:
    return datetime.utcnow()


class AutopilotDao:
    """Read/write access to autopilot runtime state."""

    # ── Runs ────────────────────────────────────────────────────────────

    def create_run(self, business_date: date, market: str = "CN") -> dict[str, Any]:
        run_id = f"ap-{business_date.strftime('%Y%m%d')}-{uuid.uuid4().hex[:8]}"
        with connection("quantmate") as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO autopilot_runs (run_id, business_date, market, status, started_at)
                    VALUES (:run_id, :bdate, :market, 'running', :now)
                    """
                ),
                {"run_id": run_id, "bdate": business_date, "market": market, "now": _now()},
            )
            conn.commit()
        return self.get_run(run_id)  # type: ignore[return-value]

    def get_run(self, run_id: str) -> Optional[dict[str, Any]]:
        with connection("quantmate") as conn:
            row = conn.execute(
                text(
                    """
                    SELECT id, run_id, business_date, market, status, started_at, ended_at, created_at, updated_at
                    FROM autopilot_runs WHERE run_id = :run_id
                    """
                ),
                {"run_id": run_id},
            ).fetchone()
        if not row:
            return None
        return dict(row._mapping)

    def get_run_for_date(self, business_date: date) -> Optional[dict[str, Any]]:
        with connection("quantmate") as conn:
            row = conn.execute(
                text("SELECT run_id FROM autopilot_runs WHERE business_date = :d ORDER BY id DESC LIMIT 1"),
                {"d": business_date},
            ).fetchone()
        if not row:
            return None
        return self.get_run(row[0])

    def update_run_status(
        self,
        run_id: str,
        status: RunStatus,
        *,
        ended_at: Optional[datetime] = None,
    ) -> None:
        with connection("quantmate") as conn:
            conn.execute(
                text(
                    """
                    UPDATE autopilot_runs
                    SET status = :status,
                        ended_at = COALESCE(:ended_at, ended_at),
                        updated_at = :now
                    WHERE run_id = :run_id
                    """
                ),
                {
                    "status": status.value,
                    "ended_at": ended_at if ended_at else _now(),
                    "now": _now(),
                    "run_id": run_id,
                },
            )
            conn.commit()

    def reset_running_runs_to_pending(self) -> int:
        """Reset any run left in 'running' after a daemon restart."""
        with connection("quantmate") as conn:
            result = conn.execute(
                text("UPDATE autopilot_runs SET status = 'pending', ended_at = NULL WHERE status = 'running'")
            )
            conn.commit()
            return result.rowcount or 0

    def list_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        with connection("quantmate") as conn:
            rows = conn.execute(
                text("SELECT run_id, business_date, status, started_at, ended_at FROM autopilot_runs ORDER BY id DESC LIMIT :n"),
                {"n": limit},
            ).fetchall()
        return [dict(r._mapping) for r in rows]

    # ── Stages ──────────────────────────────────────────────────────────

    def get_or_create_stage(self, run_id: str, stage: str) -> dict[str, Any]:
        with connection("quantmate") as conn:
            row = conn.execute(
                text("SELECT id, run_id, stage, status, attempt FROM autopilot_stages WHERE run_id = :r AND stage = :s"),
                {"r": run_id, "s": stage},
            ).fetchone()
            if row:
                return dict(row._mapping)
            result = conn.execute(
                text(
                    "INSERT INTO autopilot_stages (run_id, stage, status, attempt) VALUES (:r, :s, 'pending', 0)"
                ),
                {"r": run_id, "s": stage},
            )
            conn.commit()
            stage_id = int(result.lastrowid)
        return {"id": stage_id, "run_id": run_id, "stage": stage, "status": "pending", "attempt": 0}

    def start_stage(self, run_id: str, stage: str, params: Optional[dict[str, Any]] = None) -> None:
        self.get_or_create_stage(run_id, stage)
        with connection("quantmate") as conn:
            conn.execute(
                text(
                    """
                    UPDATE autopilot_stages
                    SET status = 'running',
                        attempt = attempt + 1,
                        params = COALESCE(:params, params),
                        started_at = :now,
                        updated_at = :now
                    WHERE run_id = :r AND stage = :s
                    """
                ),
                {
                    "params": json.dumps(params) if params else None,
                    "now": _now(),
                    "r": run_id,
                    "s": stage,
                },
            )
            conn.commit()

    def finish_stage(
        self,
        run_id: str,
        stage: str,
        status: StageStatus,
        *,
        result: Optional[dict[str, Any]] = None,
        error: Optional[str] = None,
    ) -> None:
        with connection("quantmate") as conn:
            conn.execute(
                text(
                    """
                    UPDATE autopilot_stages
                    SET status = :status,
                        result = COALESCE(:result, result),
                        error = :error,
                        ended_at = :now,
                        updated_at = :now
                    WHERE run_id = :r AND stage = :s
                    """
                ),
                {
                    "status": status.value,
                    "result": json.dumps(result) if result else None,
                    "error": error,
                    "now": _now(),
                    "r": run_id,
                    "s": stage,
                },
            )
            conn.commit()

    def get_stage(self, run_id: str, stage: str) -> Optional[dict[str, Any]]:
        with connection("quantmate") as conn:
            row = conn.execute(
                text(
                    """
                    SELECT id, run_id, stage, status, attempt, params, result, error, started_at, ended_at
                    FROM autopilot_stages WHERE run_id = :r AND stage = :s
                    """
                ),
                {"r": run_id, "s": stage},
            ).fetchone()
        if not row:
            return None
        d = dict(row._mapping)
        d["params"] = _parse_json(d.get("params"))
        d["result"] = _parse_json(d.get("result"))
        return d

    def get_stage_result(self, run_id: str, stage: str) -> Optional[dict[str, Any]]:
        stage_row = self.get_stage(run_id, stage)
        return stage_row.get("result") if stage_row else None

    def list_stages(self, run_id: str) -> list[dict[str, Any]]:
        with connection("quantmate") as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT stage, status, attempt, error, started_at, ended_at
                    FROM autopilot_stages WHERE run_id = :r ORDER BY id ASC
                    """
                ),
                {"r": run_id},
            ).fetchall()
        return [dict(r._mapping) for r in rows]

    # ── Decisions ───────────────────────────────────────────────────────

    def record_decision(
        self,
        run_id: str,
        *,
        decision_type: str,
        action: str,
        subject_type: Optional[str] = None,
        subject_id: Optional[str] = None,
        input_summary: Optional[dict[str, Any]] = None,
        reason: Optional[str] = None,
        approval_status: str = "auto",
    ) -> int:
        with connection("quantmate") as conn:
            result = conn.execute(
                text(
                    """
                    INSERT INTO autopilot_decisions
                        (run_id, decision_type, action, subject_type, subject_id, input_summary, reason, approval_status)
                    VALUES (:run_id, :dtype, :action, :stype, :sid, :summary, :reason, :approval)
                    """
                ),
                {
                    "run_id": run_id,
                    "dtype": decision_type,
                    "action": action,
                    "stype": subject_type,
                    "sid": subject_id,
                    "summary": json.dumps(input_summary) if input_summary else None,
                    "reason": reason,
                    "approval": approval_status,
                },
            )
            conn.commit()
            return int(result.lastrowid)

    def update_approval_status(self, decision_id: int, approval_status: str) -> None:
        with connection("quantmate") as conn:
            conn.execute(
                text("UPDATE autopilot_decisions SET approval_status = :a WHERE id = :id"),
                {"a": approval_status, "id": decision_id},
            )
            conn.commit()


def _parse_json(val: Any) -> Any:
    if val is None:
        return None
    if isinstance(val, (dict, list)):
        return val
    try:
        return json.loads(val)
    except Exception:
        return None