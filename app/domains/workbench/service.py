"""Workbench session service."""

from __future__ import annotations

from typing import Any

from app.api.models.workbench import WorkbenchStage, WorkbenchSessionStatus
from app.domains.workbench.dao.workbench_session_dao import WorkbenchSessionDao


def _default_state() -> dict[str, Any]:
    return {
        "stage": WorkbenchStage.FACTOR.value,
        "selected_factors": [],
        "strategy_draft": None,
        "backtest": {
            "job_id": None,
            "status": None,
            "summary": None,
            "ai_report_id": None,
        },
        "paper_trade": {
            "account_id": None,
            "mode": None,
            "deployment_id": None,
            "runtime_summary": None,
        },
    }


class WorkbenchService:
    """Orchestrates CRUD and basic transition validation for workbench sessions."""

    def __init__(self) -> None:
        self.dao = WorkbenchSessionDao()

    def list_sessions(self, user_id: int, limit: int = 10) -> list[dict[str, Any]]:
        return self.dao.list_by_user(user_id, limit=limit)

    def create_session(
        self,
        user_id: int,
        name: str | None,
        current_stage: str,
        status: str,
        state_json: dict[str, Any] | None,
    ) -> dict[str, Any]:
        state = state_json or _default_state()
        stage = current_stage or WorkbenchStage.FACTOR.value
        state.setdefault("stage", stage)
        backtest = state.get("backtest") or {}
        paper_trade = state.get("paper_trade") or {}
        session = self.dao.create(
            user_id=user_id,
            name=name or "New Workflow",
            current_stage=stage,
            status=status or WorkbenchSessionStatus.DRAFT.value,
            state_json=state,
            last_backtest_job_id=backtest.get("job_id"),
            last_deployment_id=paper_trade.get("deployment_id"),
        )
        self.dao.append_event(session["id"], "session_created", {"current_stage": stage})
        return session

    def get_session(self, user_id: int, session_id: int) -> dict[str, Any]:
        session = self.dao.get(user_id, session_id)
        if session is None:
            raise KeyError(session_id)
        return session

    def update_session(
        self,
        user_id: int,
        session_id: int,
        *,
        name: str | None,
        current_stage: str | None,
        status: str | None,
        state_json: dict[str, Any] | None,
    ) -> dict[str, Any]:
        current = self.get_session(user_id, session_id)
        next_state = state_json if state_json is not None else current["state_json"]
        next_stage = current_stage or current["current_stage"]
        if not isinstance(next_state, dict):
            raise ValueError("state_json must be an object")
        next_state["stage"] = next_stage
        backtest = next_state.get("backtest") or {}
        paper_trade = next_state.get("paper_trade") or {}
        updated = self.dao.update(
            user_id,
            session_id,
            name=name or current["name"],
            current_stage=next_stage,
            status=status or current["status"],
            state_json=next_state,
            last_backtest_job_id=backtest.get("job_id"),
            last_deployment_id=paper_trade.get("deployment_id"),
        )
        self.dao.append_event(
            session_id,
            "session_updated",
            {"current_stage": next_stage, "status": updated["status"]},
        )
        return updated

    def transition_session(self, user_id: int, session_id: int, target_stage: str) -> dict[str, Any]:
        current = self.get_session(user_id, session_id)
        state = current["state_json"]

        if target_stage == WorkbenchStage.STRATEGY.value and not state.get("selected_factors"):
            raise ValueError("Select at least one factor before moving to strategy stage")
        if target_stage == WorkbenchStage.BACKTEST.value and not state.get("strategy_draft"):
            raise ValueError("Create or load a strategy draft before moving to backtest stage")
        if target_stage == WorkbenchStage.PAPER_TRADE.value:
            backtest_summary = (state.get("backtest") or {}).get("summary")
            if not backtest_summary:
                raise ValueError("A completed backtest summary is required before paper trading")

        return self.update_session(
            user_id,
            session_id,
            name=None,
            current_stage=target_stage,
            status=None,
            state_json=state,
        )

    def list_events(self, session_id: int) -> list[dict[str, Any]]:
        return self.dao.list_events(session_id)