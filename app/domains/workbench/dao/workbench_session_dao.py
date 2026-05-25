"""Workbench session DAO."""

from __future__ import annotations

import json
from typing import Any, Optional

from sqlalchemy import text

from app.infrastructure.db.connections import connection


def _encode_json(value: Any) -> str:
    return json.dumps(value or {}, ensure_ascii=True)


def _decode_json(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value:
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _to_session(row: Any) -> dict[str, Any]:
    return {
        "id": row.id,
        "user_id": row.user_id,
        "name": row.name,
        "current_stage": row.current_stage,
        "status": row.status,
        "state_json": _decode_json(row.state_json),
        "last_backtest_job_id": row.last_backtest_job_id,
        "last_deployment_id": row.last_deployment_id,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


class WorkbenchSessionDao:
    """DAO for workbench sessions and events."""

    def list_by_user(self, user_id: int, limit: int = 10) -> list[dict[str, Any]]:
        with connection("quantmate") as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT id, user_id, name, current_stage, status, state_json,
                           last_backtest_job_id, last_deployment_id, created_at, updated_at
                    FROM workbench_sessions
                    WHERE user_id = :user_id
                    ORDER BY updated_at DESC
                    LIMIT :limit
                    """
                ),
                {"user_id": user_id, "limit": limit},
            ).fetchall()
        return [_to_session(row) for row in rows]

    def create(
        self,
        user_id: int,
        name: str,
        current_stage: str,
        status: str,
        state_json: dict[str, Any],
        last_backtest_job_id: str | None = None,
        last_deployment_id: int | None = None,
    ) -> dict[str, Any]:
        with connection("quantmate") as conn:
            result = conn.execute(
                text(
                    """
                    INSERT INTO workbench_sessions (
                        user_id, name, current_stage, status, state_json, last_backtest_job_id, last_deployment_id
                    )
                    VALUES (
                        :user_id, :name, :current_stage, :status, CAST(:state_json AS JSON), :last_backtest_job_id, :last_deployment_id
                    )
                    """
                ),
                {
                    "user_id": user_id,
                    "name": name,
                    "current_stage": current_stage,
                    "status": status,
                    "state_json": _encode_json(state_json),
                    "last_backtest_job_id": last_backtest_job_id,
                    "last_deployment_id": last_deployment_id,
                },
            )
            conn.commit()
            session_id = int(result.lastrowid)
        session = self.get(user_id, session_id)
        if session is None:
            raise KeyError(session_id)
        return session

    def get(self, user_id: int, session_id: int) -> Optional[dict[str, Any]]:
        with connection("quantmate") as conn:
            row = conn.execute(
                text(
                    """
                    SELECT id, user_id, name, current_stage, status, state_json,
                           last_backtest_job_id, last_deployment_id, created_at, updated_at
                    FROM workbench_sessions
                    WHERE id = :session_id AND user_id = :user_id
                    """
                ),
                {"session_id": session_id, "user_id": user_id},
            ).fetchone()
        return _to_session(row) if row else None

    def update(
        self,
        user_id: int,
        session_id: int,
        *,
        name: str,
        current_stage: str,
        status: str,
        state_json: dict[str, Any],
        last_backtest_job_id: str | None = None,
        last_deployment_id: int | None = None,
    ) -> dict[str, Any]:
        with connection("quantmate") as conn:
            result = conn.execute(
                text(
                    """
                    UPDATE workbench_sessions
                    SET name = :name,
                        current_stage = :current_stage,
                        status = :status,
                        state_json = CAST(:state_json AS JSON),
                        last_backtest_job_id = :last_backtest_job_id,
                        last_deployment_id = :last_deployment_id,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = :session_id AND user_id = :user_id
                    """
                ),
                {
                    "session_id": session_id,
                    "user_id": user_id,
                    "name": name,
                    "current_stage": current_stage,
                    "status": status,
                    "state_json": _encode_json(state_json),
                    "last_backtest_job_id": last_backtest_job_id,
                    "last_deployment_id": last_deployment_id,
                },
            )
            conn.commit()
        if result.rowcount == 0:
            raise KeyError(session_id)
        session = self.get(user_id, session_id)
        if session is None:
            raise KeyError(session_id)
        return session

    def append_event(self, session_id: int, event_type: str, payload: dict[str, Any]) -> None:
        with connection("quantmate") as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO workbench_session_events (session_id, event_type, payload)
                    VALUES (:session_id, :event_type, CAST(:payload AS JSON))
                    """
                ),
                {
                    "session_id": session_id,
                    "event_type": event_type,
                    "payload": _encode_json(payload),
                },
            )
            conn.commit()

    def list_events(self, session_id: int, limit: int = 50) -> list[dict[str, Any]]:
        with connection("quantmate") as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT id, session_id, event_type, payload, created_at
                    FROM workbench_session_events
                    WHERE session_id = :session_id
                    ORDER BY created_at DESC
                    LIMIT :limit
                    """
                ),
                {"session_id": session_id, "limit": limit},
            ).fetchall()
        return [
            {
                "id": row.id,
                "session_id": row.session_id,
                "event_type": row.event_type,
                "payload": _decode_json(row.payload),
                "created_at": row.created_at,
            }
            for row in rows
        ]