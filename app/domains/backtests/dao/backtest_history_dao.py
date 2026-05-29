"""Backtest history DAO.

All SQL touching `quantmate.backtest_history` lives here.
"""

from __future__ import annotations

from datetime import datetime
import numpy as np
from typing import Any, Optional
import json

from app.infrastructure.db.connections import connection


class BacktestHistoryDao:
    def upsert_history(
        self,
        *,
        user_id: int,
        job_id: str,
        strategy_id: Optional[int],
        strategy_class: Optional[str],
        strategy_version: Optional[int],
        vt_symbol: str,
        start_date: str,
        end_date: str,
        parameters: dict[str, Any],
        status: str,
        result: Optional[dict[str, Any]],
        error: Optional[str],
        created_at: datetime,
        completed_at: Optional[datetime],
        bulk_job_id: Optional[str] = None,
        source: Optional[str] = None,
        subject_type: Optional[str] = None,
        subject_id: Optional[int] = None,
        subject_name: Optional[str] = None,
        engine_type: Optional[str] = None,
        scope_type: Optional[str] = None,
        request_payload: Optional[dict[str, Any]] = None,
        summary_json: Optional[dict[str, Any]] = None,
        artifacts_json: Optional[dict[str, Any]] = None,
        diagnostics_json: Optional[dict[str, Any]] = None,
        extensions_json: Optional[dict[str, Any]] = None,
        result_schema_version: Optional[int] = None,
    ) -> None:
        def _json_default(o):
            # handle numpy types and datetimes
            try:
                if isinstance(o, np.ndarray):
                    return o.tolist()
            except Exception:
                pass
            try:
                # numpy scalar types
                if hasattr(o, "item") and (isinstance(o, (np.generic,))):
                    return o.item()
            except Exception:
                pass
            if isinstance(o, datetime):
                return o.isoformat()
            # fallback
            return str(o)

        with connection("quantmate") as conn:
            from sqlalchemy import text

            conn.execute(
                text(
                    """
                                        INSERT INTO backtest_history
                                        (user_id, job_id, bulk_job_id, strategy_id, strategy_class, strategy_version, source,
                                         vt_symbol, start_date, end_date, parameters, status, result, error, created_at,
                                         completed_at, subject_type, subject_id, subject_name, engine_type, scope_type,
                                         request_payload, summary_json, artifacts_json, diagnostics_json, extensions_json,
                                         result_schema_version)
                    VALUES
                                        (:user_id, :job_id, :bulk_job_id, :strategy_id, :strategy_class, :strategy_version, :source,
                                         :vt_symbol, :start_date, :end_date, :parameters, :status, :result, :error, :created_at,
                                         :completed_at, :subject_type, :subject_id, :subject_name, :engine_type, :scope_type,
                                         :request_payload, :summary_json, :artifacts_json, :diagnostics_json, :extensions_json,
                                         :result_schema_version)
                    ON DUPLICATE KEY UPDATE
                                            source = :source,
                      status = :status,
                      result = :result,
                      error = :error,
                                            completed_at = :completed_at,
                                            subject_type = COALESCE(:subject_type, subject_type),
                                            subject_id = COALESCE(:subject_id, subject_id),
                                            subject_name = COALESCE(:subject_name, subject_name),
                                            engine_type = COALESCE(:engine_type, engine_type),
                                            scope_type = COALESCE(:scope_type, scope_type),
                                            request_payload = COALESCE(:request_payload, request_payload),
                                            summary_json = COALESCE(:summary_json, summary_json),
                                            artifacts_json = COALESCE(:artifacts_json, artifacts_json),
                                            diagnostics_json = COALESCE(:diagnostics_json, diagnostics_json),
                                            extensions_json = COALESCE(:extensions_json, extensions_json),
                                            result_schema_version = COALESCE(:result_schema_version, result_schema_version)
                    """
                ),
                {
                    "user_id": user_id,
                    "job_id": job_id,
                    "bulk_job_id": bulk_job_id,
                    "strategy_id": strategy_id,
                    "strategy_class": strategy_class,
                    "strategy_version": strategy_version,
                    "source": source,
                    "vt_symbol": vt_symbol,
                    "start_date": start_date,
                    "end_date": end_date,
                    "parameters": json.dumps(parameters or {}, default=_json_default),
                    "status": status,
                    "result": (json.dumps(result, default=_json_default) if result is not None else None),
                    "error": error,
                    "created_at": created_at,
                    "completed_at": completed_at,
                    "subject_type": subject_type,
                    "subject_id": subject_id,
                    "subject_name": subject_name,
                    "engine_type": engine_type,
                    "scope_type": scope_type,
                    "request_payload": (
                        json.dumps(request_payload, default=_json_default) if request_payload is not None else None
                    ),
                    "summary_json": (
                        json.dumps(summary_json, default=_json_default) if summary_json is not None else None
                    ),
                    "artifacts_json": (
                        json.dumps(artifacts_json, default=_json_default) if artifacts_json is not None else None
                    ),
                    "diagnostics_json": (
                        json.dumps(diagnostics_json, default=_json_default) if diagnostics_json is not None else None
                    ),
                    "extensions_json": (
                        json.dumps(extensions_json, default=_json_default) if extensions_json is not None else None
                    ),
                    "result_schema_version": result_schema_version,
                },
            )
            conn.commit()

    @staticmethod
    def _parse_json(value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, (dict, list)):
            return value
        try:
            return json.loads(value)
        except Exception:
            return None

    def get_child_result_json(self, job_id: str) -> Optional[dict[str, Any]]:
        with connection("quantmate") as conn:
            from sqlalchemy import text

            row = conn.execute(
                text("SELECT result FROM backtest_history WHERE job_id = :jid LIMIT 1"),
                {"jid": job_id},
            ).fetchone()
            if not row or not row.result:
                return None
            try:
                return json.loads(row.result) if isinstance(row.result, str) else row.result
            except Exception:
                return None

    def get_job_row(self, job_id: str) -> Optional[dict[str, Any]]:
        """Fetch a backtest_history row by job_id."""
        with connection("quantmate") as conn:
            from sqlalchemy import text

            row = conn.execute(
                text(
                    """
                    SELECT job_id, user_id, bulk_job_id, strategy_id, strategy_class,
                           strategy_version, vt_symbol, start_date, end_date,
                           parameters, status, result, error, created_at, completed_at
                    FROM backtest_history
                    WHERE job_id = :jid
                    LIMIT 1
                    """
                ),
                {"jid": job_id},
            ).fetchone()
            return dict(row._mapping) if row else None

    def get_latest_strategy_run(self, *, user_id: int, strategy_id: int) -> Optional[dict[str, Any]]:
        """Fetch the most recent completed backtest row for a strategy."""
        with connection("quantmate") as conn:
            from sqlalchemy import text

            row = conn.execute(
                text(
                    """
                    SELECT vt_symbol, start_date, end_date
                    FROM backtest_history
                    WHERE user_id = :user_id
                      AND strategy_id = :strategy_id
                      AND status IN ('completed', 'finished')
                    ORDER BY created_at DESC
                    LIMIT 1
                    """
                ),
                {"user_id": user_id, "strategy_id": strategy_id},
            ).fetchone()
            return dict(row._mapping) if row else None

    def delete_single(self, job_id: str, user_id: int) -> None:
        with connection("quantmate") as conn:
            from sqlalchemy import text

            conn.execute(
                text("DELETE FROM backtest_history WHERE job_id = :job_id AND user_id = :user_id"),
                {"job_id": job_id, "user_id": user_id},
            )
            conn.commit()

    def delete_bulk_children(self, bulk_job_id: str, user_id: int) -> None:
        with connection("quantmate") as conn:
            from sqlalchemy import text

            conn.execute(
                text("DELETE FROM backtest_history WHERE bulk_job_id = :bulk_job_id AND user_id = :user_id"),
                {"bulk_job_id": bulk_job_id, "user_id": user_id},
            )
            conn.commit()

    def count_for_user(self, user_id: int) -> int:
        with connection("quantmate") as conn:
            from sqlalchemy import text

            row = conn.execute(
                text("SELECT COUNT(*) as total FROM backtest_history WHERE user_id = :user_id"),
                {"user_id": user_id},
            ).fetchone()
            return int(row.total) if row and hasattr(row, "total") else 0

    def count_runs_for_user(self, user_id: int, subject_type: Optional[str] = None) -> int:
        with connection("quantmate") as conn:
            from sqlalchemy import text

            query = "SELECT COUNT(*) as total FROM backtest_history WHERE user_id = :user_id"
            params: dict[str, Any] = {"user_id": user_id}
            if subject_type:
                query += " AND subject_type = :subject_type"
                params["subject_type"] = subject_type

            row = conn.execute(text(query), params).fetchone()
            return int(row.total) if row and hasattr(row, "total") else 0

    def list_for_user(self, *, user_id: int, limit: int, offset: int) -> list[dict[str, Any]]:
        with connection("quantmate") as conn:
            from sqlalchemy import text

            rows = conn.execute(
                text(
                    """
                    SELECT id, job_id, strategy_id, strategy_class, strategy_version, vt_symbol,
                           start_date, end_date, status, result, created_at, completed_at
                    FROM backtest_history
                    WHERE user_id = :user_id
                    ORDER BY created_at DESC
                    LIMIT :limit OFFSET :offset
                    """
                ),
                {"user_id": user_id, "limit": limit, "offset": offset},
            ).fetchall()
            return [dict(r._mapping) for r in rows]

    def get_detail_for_user(self, *, job_id: str, user_id: int) -> Optional[dict[str, Any]]:
        with connection("quantmate") as conn:
            from sqlalchemy import text

            row = conn.execute(
                text(
                    """
                    SELECT id, job_id, strategy_id, strategy_class, strategy_version, vt_symbol,
                           start_date, end_date, parameters, status, result, error,
                           created_at, completed_at
                    FROM backtest_history
                    WHERE job_id = :job_id AND user_id = :user_id
                    """
                ),
                {"job_id": job_id, "user_id": user_id},
            ).fetchone()
            return dict(row._mapping) if row else None

    def list_runs_for_user(
        self,
        *,
        user_id: int,
        limit: int,
        offset: int,
        subject_type: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        with connection("quantmate") as conn:
            from sqlalchemy import text

            query = """
                SELECT id, job_id, subject_type, subject_id, subject_name, engine_type, scope_type,
                       strategy_id, strategy_class, vt_symbol, start_date, end_date, status,
                       summary_json, result, created_at, completed_at
                FROM backtest_history
                WHERE user_id = :user_id
            """
            params: dict[str, Any] = {"user_id": user_id, "limit": limit, "offset": offset}
            if subject_type:
                query += " AND subject_type = :subject_type"
                params["subject_type"] = subject_type

            query += " ORDER BY created_at DESC LIMIT :limit OFFSET :offset"
            rows = conn.execute(text(query), params).fetchall()
            return [dict(r._mapping) for r in rows]

    def get_run_detail_for_user(self, *, job_id: str, user_id: int) -> Optional[dict[str, Any]]:
        with connection("quantmate") as conn:
            from sqlalchemy import text

            row = conn.execute(
                text(
                    """
                    SELECT id, job_id, subject_type, subject_id, subject_name, engine_type, scope_type,
                           strategy_id, strategy_class, vt_symbol, start_date, end_date, parameters,
                           status, result, error, request_payload, summary_json, artifacts_json,
                           diagnostics_json, extensions_json, result_schema_version, created_at, completed_at
                    FROM backtest_history
                    WHERE job_id = :job_id AND user_id = :user_id
                    LIMIT 1
                    """
                ),
                {"job_id": job_id, "user_id": user_id},
            ).fetchone()
            return dict(row._mapping) if row else None
