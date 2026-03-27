"""DAO for composite_backtests table."""

import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from app.infrastructure.db.connections import connection


class CompositeBacktestDao:
    """Data access for composite strategy backtest results."""

    def insert(
        self,
        job_id: str,
        user_id: int,
        composite_strategy_id: int,
        start_date: str,
        end_date: str,
        initial_capital: float = 1_000_000,
        benchmark: str = "000300.SH",
    ) -> int:
        with connection("quantmate") as conn:
            result = conn.execute(
                text("""
                    INSERT INTO composite_backtests
                        (job_id, user_id, composite_strategy_id, start_date, end_date,
                         initial_capital, benchmark, status)
                    VALUES
                        (:job_id, :user_id, :composite_strategy_id, :start_date, :end_date,
                         :initial_capital, :benchmark, 'queued')
                """),
                {
                    "job_id": job_id,
                    "user_id": user_id,
                    "composite_strategy_id": composite_strategy_id,
                    "start_date": start_date,
                    "end_date": end_date,
                    "initial_capital": initial_capital,
                    "benchmark": benchmark,
                },
            )
            conn.commit()
            return result.lastrowid

    def get_by_job_id(self, job_id: str) -> Optional[Dict[str, Any]]:
        with connection("quantmate") as conn:
            row = conn.execute(
                text("SELECT * FROM composite_backtests WHERE job_id = :job_id"),
                {"job_id": job_id},
            ).first()
            if not row:
                return None
            return dict(row._mapping)

    def get_for_user(self, user_id: int, backtest_id: int) -> Optional[Dict[str, Any]]:
        with connection("quantmate") as conn:
            row = conn.execute(
                text("""
                    SELECT * FROM composite_backtests
                    WHERE id = :id AND user_id = :user_id
                """),
                {"id": backtest_id, "user_id": user_id},
            ).first()
            if not row:
                return None
            return dict(row._mapping)

    def list_for_user(
        self, user_id: int, composite_strategy_id: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {"user_id": user_id}
        where = "WHERE user_id = :user_id"
        if composite_strategy_id:
            where += " AND composite_strategy_id = :composite_strategy_id"
            params["composite_strategy_id"] = composite_strategy_id
        with connection("quantmate") as conn:
            rows = conn.execute(
                text(f"""
                    SELECT id, job_id, composite_strategy_id, start_date, end_date,
                           initial_capital, benchmark, status, error_message,
                           started_at, completed_at, created_at
                    FROM composite_backtests
                    {where}
                    ORDER BY created_at DESC
                """),
                params,
            ).fetchall()
            return [dict(r._mapping) for r in rows]

    def update_status(
        self,
        job_id: str,
        status: str,
        result: Optional[Dict] = None,
        attribution: Optional[Dict] = None,
        error_message: Optional[str] = None,
    ) -> None:
        sets = ["status = :status"]
        params: Dict[str, Any] = {"job_id": job_id, "status": status}

        if status == "running":
            sets.append("started_at = :started_at")
            params["started_at"] = datetime.utcnow()
        if status in ("completed", "failed"):
            sets.append("completed_at = :completed_at")
            params["completed_at"] = datetime.utcnow()
        if result is not None:
            sets.append("result = :result")
            params["result"] = json.dumps(result, default=str)
        if attribution is not None:
            sets.append("attribution = :attribution")
            params["attribution"] = json.dumps(attribution, default=str)
        if error_message is not None:
            sets.append("error_message = :error_message")
            params["error_message"] = error_message

        with connection("quantmate") as conn:
            conn.execute(
                text(f"""
                    UPDATE composite_backtests
                    SET {', '.join(sets)}
                    WHERE job_id = :job_id
                """),
                params,
            )
            conn.commit()

    def delete_for_user(self, user_id: int, backtest_id: int) -> bool:
        with connection("quantmate") as conn:
            result = conn.execute(
                text("""
                    DELETE FROM composite_backtests
                    WHERE id = :id AND user_id = :user_id
                """),
                {"id": backtest_id, "user_id": user_id},
            )
            conn.commit()
            return result.rowcount > 0
