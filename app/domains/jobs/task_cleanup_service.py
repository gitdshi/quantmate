"""Task cleanup and dead-letter queue (DLQ) management service."""

from __future__ import annotations

from typing import Any

from sqlalchemy import text

from app.infrastructure.db.connections import connection


class TaskCleanupService:
    """Manage expired tasks and failed job cleanup."""

    def cleanup_expired_jobs(self, max_age_days: int = 30) -> dict[str, int]:
        """Remove completed/failed jobs older than max_age_days."""
        with connection("quantmate") as conn:
            result = conn.execute(
                text(
                    "DELETE FROM backtest_history "
                    "WHERE status IN ('completed', 'failed') "
                    "AND created_at < DATE_SUB(NOW(), INTERVAL :days DAY)"
                ),
                {"days": max_age_days},
            )
            deleted_backtests = result.rowcount
            conn.commit()
        return {"deleted_backtests": deleted_backtests}

    def list_failed_jobs(self, limit: int = 50) -> list[dict[str, Any]]:
        """List recent failed jobs for DLQ review."""
        with connection("quantmate") as conn:
            rows = conn.execute(
                text(
                    "SELECT job_id, strategy_class, symbol, error, created_at "
                    "FROM backtest_history "
                    "WHERE status = 'failed' "
                    "ORDER BY created_at DESC LIMIT :limit"
                ),
                {"limit": limit},
            ).fetchall()
            return [dict(r._mapping) for r in rows]

    def retry_failed_job(self, job_id: str) -> dict[str, Any]:
        """Mark a failed job for retry (resets status to pending)."""
        with connection("quantmate") as conn:
            result = conn.execute(
                text(
                    "UPDATE backtest_history SET status = 'pending', error = NULL "
                    "WHERE job_id = :jid AND status = 'failed'"
                ),
                {"jid": job_id},
            )
            conn.commit()
            if result.rowcount == 0:
                raise KeyError("Job not found or not in failed state")
        return {"job_id": job_id, "status": "pending"}

    def purge_dlq(self, older_than_days: int = 7) -> int:
        """Permanently remove old failed jobs."""
        with connection("quantmate") as conn:
            result = conn.execute(
                text(
                    "DELETE FROM backtest_history "
                    "WHERE status = 'failed' "
                    "AND created_at < DATE_SUB(NOW(), INTERVAL :days DAY)"
                ),
                {"days": older_than_days},
            )
            conn.commit()
            return result.rowcount
