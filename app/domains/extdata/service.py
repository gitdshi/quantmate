"""Sync domain service (moved to extdata)."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Dict, Optional

from sqlalchemy import text

from app.infrastructure.config import get_runtime_int
from app.infrastructure.db.connections import get_quantmate_engine

SYNC_HOUR = get_runtime_int(env_keys="SYNC_HOUR", db_key="datasync.sync_hour", default=2)
SYNC_MINUTE = get_runtime_int(env_keys="SYNC_MINUTE", db_key="datasync.sync_minute", default=0)
LOOKBACK_DAYS = get_runtime_int(env_keys="LOOKBACK_DAYS", db_key="datasync.lookback_days", default=60)


def _status_from_last_run(last_run_at: Optional[datetime], running_count: int) -> str:
    if running_count > 0:
        return "running"
    if not last_run_at:
        return "unknown"
    if datetime.utcnow() - last_run_at <= timedelta(hours=26):
        return "idle"
    return "stale"


class SyncStatusService:
    """Aggregates sync status from the new data_sync_status table."""

    def get_sync_status(self) -> Dict[str, Any]:
        engine = get_quantmate_engine()

        with engine.connect() as conn:
            # Latest finished record
            row = conn.execute(
                text("SELECT MAX(finished_at) FROM data_sync_status WHERE status = 'success'")
            ).fetchone()
            last_finished = row[0] if row else None

            # Running count
            row = conn.execute(text("SELECT COUNT(*) FROM data_sync_status WHERE status = 'running'")).fetchone()
            running_count = row[0] if row else 0

            # Failed / pending counts in lookback window
            cutoff = (date.today() - timedelta(days=LOOKBACK_DAYS)).isoformat()
            row = conn.execute(
                text(
                    "SELECT COUNT(*) FROM data_sync_status "
                    "WHERE sync_date >= :cutoff AND status IN ('error', 'partial', 'pending')"
                ),
                {"cutoff": cutoff},
            ).fetchone()
            missing_count = row[0] if row else 0

            # Per-source summary for last 7 days
            cutoff7 = (date.today() - timedelta(days=7)).isoformat()
            rows = conn.execute(
                text(
                    "SELECT source, status, COUNT(*) FROM data_sync_status "
                    "WHERE sync_date >= :cutoff GROUP BY source, status"
                ),
                {"cutoff": cutoff7},
            ).fetchall()
            source_summary: dict = {}
            for r in rows:
                src = r[0]
                if src not in source_summary:
                    source_summary[src] = {}
                source_summary[src][r[1]] = r[2]

        daemon_status = _status_from_last_run(last_finished, running_count)

        return {
            "daemon": {
                "status": daemon_status,
                "running_jobs": running_count,
                "last_run_at": last_finished.isoformat() if last_finished else None,
                "next_run_local": f"{SYNC_HOUR:02d}:{SYNC_MINUTE:02d}",
            },
            "sync": {
                "source_summary": source_summary,
                "lookback_days": LOOKBACK_DAYS,
            },
            "consistency": {
                "missing_count": missing_count,
                "is_consistent": missing_count == 0,
            },
        }
