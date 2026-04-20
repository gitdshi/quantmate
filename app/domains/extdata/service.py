"""Sync domain service (moved to extdata)."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Dict, Optional

from sqlalchemy import text

from app.infrastructure.config import get_runtime_int
from app.infrastructure.db.connections import get_quantmate_engine
from app.infrastructure.runtime_cache import ExpiringCache

SYNC_HOUR = get_runtime_int(env_keys="SYNC_HOUR", db_key="datasync.sync_hour", default=2)
SYNC_MINUTE = get_runtime_int(env_keys="SYNC_MINUTE", db_key="datasync.sync_minute", default=0)
DATASYNC_DASHBOARD_CACHE_TTL_SECONDS = get_runtime_int(
    env_keys="DATASYNC_DASHBOARD_CACHE_TTL_SECONDS",
    db_key="datasync.dashboard.cache_ttl_seconds",
    default=30,
)
DEFAULT_DASHBOARD_SUMMARY_DAYS = 7

_DATASYNC_SUMMARY_CACHE = ExpiringCache(name="datasync_summary", maxsize=16)
_DATASYNC_LATEST_CACHE = ExpiringCache(name="datasync_latest", maxsize=4)
_DATASYNC_INITIALIZATION_CACHE = ExpiringCache(name="datasync_initialization", maxsize=4)


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
        from app.datasync.service.init_service import get_coverage_window

        engine = get_quantmate_engine()
        coverage_window = get_coverage_window()
        window_start = coverage_window["start_date"]

        with engine.connect() as conn:
            # Latest finished record
            row = conn.execute(
                text("SELECT MAX(finished_at) FROM data_sync_status WHERE status = 'success'")
            ).fetchone()
            last_finished = row[0] if row else None

            # Running count
            row = conn.execute(text("SELECT COUNT(*) FROM data_sync_status WHERE status = 'running'")).fetchone()
            running_count = row[0] if row else 0

            # Failed / pending counts in the configured coverage window
            row = conn.execute(
                text(
                    "SELECT COUNT(*) FROM data_sync_status "
                    "WHERE sync_date >= :cutoff AND status IN ('error', 'partial', 'pending')"
                ),
                {"cutoff": window_start.isoformat()},
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
                "window_start": window_start.isoformat(),
            },
            "consistency": {
                "missing_count": missing_count,
                "is_consistent": missing_count == 0,
            },
        }


class DataSyncDashboardService:
    """Short-lived cached snapshot for the market page datasync widgets."""

    def get_summary(self, *, days: int = DEFAULT_DASHBOARD_SUMMARY_DAYS) -> Dict[str, Any]:
        from app.infrastructure.db.connections import get_quantmate_engine as get_engine

        cache_key = (days, id(get_engine))
        return _DATASYNC_SUMMARY_CACHE.get_or_load(
            cache_key,
            lambda: self._build_summary(days),
            ttl_seconds=DATASYNC_DASHBOARD_CACHE_TTL_SECONDS,
            stale_if_error=True,
        )

    def get_latest(self, *, days: int = DEFAULT_DASHBOARD_SUMMARY_DAYS) -> Dict[str, Any]:
        del days
        from app.infrastructure.db.connections import get_quantmate_engine as get_engine

        cache_key = id(get_engine)
        return _DATASYNC_LATEST_CACHE.get_or_load(
            cache_key,
            self._build_latest,
            ttl_seconds=DATASYNC_DASHBOARD_CACHE_TTL_SECONDS,
            stale_if_error=True,
        )

    def get_initialization(self, *, days: int = DEFAULT_DASHBOARD_SUMMARY_DAYS) -> Dict[str, Any]:
        del days
        from app.datasync.service.init_service import get_initialization_state

        cache_key = id(get_initialization_state)
        return _DATASYNC_INITIALIZATION_CACHE.get_or_load(
            cache_key,
            get_initialization_state,
            ttl_seconds=DATASYNC_DASHBOARD_CACHE_TTL_SECONDS,
            stale_if_error=True,
        )

    def _build_summary(self, days: int) -> Dict[str, Any]:
        from app.infrastructure.db.connections import get_quantmate_engine as get_engine

        engine = get_engine()
        cutoff = (date.today() - timedelta(days=days)).isoformat()

        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT sync_date, source, status, COUNT(*) as cnt "
                    "FROM data_sync_status WHERE sync_date >= :cutoff "
                    "GROUP BY sync_date, source, status "
                    "ORDER BY sync_date DESC, source, status"
                ),
                {"cutoff": cutoff},
            ).fetchall()

            summary: dict[str, dict[str, dict[str, int]]] = {}
            for row in rows:
                sync_date = row[0].isoformat() if hasattr(row[0], "isoformat") else str(row[0])
                source = row[1]
                status = row[2]
                count = row[3]
                if sync_date not in summary:
                    summary[sync_date] = {}
                if source not in summary[sync_date]:
                    summary[sync_date][source] = {
                        "success": 0,
                        "error": 0,
                        "pending": 0,
                        "running": 0,
                        "partial": 0,
                    }
                summary[sync_date][source][status] = count

            overall_rows = conn.execute(
                text("SELECT status, COUNT(*) FROM data_sync_status WHERE sync_date >= :cutoff GROUP BY status"),
                {"cutoff": cutoff},
            ).fetchall()
            overall_map = {row[0]: row[1] for row in overall_rows}

        return {
            "days": days,
            "overall": overall_map,
            "by_date": summary,
        }

    def _build_latest(self) -> Dict[str, Any]:
        from app.infrastructure.db.connections import get_quantmate_engine as get_engine

        engine = get_engine()
        with engine.connect() as conn:
            latest_row = conn.execute(text("SELECT MAX(sync_date) FROM data_sync_status")).fetchone()
            latest_date = latest_row[0] if latest_row else None
            latest_items: list[dict[str, Any]] = []
            if latest_date is not None:
                latest_items = [
                    dict(row._mapping)
                    for row in conn.execute(
                        text(
                            "SELECT source, interface_key, status, rows_synced, error_message, "
                            "retry_count, started_at, finished_at "
                            "FROM data_sync_status WHERE sync_date = :sync_date "
                            "ORDER BY source, interface_key"
                        ),
                        {"sync_date": latest_date},
                    ).fetchall()
                ]

        return {
            "latest_date": latest_date.isoformat() if hasattr(latest_date, "isoformat") else str(latest_date)
            if latest_date is not None
            else None,
            "items": latest_items,
        }
