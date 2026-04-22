"""Sync domain service (moved to extdata)."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Dict, Optional

from sqlalchemy import text

from app.infrastructure.config import get_runtime_int
from app.infrastructure.db.connections import get_quantmate_engine
from app.infrastructure.runtime_cache import ExpiringCache

def _sync_hour() -> int:
    return get_runtime_int(env_keys="SYNC_HOUR", db_key="datasync.sync_hour", default=2)


def _sync_minute() -> int:
    return get_runtime_int(env_keys="SYNC_MINUTE", db_key="datasync.sync_minute", default=0)


def _dashboard_cache_ttl_seconds() -> int:
    return get_runtime_int(
        env_keys="DATASYNC_DASHBOARD_CACHE_TTL_SECONDS",
        db_key="datasync.dashboard.cache_ttl_seconds",
        default=30,
    )
DEFAULT_DASHBOARD_SUMMARY_DAYS = 7

_DATASYNC_SUMMARY_CACHE = ExpiringCache(name="datasync_summary", maxsize=16)
_DATASYNC_LATEST_CACHE = ExpiringCache(name="datasync_latest", maxsize=4)
_DATASYNC_INITIALIZATION_CACHE = ExpiringCache(name="datasync_initialization", maxsize=4)


def clear_datasync_dashboard_cache() -> None:
    _DATASYNC_SUMMARY_CACHE.clear()
    _DATASYNC_LATEST_CACHE.clear()
    _DATASYNC_INITIALIZATION_CACHE.clear()


def _dedupe_source_item_rows(rows: list[Any]) -> list[Any]:
    deduped: list[Any] = []
    seen: set[tuple[str, str]] = set()

    for row in rows:
        source = str(row[0] or "").strip()
        item_key = str(row[2] or "").strip()
        if not source or not item_key:
            continue

        pair = (source, item_key)
        if pair in seen:
            continue

        seen.add(pair)
        deduped.append(row)

    return deduped


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
                    "next_run_local": f"{_sync_hour():02d}:{_sync_minute():02d}",
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
            ttl_seconds=_dashboard_cache_ttl_seconds(),
            stale_if_error=True,
        )

    def get_latest(self, *, days: int = DEFAULT_DASHBOARD_SUMMARY_DAYS) -> Dict[str, Any]:
        del days
        from app.infrastructure.db.connections import get_quantmate_engine as get_engine

        cache_key = id(get_engine)
        return _DATASYNC_LATEST_CACHE.get_or_load(
            cache_key,
            self._build_latest,
            ttl_seconds=_dashboard_cache_ttl_seconds(),
            stale_if_error=True,
        )

    def get_initialization(self, *, days: int = DEFAULT_DASHBOARD_SUMMARY_DAYS) -> Dict[str, Any]:
        del days
        from app.datasync.service.init_service import get_initialization_state

        cache_key = id(get_initialization_state)
        return _DATASYNC_INITIALIZATION_CACHE.get_or_load(
            cache_key,
            get_initialization_state,
            ttl_seconds=_dashboard_cache_ttl_seconds(),
            stale_if_error=True,
        )

    def get_interface_coverage(self, *, source: str | None = None) -> Dict[str, Any]:
        from app.datasync.capabilities import is_item_sync_supported, load_source_config_map
        from app.datasync.registry import build_default_registry
        from app.datasync.service.init_service import get_coverage_window
        from app.datasync.service.sync_engine import get_trade_calendar
        from app.infrastructure.db.connections import get_quantmate_engine as get_engine

        coverage_window = get_coverage_window()
        start_date = coverage_window["start_date"]
        end_date = coverage_window["end_date"]

        try:
            trade_dates = get_trade_calendar(start_date, end_date)
        except Exception:
            trade_dates = []
        expected_trade_days = len(trade_dates)

        engine = get_engine()
        clauses = ["dsi.enabled = 1", "dsc.enabled = 1"]
        params: dict[str, Any] = {
            "start_date": start_date,
            "end_date": end_date,
        }
        if source:
            clauses.append("dsi.source = :source")
            params["source"] = source

        item_sql = (
            "SELECT dsi.source, dsc.display_name AS source_name, dsi.item_key, "
            "dsi.item_name AS item_name, dsi.sync_priority, dsi.api_name, "
            "dsi.permission_points, dsi.requires_permission "
            "FROM data_source_items dsi "
            "JOIN data_source_configs dsc ON dsi.source = dsc.source_key "
            f"WHERE {' AND '.join(clauses)} "
            "ORDER BY dsi.source, dsi.sync_priority, dsi.item_key"
        )
        legacy_item_sql = (
            "SELECT dsi.source, dsc.display_name AS source_name, dsi.item_key, "
            "dsi.item_name AS item_name, dsi.sync_priority, dsi.item_key AS api_name, "
            "0 AS permission_points, dsi.requires_permission "
            "FROM data_source_items dsi "
            "JOIN data_source_configs dsc ON dsi.source = dsc.source_key "
            f"WHERE {' AND '.join(clauses)} "
            "ORDER BY dsi.source, dsi.sync_priority, dsi.item_key"
        )
        status_where = "WHERE sync_date BETWEEN :start_date AND :end_date"
        init_where = ""
        init_params: dict[str, Any] = {}
        if source:
            status_where += " AND source = :source"
            init_where = "WHERE source = :source"
            init_params["source"] = source

        with engine.connect() as conn:
            try:
                item_rows = conn.execute(text(item_sql), params).fetchall()
            except Exception as exc:
                if "unknown column" not in str(exc or "").lower():
                    raise
                item_rows = conn.execute(text(legacy_item_sql), params).fetchall()
            item_rows = _dedupe_source_item_rows(list(item_rows))

            status_rows = conn.execute(
                text(
                    "SELECT source, interface_key, "
                    "SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS success_count, "
                    "SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS error_count, "
                    "SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending_count, "
                    "SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running_count, "
                    "SUM(CASE WHEN status = 'partial' THEN 1 ELSE 0 END) AS partial_count, "
                    "COUNT(*) AS total_count, MAX(sync_date) AS latest_sync_date "
                    "FROM data_sync_status "
                    f"{status_where} "
                    "GROUP BY source, interface_key"
                ),
                params,
            ).fetchall()
            init_rows = conn.execute(
                text(
                    "SELECT source, interface_key, initialized_from, initialized_to "
                    "FROM sync_status_init "
                    f"{init_where}"
                ),
                init_params,
            ).fetchall()

        status_map = {
            (row[0], row[1]): {
                "success": int(row[2] or 0),
                "error": int(row[3] or 0),
                "pending": int(row[4] or 0),
                "running": int(row[5] or 0),
                "partial": int(row[6] or 0),
                "total": int(row[7] or 0),
                "latest_sync_date": row[8],
            }
            for row in status_rows
        }
        init_map = {
            (row[0], row[1]): {
                "initialized_from": row[2],
                "initialized_to": row[3],
            }
            for row in init_rows
        }

        registry = build_default_registry()
        source_configs = load_source_config_map(source)

        items: list[dict[str, Any]] = []
        missing_items = 0
        repairable_items = 0
        unsupported_items = 0

        for row in item_rows:
            source_key = str(row[0])
            item_key = str(row[2])
            item_meta = {
                "source": source_key,
                "item_key": item_key,
                "api_name": row[5] if len(row) > 5 else None,
                "permission_points": row[6] if len(row) > 6 else None,
                "requires_permission": row[7] if len(row) > 7 else None,
            }
            if not is_item_sync_supported(registry, item_meta, source_configs=source_configs):
                unsupported_items += 1
                continue

            iface = registry.get_interface(source_key, item_key)
            if iface is None:
                unsupported_items += 1
                continue

            supports_backfill = True
            method = getattr(iface, "supports_backfill", None)
            if callable(method):
                supports_backfill = bool(method())

            counts = status_map.get((source_key, item_key), {})
            total_sync_dates = int(counts.get("total", 0) or 0)
            expected_sync_dates = expected_trade_days if supports_backfill else 1
            missing_sync_dates = max(expected_sync_dates - total_sync_dates, 0)
            latest_sync_date = counts.get("latest_sync_date")
            init_state = init_map.get((source_key, item_key), {})

            if missing_sync_dates > 0:
                missing_items += 1
            if supports_backfill:
                repairable_items += 1

            items.append(
                {
                    "source": source_key,
                    "source_name": row[1] or source_key,
                    "item_key": item_key,
                    "item_name": row[3] or item_key,
                    "sync_priority": int(row[4] or 0),
                    "api_name": row[5],
                    "supports_backfill": supports_backfill,
                    "expected_sync_dates": expected_sync_dates,
                    "total_sync_dates": total_sync_dates,
                    "missing_sync_dates": missing_sync_dates,
                    "latest_sync_date": latest_sync_date.isoformat()
                    if hasattr(latest_sync_date, "isoformat")
                    else str(latest_sync_date)
                    if latest_sync_date is not None
                    else None,
                    "initialized_from": init_state.get("initialized_from").isoformat()
                    if hasattr(init_state.get("initialized_from"), "isoformat")
                    else None,
                    "initialized_to": init_state.get("initialized_to").isoformat()
                    if hasattr(init_state.get("initialized_to"), "isoformat")
                    else None,
                    "counts": {
                        "success": int(counts.get("success", 0) or 0),
                        "error": int(counts.get("error", 0) or 0),
                        "pending": int(counts.get("pending", 0) or 0),
                        "running": int(counts.get("running", 0) or 0),
                        "partial": int(counts.get("partial", 0) or 0),
                    },
                }
            )

        return {
            "window_start": start_date.isoformat(),
            "window_end": end_date.isoformat(),
            "expected_trade_days": expected_trade_days,
            "items": items,
            "summary": {
                "items": len(items),
                "missing_items": missing_items,
                "repairable_items": repairable_items,
                "unsupported_items": unsupported_items,
            },
        }

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
