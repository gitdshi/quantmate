"""Environment-aware initialization service for dynamic data sync state."""

from __future__ import annotations

import logging
from datetime import date, timedelta

from sqlalchemy import text

from app.datasync.registry import DataSourceRegistry
from app.datasync.service.sync_init_service import ensure_sync_status_init_table, reconcile_enabled_sync_status
from app.datasync.table_manager import ensure_table
from app.domains.extdata.dao.data_sync_status_dao import ensure_backfill_lock_table, ensure_tables
from app.infrastructure.config import get_runtime_str
from app.infrastructure.db.connections import get_quantmate_engine

logger = logging.getLogger(__name__)

_ENABLED_ITEM_METADATA_SQL = (
    "SELECT dsi.source, dsi.item_key, dsi.api_name, dsi.permission_points, dsi.requires_permission "
    "FROM data_source_items dsi "
    "JOIN data_source_configs dsc ON dsi.source = dsc.source_key "
    "WHERE dsi.enabled = 1 AND dsc.enabled = 1 "
    "ORDER BY dsi.source, dsi.sync_priority, dsi.item_key"
)

_ENABLED_ITEM_METADATA_LEGACY_SQL = (
    "SELECT dsi.source, dsi.item_key, dsi.item_key AS api_name, 0 AS permission_points, dsi.requires_permission "
    "FROM data_source_items dsi "
    "JOIN data_source_configs dsc ON dsi.source = dsc.source_key "
    "WHERE dsi.enabled = 1 AND dsc.enabled = 1 "
    "ORDER BY dsi.source, dsi.sync_priority, dsi.item_key"
)

_TUSHARE_ITEM_METADATA_SQL = (
    "SELECT source, item_key, enabled, api_name, permission_points, requires_permission "
    "FROM data_source_items "
    "WHERE source = 'tushare' "
    "ORDER BY sync_priority, item_key"
)

_TUSHARE_ITEM_METADATA_LEGACY_SQL = (
    "SELECT source, item_key, enabled, item_key AS api_name, 0 AS permission_points, requires_permission "
    "FROM data_source_items "
    "WHERE source = 'tushare' "
    "ORDER BY sync_priority, item_key"
)

DEFAULT_ENV_WINDOW_YEARS = {
    "dev": 1,
    "development": 1,
    "staging": 10,
    "prod": 20,
    "production": 20,
}

ENV_ALIASES = {
    "development": "dev",
    "production": "prod",
}


def _normalize_env_name(env: str | None) -> str:
    resolved = (env or "dev").strip().lower()
    return ENV_ALIASES.get(resolved, resolved)


def _get_env() -> str:
    return _normalize_env_name(
        get_runtime_str(
            env_keys=("APP_ENV", "ENVIRONMENT"),
            db_key="app.environment",
            default="dev",
        )
    )


def _get_env_window_years(env: str | None = None) -> int:
    resolved_env = _normalize_env_name(env or _get_env())
    return DEFAULT_ENV_WINDOW_YEARS.get(resolved_env, DEFAULT_ENV_WINDOW_YEARS["dev"])


def _get_configured_sync_start_date(reference_date: date | None = None) -> date | None:
    raw_value = get_runtime_str(
        env_keys="SYNC_INIT_DEFAULT_START_DATE",
        db_key="datasync.sync_init.default_start_date",
        default="",
    ).strip()
    if not raw_value:
        return None

    try:
        return date.fromisoformat(raw_value)
    except ValueError:
        logger.warning("Ignoring invalid SYNC_INIT_DEFAULT_START_DATE=%r", raw_value)
        return None


def _get_env_floor_start_date(reference_date: date | None = None, env: str | None = None) -> date:
    resolved_reference = reference_date or date.today()
    window_years = _get_env_window_years(env)
    return resolved_reference - timedelta(days=365 * window_years)


def _is_unknown_data_source_items_column_error(exc: Exception) -> bool:
    return "unknown column" in str(exc or "").lower()


def _execute_data_source_item_metadata_query(conn, primary_sql: str, legacy_sql: str):
    try:
        return conn.execute(text(primary_sql)).fetchall()
    except Exception as exc:
        if not _is_unknown_data_source_items_column_error(exc):
            raise
        logger.warning("Falling back to legacy data_source_items metadata query: %s", exc)
        return conn.execute(text(legacy_sql)).fetchall()


def _fetch_enabled_item_metadata_rows(conn):
    return _execute_data_source_item_metadata_query(
        conn,
        _ENABLED_ITEM_METADATA_SQL,
        _ENABLED_ITEM_METADATA_LEGACY_SQL,
    )


def _fetch_tushare_item_metadata_rows(conn):
    return _execute_data_source_item_metadata_query(
        conn,
        _TUSHARE_ITEM_METADATA_SQL,
        _TUSHARE_ITEM_METADATA_LEGACY_SQL,
    )


def get_coverage_window(target_end_date: date | None = None) -> dict[str, object]:
    today = date.today()
    end_date = target_end_date or (today - timedelta(days=1))
    env = _get_env()
    reference_date = today
    configured_start_date = _get_configured_sync_start_date(reference_date)
    env_floor_start_date = _get_env_floor_start_date(reference_date, env)
    start_date = configured_start_date or env_floor_start_date
    if start_date > end_date:
        start_date = end_date
    return {
        "env": env,
        "window_years": _get_env_window_years(env),
        "configured_start_date": configured_start_date,
        "env_floor_start_date": env_floor_start_date,
        "start_date": start_date,
        "end_date": end_date,
    }


def _get_sync_status_coverage_state() -> dict[str, object]:
    from app.datasync.capabilities import is_item_sync_supported, load_source_config_map
    from app.datasync.registry import build_default_registry
    from app.domains.extdata.dao.data_sync_status_dao import get_cached_trade_dates

    coverage_window = get_coverage_window()
    start_date = coverage_window["start_date"]
    end_date = coverage_window["end_date"]

    registry = build_default_registry()
    engine = get_quantmate_engine()
    with engine.connect() as conn:
        enabled_rows = _fetch_enabled_item_metadata_rows(conn)
        init_rows = conn.execute(
            text("SELECT source, interface_key, initialized_from, initialized_to FROM sync_status_init")
        ).fetchall()
        window_counts = conn.execute(
            text(
                "SELECT source, interface_key, COUNT(*) AS row_count "
                "FROM data_sync_status "
                "WHERE sync_date BETWEEN :start_date AND :end_date "
                "GROUP BY source, interface_key"
            ),
            {"start_date": start_date, "end_date": end_date},
        ).fetchall()
        latest_counts = conn.execute(
            text(
                "SELECT source, interface_key, COUNT(*) AS row_count "
                "FROM data_sync_status "
                "WHERE sync_date = :end_date "
                "GROUP BY source, interface_key"
            ),
            {"end_date": end_date},
        ).fetchall()

    try:
        trade_days = get_cached_trade_dates(start_date, end_date)
    except Exception:
        logger.exception("Failed to inspect cached trade days for sync status coverage %s -> %s", start_date, end_date)
        trade_days = []

    init_map = {(row[0], row[1]): (row[2], row[3]) for row in init_rows}
    window_count_map = {(row[0], row[1]): int(row[2] or 0) for row in window_counts}
    latest_count_map = {(row[0], row[1]): int(row[2] or 0) for row in latest_counts}

    missing_items: list[dict[str, str]] = []
    incomplete_items: list[dict[str, object]] = []
    unsupported_items: list[dict[str, str]] = []
    enabled_sync_items = 0
    source_configs = load_source_config_map()

    expected_trade_days = len(trade_days)

    for row in enabled_rows:
        source = row[0]
        item_key = row[1]
        item = {
            "source": source,
            "item_key": item_key,
            "api_name": row[2] if len(row) > 2 else None,
            "permission_points": row[3] if len(row) > 3 else None,
            "requires_permission": row[4] if len(row) > 4 else None,
        }
        if not is_item_sync_supported(registry, item, source_configs=source_configs):
            unsupported_items.append({"source": source, "item_key": item_key})
            continue

        iface = registry.get_interface(source, item_key)
        if iface is None:
            unsupported_items.append({"source": source, "item_key": item_key})
            continue

        enabled_sync_items += 1
        supports_backfill = True
        method = getattr(iface, "supports_backfill", None)
        if callable(method):
            supports_backfill = bool(method())

        init_bounds = init_map.get((source, item_key))
        if init_bounds is None:
            missing_items.append({"source": source, "item_key": item_key})
            continue

        initialized_from, initialized_to = init_bounds
        if supports_backfill:
            window_count = window_count_map.get((source, item_key), 0)
            if initialized_from > start_date or initialized_to < end_date or window_count != expected_trade_days:
                incomplete_items.append(
                    {
                        "source": source,
                        "item_key": item_key,
                        "initialized_from": initialized_from.isoformat(),
                        "initialized_to": initialized_to.isoformat(),
                        "expected_rows": expected_trade_days,
                        "actual_rows": window_count,
                    }
                )
            continue

        latest_count = latest_count_map.get((source, item_key), 0)
        if initialized_to < end_date or latest_count < 1:
            incomplete_items.append(
                {
                    "source": source,
                    "item_key": item_key,
                    "initialized_from": initialized_from.isoformat(),
                    "initialized_to": initialized_to.isoformat(),
                    "expected_rows": 1,
                    "actual_rows": latest_count,
                }
            )

    return {
        "window_start": start_date,
        "window_end": end_date,
        "trade_days_in_window": expected_trade_days,
        "enabled_sync_items": enabled_sync_items,
        "missing_items": missing_items,
        "incomplete_items": incomplete_items,
        "unsupported_items": unsupported_items,
    }


def get_initialization_state() -> dict[str, bool]:
    from app.datasync.cli.init_market_data import ensure_init_progress_table

    ensure_init_progress_table()
    ensure_sync_status_init_table()

    engine = get_quantmate_engine()
    with engine.connect() as conn:
        bootstrap_completed = conn.execute(
            text(
                "SELECT 1 FROM init_progress "
                "WHERE id = 1 AND phase = 'finished' AND status = 'completed' "
                "LIMIT 1"
            )
        ).fetchone() is not None

    coverage_state = _get_sync_status_coverage_state()
    sync_status_initialized = not coverage_state["missing_items"] and not coverage_state["incomplete_items"]

    return {
        "bootstrap_completed": bootstrap_completed,
        "sync_status_initialized": sync_status_initialized,
        "needs_initialization": not (bootstrap_completed and sync_status_initialized),
        "sync_status_window_start": coverage_state["window_start"],
        "sync_status_window_end": coverage_state["window_end"],
        "trade_days_in_window": coverage_state["trade_days_in_window"],
        "enabled_sync_items": coverage_state["enabled_sync_items"],
        "sync_status_missing_items": coverage_state["missing_items"],
        "sync_status_incomplete_items": coverage_state["incomplete_items"],
        "sync_status_unsupported_enabled_items": coverage_state["unsupported_items"],
    }


def needs_initialization() -> bool:
    return bool(get_initialization_state()["needs_initialization"])


def _sync_registry_state(engine, registry: DataSourceRegistry) -> dict[str, int]:
    _seed_configs(engine, registry)
    _seed_items(engine, registry)
    return {
        "bootstrap_item_enablement_updates": _sync_bootstrap_item_enablement(engine, registry),
        "items_normalized": _normalize_item_targets(engine),
        "tables_created": _ensure_tables(engine, registry),
    }


def _is_bootstrap_item_enablement_pending(engine) -> bool:
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT 1 FROM init_progress "
                    "WHERE id = 1 AND phase = 'finished' AND status = 'completed' "
                    "LIMIT 1"
                )
            ).fetchone()
    except Exception as exc:
        logger.warning("Unable to inspect init_progress for bootstrap enablement sync: %s", exc)
        return True
    return row is None


def _sync_bootstrap_item_enablement(engine, registry: DataSourceRegistry) -> int:
    """Apply initial Tushare item enablement from runtime capability config.

    During the first bootstrap we normalize ``data_source_items.enabled`` to the
    interfaces the current environment can actually access. Once bootstrap is
    complete, runtime toggles are left untouched.
    """
    from app.datasync.capabilities import is_item_sync_supported, load_source_config_map

    if not _is_bootstrap_item_enablement_pending(engine):
        return 0

    try:
        with engine.connect() as conn:
            rows = _fetch_tushare_item_metadata_rows(conn)
    except Exception:
        logger.exception("Failed to load Tushare items for bootstrap enablement sync")
        return 0

    if not rows:
        return 0

    source_configs = load_source_config_map()
    updates: list[dict[str, object]] = []

    for row in rows:
        item = {
            "source": row[0],
            "item_key": row[1],
            "api_name": row[3],
            "permission_points": row[4],
            "requires_permission": row[5],
        }
        desired_enabled = int(is_item_sync_supported(registry, item, source_configs=source_configs))
        current_enabled = int(row[2] or 0)
        if desired_enabled == current_enabled:
            continue
        updates.append(
            {
                "source": row[0],
                "item_key": row[1],
                "enabled": desired_enabled,
            }
        )

    if not updates:
        return 0

    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE data_source_items "
                "SET enabled = :enabled "
                "WHERE source = :source AND item_key = :item_key"
            ),
            updates,
        )

    logger.info("Normalized bootstrap Tushare enablement for %d items", len(updates))
    return len(updates)


def _ensure_trade_calendar_window(start_date: date, end_date: date) -> tuple[list[date], bool]:
    from app.datasync.service.data_sync_daemon import refresh_trade_calendar
    from app.datasync.service.sync_engine import get_trade_calendar
    from app.domains.extdata.dao.data_sync_status_dao import get_cached_trade_dates

    cached_dates: list[date] = []
    try:
        cached_dates = get_cached_trade_dates(start_date, end_date)
    except Exception:
        logger.exception("Failed to inspect cached trade calendar for %s -> %s", start_date, end_date)

    refreshed = False
    if not cached_dates or cached_dates[0] > start_date or cached_dates[-1] < end_date:
        try:
            refresh_trade_calendar()
            refreshed = True
        except Exception:
            logger.exception("Failed to refresh trade calendar before reconciliation")

    trade_dates = get_trade_calendar(start_date, end_date)
    return trade_dates, refreshed


def reconcile_runtime_state(
    registry: DataSourceRegistry,
    target_end_date: date | None = None,
) -> dict[str, object]:
    coverage_window = get_coverage_window(target_end_date)
    start_date = coverage_window["start_date"]
    end_date = coverage_window["end_date"]

    logger.info(
        "=== DataSync Runtime Reconcile (env=%s start=%s end=%s window_years=%s configured_start=%s env_floor_start=%s) ===",
        coverage_window["env"],
        start_date,
        end_date,
        coverage_window["window_years"],
        coverage_window["configured_start_date"],
        coverage_window["env_floor_start_date"],
    )

    engine = get_quantmate_engine()
    ensure_tables()
    ensure_backfill_lock_table()
    ensure_sync_status_init_table()

    registry_state = _sync_registry_state(engine, registry)
    trade_dates, trade_calendar_refreshed = _ensure_trade_calendar_window(start_date, end_date)
    pending_result = _reconcile_pending_records(registry, start_date=start_date, end_date=end_date)

    result = {
        "env": coverage_window["env"],
        "window_years": coverage_window["window_years"],
        "configured_start_date": coverage_window["configured_start_date"],
        "env_floor_start_date": coverage_window["env_floor_start_date"],
        "start_date": start_date,
        "end_date": end_date,
        "trade_calendar_days": len(trade_dates),
        "trade_calendar_refreshed": trade_calendar_refreshed,
        **registry_state,
        "pending_records": pending_result["pending_records"],
        "items_reconciled": pending_result["items_reconciled"],
        "skipped_unsupported": pending_result["skipped_unsupported"],
    }
    logger.info("=== Runtime reconcile complete: %s ===", result)
    return result


def initialize(registry: DataSourceRegistry, run_backfill: bool = False) -> dict:
    """Run initialization sequence.

    1. Ensure sync status support tables exist
    2. Ensure data_source_configs and data_source_items have seed data
    3. Align first-boot Tushare enablement with runtime capability config
    4. Create bootstrap tables and let premium Tushare tables be created on demand
    4. Reconcile pending sync status records for enabled interfaces
    5. Optionally run backfill
    """
    coverage_window = get_coverage_window()
    logger.info(
        "=== DataSync Initialization (env=%s start=%s end=%s window_years=%s configured_start=%s env_floor_start=%s) ===",
        coverage_window["env"],
        coverage_window["start_date"],
        coverage_window["end_date"],
        coverage_window["window_years"],
        coverage_window["configured_start_date"],
        coverage_window["env_floor_start_date"],
    )

    engine = get_quantmate_engine()

    ensure_tables()
    ensure_backfill_lock_table()
    ensure_sync_status_init_table()

    registry_state = _sync_registry_state(engine, registry)
    trade_dates, trade_calendar_refreshed = _ensure_trade_calendar_window(
        coverage_window["start_date"],
        coverage_window["end_date"],
    )
    pending_result = _reconcile_pending_records(
        registry,
        start_date=coverage_window["start_date"],
        end_date=coverage_window["end_date"],
    )

    result = {
        "env": coverage_window["env"],
        "window_years": coverage_window["window_years"],
        "configured_start_date": coverage_window["configured_start_date"],
        "env_floor_start_date": coverage_window["env_floor_start_date"],
        "start_date": coverage_window["start_date"],
        "end_date": coverage_window["end_date"],
        "trade_calendar_days": len(trade_dates),
        "trade_calendar_refreshed": trade_calendar_refreshed,
        **registry_state,
        "pending_records": pending_result["pending_records"],
        "items_reconciled": pending_result["items_reconciled"],
        "skipped_unsupported": pending_result["skipped_unsupported"],
    }

    if run_backfill:
        from app.datasync.service.sync_engine import backfill_retry

        backfill_result = backfill_retry(registry)
        result["backfill"] = backfill_result

    logger.info("=== Initialization complete: %s ===", result)
    return result


def _seed_configs(engine, registry: DataSourceRegistry) -> None:
    """Insert data_source_configs for registered sources if not yet present."""
    with engine.begin() as conn:
        for source in registry.all_sources():
            conn.execute(
                text(
                    "INSERT IGNORE INTO data_source_configs (source_key, display_name, enabled, requires_token) "
                    "VALUES (:key, :name, 1, :token)"
                ),
                {"key": source.source_key, "name": source.display_name, "token": int(source.requires_token)},
            )


def _seed_items(engine, registry: DataSourceRegistry) -> None:
    """Insert data_source_items for registered interfaces if not yet present."""
    with engine.begin() as conn:
        for iface in registry.all_interfaces():
            info = iface.info
            conn.execute(
                text(
                    "INSERT IGNORE INTO data_source_items "
                    "(source, item_key, item_name, enabled, description, requires_permission, "
                    "target_database, target_table, sync_priority) "
                    "VALUES (:src, :key, :name, :en, :desc, :perm, :db, :tbl, :pri)"
                ),
                {
                    "src": info.source_key,
                    "key": info.interface_key,
                    "name": info.display_name,
                    "en": int(info.enabled_by_default),
                    "desc": info.description,
                    "perm": info.requires_permission,
                    "db": info.target_database,
                    "tbl": info.target_table,
                    "pri": info.sync_priority,
                },
            )


def _normalize_item_targets(engine) -> int:
    """Keep `target_database` aligned with `source` for all catalog rows."""
    with engine.begin() as conn:
        result = conn.execute(
            text(
                "UPDATE data_source_items "
                "SET target_database = source "
                "WHERE target_database <> source"
            )
        )
    normalized = int(result.rowcount or 0)
    if normalized:
        logger.info("Normalized %d data_source_items rows to target_database = source", normalized)
    return normalized


def _ensure_tables(engine, registry: DataSourceRegistry) -> int:
    """Ensure bootstrap tables during initialization.

    High-permission Tushare tables are created on demand when an interface is
    enabled or when sync/backfill/init executes that specific interface.
    """
    from app.datasync.sources.tushare.ddl import should_bootstrap_table

    created = 0
    seen_tables: set[tuple[str, str]] = set()

    for iface in registry.all_interfaces():
        info = iface.info
        target_db = info.target_database
        target_tbl = info.target_table
        if not target_db or not target_tbl:
            continue
        if info.source_key == "tushare" and not should_bootstrap_table(target_tbl):
            continue
        table_key = (target_db, target_tbl)
        if table_key in seen_tables:
            continue
        seen_tables.add(table_key)
        try:
            if ensure_table(target_db, target_tbl, iface.get_ddl()):
                created += 1
        except Exception:
            logger.exception("Failed to create table %s.%s", target_db, target_tbl)

    return created


def _reconcile_pending_records(
    registry: DataSourceRegistry,
    start_date: date | None = None,
    end_date: date | None = None,
) -> dict[str, object]:
    if start_date is None or end_date is None:
        coverage_window = get_coverage_window()
        start_date = coverage_window["start_date"]
        end_date = coverage_window["end_date"]

    result = reconcile_enabled_sync_status(
        registry,
        start_date=start_date,
        end_date=end_date,
    )
    if result["skipped_unsupported"]:
        logger.warning("Skipped unsupported enabled interfaces during init: %s", result["skipped_unsupported"])
    return result


def _generate_pending_records(engine, registry: DataSourceRegistry) -> int:
    """Generate pending sync_status records for the configured coverage window."""
    del engine
    return int(_reconcile_pending_records(registry)["pending_records"])