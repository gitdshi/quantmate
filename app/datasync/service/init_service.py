"""Environment-aware initialization service for dynamic data sync state."""

from __future__ import annotations

import logging
import os
from datetime import date, timedelta

from sqlalchemy import text

from app.datasync.registry import DataSourceRegistry
from app.datasync.service.sync_init_service import ensure_sync_status_init_table, reconcile_enabled_sync_status
from app.datasync.table_manager import ensure_table
from app.domains.extdata.dao.data_sync_status_dao import ensure_backfill_lock_table, ensure_tables
from app.infrastructure.db.connections import get_quantmate_engine

logger = logging.getLogger(__name__)

ENV_LOOKBACK = {
    "dev": 365,
    "development": 365,
    "staging": 365 * 5,
    "prod": 365 * 30,
    "production": 365 * 30,
}


def _get_env() -> str:
    return os.getenv("APP_ENV", os.getenv("ENVIRONMENT", "dev")).lower()


def _lookback_days() -> int:
    env = _get_env()
    return ENV_LOOKBACK.get(env, 365)


def initialize(registry: DataSourceRegistry, run_backfill: bool = False) -> dict:
    """Run initialization sequence.

    1. Ensure sync status support tables exist
    2. Ensure data_source_configs and data_source_items have seed data
    3. Create tables for all enabled interfaces
    4. Reconcile pending sync status records for enabled interfaces
    5. Optionally run backfill
    """
    logger.info("=== DataSync Initialization (env=%s) ===", _get_env())

    engine = get_quantmate_engine()

    ensure_tables()
    ensure_backfill_lock_table()
    ensure_sync_status_init_table()

    _seed_configs(engine, registry)
    _seed_items(engine, registry)
    tables_created = _ensure_tables(engine, registry)
    pending_result = _reconcile_pending_records(registry)

    result = {
        "env": _get_env(),
        "tables_created": tables_created,
        "pending_records": pending_result["pending_records"],
        "items_reconciled": pending_result["items_reconciled"],
        "skipped_unsupported": pending_result["skipped_unsupported"],
    }

    if run_backfill:
        from app.datasync.service.sync_engine import backfill_retry

        backfill_result = backfill_retry(registry, lookback_days=_lookback_days())
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


def _ensure_tables(engine, registry: DataSourceRegistry) -> int:
    """Create tables for enabled interfaces that don't have tables yet."""
    created = 0
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT dsi.source, dsi.item_key, dsi.target_database, dsi.target_table "
                "FROM data_source_items dsi "
                "JOIN data_source_configs dsc ON dsi.source = dsc.source_key "
                "WHERE dsi.enabled = 1 AND dsc.enabled = 1 AND dsi.table_created = 0"
            )
        ).fetchall()

    for source, item_key, target_db, target_tbl in rows:
        iface = registry.get_interface(source, item_key)
        if iface is None:
            continue
        try:
            if ensure_table(target_db, target_tbl, iface.get_ddl()):
                created += 1
        except Exception:
            logger.exception("Failed to create table %s.%s", target_db, target_tbl)

    return created


def _reconcile_pending_records(registry: DataSourceRegistry) -> dict[str, object]:
    lookback = _lookback_days()
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=lookback)

    result = reconcile_enabled_sync_status(
        registry,
        start_date=start_date,
        end_date=end_date,
    )
    if result["skipped_unsupported"]:
        logger.warning("Skipped unsupported enabled interfaces during init: %s", result["skipped_unsupported"])
    return result


def _generate_pending_records(engine, registry: DataSourceRegistry) -> int:
    """Generate pending sync_status records for the lookback window."""
    del engine
    return int(_reconcile_pending_records(registry)["pending_records"])