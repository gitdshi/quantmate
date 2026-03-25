"""Environment-aware initialization service.

Handles first-time data initialization:
- Seeds data_source_configs and data_source_items if empty
- Creates tables for enabled interfaces
- Generates pending sync status records for the initialization window
- Optionally runs initial backfill

Environment windows:
- dev: 1 year
- staging: 5 years
- prod: max available
"""

from __future__ import annotations

import logging
import os
from datetime import date, timedelta

from sqlalchemy import text

from app.datasync.base import SyncStatus
from app.datasync.registry import DataSourceRegistry
from app.datasync.table_manager import ensure_table
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

    1. Ensure data_source_configs and data_source_items have seed data
    2. Create tables for all enabled interfaces
    3. Generate pending sync status records
    4. Optionally run backfill
    """
    logger.info("=== DataSync Initialization (env=%s) ===", _get_env())

    engine = get_quantmate_engine()

    # Step 1: Seed configs if empty
    _seed_configs(engine, registry)

    # Step 2: Seed items if empty
    _seed_items(engine, registry)

    # Step 3: Create tables for enabled interfaces
    tables_created = _ensure_tables(engine, registry)

    # Step 4: Generate pending status records
    pending_count = _generate_pending_records(engine, registry)

    result = {
        "env": _get_env(),
        "tables_created": tables_created,
        "pending_records": pending_count,
    }

    # Step 5: Optional backfill
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
                "SELECT source, item_key, target_database, target_table "
                "FROM data_source_items WHERE enabled = 1 AND table_created = 0"
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


def _generate_pending_records(engine, registry: DataSourceRegistry) -> int:
    """Generate pending sync_status records for the lookback window."""
    lookback = _lookback_days()
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=lookback)

    # Get trade dates
    try:
        from app.datasync.service.sync_engine import get_trade_calendar

        trade_dates = get_trade_calendar(start_date, end_date)
    except Exception:
        logger.warning("Could not get trade calendar, using weekdays")
        trade_dates = []
        cur = start_date
        while cur <= end_date:
            if cur.weekday() < 5:
                trade_dates.append(cur)
            cur += timedelta(days=1)

    # Get enabled items
    with engine.connect() as conn:
        items = conn.execute(text("SELECT source, item_key FROM data_source_items WHERE enabled = 1")).fetchall()

    if not items or not trade_dates:
        return 0

    # Bulk insert pending records (skip existing)
    count = 0
    raw_conn = engine.raw_connection()
    try:
        cursor = raw_conn.cursor()
        sql = "INSERT IGNORE INTO data_sync_status (sync_date, source, interface_key, status) VALUES (%s, %s, %s, %s)"
        batch = []
        for td in trade_dates:
            for source, item_key in items:
                batch.append((td.strftime("%Y-%m-%d"), source, item_key, SyncStatus.PENDING.value))
                if len(batch) >= 5000:
                    cursor.executemany(sql, batch)
                    raw_conn.commit()
                    count += cursor.rowcount
                    batch = []
        if batch:
            cursor.executemany(sql, batch)
            raw_conn.commit()
            count += cursor.rowcount
    finally:
        try:
            cursor.close()
        except Exception:
            pass
        try:
            raw_conn.close()
        except Exception:
            pass

    logger.info("Generated %d pending sync status records (%d dates x %d items)", count, len(trade_dates), len(items))
    return count
