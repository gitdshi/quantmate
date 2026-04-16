"""Calendar-based sync status reconciliation for enabled interfaces."""

from __future__ import annotations

import logging
import os
from datetime import date, timedelta

from sqlalchemy import text

from app.datasync.registry import DataSourceRegistry
from app.infrastructure.db.connections import get_quantmate_engine

logger = logging.getLogger(__name__)

DEFAULT_START_DATE = date(2020, 1, 1)
BATCH_SIZE = int(os.getenv("SYNC_INIT_BATCH_SIZE", "500"))

SYNC_STATUS_INIT_SQL = """
CREATE TABLE IF NOT EXISTS sync_status_init (
    source VARCHAR(50) NOT NULL,
    interface_key VARCHAR(100) NOT NULL,
    initialized_from DATE NOT NULL,
    initialized_to DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (source, interface_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Coverage window for sync status initialization';
"""


def ensure_sync_status_init_table() -> None:
    engine = get_quantmate_engine()
    with engine.begin() as conn:
        conn.execute(text(SYNC_STATUS_INIT_SQL))


def _already_initialized(source: str, item_key: str) -> bool:
    engine = get_quantmate_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT 1 FROM sync_status_init WHERE source = :s AND interface_key = :k LIMIT 1"),
            {"s": source, "k": item_key},
        ).fetchone()
        return row is not None


def _record_init(source: str, item_key: str, start: date, end: date) -> None:
    engine = get_quantmate_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO sync_status_init (source, interface_key, initialized_from, initialized_to) "
                "VALUES (:s, :k, :f, :t) "
                "ON DUPLICATE KEY UPDATE "
                "initialized_from = LEAST(initialized_from, VALUES(initialized_from)), "
                "initialized_to = GREATEST(initialized_to, VALUES(initialized_to)), "
                "updated_at = CURRENT_TIMESTAMP"
            ),
            {"s": source, "k": item_key, "f": start, "t": end},
        )


def initialize_sync_status(
    source: str,
    item_key: str,
    start_date: date | None = None,
    end_date: date | None = None,
    reconcile_missing: bool = True,
) -> int:
    """Seed pending ``data_sync_status`` rows for every trading day in range.

    Returns the number of rows inserted or reconciled.
    """
    ensure_sync_status_init_table()

    if not reconcile_missing and _already_initialized(source, item_key):
        logger.info("Sync status already initialized for %s/%s, skipping", source, item_key)
        return 0

    if start_date is None:
        start_date = DEFAULT_START_DATE
    if end_date is None:
        end_date = date.today() - timedelta(days=1)

    from app.datasync.service.sync_engine import get_trade_calendar

    trade_days = get_trade_calendar(start_date, end_date)
    if not trade_days:
        logger.warning("No trading days found between %s and %s", start_date, end_date)
        return 0

    logger.info(
        "Initializing sync status for %s/%s: %d trading days (%s -> %s)",
        source, item_key, len(trade_days), trade_days[0], trade_days[-1],
    )

    engine = get_quantmate_engine()
    inserted = 0

    for i in range(0, len(trade_days), BATCH_SIZE):
        batch = trade_days[i : i + BATCH_SIZE]
        values = [{"sd": d, "src": source, "ik": item_key, "st": "pending"} for d in batch]
        with engine.begin() as conn:
            result = conn.execute(
                text(
                    "INSERT IGNORE INTO data_sync_status "
                    "(sync_date, source, interface_key, status) "
                    "VALUES (:sd, :src, :ik, :st)"
                ),
                values,
            )
        rowcount = getattr(result, "rowcount", None)
        inserted += int(rowcount) if isinstance(rowcount, int) and rowcount >= 0 else len(batch)

    _record_init(source, item_key, start_date, end_date)
    logger.info("Reconciled %d sync status rows for %s/%s", inserted, source, item_key)
    return inserted


def reconcile_enabled_sync_status(
    registry: DataSourceRegistry,
    source: str | None = None,
    item_key: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> dict[str, object]:
    """Ensure enabled, registry-backed interfaces have pending status rows."""
    ensure_sync_status_init_table()

    engine = get_quantmate_engine()
    clauses = ["dsi.enabled = 1", "dsc.enabled = 1"]
    params: dict[str, object] = {}
    if source is not None:
        clauses.append("dsi.source = :source")
        params["source"] = source
    if item_key is not None:
        clauses.append("dsi.item_key = :item_key")
        params["item_key"] = item_key

    sql = (
        "SELECT dsi.source, dsi.item_key "
        "FROM data_source_items dsi "
        "JOIN data_source_configs dsc ON dsi.source = dsc.source_key "
        f"WHERE {' AND '.join(clauses)} "
        "ORDER BY dsi.source, dsi.sync_priority, dsi.item_key"
    )

    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).fetchall()

    pending_records = 0
    items_reconciled = 0
    skipped_unsupported: list[dict[str, str]] = []

    for row in rows:
        source_key = row[0]
        enabled_item_key = row[1]
        iface = registry.get_interface(source_key, enabled_item_key)
        if iface is None:
            skipped_unsupported.append({"source": source_key, "item_key": enabled_item_key})
            continue

        item_start_date = start_date
        item_end_date = end_date
        method = getattr(iface, "supports_backfill", None)
        if callable(method) and not bool(method()):
            item_end_date = end_date if end_date is not None else date.today() - timedelta(days=1)
            item_start_date = item_end_date

        pending_records += initialize_sync_status(
            source_key,
            enabled_item_key,
            start_date=item_start_date,
            end_date=item_end_date,
            reconcile_missing=True,
        )
        items_reconciled += 1

    return {
        "pending_records": pending_records,
        "items_reconciled": items_reconciled,
        "skipped_unsupported": skipped_unsupported,
    }
