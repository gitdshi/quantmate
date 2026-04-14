"""Calendar-based sync status initialization.

When a data source item is enabled for the first time, this module seeds
``data_sync_status`` rows for every trading day in the range
[DEFAULT_START … yesterday] so the backfill engine can pick them up.
"""

from __future__ import annotations

import logging
import os
from datetime import date, timedelta

from sqlalchemy import text

from app.infrastructure.db.connections import get_quantmate_engine

logger = logging.getLogger(__name__)

DEFAULT_START_DATE = date(2020, 1, 1)
BATCH_SIZE = int(os.getenv("SYNC_INIT_BATCH_SIZE", "500"))


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
                "ON DUPLICATE KEY UPDATE initialized_to = VALUES(initialized_to), updated_at = CURRENT_TIMESTAMP"
            ),
            {"s": source, "k": item_key, "f": start, "t": end},
        )


def initialize_sync_status(
    source: str,
    item_key: str,
    start_date: date | None = None,
    end_date: date | None = None,
) -> int:
    """Seed pending ``data_sync_status`` rows for every trading day in range.

    Returns the number of rows inserted.
    """
    if _already_initialized(source, item_key):
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
        "Initializing sync status for %s/%s: %d trading days (%s → %s)",
        source, item_key, len(trade_days), trade_days[0], trade_days[-1],
    )

    engine = get_quantmate_engine()
    inserted = 0

    for i in range(0, len(trade_days), BATCH_SIZE):
        batch = trade_days[i : i + BATCH_SIZE]
        values = [
            {"sd": d, "src": source, "ik": item_key, "st": "pending"}
            for d in batch
        ]
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT IGNORE INTO data_sync_status "
                    "(sync_date, source, interface_key, status) "
                    "VALUES (:sd, :src, :ik, :st)"
                ),
                values,
            )
        inserted += len(batch)

    _record_init(source, item_key, start_date, end_date)
    logger.info("Initialized %d sync status rows for %s/%s", inserted, source, item_key)
    return inserted
