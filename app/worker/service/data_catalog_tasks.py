"""data_catalog table maintenance tasks.

The ``qlib.data_catalog`` table describes every column available across the
tushare/akshare databases. RD-Agent consumes it to ground LLM prompts about
which data fields can be used in factor expressions. Without periodic refresh
the catalog drifts as DataSync ingests new tables/columns.

This module provides a background task that scans the live MySQL schemas via
``INFORMATION_SCHEMA`` and upserts the catalog rows.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

from sqlalchemy import text

logger = logging.getLogger(__name__)


def update_data_catalog() -> Dict[str, Any]:
    """Scan tushare/akshare schemas and upsert data_catalog entries.

    Runs inside an RQ worker. Returns a summary dict.
    """
    from app.infrastructure.db.connections import get_qlib_engine

    # Reuse the classification logic that lives next to the catalog table.
    from app.domains.factors.data_catalog import (
        _classify_column,
        _is_numeric_type,
        scan_database_columns,
    )

    qlib_engine = get_qlib_engine()

    entries: List[Dict[str, Any]] = []
    for source, db_name in (("tushare", "tushare"), ("akshare", "akshare")):
        try:
            cols = scan_database_columns(source, db_name)
        except Exception:
            logger.warning("[data-catalog] Failed to scan %s", source, exc_info=True)
            continue
        for col in cols:
            entries.append(
                {
                    "source": source,
                    "table_name": col["table_name"],
                    "column_name": col["column_name"],
                    "data_type": col["data_type"],
                    "category": col["category"],
                    "is_numeric": 1 if col["is_numeric"] else 0,
                }
            )

    if not entries:
        logger.warning("[data-catalog] No columns discovered; leaving catalog unchanged")
        return {"updated": 0, "status": "empty"}

    upsert_sql = text(
        "INSERT INTO data_catalog "
        "(source, table_name, column_name, data_type, category, is_numeric, scanned_at) "
        "VALUES (:source, :table_name, :column_name, :data_type, :category, :is_numeric, CURRENT_TIMESTAMP) "
        "ON DUPLICATE KEY UPDATE "
        "data_type = VALUES(data_type), category = VALUES(category), "
        "is_numeric = VALUES(is_numeric), scanned_at = CURRENT_TIMESTAMP"
    )

    inserted = 0
    batch_size = 500
    try:
        with qlib_engine.begin() as conn:
            for i in range(0, len(entries), batch_size):
                batch = entries[i : i + batch_size]
                conn.execute(upsert_sql, batch)
                inserted += len(batch)
    except Exception:
        logger.exception("[data-catalog] Failed to upsert catalog entries")
        return {"updated": 0, "status": "error"}

    logger.info("[data-catalog] Refreshed %d entries", inserted)
    return {"updated": inserted, "status": "completed"}


def enqueue_data_catalog_refresh() -> bool:
    """Enqueue the catalog refresh task on the low queue.

    Returns True if successfully enqueued.
    """
    try:
        from app.worker.service.config import get_queue

        queue = get_queue("low")
        queue.enqueue(update_data_catalog, job_timeout=300)
        return True
    except Exception:
        logger.warning("[data-catalog] Failed to enqueue refresh", exc_info=True)
        return False
