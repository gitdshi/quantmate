"""Calendar-aware sync status reconciliation for enabled interfaces."""

from __future__ import annotations

import logging
from datetime import date, timedelta

from sqlalchemy import text

from app.datasync.registry import DataSourceRegistry
from app.infrastructure.config import get_runtime_int
from app.infrastructure.db.connections import get_quantmate_engine

logger = logging.getLogger(__name__)

BATCH_SIZE = get_runtime_int(
    env_keys="SYNC_INIT_BATCH_SIZE",
    db_key="datasync.sync_init.batch_size",
    default=500,
)


def _resolve_default_sync_window(end_date: date | None = None) -> tuple[date, date]:
    resolved_end_date = end_date or (date.today() - timedelta(days=1))

    try:
        from app.datasync.service.init_service import get_coverage_window

        coverage_window = get_coverage_window(target_end_date=resolved_end_date)
        return coverage_window["start_date"], resolved_end_date
    except Exception:
        logger.exception("Failed to resolve env-aware sync init window; falling back to current end date")
        return resolved_end_date, resolved_end_date

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


def _get_initialized_bounds(source: str, item_key: str) -> tuple[date, date] | None:
    engine = get_quantmate_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT initialized_from, initialized_to "
                "FROM sync_status_init WHERE source = :s AND interface_key = :k LIMIT 1"
            ),
            {"s": source, "k": item_key},
        ).fetchone()
    if row is None:
        return None
    try:
        initialized_from = row[0]
        initialized_to = row[1]
    except Exception:
        return None

    if not isinstance(initialized_from, date) or not isinstance(initialized_to, date):
        return None

    return initialized_from, initialized_to


def _get_source_initialized_bounds(source: str) -> tuple[date, date] | None:
    engine = get_quantmate_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT MIN(ssi.initialized_from), MAX(ssi.initialized_to) "
                "FROM sync_status_init ssi "
                "JOIN data_source_items dsi "
                "  ON dsi.source = ssi.source AND dsi.item_key = ssi.interface_key "
                "JOIN data_source_configs dsc ON dsi.source = dsc.source_key "
                "WHERE ssi.source = :s AND dsi.enabled = 1 AND dsc.enabled = 1"
            ),
            {"s": source},
        ).fetchone()
    if row is None:
        return None

    try:
        initialized_from = row[0]
        initialized_to = row[1]
    except Exception:
        return None

    if not isinstance(initialized_from, date) or not isinstance(initialized_to, date):
        return None

    return initialized_from, initialized_to


def _resolve_reconcile_bounds(
    source: str,
    item_key: str,
    start_date: date | None,
    end_date: date | None,
    use_trade_calendar: bool,
) -> tuple[date, date, bool]:
    item_bounds = _get_initialized_bounds(source, item_key)
    source_bounds = None
    if item_bounds is None and (start_date is None or end_date is None):
        source_bounds = _get_source_initialized_bounds(source)

    default_start_date, default_end_date = _resolve_default_sync_window(end_date)

    resolved_start_date = (
        start_date
        or (item_bounds[0] if item_bounds is not None else None)
        or (source_bounds[0] if source_bounds is not None else None)
        or default_start_date
    )
    resolved_end_date = (
        end_date
        or (item_bounds[1] if item_bounds is not None else None)
        or (source_bounds[1] if source_bounds is not None else None)
        or default_end_date
    )

    if use_trade_calendar:
        if resolved_start_date > resolved_end_date:
            resolved_start_date = resolved_end_date
    else:
        resolved_start_date = resolved_end_date

    return resolved_start_date, resolved_end_date, item_bounds is None and source_bounds is not None


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


def _insert_pending_rows(source: str, item_key: str, trade_days: list[date]) -> int:
    if not trade_days:
        return 0

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

    return inserted


def _build_calendar_dates(start_date: date, end_date: date) -> list[date]:
    if start_date > end_date:
        return []

    total_days = (end_date - start_date).days + 1
    return [start_date + timedelta(days=offset) for offset in range(total_days)]


def initialize_sync_status(
    source: str,
    item_key: str,
    start_date: date | None = None,
    end_date: date | None = None,
    reconcile_missing: bool = True,
    use_trade_calendar: bool = True,
) -> int:
    """Seed pending ``data_sync_status`` rows for every trading day in range.

    Returns the number of rows inserted or reconciled.
    """
    ensure_sync_status_init_table()

    if not reconcile_missing and _already_initialized(source, item_key):
        logger.info("Sync status already initialized for %s/%s, skipping", source, item_key)
        return 0

    initialized_bounds = _get_initialized_bounds(source, item_key)

    default_start_date, default_end_date = _resolve_default_sync_window(end_date)
    if start_date is None:
        start_date = default_start_date
    if end_date is None:
        end_date = default_end_date

    from app.datasync.service.sync_engine import get_trade_calendar

    ranges: list[tuple[date, date]] = []
    if initialized_bounds is None or (reconcile_missing and not use_trade_calendar):
        ranges.append((start_date, end_date))
    else:
        initialized_from, initialized_to = initialized_bounds
        if start_date < initialized_from:
            ranges.append((start_date, min(end_date, initialized_from - timedelta(days=1))))
        if end_date > initialized_to:
            ranges.append((max(start_date, initialized_to + timedelta(days=1)), end_date))

    inserted = 0
    for range_start, range_end in ranges:
        sync_dates = get_trade_calendar(range_start, range_end) if use_trade_calendar else _build_calendar_dates(range_start, range_end)
        if not sync_dates:
            if use_trade_calendar:
                logger.warning("No trading days found between %s and %s", range_start, range_end)
            continue

        logger.info(
            "Initializing sync status for %s/%s: %d %s (%s -> %s)",
            source,
            item_key,
            len(sync_dates),
            "trading days" if use_trade_calendar else "calendar days",
            sync_dates[0],
            sync_dates[-1],
        )
        inserted += _insert_pending_rows(source, item_key, sync_dates)

    _record_init(source, item_key, start_date, end_date)
    logger.info("Reconciled %d sync status rows for %s/%s", inserted, source, item_key)
    return inserted


def reconcile_sync_status_item(
    registry: DataSourceRegistry,
    source: str,
    item_key: str,
    start_date: date | None = None,
    end_date: date | None = None,
) -> dict[str, object] | None:
    iface = registry.get_interface(source, item_key)
    if iface is None:
        return None

    use_trade_calendar = True
    method = getattr(iface, "supports_backfill", None)
    if callable(method) and not bool(method()):
        use_trade_calendar = False

    item_start_date, item_end_date, inherited_bounds = _resolve_reconcile_bounds(
        source,
        item_key,
        start_date,
        end_date,
        use_trade_calendar=use_trade_calendar,
    )

    pending_records = initialize_sync_status(
        source,
        item_key,
        start_date=item_start_date,
        end_date=item_end_date,
        reconcile_missing=True,
        use_trade_calendar=use_trade_calendar,
    )

    return {
        "source": source,
        "item_key": item_key,
        "start_date": item_start_date.isoformat(),
        "end_date": item_end_date.isoformat(),
        "pending_records": pending_records,
        "supports_backfill": use_trade_calendar,
        "inherited_bounds": inherited_bounds,
    }


def reconcile_enabled_sync_status(
    registry: DataSourceRegistry,
    source: str | None = None,
    item_key: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> dict[str, object]:
    """Ensure enabled, registry-backed interfaces have pending status rows."""
    from app.datasync.capabilities import is_item_sync_supported, load_source_config_map
    from app.datasync.service.init_service import _is_unknown_data_source_items_column_error

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
        "SELECT dsi.source, dsi.item_key, dsi.api_name, dsi.permission_points, dsi.requires_permission "
        "FROM data_source_items dsi "
        "JOIN data_source_configs dsc ON dsi.source = dsc.source_key "
        f"WHERE {' AND '.join(clauses)} "
        "ORDER BY dsi.source, dsi.sync_priority, dsi.item_key"
    )

    legacy_sql = (
        "SELECT dsi.source, dsi.item_key, dsi.item_key AS api_name, 0 AS permission_points, dsi.requires_permission "
        "FROM data_source_items dsi "
        "JOIN data_source_configs dsc ON dsi.source = dsc.source_key "
        f"WHERE {' AND '.join(clauses)} "
        "ORDER BY dsi.source, dsi.sync_priority, dsi.item_key"
    )

    with engine.connect() as conn:
        try:
            rows = conn.execute(text(sql), params).fetchall()
        except Exception as exc:
            if not _is_unknown_data_source_items_column_error(exc):
                raise
            logger.warning("Falling back to legacy data_source_items metadata query: %s", exc)
            rows = conn.execute(text(legacy_sql), params).fetchall()

    pending_records = 0
    items_reconciled = 0
    item_results: list[dict[str, object]] = []
    skipped_unsupported: list[dict[str, str]] = []
    source_configs = load_source_config_map(source)

    for row in rows:
        source_key = row[0]
        enabled_item_key = row[1]
        item = {
            "source": source_key,
            "item_key": enabled_item_key,
            "api_name": row[2] if len(row) > 2 else None,
            "permission_points": row[3] if len(row) > 3 else None,
            "requires_permission": row[4] if len(row) > 4 else None,
        }
        if not is_item_sync_supported(registry, item, source_configs=source_configs):
            skipped_unsupported.append({"source": source_key, "item_key": enabled_item_key})
            continue

        iface = registry.get_interface(source_key, enabled_item_key)
        if iface is None:
            skipped_unsupported.append({"source": source_key, "item_key": enabled_item_key})
            continue

        item_result = reconcile_sync_status_item(
            registry,
            source_key,
            enabled_item_key,
            start_date=start_date,
            end_date=end_date,
        )
        if item_result is None:
            skipped_unsupported.append({"source": source_key, "item_key": enabled_item_key})
            continue

        pending_records += int(item_result["pending_records"])
        items_reconciled += 1
        item_results.append(item_result)

    return {
        "pending_records": pending_records,
        "items_reconciled": items_reconciled,
        "item_results": item_results,
        "skipped_unsupported": skipped_unsupported,
    }
