#!/usr/bin/env python3
"""Initialize/rebuild QuantMate market bootstrap data after DB loss.

This script supports resumable initialization via `init_progress` checkpoints.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable
from functools import lru_cache
import logging
import sys
from datetime import date, datetime
from pathlib import Path

from sqlalchemy import text


ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.datasync.service.tushare_ingest import (
    TushareQuotaExceededError,
    ingest_stock_basic,
    ingest_stock_company_snapshot,
    ingest_new_share_by_date_range,
    ingest_all_daily,
    ingest_daily_by_trade_date_range,
    ingest_bak_daily_by_trade_dates,
    ingest_moneyflow_by_trade_dates,
    ingest_suspend_d_by_trade_dates,
    ingest_suspend_by_trade_dates,
    ingest_adj_factor_by_trade_dates,
    ingest_adj_factor_by_date_range,
    ingest_fina_indicator_by_date_range,
    ingest_income_by_date_range,
    ingest_balancesheet_by_date_range,
    ingest_cashflow_by_date_range,
    ingest_dividend_by_ann_date_range,
    ingest_dividend_by_date_range,
    ingest_top10_holders_marketwide_by_date_range,
    ingest_top10_holders_by_date_range,
    ingest_weekly_by_trade_dates,
    ingest_monthly_by_trade_dates,
)
from app.datasync.service.akshare_ingest import ingest_all_indexes
from app.datasync.service.vnpy_ingest import sync_all_to_vnpy
from app.infrastructure.config import get_runtime_int
from app.infrastructure.db.connections import get_mysql_server_engine, get_quantmate_engine

# Metrics integration for backfill lock status
try:
    from app.datasync.metrics import set_backfill_lock_status
    _has_metrics = True
except Exception:
    _has_metrics = False
    set_backfill_lock_status = None


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROGRESS_ID = 1
PHASES = [
    'schema',
    'stock_basic',
    'stock_company',
    'new_share',
    'daily',
    'weekly',
    'monthly',
    'indexes',
    'adj_factor',
    'dividend',
    'top10_holders',
    'bak_daily',
    'moneyflow',
    'suspend_d',
    'suspend',
    'fina_indicator',
    'income',
    'balancesheet',
    'cashflow',
    'vnpy',
    'sync_status',
    'finished',
]

_AUX_INIT_PHASES = {
    'adj_factor',
    'dividend',
    'top10_holders',
    'bak_daily',
    'moneyflow',
    'suspend_d',
    'suspend',
    'fina_indicator',
    'income',
    'balancesheet',
    'cashflow',
}

_INIT_STATUS_CONFIG = {
    'stock_basic': {
        'source': 'tushare',
        'item_key': 'stock_basic',
        'database': 'tushare',
        'table': 'stock_basic',
        'window': 'end',
    },
    'stock_company': {
        'source': 'tushare',
        'item_key': 'stock_company',
        'database': 'tushare',
        'table': 'stock_company',
        'window': 'end',
    },
    'new_share': {
        'source': 'tushare',
        'item_key': 'new_share',
        'database': 'tushare',
        'table': 'new_share',
        'date_column': 'ipo_date',
        'window': 'aux',
    },
    'daily': {
        'source': 'tushare',
        'item_key': 'stock_daily',
        'database': 'tushare',
        'table': 'stock_daily',
        'date_column': 'trade_date',
        'window': 'daily',
    },
    'weekly': {
        'source': 'tushare',
        'item_key': 'stock_weekly',
        'database': 'tushare',
        'table': 'stock_weekly',
        'date_column': 'trade_date',
        'window': 'daily',
    },
    'monthly': {
        'source': 'tushare',
        'item_key': 'stock_monthly',
        'database': 'tushare',
        'table': 'stock_monthly',
        'date_column': 'trade_date',
        'window': 'daily',
    },
    'indexes': {
        'source': 'akshare',
        'item_key': 'index_daily',
        'database': 'akshare',
        'table': 'index_daily',
        'date_column': 'trade_date',
        'window': 'daily',
    },
    'adj_factor': {
        'source': 'tushare',
        'item_key': 'adj_factor',
        'database': 'tushare',
        'table': 'adj_factor',
        'date_column': 'trade_date',
        'window': 'aux',
    },
    'dividend': {
        'source': 'tushare',
        'item_key': 'dividend',
        'database': 'tushare',
        'table': 'dividend',
        'date_column': 'ann_date',
        'window': 'aux',
    },
    'top10_holders': {
        'source': 'tushare',
        'item_key': 'top10_holders',
        'database': 'tushare',
        'table': 'top10_holders',
        'date_column': 'end_date',
        'window': 'aux',
    },
    'bak_daily': {
        'source': 'tushare',
        'item_key': 'bak_daily',
        'database': 'tushare',
        'table': 'bak_daily',
        'date_column': 'trade_date',
        'window': 'daily',
    },
    'moneyflow': {
        'source': 'tushare',
        'item_key': 'moneyflow',
        'database': 'tushare',
        'table': 'moneyflow',
        'date_column': 'trade_date',
        'window': 'daily',
    },
    'suspend_d': {
        'source': 'tushare',
        'item_key': 'suspend_d',
        'database': 'tushare',
        'table': 'suspend_d',
        'date_column': 'trade_date',
        'window': 'daily',
    },
    'suspend': {
        'source': 'tushare',
        'item_key': 'suspend',
        'database': 'tushare',
        'table': 'suspend',
        'date_column': 'suspend_date',
        'window': 'daily',
    },
    'fina_indicator': {
        'source': 'tushare',
        'item_key': 'fina_indicator',
        'database': 'tushare',
        'table': 'fina_indicator',
        'date_column': 'end_date',
        'window': 'aux',
    },
    'income': {
        'source': 'tushare',
        'item_key': 'income',
        'database': 'tushare',
        'table': 'income',
        'date_column': 'end_date',
        'window': 'aux',
    },
    'balancesheet': {
        'source': 'tushare',
        'item_key': 'balancesheet',
        'database': 'tushare',
        'table': 'balancesheet',
        'date_column': 'end_date',
        'window': 'aux',
    },
    'cashflow': {
        'source': 'tushare',
        'item_key': 'cashflow',
        'database': 'tushare',
        'table': 'cashflow',
        'date_column': 'end_date',
        'window': 'aux',
    },
}


def split_sql_statements(sql_text: str) -> list[str]:
    statements: list[str] = []
    buf: list[str] = []
    in_single = False
    in_double = False

    for ch in sql_text:
        if ch == "'" and not in_double:
            in_single = not in_single
            buf.append(ch)
            continue
        if ch == '"' and not in_single:
            in_double = not in_double
            buf.append(ch)
            continue
        if ch == ';' and not in_single and not in_double:
            stmt = ''.join(buf).strip()
            if stmt:
                statements.append(stmt)
            buf = []
            continue
        buf.append(ch)

    tail = ''.join(buf).strip()
    if tail:
        statements.append(tail)

    cleaned: list[str] = []
    for stmt in statements:
        lines = []
        for line in stmt.splitlines():
            s = line.strip()
            if s.startswith('--'):
                continue
            lines.append(line)
        normalized = '\n'.join(lines).strip()
        if normalized:
            cleaned.append(normalized)
    return cleaned


def get_server_engine():
    return get_mysql_server_engine()


def ensure_init_progress_table() -> None:
    engine = get_quantmate_engine()
    ddl = """
    CREATE TABLE IF NOT EXISTS init_progress (
        id TINYINT PRIMARY KEY,
        phase VARCHAR(64) NOT NULL,
        cursor_ts_code VARCHAR(32) NULL,
        cursor_date VARCHAR(16) NULL,
        status VARCHAR(16) NOT NULL,
        error TEXT NULL,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def load_progress() -> dict | None:
    engine = get_quantmate_engine()
    sql = text(
        "SELECT phase, cursor_ts_code, cursor_date, status, error, updated_at "
        "FROM init_progress WHERE id = :id"
    )
    try:
        with engine.connect() as conn:
            row = conn.execute(sql, {'id': PROGRESS_ID}).mappings().first()
        if _has_metrics and set_backfill_lock_status:
            set_backfill_lock_status(True)
        return dict(row) if row else None
    except Exception as e:
        if _has_metrics and set_backfill_lock_status:
            set_backfill_lock_status(False)
        logger.error("Failed to load progress: %s", e)
        raise


def save_progress(phase: str, status: str, cursor_ts_code: str | None = None, cursor_date: str | None = None, error: str | None = None) -> None:
    engine = get_quantmate_engine()
    upsert = text(
        """
        INSERT INTO init_progress (id, phase, cursor_ts_code, cursor_date, status, error)
        VALUES (:id, :phase, :cursor_ts_code, :cursor_date, :status, :error)
        ON DUPLICATE KEY UPDATE
            phase = VALUES(phase),
            cursor_ts_code = VALUES(cursor_ts_code),
            cursor_date = VALUES(cursor_date),
            status = VALUES(status),
            error = VALUES(error),
            updated_at = CURRENT_TIMESTAMP
        """
    )
    try:
        with engine.begin() as conn:
            conn.execute(
                upsert,
                {
                    'id': PROGRESS_ID,
                    'phase': phase,
                    'cursor_ts_code': cursor_ts_code,
                    'cursor_date': cursor_date,
                    'status': status,
                    'error': error,
                },
            )
        if _has_metrics and set_backfill_lock_status:
            set_backfill_lock_status(True)
    except Exception as e:
        if _has_metrics and set_backfill_lock_status:
            set_backfill_lock_status(False)
        logger.error("Failed to save progress: %s", e)
        raise


def reset_progress() -> None:
    engine = get_quantmate_engine()
    with engine.begin() as conn:
        conn.execute(text('DELETE FROM init_progress WHERE id = :id'), {'id': PROGRESS_ID})


def phase_rank(phase: str) -> int:
    try:
        return PHASES.index(phase)
    except ValueError:
        return -1


def should_run_phase(current_progress: dict | None, phase: str, resume: bool) -> bool:
    if not resume or not current_progress:
        return True
    saved_phase = current_progress.get('phase') or ''
    saved_status = current_progress.get('status') or ''
    saved_rank = phase_rank(saved_phase)
    current_rank = phase_rank(phase)
    # If the phase in question is the saved phase and it's already completed, skip it
    if phase == saved_phase and saved_status == 'completed':
        return False
    # Otherwise run if this phase comes after the saved phase (or if saved phase not found)
    return current_rank >= max(saved_rank, 0)


def apply_schema_files() -> None:
    schema_files = [
        ROOT / 'mysql' / 'init' / 'quantmate.sql',
        ROOT / 'mysql' / 'init' / 'tushare.sql',
        ROOT / 'mysql' / 'init' / 'akshare.sql',
        ROOT / 'mysql' / 'init' / 'vnpy.sql',
    ]
    engine = get_server_engine()
    with engine.begin() as conn:
        for file_path in schema_files:
            logger.info('Applying schema: %s', file_path.relative_to(ROOT))
            sql_text = file_path.read_text(encoding='utf-8')
            statements = split_sql_statements(sql_text)
            for stmt in statements:
                try:
                    conn.execute(text(stmt))
                except Exception as e:
                    msg = str(e)
                    if 'Duplicate key name' in msg or '(1061' in msg:
                        logger.warning('Ignoring duplicate index/schema error: %s', msg)
                        continue
                    raise


def print_summary() -> None:
    engine = get_quantmate_engine()
    checks = [
        ('tushare.stock_basic', 'SELECT COUNT(*) FROM tushare.stock_basic'),
        ('tushare.stock_company', 'SELECT COUNT(*) FROM tushare.stock_company'),
        ('tushare.new_share', 'SELECT COUNT(*) FROM tushare.new_share'),
        ('tushare.stock_daily', 'SELECT COUNT(*) FROM tushare.stock_daily'),
        ('tushare.stock_weekly', 'SELECT COUNT(*) FROM tushare.stock_weekly'),
        ('tushare.stock_monthly', 'SELECT COUNT(*) FROM tushare.stock_monthly'),
        ('tushare.bak_daily', 'SELECT COUNT(*) FROM tushare.bak_daily'),
        ('tushare.moneyflow', 'SELECT COUNT(*) FROM tushare.moneyflow'),
        ('tushare.suspend_d', 'SELECT COUNT(*) FROM tushare.suspend_d'),
        ('tushare.suspend', 'SELECT COUNT(*) FROM tushare.`suspend`'),
        ('tushare.adj_factor', 'SELECT COUNT(*) FROM tushare.adj_factor'),
        ('tushare.fina_indicator', 'SELECT COUNT(*) FROM tushare.fina_indicator'),
        ('tushare.income', 'SELECT COUNT(*) FROM tushare.income'),
        ('tushare.balancesheet', 'SELECT COUNT(*) FROM tushare.balancesheet'),
        ('tushare.cashflow', 'SELECT COUNT(*) FROM tushare.cashflow'),
        ('tushare.dividend', 'SELECT COUNT(*) FROM tushare.dividend'),
        ('tushare.top10_holders', 'SELECT COUNT(*) FROM tushare.top10_holders'),
        ('akshare.index_daily', 'SELECT COUNT(*) FROM akshare.index_daily'),
        ('vnpy.dbbardata', 'SELECT COUNT(*) FROM vnpy.dbbardata'),
    ]
    logger.info('Recovery summary (row counts):')
    with engine.connect() as conn:
        for name, sql in checks:
            value = conn.execute(text(sql)).scalar() or 0
            logger.info('  %-22s %s', name + ':', f'{value:,}')


def get_tushare_row_count(table_name: str) -> int:
    engine = get_quantmate_engine()
    with engine.connect() as conn:
        try:
            return conn.execute(text(f'SELECT COUNT(*) FROM tushare.`{table_name}`')).scalar() or 0
        except Exception as exc:
            message = str(exc).lower()
            if "doesn't exist" in message or "1146" in message or "unknown table" in message:
                return 0
            raise


def require_tushare_rows(table_name: str, phase: str) -> int:
    rows = get_tushare_row_count(table_name)
    if rows <= 0:
        raise RuntimeError(f"{phase} phase completed but tushare.{table_name} is empty")
    return rows


def ensure_source_item_table(source: str, item_key: str) -> None:
    from app.datasync.registry import build_default_registry
    from app.datasync.table_manager import ensure_table
    from app.domains.market.dao.data_source_item_dao import DataSourceItemDao

    registry = build_default_registry()
    iface = registry.get_interface(source, item_key)
    if iface is None:
        raise RuntimeError(f"No registered interface for {source}/{item_key}")

    item = DataSourceItemDao().get_by_key(source, item_key)
    target_db = (item or {}).get('target_database') or iface.info.target_database
    target_tbl = (item or {}).get('target_table') or iface.info.target_table
    if not target_db or not target_tbl:
        raise RuntimeError(f"Missing target mapping for {source}/{item_key}")
    if not iface.should_ensure_table_before_sync():
        return

    ensure_table(target_db, target_tbl, iface.get_ddl())


def ensure_source_item_tables(source: str, *item_keys: str) -> None:
    for item_key in item_keys:
        ensure_source_item_table(source, item_key)


def _coerce_sync_date(value) -> date | None:
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    raw = str(value or '').strip()
    if not raw:
        return None
    for candidate in (raw[:10], raw):
        try:
            return date.fromisoformat(candidate)
        except ValueError:
            continue
    return None


def _get_init_status_config(phase: str) -> dict | None:
    return _INIT_STATUS_CONFIG.get(phase)


@lru_cache(maxsize=64)
def _get_init_status_sync_mode(source: str, item_key: str, window: str | None, has_date_column: bool) -> str:
    from app.datasync.sync_mode import SYNC_MODE_BACKFILL, SYNC_MODE_LATEST_ONLY, normalize_sync_mode
    from app.infrastructure.db.connections import get_quantmate_engine

    try:
        engine = get_quantmate_engine()
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT sync_mode FROM data_source_items "
                    "WHERE source = :source AND item_key = :item_key LIMIT 1"
                ),
                {"source": source, "item_key": item_key},
            ).fetchone()
        if row is not None:
            return normalize_sync_mode(row[0] if len(row) > 0 else None)
    except Exception:
        logger.warning("Failed to load sync_mode for init phase %s/%s", source, item_key, exc_info=True)

    if window == 'end' and not has_date_column:
        return SYNC_MODE_LATEST_ONLY
    return SYNC_MODE_BACKFILL


def _is_init_status_latest_only(config: dict) -> bool:
    from app.datasync.sync_mode import sync_mode_supports_backfill

    sync_mode = _get_init_status_sync_mode(
        config['source'],
        config['item_key'],
        str(config.get('window') or ''),
        bool(config.get('date_column')),
    )
    return not sync_mode_supports_backfill(sync_mode)


def _iter_init_status_phases(*, skip_aux: bool) -> list[str]:
    phases = [
        'stock_basic',
        'stock_company',
        'new_share',
        'daily',
        'weekly',
        'monthly',
        'indexes',
        'adj_factor',
        'dividend',
        'top10_holders',
        'bak_daily',
        'moneyflow',
        'suspend_d',
        'suspend',
        'fina_indicator',
        'income',
        'balancesheet',
        'cashflow',
    ]
    if skip_aux:
        return [phase for phase in phases if phase not in _AUX_INIT_PHASES]
    return phases


def _resolve_phase_window(phase: str, start_date: str, daily_range_start: str, end_date: str) -> tuple[date, date]:
    config = _get_init_status_config(phase)
    if config is None:
        raise KeyError(f'Unsupported init status phase: {phase}')

    window = config.get('window')
    if window == 'daily':
        return date.fromisoformat(daily_range_start), date.fromisoformat(end_date)
    if window == 'aux':
        return date.fromisoformat(start_date), date.fromisoformat(end_date)
    return date.fromisoformat(end_date), date.fromisoformat(end_date)


def _ensure_pending_sync_status_rows(source: str, item_key: str, sync_dates: list[date]) -> None:
    if not sync_dates:
        return

    engine = get_quantmate_engine()
    values = [
        {
            'sd': sync_date,
            'src': source,
            'ik': item_key,
            'st': 'pending',
        }
        for sync_date in sync_dates
    ]
    with engine.begin() as conn:
        conn.execute(
            text(
                'INSERT IGNORE INTO data_sync_status '
                '(sync_date, source, interface_key, status) '
                'VALUES (:sd, :src, :ik, :st)'
            ),
            values,
        )


def _get_table_row_count(database_name: str, table_name: str) -> int:
    engine = get_quantmate_engine()
    with engine.connect() as conn:
        return int(conn.execute(text(f'SELECT COUNT(*) FROM {database_name}.`{table_name}`')).scalar() or 0)


def _get_table_counts_by_date(database_name: str, table_name: str, date_column: str, start: date, end: date) -> dict[date, int]:
    engine = get_quantmate_engine()
    sql = text(
        f'SELECT `{date_column}`, COUNT(*) FROM {database_name}.`{table_name}` '
        f'WHERE `{date_column}` BETWEEN :start_date AND :end_date '
        f'GROUP BY `{date_column}`'
    )
    with engine.connect() as conn:
        rows = conn.execute(sql, {'start_date': start, 'end_date': end}).fetchall()
    return {row[0]: int(row[1]) for row in rows if row[0] is not None}


def bootstrap_init_sync_status(start_date: str, daily_range_start: str, end_date: str, *, skip_aux: bool) -> None:
    from app.domains.extdata.dao.data_sync_status_dao import ensure_tables
    from app.datasync.service.sync_init_service import initialize_sync_status

    ensure_tables()

    for phase in _iter_init_status_phases(skip_aux=skip_aux):
        config = _get_init_status_config(phase)
        if config is None:
            continue

        window_start, window_end = _resolve_phase_window(phase, start_date, daily_range_start, end_date)
        initialize_sync_status(
            config['source'],
            config['item_key'],
            start_date=window_start,
            end_date=window_end,
            reconcile_missing=True,
        )
        if _is_init_status_latest_only(config):
            _ensure_pending_sync_status_rows(config['source'], config['item_key'], [window_end])


def _get_status_registry():
    from app.datasync.registry import build_default_registry

    return build_default_registry()


def build_phase_progress_callback(
    phase: str,
    *,
    source: str | None = None,
    item_key: str | None = None,
) -> Callable[[str | None, str | None], None]:
    from app.datasync.base import SyncStatus
    from app.datasync.service.sync_engine import _write_status

    def _callback(ts_code=None, cursor_date=None):
        save_progress(phase, 'running', cursor_ts_code=ts_code, cursor_date=cursor_date)
        sync_date = _coerce_sync_date(cursor_date)
        if source and item_key and sync_date is not None:
            _write_status(sync_date, source, item_key, SyncStatus.RUNNING.value)

    return _callback


def finalize_init_phase_sync_status(phase: str, start_date: str, daily_range_start: str, end_date: str) -> None:
    from app.datasync.base import SyncResult, SyncStatus
    from app.datasync.service.sync_engine import _normalize_zero_row_success, _write_status, get_trade_calendar

    config = _get_init_status_config(phase)
    if config is None:
        return

    registry = _get_status_registry()
    iface = registry.get_interface(config['source'], config['item_key'])
    if iface is None:
        logger.warning('No registry interface found for init phase %s -> %s/%s', phase, config['source'], config['item_key'])
        return

    window_start, window_end = _resolve_phase_window(phase, start_date, daily_range_start, end_date)
    if _is_init_status_latest_only(config):
        rows_synced = _get_table_row_count(config['database'], config['table'])
        result = _normalize_zero_row_success(
            iface,
            window_end,
            config['source'],
            config['item_key'],
            SyncResult(SyncStatus.SUCCESS, rows_synced),
        )
        _write_status(
            window_end,
            config['source'],
            config['item_key'],
            result.status.value,
            result.rows_synced,
            result.error_message,
        )
        return

    rows_by_date = _get_table_counts_by_date(
        config['database'],
        config['table'],
        config['date_column'],
        window_start,
        window_end,
    )
    status_dates = get_trade_calendar(window_start, window_end)
    if not status_dates and rows_by_date:
        status_dates = sorted(rows_by_date.keys())

    _ensure_pending_sync_status_rows(config['source'], config['item_key'], status_dates)

    for sync_date in status_dates:
        result = _normalize_zero_row_success(
            iface,
            sync_date,
            config['source'],
            config['item_key'],
            SyncResult(SyncStatus.SUCCESS, rows_by_date.get(sync_date, 0)),
        )
        _write_status(
            sync_date,
            config['source'],
            config['item_key'],
            result.status.value,
            result.rows_synced,
            result.error_message,
        )


def mark_init_phase_status_from_exception(
    phase: str,
    error_message: str,
    *,
    start_date: str,
    daily_range_start: str,
    end_date: str,
    status: str,
    cursor_date=None,
) -> None:
    from app.datasync.service.sync_engine import _write_status

    config = _get_init_status_config(phase)
    if config is None:
        return

    sync_date = _coerce_sync_date(cursor_date)
    if sync_date is None:
        _, sync_date = _resolve_phase_window(phase, start_date, daily_range_start, end_date)

    _write_status(sync_date, config['source'], config['item_key'], status, 0, error_message)


def get_loaded_trade_dates(start_date: str, end_date: str) -> list[str]:
    engine = get_quantmate_engine()
    sql = text(
        "SELECT DISTINCT trade_date FROM tushare.stock_daily "
        "WHERE trade_date BETWEEN :start_date AND :end_date ORDER BY trade_date"
    )
    with engine.connect() as conn:
        rows = conn.execute(sql, {'start_date': start_date, 'end_date': end_date}).scalars().all()
    return [value.isoformat() if hasattr(value, 'isoformat') else str(value) for value in rows if value]


def select_period_end_trade_dates(trade_dates: list[str], period: str) -> list[str]:
    if period not in {'weekly', 'monthly'}:
        raise ValueError(f'Unsupported period: {period}')

    grouped: dict[tuple[int, int], str] = {}
    for iso_date in sorted(trade_dates):
        current = date.fromisoformat(iso_date)
        if period == 'weekly':
            iso_year, iso_week, _ = current.isocalendar()
            key = (iso_year, iso_week)
        else:
            key = (current.year, current.month)
        grouped[key] = iso_date
    return list(grouped.values())


def seed_stock_basic_from_daily(start_date: str, end_date: str) -> int:
    engine = get_quantmate_engine()
    before = get_tushare_row_count('stock_basic')
    insert_sql = text(
        """
        INSERT INTO tushare.stock_basic (ts_code, symbol, exchange, list_status)
        SELECT src.ts_code,
               SUBSTRING_INDEX(src.ts_code, '.', 1),
               SUBSTRING_INDEX(src.ts_code, '.', -1),
               'L'
        FROM (
            SELECT DISTINCT ts_code
            FROM tushare.stock_daily
            WHERE trade_date BETWEEN :start_date AND :end_date
        ) AS src
        LEFT JOIN tushare.stock_basic sb ON sb.ts_code = src.ts_code
        WHERE sb.ts_code IS NULL
        """
    )
    update_sql = text(
        """
        UPDATE tushare.stock_basic
        SET symbol = COALESCE(symbol, SUBSTRING_INDEX(ts_code, '.', 1)),
            exchange = COALESCE(exchange, SUBSTRING_INDEX(ts_code, '.', -1)),
            list_status = COALESCE(list_status, 'L')
        WHERE symbol IS NULL OR exchange IS NULL OR list_status IS NULL
        """
    )
    with engine.begin() as conn:
        conn.execute(insert_sql, {'start_date': start_date, 'end_date': end_date})
        conn.execute(update_sql)
    after = get_tushare_row_count('stock_basic')
    return max(0, after - before)


def main() -> int:
    from app.datasync.service.init_service import get_coverage_window

    default_start_date = get_coverage_window()["start_date"].isoformat()

    parser = argparse.ArgumentParser(description='Initialize QuantMate market data')
    parser.add_argument('--start-date', default=default_start_date, help='Start date for aux backfill (YYYY-MM-DD)')
    parser.add_argument('--skip-schema', action='store_true', help='Skip schema initialization SQL')
    parser.add_argument('--skip-aux', action='store_true', help='Skip adj/dividend/top10 backfill')
    parser.add_argument('--skip-vnpy', action='store_true', help='Skip vnpy full sync')
    parser.add_argument('--stock-statuses', default='L', help='Comma-separated stock_basic list_status values (e.g. L or L,D,P)')
    parser.add_argument(
        '--batch-size',
        type=int,
        default=get_runtime_int(env_keys='BATCH_SIZE', db_key='datasync.batch_size', default=100),
    )
    parser.add_argument('--sleep-between', type=float, default=0.02)
    parser.add_argument('--daily-start-date', default=None, help='Limit stock_daily ingest to start at this date (YYYY-MM-DD)')
    parser.add_argument('--resume', action='store_true', default=True, help='Resume from init_progress checkpoint (default on)')
    parser.add_argument('--reset-progress', action='store_true', help='Reset init_progress before execution')
    args = parser.parse_args()

    end_date = date.today().isoformat()
    start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date().isoformat()
    daily_start_date = None
    if args.daily_start_date:
        daily_start_date = args.daily_start_date

    logger.info('Starting market data initialization (start_date=%s, end_date=%s)', start_date, end_date)

    daily_range_start = daily_start_date or start_date
    aux_window_days = (datetime.strptime(end_date, '%Y-%m-%d').date() - datetime.strptime(start_date, '%Y-%m-%d').date()).days + 1
    use_marketwide_daily = daily_start_date is not None
    use_marketwide_aux = aux_window_days <= 400

    if not args.skip_schema:
        apply_schema_files()

    ensure_init_progress_table()
    if args.reset_progress:
        logger.info('Resetting init_progress checkpoint as requested')
        reset_progress()

    progress = load_progress() if args.resume else None
    if progress:
        logger.info(
            'Resuming from checkpoint: phase=%s cursor_ts_code=%s cursor_date=%s status=%s',
            progress.get('phase'),
            progress.get('cursor_ts_code'),
            progress.get('cursor_date'),
            progress.get('status'),
        )

    statuses = [s.strip().upper() for s in args.stock_statuses.split(',') if s.strip()]
    if not statuses:
        statuses = ['L']

    trade_dates: list[str] | None = None

    def ensure_trade_dates() -> list[str]:
        nonlocal trade_dates
        if trade_dates is None:
            trade_dates = get_loaded_trade_dates(daily_range_start, end_date)
        return trade_dates

    try:
        logger.info('Bootstrapping data_sync_status coverage before market data initialization')
        bootstrap_init_sync_status(start_date, daily_range_start, end_date, skip_aux=args.skip_aux)

        if should_run_phase(progress, 'stock_basic', args.resume):
            ensure_source_item_tables('tushare', 'stock_basic')
            existing_stock_basic = get_tushare_row_count('stock_basic')
            if existing_stock_basic > 0:
                logger.info(
                    'Skipping stock_basic refresh because tushare.stock_basic already has %d rows',
                    existing_stock_basic,
                )
                save_progress('stock_basic', 'completed')
            else:
                logger.info('Rebuilding Tushare stock_basic (statuses=%s)', ','.join(statuses))
                save_progress('stock_basic', 'running')
                resume_after = progress.get('cursor_ts_code') if progress and progress.get('phase') == 'stock_basic' else None
                skip_until_found = bool(resume_after)
                stock_basic_warning = None
                for status in statuses:
                    if skip_until_found:
                        if status == resume_after:
                            skip_until_found = False
                        continue
                    save_progress('stock_basic', 'running', cursor_ts_code=status)
                    try:
                        ingest_stock_basic(list_status=status, max_retries=1)
                    except Exception as exc:
                        stock_basic_warning = str(exc)
                        logger.warning(
                            'stock_basic ingest failed for list_status=%s: %s; continuing with daily-driven symbol bootstrap',
                            status,
                            exc,
                        )
                        if status == 'L':
                            break
                save_progress('stock_basic', 'completed', error=stock_basic_warning)
            finalize_init_phase_sync_status('stock_basic', start_date, daily_range_start, end_date)

        if should_run_phase(progress, 'stock_company', args.resume):
            ensure_source_item_tables('tushare', 'stock_company')
            logger.info('Refreshing stock_company snapshot')
            save_progress('stock_company', 'running')
            resume_after_exchange = progress.get('cursor_ts_code') if progress and progress.get('phase') == 'stock_company' else None
            ingest_stock_company_snapshot(
                sleep_between=args.sleep_between,
                start_after_exchange=resume_after_exchange,
                progress_cb=build_phase_progress_callback('stock_company'),
            )
            save_progress('stock_company', 'completed')
            require_tushare_rows('stock_company', 'stock_company')
            finalize_init_phase_sync_status('stock_company', start_date, daily_range_start, end_date)

        if should_run_phase(progress, 'new_share', args.resume):
            ensure_source_item_tables('tushare', 'new_share')
            logger.info('Backfilling new_share from %s to %s', start_date, end_date)
            save_progress('new_share', 'running', cursor_date=end_date)
            ingest_new_share_by_date_range(
                start_date,
                end_date,
                progress_cb=build_phase_progress_callback('new_share', source='tushare', item_key='new_share'),
            )
            save_progress('new_share', 'completed', cursor_date=end_date)
            finalize_init_phase_sync_status('new_share', start_date, daily_range_start, end_date)

        if should_run_phase(progress, 'daily', args.resume):
            ensure_source_item_tables('tushare', 'stock_daily', 'stock_basic')
            if use_marketwide_daily:
                logger.info('Rebuilding marketwide Tushare stock_daily history from %s', daily_range_start)
            else:
                logger.info('Rebuilding full Tushare stock_daily history (this can take a long time)')
            save_progress('daily', 'running')
            if use_marketwide_daily:
                resume_after_date = progress.get('cursor_date') if progress and progress.get('phase') == 'daily' else None
                ingest_daily_by_trade_date_range(
                    daily_range_start,
                    end_date,
                    sleep_between=args.sleep_between,
                    start_after_date=resume_after_date,
                    progress_cb=build_phase_progress_callback('daily', source='tushare', item_key='stock_daily'),
                )
            else:
                resume_after = progress.get('cursor_ts_code') if progress and progress.get('phase') == 'daily' else None
                ingest_all_daily(
                    batch_size=args.batch_size,
                    sleep_between=args.sleep_between,
                    force_full_per_stock=True,
                    start_after_ts_code=resume_after,
                    start_date=daily_start_date,
                    end_date=end_date,
                    progress_cb=build_phase_progress_callback('daily', source='tushare', item_key='stock_daily'),
                )
            seeded = seed_stock_basic_from_daily(daily_range_start, end_date)
            if seeded:
                logger.info('Seeded %d stock_basic placeholder rows from stock_daily', seeded)
            save_progress('daily', 'completed')
            require_tushare_rows('stock_daily', 'daily')
            require_tushare_rows('stock_basic', 'stock_basic')
            finalize_init_phase_sync_status('daily', start_date, daily_range_start, end_date)
            finalize_init_phase_sync_status('stock_basic', start_date, daily_range_start, end_date)

        if should_run_phase(progress, 'weekly', args.resume):
            ensure_source_item_tables('tushare', 'stock_weekly')
            logger.info('Backfilling stock_weekly from %s to %s', daily_range_start, end_date)
            save_progress('weekly', 'running')
            loaded_trade_dates = ensure_trade_dates()
            weekly_trade_dates = select_period_end_trade_dates(loaded_trade_dates, 'weekly')
            resume_after_date = progress.get('cursor_date') if progress and progress.get('phase') == 'weekly' else None
            ingest_weekly_by_trade_dates(
                weekly_trade_dates,
                sleep_between=args.sleep_between,
                start_after_date=resume_after_date,
                progress_cb=build_phase_progress_callback('weekly', source='tushare', item_key='stock_weekly'),
            )
            save_progress('weekly', 'completed', cursor_date=weekly_trade_dates[-1] if weekly_trade_dates else None)
            require_tushare_rows('stock_weekly', 'weekly')
            finalize_init_phase_sync_status('weekly', start_date, daily_range_start, end_date)

        if should_run_phase(progress, 'monthly', args.resume):
            ensure_source_item_tables('tushare', 'stock_monthly')
            logger.info('Backfilling stock_monthly from %s to %s', daily_range_start, end_date)
            save_progress('monthly', 'running')
            loaded_trade_dates = ensure_trade_dates()
            monthly_trade_dates = select_period_end_trade_dates(loaded_trade_dates, 'monthly')
            resume_after_date = progress.get('cursor_date') if progress and progress.get('phase') == 'monthly' else None
            ingest_monthly_by_trade_dates(
                monthly_trade_dates,
                sleep_between=args.sleep_between,
                start_after_date=resume_after_date,
                progress_cb=build_phase_progress_callback('monthly', source='tushare', item_key='stock_monthly'),
            )
            save_progress('monthly', 'completed', cursor_date=monthly_trade_dates[-1] if monthly_trade_dates else None)
            require_tushare_rows('stock_monthly', 'monthly')
            finalize_init_phase_sync_status('monthly', start_date, daily_range_start, end_date)

        if should_run_phase(progress, 'indexes', args.resume):
            logger.info('Rebuilding AkShare index history from %s to %s', daily_range_start, end_date)
            save_progress('indexes', 'running')
            ingest_all_indexes(start_date=daily_range_start)
            save_progress('indexes', 'completed')
            finalize_init_phase_sync_status('indexes', start_date, daily_range_start, end_date)

        if not args.skip_aux:
            if should_run_phase(progress, 'adj_factor', args.resume):
                ensure_source_item_tables('tushare', 'adj_factor')
                logger.info('Backfilling adj_factor from %s to %s', start_date, end_date)
                save_progress('adj_factor', 'running')
                loaded_trade_dates = ensure_trade_dates()
                if use_marketwide_aux:
                    resume_after_date = progress.get('cursor_date') if progress and progress.get('phase') == 'adj_factor' else None
                    ingest_adj_factor_by_trade_dates(
                        loaded_trade_dates,
                        sleep_between=args.sleep_between,
                        start_after_date=resume_after_date,
                        progress_cb=build_phase_progress_callback('adj_factor', source='tushare', item_key='adj_factor'),
                    )
                    save_progress(
                        'adj_factor',
                        'completed',
                        cursor_date=loaded_trade_dates[-1] if loaded_trade_dates else end_date,
                    )
                else:
                    resume_after = progress.get('cursor_ts_code') if progress and progress.get('phase') == 'adj_factor' else None
                    ingest_adj_factor_by_date_range(
                        start_date,
                        end_date,
                        batch_size=args.batch_size,
                        sleep_between=args.sleep_between,
                        start_after_ts_code=resume_after,
                        progress_cb=build_phase_progress_callback('adj_factor', source='tushare', item_key='adj_factor'),
                    )
                    save_progress('adj_factor', 'completed', cursor_date=end_date)
                require_tushare_rows('adj_factor', 'adj_factor')
                finalize_init_phase_sync_status('adj_factor', start_date, daily_range_start, end_date)

            if should_run_phase(progress, 'dividend', args.resume):
                ensure_source_item_tables('tushare', 'dividend')
                logger.info('Backfilling dividend from %s to %s', start_date, end_date)
                save_progress('dividend', 'running')
                if use_marketwide_aux:
                    resume_after_date = progress.get('cursor_date') if progress and progress.get('phase') == 'dividend' else None
                    ingest_dividend_by_ann_date_range(
                        start_date,
                        end_date,
                        sleep_between=args.sleep_between,
                        start_after_date=resume_after_date,
                        progress_cb=build_phase_progress_callback('dividend', source='tushare', item_key='dividend'),
                    )
                    save_progress('dividend', 'completed', cursor_date=end_date)
                else:
                    resume_after = progress.get('cursor_ts_code') if progress and progress.get('phase') == 'dividend' else None
                    ingest_dividend_by_date_range(
                        start_date,
                        end_date,
                        batch_size=args.batch_size,
                        sleep_between=args.sleep_between,
                        start_after_ts_code=resume_after,
                        progress_cb=build_phase_progress_callback('dividend', source='tushare', item_key='dividend'),
                    )
                    save_progress('dividend', 'completed', cursor_date=end_date)
                require_tushare_rows('dividend', 'dividend')
                finalize_init_phase_sync_status('dividend', start_date, daily_range_start, end_date)

            if should_run_phase(progress, 'top10_holders', args.resume):
                ensure_source_item_tables('tushare', 'top10_holders')
                logger.info('Backfilling top10_holders from %s to %s', start_date, end_date)
                save_progress('top10_holders', 'running', cursor_date=end_date)
                if use_marketwide_aux:
                    ingest_top10_holders_marketwide_by_date_range(
                        start_date,
                        end_date,
                        progress_cb=build_phase_progress_callback('top10_holders', source='tushare', item_key='top10_holders'),
                    )
                else:
                    resume_after = progress.get('cursor_ts_code') if progress and progress.get('phase') == 'top10_holders' else None
                    ingest_top10_holders_by_date_range(
                        start_date,
                        end_date,
                        batch_size=args.batch_size,
                        sleep_between=args.sleep_between,
                        start_after_ts_code=resume_after,
                        progress_cb=build_phase_progress_callback('top10_holders', source='tushare', item_key='top10_holders'),
                    )
                save_progress('top10_holders', 'completed', cursor_date=end_date)
                require_tushare_rows('top10_holders', 'top10_holders')
                finalize_init_phase_sync_status('top10_holders', start_date, daily_range_start, end_date)

            if should_run_phase(progress, 'bak_daily', args.resume):
                ensure_source_item_tables('tushare', 'bak_daily')
                logger.info('Backfilling bak_daily from %s to %s', daily_range_start, end_date)
                save_progress('bak_daily', 'running')
                loaded_trade_dates = ensure_trade_dates()
                resume_after_date = progress.get('cursor_date') if progress and progress.get('phase') == 'bak_daily' else None
                ingest_bak_daily_by_trade_dates(
                    loaded_trade_dates,
                    sleep_between=args.sleep_between,
                    start_after_date=resume_after_date,
                    progress_cb=build_phase_progress_callback('bak_daily', source='tushare', item_key='bak_daily'),
                )
                save_progress('bak_daily', 'completed', cursor_date=loaded_trade_dates[-1] if loaded_trade_dates else None)
                require_tushare_rows('bak_daily', 'bak_daily')
                finalize_init_phase_sync_status('bak_daily', start_date, daily_range_start, end_date)

            if should_run_phase(progress, 'moneyflow', args.resume):
                ensure_source_item_tables('tushare', 'moneyflow')
                logger.info('Backfilling moneyflow from %s to %s', daily_range_start, end_date)
                save_progress('moneyflow', 'running')
                loaded_trade_dates = ensure_trade_dates()
                resume_after_date = progress.get('cursor_date') if progress and progress.get('phase') == 'moneyflow' else None
                ingest_moneyflow_by_trade_dates(
                    loaded_trade_dates,
                    sleep_between=args.sleep_between,
                    start_after_date=resume_after_date,
                    progress_cb=build_phase_progress_callback('moneyflow', source='tushare', item_key='moneyflow'),
                )
                save_progress('moneyflow', 'completed', cursor_date=loaded_trade_dates[-1] if loaded_trade_dates else None)
                require_tushare_rows('moneyflow', 'moneyflow')
                finalize_init_phase_sync_status('moneyflow', start_date, daily_range_start, end_date)

            if should_run_phase(progress, 'suspend_d', args.resume):
                ensure_source_item_tables('tushare', 'suspend_d')
                logger.info('Backfilling suspend_d from %s to %s', daily_range_start, end_date)
                save_progress('suspend_d', 'running')
                loaded_trade_dates = ensure_trade_dates()
                resume_after_date = progress.get('cursor_date') if progress and progress.get('phase') == 'suspend_d' else None
                ingest_suspend_d_by_trade_dates(
                    loaded_trade_dates,
                    sleep_between=args.sleep_between,
                    start_after_date=resume_after_date,
                    progress_cb=build_phase_progress_callback('suspend_d', source='tushare', item_key='suspend_d'),
                )
                save_progress('suspend_d', 'completed', cursor_date=loaded_trade_dates[-1] if loaded_trade_dates else None)
                require_tushare_rows('suspend_d', 'suspend_d')
                finalize_init_phase_sync_status('suspend_d', start_date, daily_range_start, end_date)

            if should_run_phase(progress, 'suspend', args.resume):
                ensure_source_item_tables('tushare', 'suspend')
                logger.info('Backfilling suspend from %s to %s', daily_range_start, end_date)
                save_progress('suspend', 'running')
                loaded_trade_dates = ensure_trade_dates()
                resume_after_date = progress.get('cursor_date') if progress and progress.get('phase') == 'suspend' else None
                ingest_suspend_by_trade_dates(
                    loaded_trade_dates,
                    sleep_between=args.sleep_between,
                    start_after_date=resume_after_date,
                    progress_cb=build_phase_progress_callback('suspend', source='tushare', item_key='suspend'),
                )
                save_progress('suspend', 'completed', cursor_date=loaded_trade_dates[-1] if loaded_trade_dates else None)
                require_tushare_rows('suspend', 'suspend')
                finalize_init_phase_sync_status('suspend', start_date, daily_range_start, end_date)

            if should_run_phase(progress, 'fina_indicator', args.resume):
                ensure_source_item_tables('tushare', 'fina_indicator')
                logger.info('Backfilling fina_indicator from %s to %s', start_date, end_date)
                save_progress('fina_indicator', 'running')
                resume_after = progress.get('cursor_ts_code') if progress and progress.get('phase') == 'fina_indicator' else None
                ingest_fina_indicator_by_date_range(
                    start_date,
                    end_date,
                    batch_size=args.batch_size,
                    sleep_between=args.sleep_between,
                    start_after_ts_code=resume_after,
                    progress_cb=build_phase_progress_callback('fina_indicator', source='tushare', item_key='fina_indicator'),
                )
                save_progress('fina_indicator', 'completed', cursor_date=end_date)
                require_tushare_rows('fina_indicator', 'fina_indicator')
                finalize_init_phase_sync_status('fina_indicator', start_date, daily_range_start, end_date)

            if should_run_phase(progress, 'income', args.resume):
                ensure_source_item_tables('tushare', 'income')
                logger.info('Backfilling income from %s to %s', start_date, end_date)
                save_progress('income', 'running')
                resume_after = progress.get('cursor_ts_code') if progress and progress.get('phase') == 'income' else None
                ingest_income_by_date_range(
                    start_date,
                    end_date,
                    batch_size=args.batch_size,
                    sleep_between=args.sleep_between,
                    start_after_ts_code=resume_after,
                    progress_cb=build_phase_progress_callback('income', source='tushare', item_key='income'),
                )
                save_progress('income', 'completed', cursor_date=end_date)
                require_tushare_rows('income', 'income')
                finalize_init_phase_sync_status('income', start_date, daily_range_start, end_date)

            if should_run_phase(progress, 'balancesheet', args.resume):
                ensure_source_item_tables('tushare', 'balancesheet')
                logger.info('Backfilling balancesheet from %s to %s', start_date, end_date)
                save_progress('balancesheet', 'running')
                resume_after = progress.get('cursor_ts_code') if progress and progress.get('phase') == 'balancesheet' else None
                ingest_balancesheet_by_date_range(
                    start_date,
                    end_date,
                    batch_size=args.batch_size,
                    sleep_between=args.sleep_between,
                    start_after_ts_code=resume_after,
                    progress_cb=build_phase_progress_callback('balancesheet', source='tushare', item_key='balancesheet'),
                )
                save_progress('balancesheet', 'completed', cursor_date=end_date)
                require_tushare_rows('balancesheet', 'balancesheet')
                finalize_init_phase_sync_status('balancesheet', start_date, daily_range_start, end_date)

            if should_run_phase(progress, 'cashflow', args.resume):
                ensure_source_item_tables('tushare', 'cashflow')
                logger.info('Backfilling cashflow from %s to %s', start_date, end_date)
                save_progress('cashflow', 'running')
                resume_after = progress.get('cursor_ts_code') if progress and progress.get('phase') == 'cashflow' else None
                ingest_cashflow_by_date_range(
                    start_date,
                    end_date,
                    batch_size=args.batch_size,
                    sleep_between=args.sleep_between,
                    start_after_ts_code=resume_after,
                    progress_cb=build_phase_progress_callback('cashflow', source='tushare', item_key='cashflow'),
                )
                save_progress('cashflow', 'completed', cursor_date=end_date)
                require_tushare_rows('cashflow', 'cashflow')
                finalize_init_phase_sync_status('cashflow', start_date, daily_range_start, end_date)

        if not args.skip_vnpy and should_run_phase(progress, 'vnpy', args.resume):
            logger.info('Syncing all stock bars from tushare to vnpy')
            save_progress('vnpy', 'running')
            sync_all_to_vnpy(full_refresh=True)
            save_progress('vnpy', 'completed')

        if should_run_phase(progress, 'sync_status', args.resume):
            logger.info('data_sync_status was bootstrapped before init and updated after each phase')
            save_progress('sync_status', 'running')
            save_progress('sync_status', 'completed')

        save_progress('finished', 'completed')
        print_summary()
        logger.info('Market data initialization finished')
        return 0
    except TushareQuotaExceededError as exc:
        current_progress = load_progress() or {}
        current_phase = current_progress.get('phase', 'unknown')
        cursor_ts_code = current_progress.get('cursor_ts_code')
        cursor_date = current_progress.get('cursor_date')
        logger.warning(
            'Initialization paused at phase=%s cursor_ts_code=%s cursor_date=%s due to %s quota: %s',
            current_phase,
            cursor_ts_code,
            cursor_date,
            exc.scope or 'rate-limit',
            exc,
        )
        save_progress(
            current_phase,
            'paused',
            cursor_ts_code=cursor_ts_code,
            cursor_date=cursor_date,
            error=str(exc),
        )
        mark_init_phase_status_from_exception(
            current_phase,
            str(exc),
            start_date=start_date,
            daily_range_start=daily_range_start,
            end_date=end_date,
            status='pending',
            cursor_date=cursor_date,
        )
        print_summary()
        return 0
    except Exception as exc:
        current_progress = load_progress() or {}
        current_phase = current_progress.get('phase', 'unknown')
        logger.exception('Initialization failed at phase=%s: %s', current_phase, exc)
        save_progress(current_phase, 'error', error=str(exc))
        mark_init_phase_status_from_exception(
            current_phase,
            str(exc),
            start_date=start_date,
            daily_range_start=daily_range_start,
            end_date=end_date,
            status='error',
            cursor_date=current_progress.get('cursor_date'),
        )
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
