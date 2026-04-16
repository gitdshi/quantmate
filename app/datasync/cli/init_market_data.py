#!/usr/bin/env python3
"""Initialize/rebuild QuantMate market bootstrap data after DB loss.

This script supports resumable initialization via `init_progress` checkpoints.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, datetime, timedelta
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
        ('tushare.stock_moneyflow', 'SELECT COUNT(*) FROM tushare.stock_moneyflow'),
        ('tushare.suspend_d', 'SELECT COUNT(*) FROM tushare.suspend_d'),
        ('tushare.suspend', 'SELECT COUNT(*) FROM tushare.`suspend`'),
        ('tushare.adj_factor', 'SELECT COUNT(*) FROM tushare.adj_factor'),
        ('tushare.fina_indicator', 'SELECT COUNT(*) FROM tushare.fina_indicator'),
        ('tushare.income', 'SELECT COUNT(*) FROM tushare.income'),
        ('tushare.balancesheet', 'SELECT COUNT(*) FROM tushare.balancesheet'),
        ('tushare.cashflow', 'SELECT COUNT(*) FROM tushare.cashflow'),
        ('tushare.stock_dividend', 'SELECT COUNT(*) FROM tushare.stock_dividend'),
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
        return conn.execute(text(f'SELECT COUNT(*) FROM tushare.`{table_name}`')).scalar() or 0


def require_tushare_rows(table_name: str, phase: str) -> int:
    rows = get_tushare_row_count(table_name)
    if rows <= 0:
        raise RuntimeError(f"{phase} phase completed but tushare.{table_name} is empty")
    return rows


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
    parser = argparse.ArgumentParser(description='Initialize QuantMate market data')
    parser.add_argument('--start-date', default='2005-01-01', help='Start date for aux backfill (YYYY-MM-DD)')
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
    parser.add_argument('--daily-lookback-days', type=int, default=None, help='Limit stock_daily ingest to last N days')
    parser.add_argument('--resume', action='store_true', default=True, help='Resume from init_progress checkpoint (default on)')
    parser.add_argument('--reset-progress', action='store_true', help='Reset init_progress before execution')
    args = parser.parse_args()

    end_date = date.today().isoformat()
    start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date().isoformat()
    daily_start_date = None
    if args.daily_start_date:
        daily_start_date = args.daily_start_date
    elif args.daily_lookback_days:
        daily_start_date = (date.today() - timedelta(days=args.daily_lookback_days)).isoformat()

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
        if should_run_phase(progress, 'stock_basic', args.resume):
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

        if should_run_phase(progress, 'stock_company', args.resume):
            logger.info('Refreshing stock_company snapshot')
            save_progress('stock_company', 'running')
            resume_after_exchange = progress.get('cursor_ts_code') if progress and progress.get('phase') == 'stock_company' else None
            ingest_stock_company_snapshot(
                sleep_between=args.sleep_between,
                start_after_exchange=resume_after_exchange,
                progress_cb=lambda ts_code=None, cursor_date=None: save_progress(
                    'stock_company', 'running', cursor_ts_code=ts_code, cursor_date=cursor_date
                ),
            )
            save_progress('stock_company', 'completed')
            require_tushare_rows('stock_company', 'stock_company')

        if should_run_phase(progress, 'new_share', args.resume):
            logger.info('Backfilling new_share from %s to %s', start_date, end_date)
            save_progress('new_share', 'running', cursor_date=end_date)
            ingest_new_share_by_date_range(
                start_date,
                end_date,
                progress_cb=lambda ts_code=None, cursor_date=None: save_progress(
                    'new_share', 'running', cursor_date=cursor_date
                ),
            )
            save_progress('new_share', 'completed', cursor_date=end_date)

        if should_run_phase(progress, 'daily', args.resume):
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
                    progress_cb=lambda ts_code=None, cursor_date=None: save_progress(
                        'daily', 'running', cursor_date=cursor_date
                    ),
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
                    progress_cb=lambda ts_code, cursor_date=None: save_progress(
                        'daily', 'running', cursor_ts_code=ts_code, cursor_date=cursor_date
                    ),
                )
            seeded = seed_stock_basic_from_daily(daily_range_start, end_date)
            if seeded:
                logger.info('Seeded %d stock_basic placeholder rows from stock_daily', seeded)
            save_progress('daily', 'completed')
            require_tushare_rows('stock_daily', 'daily')
            require_tushare_rows('stock_basic', 'stock_basic')

        if should_run_phase(progress, 'weekly', args.resume):
            logger.info('Backfilling stock_weekly from %s to %s', daily_range_start, end_date)
            save_progress('weekly', 'running')
            loaded_trade_dates = ensure_trade_dates()
            weekly_trade_dates = select_period_end_trade_dates(loaded_trade_dates, 'weekly')
            resume_after_date = progress.get('cursor_date') if progress and progress.get('phase') == 'weekly' else None
            ingest_weekly_by_trade_dates(
                weekly_trade_dates,
                sleep_between=args.sleep_between,
                start_after_date=resume_after_date,
                progress_cb=lambda ts_code=None, cursor_date=None: save_progress(
                    'weekly', 'running', cursor_date=cursor_date
                ),
            )
            save_progress('weekly', 'completed', cursor_date=weekly_trade_dates[-1] if weekly_trade_dates else None)
            require_tushare_rows('stock_weekly', 'weekly')

        if should_run_phase(progress, 'monthly', args.resume):
            logger.info('Backfilling stock_monthly from %s to %s', daily_range_start, end_date)
            save_progress('monthly', 'running')
            loaded_trade_dates = ensure_trade_dates()
            monthly_trade_dates = select_period_end_trade_dates(loaded_trade_dates, 'monthly')
            resume_after_date = progress.get('cursor_date') if progress and progress.get('phase') == 'monthly' else None
            ingest_monthly_by_trade_dates(
                monthly_trade_dates,
                sleep_between=args.sleep_between,
                start_after_date=resume_after_date,
                progress_cb=lambda ts_code=None, cursor_date=None: save_progress(
                    'monthly', 'running', cursor_date=cursor_date
                ),
            )
            save_progress('monthly', 'completed', cursor_date=monthly_trade_dates[-1] if monthly_trade_dates else None)
            require_tushare_rows('stock_monthly', 'monthly')

        if should_run_phase(progress, 'indexes', args.resume):
            logger.info('Rebuilding AkShare index history from %s to %s', daily_range_start, end_date)
            save_progress('indexes', 'running')
            ingest_all_indexes(start_date=daily_range_start)
            save_progress('indexes', 'completed')

        if not args.skip_aux:
            if should_run_phase(progress, 'adj_factor', args.resume):
                logger.info('Backfilling adj_factor from %s to %s', start_date, end_date)
                save_progress('adj_factor', 'running')
                loaded_trade_dates = ensure_trade_dates()
                if use_marketwide_aux:
                    resume_after_date = progress.get('cursor_date') if progress and progress.get('phase') == 'adj_factor' else None
                    ingest_adj_factor_by_trade_dates(
                        loaded_trade_dates,
                        sleep_between=args.sleep_between,
                        start_after_date=resume_after_date,
                        progress_cb=lambda ts_code=None, cursor_date=None: save_progress(
                            'adj_factor', 'running', cursor_date=cursor_date
                        ),
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
                        progress_cb=lambda ts_code, cursor_date=None: save_progress(
                            'adj_factor', 'running', cursor_ts_code=ts_code, cursor_date=cursor_date
                        ),
                    )
                    save_progress('adj_factor', 'completed', cursor_date=end_date)
                require_tushare_rows('adj_factor', 'adj_factor')

            if should_run_phase(progress, 'dividend', args.resume):
                logger.info('Backfilling dividend from %s to %s', start_date, end_date)
                save_progress('dividend', 'running')
                if use_marketwide_aux:
                    resume_after_date = progress.get('cursor_date') if progress and progress.get('phase') == 'dividend' else None
                    ingest_dividend_by_ann_date_range(
                        start_date,
                        end_date,
                        sleep_between=args.sleep_between,
                        start_after_date=resume_after_date,
                        progress_cb=lambda ts_code=None, cursor_date=None: save_progress(
                            'dividend', 'running', cursor_date=cursor_date
                        ),
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
                        progress_cb=lambda ts_code, cursor_date=None: save_progress(
                            'dividend', 'running', cursor_ts_code=ts_code, cursor_date=cursor_date
                        ),
                    )
                    save_progress('dividend', 'completed', cursor_date=end_date)
                require_tushare_rows('stock_dividend', 'dividend')

            if should_run_phase(progress, 'top10_holders', args.resume):
                logger.info('Backfilling top10_holders from %s to %s', start_date, end_date)
                save_progress('top10_holders', 'running', cursor_date=end_date)
                if use_marketwide_aux:
                    ingest_top10_holders_marketwide_by_date_range(
                        start_date,
                        end_date,
                        progress_cb=lambda ts_code=None, cursor_date=None: save_progress(
                            'top10_holders', 'running', cursor_date=cursor_date
                        ),
                    )
                else:
                    resume_after = progress.get('cursor_ts_code') if progress and progress.get('phase') == 'top10_holders' else None
                    ingest_top10_holders_by_date_range(
                        start_date,
                        end_date,
                        batch_size=args.batch_size,
                        sleep_between=args.sleep_between,
                        start_after_ts_code=resume_after,
                        progress_cb=lambda ts_code, cursor_date=None: save_progress(
                            'top10_holders', 'running', cursor_ts_code=ts_code, cursor_date=cursor_date
                        ),
                    )
                save_progress('top10_holders', 'completed', cursor_date=end_date)
                require_tushare_rows('top10_holders', 'top10_holders')

            if should_run_phase(progress, 'bak_daily', args.resume):
                logger.info('Backfilling bak_daily from %s to %s', daily_range_start, end_date)
                save_progress('bak_daily', 'running')
                loaded_trade_dates = ensure_trade_dates()
                resume_after_date = progress.get('cursor_date') if progress and progress.get('phase') == 'bak_daily' else None
                ingest_bak_daily_by_trade_dates(
                    loaded_trade_dates,
                    sleep_between=args.sleep_between,
                    start_after_date=resume_after_date,
                    progress_cb=lambda ts_code=None, cursor_date=None: save_progress(
                        'bak_daily', 'running', cursor_date=cursor_date
                    ),
                )
                save_progress('bak_daily', 'completed', cursor_date=loaded_trade_dates[-1] if loaded_trade_dates else None)
                require_tushare_rows('bak_daily', 'bak_daily')

            if should_run_phase(progress, 'moneyflow', args.resume):
                logger.info('Backfilling moneyflow from %s to %s', daily_range_start, end_date)
                save_progress('moneyflow', 'running')
                loaded_trade_dates = ensure_trade_dates()
                resume_after_date = progress.get('cursor_date') if progress and progress.get('phase') == 'moneyflow' else None
                ingest_moneyflow_by_trade_dates(
                    loaded_trade_dates,
                    sleep_between=args.sleep_between,
                    start_after_date=resume_after_date,
                    progress_cb=lambda ts_code=None, cursor_date=None: save_progress(
                        'moneyflow', 'running', cursor_date=cursor_date
                    ),
                )
                save_progress('moneyflow', 'completed', cursor_date=loaded_trade_dates[-1] if loaded_trade_dates else None)
                require_tushare_rows('stock_moneyflow', 'moneyflow')

            if should_run_phase(progress, 'suspend_d', args.resume):
                logger.info('Backfilling suspend_d from %s to %s', daily_range_start, end_date)
                save_progress('suspend_d', 'running')
                loaded_trade_dates = ensure_trade_dates()
                resume_after_date = progress.get('cursor_date') if progress and progress.get('phase') == 'suspend_d' else None
                ingest_suspend_d_by_trade_dates(
                    loaded_trade_dates,
                    sleep_between=args.sleep_between,
                    start_after_date=resume_after_date,
                    progress_cb=lambda ts_code=None, cursor_date=None: save_progress(
                        'suspend_d', 'running', cursor_date=cursor_date
                    ),
                )
                save_progress('suspend_d', 'completed', cursor_date=loaded_trade_dates[-1] if loaded_trade_dates else None)
                require_tushare_rows('suspend_d', 'suspend_d')

            if should_run_phase(progress, 'suspend', args.resume):
                logger.info('Backfilling suspend from %s to %s', daily_range_start, end_date)
                save_progress('suspend', 'running')
                loaded_trade_dates = ensure_trade_dates()
                resume_after_date = progress.get('cursor_date') if progress and progress.get('phase') == 'suspend' else None
                ingest_suspend_by_trade_dates(
                    loaded_trade_dates,
                    sleep_between=args.sleep_between,
                    start_after_date=resume_after_date,
                    progress_cb=lambda ts_code=None, cursor_date=None: save_progress(
                        'suspend', 'running', cursor_date=cursor_date
                    ),
                )
                save_progress('suspend', 'completed', cursor_date=loaded_trade_dates[-1] if loaded_trade_dates else None)
                require_tushare_rows('suspend', 'suspend')

            if should_run_phase(progress, 'fina_indicator', args.resume):
                logger.info('Backfilling fina_indicator from %s to %s', start_date, end_date)
                save_progress('fina_indicator', 'running')
                resume_after = progress.get('cursor_ts_code') if progress and progress.get('phase') == 'fina_indicator' else None
                ingest_fina_indicator_by_date_range(
                    start_date,
                    end_date,
                    batch_size=args.batch_size,
                    sleep_between=args.sleep_between,
                    start_after_ts_code=resume_after,
                    progress_cb=lambda ts_code, cursor_date=None: save_progress(
                        'fina_indicator', 'running', cursor_ts_code=ts_code, cursor_date=cursor_date
                    ),
                )
                save_progress('fina_indicator', 'completed', cursor_date=end_date)
                require_tushare_rows('fina_indicator', 'fina_indicator')

            if should_run_phase(progress, 'income', args.resume):
                logger.info('Backfilling income from %s to %s', start_date, end_date)
                save_progress('income', 'running')
                resume_after = progress.get('cursor_ts_code') if progress and progress.get('phase') == 'income' else None
                ingest_income_by_date_range(
                    start_date,
                    end_date,
                    batch_size=args.batch_size,
                    sleep_between=args.sleep_between,
                    start_after_ts_code=resume_after,
                    progress_cb=lambda ts_code, cursor_date=None: save_progress(
                        'income', 'running', cursor_ts_code=ts_code, cursor_date=cursor_date
                    ),
                )
                save_progress('income', 'completed', cursor_date=end_date)
                require_tushare_rows('income', 'income')

            if should_run_phase(progress, 'balancesheet', args.resume):
                logger.info('Backfilling balancesheet from %s to %s', start_date, end_date)
                save_progress('balancesheet', 'running')
                resume_after = progress.get('cursor_ts_code') if progress and progress.get('phase') == 'balancesheet' else None
                ingest_balancesheet_by_date_range(
                    start_date,
                    end_date,
                    batch_size=args.batch_size,
                    sleep_between=args.sleep_between,
                    start_after_ts_code=resume_after,
                    progress_cb=lambda ts_code, cursor_date=None: save_progress(
                        'balancesheet', 'running', cursor_ts_code=ts_code, cursor_date=cursor_date
                    ),
                )
                save_progress('balancesheet', 'completed', cursor_date=end_date)
                require_tushare_rows('balancesheet', 'balancesheet')

            if should_run_phase(progress, 'cashflow', args.resume):
                logger.info('Backfilling cashflow from %s to %s', start_date, end_date)
                save_progress('cashflow', 'running')
                resume_after = progress.get('cursor_ts_code') if progress and progress.get('phase') == 'cashflow' else None
                ingest_cashflow_by_date_range(
                    start_date,
                    end_date,
                    batch_size=args.batch_size,
                    sleep_between=args.sleep_between,
                    start_after_ts_code=resume_after,
                    progress_cb=lambda ts_code, cursor_date=None: save_progress(
                        'cashflow', 'running', cursor_ts_code=ts_code, cursor_date=cursor_date
                    ),
                )
                save_progress('cashflow', 'completed', cursor_date=end_date)
                require_tushare_rows('cashflow', 'cashflow')

        if not args.skip_vnpy and should_run_phase(progress, 'vnpy', args.resume):
            logger.info('Syncing all stock bars from tushare to vnpy')
            save_progress('vnpy', 'running')
            sync_all_to_vnpy(full_refresh=True)
            save_progress('vnpy', 'completed')

        if should_run_phase(progress, 'sync_status', args.resume):
            logger.info('Skipping sync_status bootstrap; scheduler --init now owns dynamic status reconciliation')
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
        print_summary()
        return 0
    except Exception as exc:
        current_phase = (load_progress() or {}).get('phase', 'unknown')
        logger.exception('Initialization failed at phase=%s: %s', current_phase, exc)
        save_progress(current_phase, 'error', error=str(exc))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
