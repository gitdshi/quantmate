#!/usr/bin/env python3
"""Initialize/rebuild TraderMate market data after DB loss.

This script supports resumable initialization via `init_progress` checkpoints.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, datetime
from pathlib import Path

from sqlalchemy import text, create_engine
from sqlalchemy.engine.url import make_url


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.datasync.service.tushare_ingest import (
    ingest_stock_basic,
    ingest_all_daily,
    ingest_adj_factor_by_date_range,
    ingest_dividend_by_date_range,
    ingest_top10_holders_by_date_range,
)
from app.datasync.service.akshare_ingest import ingest_all_indexes
from app.datasync.service.vnpy_ingest import sync_all_to_vnpy
from app.datasync.service.data_sync_daemon import initialize_sync_status_table
from app.infrastructure.config import get_settings


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROGRESS_ID = 1
PHASES = [
    'schema',
    'stock_basic',
    'daily',
    'indexes',
    'adj_factor',
    'dividend',
    'top10_holders',
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
    settings = get_settings()
    mysql_url = os.getenv('MYSQL_URL', settings.mysql_url)
    url = make_url(mysql_url)
    admin_url = url.set(database=None)
    return create_engine(admin_url, pool_pre_ping=True)


def get_tradermate_engine():
    settings = get_settings()
    return create_engine(settings.mysql_url + '/tradermate', pool_pre_ping=True)


def ensure_init_progress_table() -> None:
    engine = get_tradermate_engine()
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
    engine = get_tradermate_engine()
    sql = text(
        "SELECT phase, cursor_ts_code, cursor_date, status, error, updated_at "
        "FROM init_progress WHERE id = :id"
    )
    with engine.connect() as conn:
        row = conn.execute(sql, {'id': PROGRESS_ID}).mappings().first()
    return dict(row) if row else None


def save_progress(phase: str, status: str, cursor_ts_code: str | None = None, cursor_date: str | None = None, error: str | None = None) -> None:
    engine = get_tradermate_engine()
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


def reset_progress() -> None:
    engine = get_tradermate_engine()
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
        ROOT / 'mysql' / 'init' / 'tradermate.sql',
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
    engine = get_tradermate_engine()
    checks = [
        ('tushare.stock_basic', 'SELECT COUNT(*) FROM tushare.stock_basic'),
        ('tushare.stock_daily', 'SELECT COUNT(*) FROM tushare.stock_daily'),
        ('tushare.adj_factor', 'SELECT COUNT(*) FROM tushare.adj_factor'),
        ('akshare.index_daily', 'SELECT COUNT(*) FROM akshare.index_daily'),
        ('vnpy.dbbardata', 'SELECT COUNT(*) FROM vnpy.dbbardata'),
    ]
    logger.info('Recovery summary (row counts):')
    with engine.connect() as conn:
        for name, sql in checks:
            value = conn.execute(text(sql)).scalar() or 0
            logger.info('  %-22s %s', name + ':', f'{value:,}')


def main() -> int:
    parser = argparse.ArgumentParser(description='Initialize TraderMate market data')
    parser.add_argument('--start-date', default='2005-01-01', help='Start date for aux backfill (YYYY-MM-DD)')
    parser.add_argument('--skip-schema', action='store_true', help='Skip schema initialization SQL')
    parser.add_argument('--skip-aux', action='store_true', help='Skip adj/dividend/top10 backfill')
    parser.add_argument('--skip-vnpy', action='store_true', help='Skip vnpy full sync')
    parser.add_argument('--stock-statuses', default='L', help='Comma-separated stock_basic list_status values (e.g. L or L,D,P)')
    parser.add_argument('--batch-size', type=int, default=int(os.getenv('BATCH_SIZE', '100')))
    parser.add_argument('--sleep-between', type=float, default=0.02)
    parser.add_argument('--resume', action='store_true', default=True, help='Resume from init_progress checkpoint (default on)')
    parser.add_argument('--reset-progress', action='store_true', help='Reset init_progress before execution')
    args = parser.parse_args()

    end_date = date.today().isoformat()
    start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date().isoformat()

    logger.info('Starting market data initialization (start_date=%s, end_date=%s)', start_date, end_date)

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

    try:
        if should_run_phase(progress, 'stock_basic', args.resume):
            logger.info('Rebuilding Tushare stock_basic (statuses=%s)', ','.join(statuses))
            save_progress('stock_basic', 'running')
            resume_after = progress.get('cursor_ts_code') if progress and progress.get('phase') == 'stock_basic' else None
            skip_until_found = bool(resume_after)
            for status in statuses:
                if skip_until_found:
                    if status == resume_after:
                        skip_until_found = False
                    continue
                save_progress('stock_basic', 'running', cursor_ts_code=status)
                try:
                    ingest_stock_basic(list_status=status)
                except Exception as exc:
                    logger.warning('stock_basic ingest failed for list_status=%s: %s', status, exc)
                    if status == 'L':
                        raise
            save_progress('stock_basic', 'done')

        if should_run_phase(progress, 'daily', args.resume):
            logger.info('Rebuilding full Tushare stock_daily history (this can take a long time)')
            save_progress('daily', 'running')
            resume_after = progress.get('cursor_ts_code') if progress and progress.get('phase') == 'daily' else None
            ingest_all_daily(
                batch_size=args.batch_size,
                sleep_between=args.sleep_between,
                force_full_per_stock=True,
                start_after_ts_code=resume_after,
                progress_cb=lambda ts_code, cursor_date=None: save_progress('daily', 'running', cursor_ts_code=ts_code, cursor_date=cursor_date),
            )
            save_progress('daily', 'done')

        if should_run_phase(progress, 'indexes', args.resume):
            logger.info('Rebuilding AkShare index history')
            save_progress('indexes', 'running')
            ingest_all_indexes()
            save_progress('indexes', 'done')

        if not args.skip_aux:
            if should_run_phase(progress, 'adj_factor', args.resume):
                logger.info('Backfilling adj_factor from %s to %s', start_date, end_date)
                save_progress('adj_factor', 'running', cursor_date=end_date)
                resume_after = progress.get('cursor_ts_code') if progress and progress.get('phase') == 'adj_factor' else None
                ingest_adj_factor_by_date_range(
                    start_date,
                    end_date,
                    batch_size=args.batch_size,
                    sleep_between=args.sleep_between,
                    start_after_ts_code=resume_after,
                    progress_cb=lambda ts_code, cursor_date=None: save_progress('adj_factor', 'running', cursor_ts_code=ts_code, cursor_date=cursor_date),
                )
                save_progress('adj_factor', 'done', cursor_date=end_date)

            if should_run_phase(progress, 'dividend', args.resume):
                logger.info('Backfilling dividend from %s to %s', start_date, end_date)
                save_progress('dividend', 'running', cursor_date=end_date)
                resume_after = progress.get('cursor_ts_code') if progress and progress.get('phase') == 'dividend' else None
                ingest_dividend_by_date_range(
                    start_date,
                    end_date,
                    batch_size=args.batch_size,
                    sleep_between=args.sleep_between,
                    start_after_ts_code=resume_after,
                    progress_cb=lambda ts_code, cursor_date=None: save_progress('dividend', 'running', cursor_ts_code=ts_code, cursor_date=cursor_date),
                )
                save_progress('dividend', 'done', cursor_date=end_date)

            if should_run_phase(progress, 'top10_holders', args.resume):
                logger.info('Backfilling top10_holders from %s to %s', start_date, end_date)
                save_progress('top10_holders', 'running', cursor_date=end_date)
                resume_after = progress.get('cursor_ts_code') if progress and progress.get('phase') == 'top10_holders' else None
                ingest_top10_holders_by_date_range(
                    start_date,
                    end_date,
                    batch_size=args.batch_size,
                    sleep_between=args.sleep_between,
                    start_after_ts_code=resume_after,
                    progress_cb=lambda ts_code, cursor_date=None: save_progress('top10_holders', 'running', cursor_ts_code=ts_code, cursor_date=cursor_date),
                )
                save_progress('top10_holders', 'done', cursor_date=end_date)

        if not args.skip_vnpy and should_run_phase(progress, 'vnpy', args.resume):
            logger.info('Syncing all stock bars from tushare to vnpy')
            save_progress('vnpy', 'running')
            sync_all_to_vnpy(full_refresh=True)
            save_progress('vnpy', 'done')

        if should_run_phase(progress, 'sync_status', args.resume):
            lookback_years = max(1, date.today().year - datetime.strptime(start_date, '%Y-%m-%d').year + 1)
            logger.info('Initializing data_sync_status table (lookback_years=%d)', lookback_years)
            save_progress('sync_status', 'running')
            initialize_sync_status_table(lookback_years=lookback_years)
            save_progress('sync_status', 'done')

        save_progress('finished', 'completed')
        print_summary()
        logger.info('Market data initialization finished')
        return 0
    except Exception as exc:
        current_phase = (load_progress() or {}).get('phase', 'unknown')
        logger.exception('Initialization failed at phase=%s: %s', current_phase, exc)
        save_progress(current_phase, 'error', error=str(exc))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
