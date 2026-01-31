"""
Tushare sync daemon

- Runs daily (configurable time) and syncs all accessible endpoints for trading days.
- Records per-day, per-endpoint sync results in `tushare_stock_sync_log`.
- On startup it will detect missed trading days and attempt to sync them.
- Provides a CLI for one-off syncs (date or date-range) and a long-running daemon mode.

Configuration (env):
- TUSHARE_TOKEN (existing)
- SYNC_HOUR (HH:MM) default '02:00' local time
- TUSHARE_CALLS_PER_MIN (existing)
- BATCH_SIZE, MAX_RETRIES (existing)
- DRY_RUN (if set to '1' will not write sync_log entries)

Usage examples:
- Run once for yesterday: `python3 app/services/tushare_sync_daemon.py --once --date 2026-01-29`
- Start daemon: `python3 app/services/tushare_sync_daemon.py --daemon`

"""

import os
import sys
import time
import logging
import argparse
from datetime import datetime, timedelta, date
import importlib.util

logging.basicConfig(level=logging.INFO)

# Import the ingest module (re-use ingest functions and call_pro rate-limiter)
spec = importlib.util.spec_from_file_location('ti','app/services/tushare_ingest.py')
ti = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ti)

from sqlalchemy import text

ENGINE = ti.engine
CALL_PRO = ti.call_pro

SYNC_HOUR = os.getenv('SYNC_HOUR', '02:00')
DRY_RUN = os.getenv('DRY_RUN', '0') == '1'

# Which endpoints the daemon will try to sync (these map to ingest functions in `ti`)
ENDPOINTS = {
    'daily': lambda dt: ti.ingest_all_daily(start_date=None, sleep_between=0.02),
    # For daily we keep using existing per-symbol resume behavior; daemon will call per-date per-symbol instead
    'daily_by_date': None,  # placeholder, implemented below
    'daily_basic': lambda dt: ti.ingest_all_other_data(),  # daily_basic included in that runner
    'adj_factor': lambda dt: ti.ingest_all_other_data(),
    'moneyflow': lambda dt: ti.ingest_all_other_data(),
    'dividend': lambda dt: ti.ingest_all_other_data(),
    'top10_holders': lambda dt: ti.ingest_all_other_data(),
    'margin': lambda dt: ti.ingest_all_other_data(),
    'block_trade': lambda dt: ti.ingest_all_other_data(),
    'repo': lambda dt: ti.ingest_repo(repo_date=dt.strftime('%Y-%m-%d'))
}

# We'll implement a per-date daily sync that loops ts_codes and calls ti.ingest_daily for that date.

def get_trade_days(start_d: date, end_d: date):
    """Return list of trade dates (YYYY-MM-DD) between start_d and end_d inclusive.
    Tries to use Tushare `trade_cal` via `call_pro`; falls back to weekdays if access denied.
    """
    s = start_d.strftime('%Y%m%d')
    e = end_d.strftime('%Y%m%d')
    try:
        df = CALL_PRO('trade_cal', exchange='SSE', start_date=s, end_date=e)
        if df is None:
            raise Exception('trade_cal returned None')
        df = df[df['is_open'] == 1]
        dates = [str(pd.to_datetime(d).date()) for d in df['calendar_date']]
        return dates
    except Exception as exc:
        logging.warning('Could not use trade_cal (fallback to weekdays): %s', exc)
        # fallback to weekdays
        days = []
        cur = start_d
        while cur <= end_d:
            if cur.weekday() < 5:
                days.append(str(cur))
            cur = cur + timedelta(days=1)
        return days


# small helper to insert/update sync_log
def write_sync_log(sync_date: date, endpoint: str, status: str, rows: int = 0, err: str = None):
    if DRY_RUN:
        logging.info('DRY RUN - skip writing sync log: %s %s %s', sync_date, endpoint, status)
        return
    with ENGINE.begin() as conn:
        # try insert; on duplicate update
        sql = text(
            "INSERT INTO tushare_stock_sync_log (sync_date, endpoint, status, rows_synced, error_message, started_at, finished_at)"
            " VALUES (:sd, :ep, :st, :rows, :err, NOW(), NOW())"
            " ON DUPLICATE KEY UPDATE status=VALUES(status), rows_synced=VALUES(rows_synced), error_message=VALUES(error_message), finished_at=NOW()"
        )
        conn.execute(sql, {
            'sd': sync_date.strftime('%Y-%m-%d'),
            'ep': endpoint,
            'st': status,
            'rows': rows,
            'err': err
        })


def get_last_success_date(endpoint: str):
    with ENGINE.connect() as conn:
        res = conn.execute(text("SELECT MAX(sync_date) FROM tushare_stock_sync_log WHERE endpoint=:ep AND status='success'"), {'ep': endpoint})
        row = res.fetchone()
        return row[0] if row and row[0] else None


def sync_daily_for_date(d: date):
    """Sync `daily` data for all ts_codes on a given date by calling `ti.ingest_daily` per symbol.
    This is conservative (calling per-symbol) but respects rate-limiter in `call_pro`.
    """
    logging.info('Starting daily sync for %s', d)
    ts_codes = ti.get_all_ts_codes()
    total = len(ts_codes)
    rows_total = 0
    failures = 0
    for i, ts_code in enumerate(ts_codes, start=1):
        try:
            ti.ingest_daily(ts_code=ts_code, start_date=d.strftime('%Y%m%d'), end_date=d.strftime('%Y%m%d'))
            # the audit table stores rows; we don't aggregate here reliably
        except Exception as e:
            failures += 1
            logging.warning('Failed daily for %s on %s: %s', ts_code, d, e)
        # small sleep to avoid bursts; call_pro already spaces, but this keeps loop friendly
        time.sleep(0.02)
        if i % 500 == 0:
            logging.info('Daily sync progress: %d/%d', i, total)
    status = 'success' if failures == 0 else 'partial' if failures < total else 'error'
    write_sync_log(d, 'daily', status, rows_total, f'failures={failures}' if failures else None)
    logging.info('Daily sync finished for %s: status=%s failures=%d', d, status, failures)


# Map endpoints accessible functions to perform per-date syncing
def run_sync_for_date(d: date, allowed_endpoints: list):
    logging.info('Running sync for date %s, endpoints: %s', d, allowed_endpoints)
    for ep in allowed_endpoints:
        try:
            if ep == 'daily':
                sync_daily_for_date(d)
            elif ep == 'repo':
                rows_before = None
                try:
                    if not DRY_RUN:
                        # call repo with repo_date param
                        ti.ingest_repo(repo_date=d.strftime('%Y-%m-%d'))
                        write_sync_log(d, 'repo', 'success', 0, None)
                except Exception as e:
                    write_sync_log(d, 'repo', 'error', 0, str(e))
            else:
                # many endpoints are grouped under ingest_all_other_data which isn't date-specific; call and record
                try:
                    ti.ingest_daily_basic() if ep == 'daily_basic' else None
                    # For other endpoints fall back to calling ingest_all_other_data once per date
                    if ep in ('daily_basic','adj_factor','moneyflow','dividend','top10_holders','margin','block_trade'):
                        ti.ingest_all_other_data()
                        write_sync_log(d, ep, 'success', 0, None)
                except Exception as e:
                    write_sync_log(d, ep, 'error', 0, str(e))
        except Exception as e:
            logging.exception('Error syncing endpoint %s for %s: %s', ep, d, e)
            write_sync_log(d, ep, 'error', 0, str(e))


def probe_allowed_endpoints():
    # Probe a list of endpoints to see which APIs the token can access
    candidates = ['daily', 'daily_basic', 'adj_factor', 'moneyflow', 'dividend', 'top10_holders', 'margin', 'block_trade', 'repo']
    allowed = []
    sample_ts = None
    all_codes = ti.get_all_ts_codes()
    if all_codes:
        sample_ts = all_codes[0]
    for a in candidates:
        try:
            if a == 'repo':
                CALL_PRO('repo')
            elif a == 'daily':
                # daily is basic and usually allowed via previous runs; assume allowed if call_pro('daily') works
                if sample_ts:
                    CALL_PRO('daily', ts_code=sample_ts, start_date=None, end_date=None)
                else:
                    # no ts_codes yet, mark daily allowed
                    allowed.append('daily')
                    continue
            else:
                # other endpoints require ts_code sample
                if sample_ts:
                    CALL_PRO(a, ts_code=sample_ts)
                else:
                    # cannot probe without sample, skip
                    continue
            allowed.append(a)
        except Exception as e:
            msg = str(e)
            if '没有接口访问权限' in msg or '权限' in msg:
                logging.info('API not permitted: %s', a)
            else:
                logging.info('API probe conditional skip: %s -> %s', a, msg[:200])
    logging.info('Allowed endpoints after probe: %s', allowed)
    return allowed


def find_missing_trade_dates(allowed_endpoints: list, lookback_days: int = 30):
    """Compute which trade dates need syncing for endpoints based on `tushare_stock_sync_log`.
    We'll look back up to `lookback_days` and return trade dates that have not been marked success for at least one endpoint.
    """
    today = date.today()
    end = today - timedelta(days=1)
    start = end - timedelta(days=lookback_days)
    # get trade days in range
    trade_days = get_trade_days(start, end)
    missing = []
    for td in trade_days:
        td_date = datetime.strptime(td, '%Y-%m-%d').date()
        with ENGINE.connect() as conn:
            for ep in allowed_endpoints:
                res = conn.execute(text("SELECT 1 FROM tushare_stock_sync_log WHERE sync_date=:d AND endpoint=:ep AND status='success' LIMIT 1"), {'d': td_date.strftime('%Y-%m-%d'), 'ep': ep})
                if res.fetchone() is None:
                    missing.append(td_date)
                    break
    missing_sorted = sorted(set(missing))
    logging.info('Missing trade dates to sync: %s', missing_sorted)
    return missing_sorted


def run_daemon_loop():
    # probe allowed endpoints
    allowed = probe_allowed_endpoints()
    if not allowed:
        logging.warning('No endpoints allowed for this token; daemon exiting')
        return

    # on startup handle missing trade dates (last 30 days)
    missing = find_missing_trade_dates(allowed, lookback_days=60)
    for d in missing:
        run_sync_for_date(d, allowed)

    # compute next run time daily at SYNC_HOUR
    hh, mm = map(int, SYNC_HOUR.split(':'))
    logging.info('Daemon scheduled to run daily at %02d:%02d', hh, mm)
    while True:
        now = datetime.now()
        next_run = datetime(now.year, now.month, now.day, hh, mm)
        if next_run <= now:
            next_run += timedelta(days=1)
        to_sleep = (next_run - now).total_seconds()
        logging.info('Sleeping until next run at %s (%.0fs)', next_run.isoformat(), to_sleep)
        time.sleep(to_sleep)
        # run for yesterday (market data for previous trading day)
        run_date = date.today() - timedelta(days=1)
        run_sync_for_date(run_date, allowed)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--daemon', action='store_true', help='Run as a long-running daemon')
    p.add_argument('--once', action='store_true', help='Run once and exit')
    p.add_argument('--date', type=str, help='Sync specific date (YYYY-MM-DD)')
    p.add_argument('--from', dest='from_date', type=str, help='Start date for range (YYYY-MM-DD)')
    p.add_argument('--to', dest='to_date', type=str, help='End date for range (YYYY-MM-DD)')
    return p.parse_args()


if __name__ == '__main__':
    args = parse_args()
    allowed = probe_allowed_endpoints()
    if args.daemon:
        run_daemon_loop()
    elif args.once:
        if args.date:
            d = datetime.strptime(args.date, '%Y-%m-%d').date()
            run_sync_for_date(d, allowed)
        elif args.from_date and args.to_date:
            s = datetime.strptime(args.from_date, '%Y-%m-%d').date()
            e = datetime.strptime(args.to_date, '%Y-%m-%d').date()
            days = get_trade_days(s, e)
            for dd in days:
                run_sync_for_date(datetime.strptime(dd, '%Y-%m-%d').date(), allowed)
        else:
            # default run for yesterday
            run_sync_for_date(date.today() - timedelta(days=1), allowed)
    else:
        # default to a single run for yesterday
        run_sync_for_date(date.today() - timedelta(days=1), allowed)
