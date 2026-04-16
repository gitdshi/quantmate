#!/usr/bin/env python3
"""Validate and optionally repair key data_sync_status records.

Usage:
    PYTHONPATH=. python3 -m app.datasync.cli.validate_sync_status --days 60
    PYTHONPATH=. python3 -m app.datasync.cli.validate_sync_status --days 60 --fix
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ["LOG_LEVEL"] = "ERROR"

from app.infrastructure.db.connections import (  # noqa: E402
    get_akshare_engine,
    get_quantmate_engine,
    get_tushare_engine,
    get_vnpy_engine,
)

engine_tm = get_quantmate_engine()
engine_ts = get_tushare_engine()
engine_ak = get_akshare_engine()
engine_vn = get_vnpy_engine()

try:
    import akshare as ak

    AKSHARE_AVAILABLE = True
except ImportError:
    AKSHARE_AVAILABLE = False


Checker = Callable[[date], tuple[bool, int]]
ALWAYS_SUCCESS_EMPTY = {
    ("tushare", "dividend"),
    ("tushare", "top10_holders"),
}


def get_trade_dates(start: date, end: date) -> list[date]:
    """Get trade dates from cached calendar or AkShare."""
    try:
        with engine_ak.connect() as conn:
            res = conn.execute(
                text(
                    "SELECT trade_date FROM trade_cal "
                    "WHERE trade_date BETWEEN :s AND :e AND is_trade_day = 1 "
                    "ORDER BY trade_date ASC"
                ),
                {"s": start, "e": end},
            )
            dates = [row[0] for row in res.fetchall()]
            if dates:
                return dates
    except Exception:
        pass

    if AKSHARE_AVAILABLE:
        try:
            import pandas as pd

            df = ak.tool_trade_date_hist_sina()
            if df is not None and not df.empty:
                df["trade_date"] = pd.to_datetime(df["trade_date"])
                mask = (df["trade_date"].dt.date >= start) & (df["trade_date"].dt.date <= end)
                return df[mask]["trade_date"].dt.date.tolist()
        except Exception as exc:
            print(f"Warning: AkShare trade calendar failed: {exc}")

    days: list[date] = []
    current = start
    while current <= end:
        if current.weekday() < 5:
            days.append(current)
        current += timedelta(days=1)
    return days


def check_akshare_index(trade_date: date) -> tuple[bool, int]:
    with engine_ak.connect() as conn:
        count = conn.execute(
            text("SELECT COUNT(*) FROM index_daily WHERE trade_date = :d"),
            {"d": trade_date},
        ).scalar() or 0
    return count > 0, int(count)


def check_tushare_stock_basic() -> tuple[bool, int]:
    with engine_ts.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM stock_basic")).scalar() or 0
    return count > 0, int(count)


def check_tushare_stock_daily(trade_date: date) -> tuple[bool, int]:
    with engine_ts.connect() as conn:
        count = conn.execute(
            text("SELECT COUNT(*) FROM stock_daily WHERE trade_date = :d"),
            {"d": trade_date},
        ).scalar() or 0
    return count > 0, int(count)


def check_tushare_adj_factor(trade_date: date) -> tuple[bool, int]:
    with engine_ts.connect() as conn:
        count = conn.execute(
            text("SELECT COUNT(*) FROM adj_factor WHERE trade_date = :d"),
            {"d": trade_date},
        ).scalar() or 0
    return count > 0, int(count)


def check_tushare_dividend(trade_date: date) -> tuple[bool, int]:
    with engine_ts.connect() as conn:
        count = conn.execute(
            text("SELECT COUNT(*) FROM stock_dividend WHERE ann_date = :d"),
            {"d": trade_date},
        ).scalar() or 0
    return True, int(count)


def check_tushare_top10_holders(trade_date: date) -> tuple[bool, int]:
    with engine_ts.connect() as conn:
        count = conn.execute(
            text("SELECT COUNT(*) FROM top10_holders WHERE end_date = :d"),
            {"d": trade_date},
        ).scalar() or 0
    return True, int(count)


def check_vnpy_sync(trade_date: date) -> tuple[bool, int]:
    with engine_vn.connect() as conn:
        count = conn.execute(
            text("SELECT COUNT(*) FROM dbbardata WHERE DATE(`datetime`) = :d"),
            {"d": trade_date},
        ).scalar() or 0
    return count > 0, int(count)


INTERFACE_CHECKERS: dict[tuple[str, str], Checker] = {
    ("akshare", "index_daily"): check_akshare_index,
    ("tushare", "stock_daily"): check_tushare_stock_daily,
    ("tushare", "adj_factor"): check_tushare_adj_factor,
    ("tushare", "dividend"): check_tushare_dividend,
    ("tushare", "top10_holders"): check_tushare_top10_holders,
    ("vnpy", "vnpy_sync"): check_vnpy_sync,
}


def get_interface_status(sync_date: date, source: str, item_key: str) -> str | None:
    with engine_tm.connect() as conn:
        res = conn.execute(
            text(
                "SELECT status FROM data_sync_status "
                "WHERE sync_date = :sd AND source = :source AND interface_key = :item_key"
            ),
            {"sd": sync_date, "source": source, "item_key": item_key},
        )
        row = res.fetchone()
        return row[0] if row else None


def write_interface_status(
    sync_date: date,
    source: str,
    item_key: str,
    status: str,
    rows_synced: int,
    error_message: str | None,
) -> None:
    with engine_tm.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO data_sync_status "
                "(sync_date, source, interface_key, status, rows_synced, error_message, started_at, finished_at) "
                "VALUES (:sd, :source, :item_key, :status, :rows, :err, NOW(), NOW()) "
                "ON DUPLICATE KEY UPDATE "
                "status = VALUES(status), "
                "rows_synced = VALUES(rows_synced), "
                "error_message = VALUES(error_message), "
                "finished_at = VALUES(finished_at), "
                "updated_at = CURRENT_TIMESTAMP"
            ),
            {
                "sd": sync_date,
                "source": source,
                "item_key": item_key,
                "status": status,
                "rows": rows_synced,
                "err": error_message,
            },
        )


def validate_and_fix(start_date: date, end_date: date, fix: bool = False):
    trade_dates = get_trade_dates(start_date, end_date)
    print(f"Validating {len(trade_dates)} trade dates from {start_date} to {end_date}")

    stock_basic_exists, stock_basic_count = check_tushare_stock_basic()
    print(f"\nStock Basic: {stock_basic_count} records")

    discrepancies = []
    total_checks = 0
    total_fixed = 0

    for trade_date in trade_dates:
        print(f"\n--- Checking {trade_date} ---")

        for (source, item_key), checker in INTERFACE_CHECKERS.items():
            total_checks += 1
            has_data, count = checker(trade_date)
            recorded_status = get_interface_status(trade_date, source, item_key)

            if has_data and count > 0:
                expected_status = "success"
            elif (source, item_key) in ALWAYS_SUCCESS_EMPTY:
                expected_status = "success"
            else:
                expected_status = None

            if recorded_status != expected_status:
                discrepancy_type = f"{recorded_status or 'NULL'} -> {expected_status or 'NULL'}"
                discrepancies.append((trade_date, source, item_key, recorded_status, expected_status, count))
                print(f"  ERR {source}/{item_key}: {discrepancy_type} (actual count: {count})")

                if fix and expected_status:
                    try:
                        write_interface_status(trade_date, source, item_key, expected_status, count, None)
                        total_fixed += 1
                        print(f"     FIXED to {expected_status}")
                    except Exception as exc:
                        print(f"     FIX FAILED: {exc}")
            else:
                print(f"  OK  {source}/{item_key}: {recorded_status} (count: {count})")

        recorded_basic = get_interface_status(trade_date, "tushare", "stock_basic")
        expected_basic = "success" if stock_basic_exists else None
        if recorded_basic != expected_basic:
            discrepancies.append(
                (trade_date, "tushare", "stock_basic", recorded_basic, expected_basic, stock_basic_count)
            )
            print(f"  ERR tushare/stock_basic: {recorded_basic or 'NULL'} -> {expected_basic or 'NULL'}")
            if fix and expected_basic:
                try:
                    write_interface_status(trade_date, "tushare", "stock_basic", expected_basic, stock_basic_count, None)
                    total_fixed += 1
                    print("     FIXED to success")
                except Exception as exc:
                    print(f"     FIX FAILED: {exc}")

    print(f"\n{'=' * 60}")
    print("Validation Summary:")
    print(f"  Total checks: {total_checks}")
    print(f"  Discrepancies: {len(discrepancies)}")
    if fix:
        print(f"  Fixed: {total_fixed}")
    print(f"{'=' * 60}")

    if discrepancies and not fix:
        print("\nRun with --fix to update data_sync_status")

    return discrepancies


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate and repair data_sync_status")
    parser.add_argument("--days", type=int, default=60, help="Number of days to check (default: 60)")
    parser.add_argument("--fix", action="store_true", help="Fix discrepancies (update data_sync_status)")
    parser.add_argument("--start", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="End date (YYYY-MM-DD)")
    args = parser.parse_args()

    if args.start and args.end:
        start_date = datetime.strptime(args.start, "%Y-%m-%d").date()
        end_date = datetime.strptime(args.end, "%Y-%m-%d").date()
    else:
        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=args.days - 1)

    validate_and_fix(start_date, end_date, fix=args.fix)


if __name__ == "__main__":
    main()
