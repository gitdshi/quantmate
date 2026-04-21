#!/usr/bin/env python3
"""Rebuild legacy payload-shaped Tushare dynamic tables into parsed schemas.

Usage:
    PYTHONPATH=. python3 -m app.datasync.cli.migrate_tushare_dynamic_tables --dry-run
    PYTHONPATH=. python3 -m app.datasync.cli.migrate_tushare_dynamic_tables --table report_rc
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.datasync.service.tushare_dynamic_table_migration import migrate_dynamic_tables  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Migrate legacy payload-shaped Tushare dynamic tables into parsed-column schemas"
    )
    parser.add_argument(
        "--table",
        action="append",
        default=[],
        help="Specific target table to migrate; repeat to select multiple tables",
    )
    parser.add_argument("--dry-run", action="store_true", help="Infer schemas and report without copying or swapping")
    parser.add_argument(
        "--drop-legacy-backup",
        action="store_true",
        help="Drop the renamed legacy payload backup after a successful swap",
    )
    parser.add_argument("--sample-size", type=int, default=200, help="Sample rows used for schema inference")
    parser.add_argument("--batch-size", type=int, default=1000, help="Rows copied per batch during migration")
    args = parser.parse_args()

    results = migrate_dynamic_tables(
        table_names=args.table,
        dry_run=args.dry_run,
        keep_legacy_backup=not args.drop_legacy_backup,
        sample_size=max(int(args.sample_size or 0), 1),
        batch_size=max(int(args.batch_size or 0), 1),
    )

    if not results:
        print("No matching dynamic Tushare tables found.")
        return 0

    exit_code = 0
    for result in results:
        print(
            f"[{result.status}] {result.table_name} "
            f"(rows={result.source_rows}, migrated={result.migrated_rows}, processed={result.records_processed})"
        )
        if result.temp_table:
            print(f"  temp_table: {result.temp_table}")
        if result.backup_table:
            print(f"  backup_table: {result.backup_table}")
        if result.message:
            print(f"  note: {result.message}")
        if result.status == "error":
            exit_code = 1

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
