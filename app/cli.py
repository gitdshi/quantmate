"""QuantMate CLI tool — manage the platform from the command line.

Usage:
    python -m app.cli health
    python -m app.cli db-status
    python -m app.cli sync-status
    python -m app.cli create-user --username admin2 --email admin2@test.com
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

# Ensure project root is importable
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _parse_cli_bool(raw: str | None) -> bool | None:
    text = str(raw or "").strip().lower()
    if not text:
        return None
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return None


def _split_csv_field(raw: str | None) -> list[str]:
    if not raw:
        return []
    normalized = str(raw).replace(";", ",").replace("|", ",")
    return [part.strip() for part in normalized.split(",") if part and part.strip()]


def _build_input_params_meta(
    input_params: str | None,
    analysis_date_params: str | None,
    supports_backfill: bool | None,
    backfill_mode: str | None,
) -> dict[str, object]:
    return {
        "input_params": _split_csv_field(input_params),
        "analysis_date_params": _split_csv_field(analysis_date_params),
        "supports_backfill": supports_backfill,
        "backfill_mode": backfill_mode,
    }


def _format_source_item_key(source: str, item_key: str) -> str:
    return f"{source}/{item_key}"


def _summarize_source_item_keys(keys: list[tuple[str, str]], limit: int = 10) -> str:
    preview = ", ".join(_format_source_item_key(source, item_key) for source, item_key in keys[:limit])
    if len(keys) <= limit:
        return preview
    return f"{preview} ... and {len(keys) - limit} more"


def _build_backfill_analysis_items(csv_path: Path) -> list[dict[str, object]]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        items: list[dict[str, object]] = []
        seen_keys: set[tuple[str, str]] = set()
        for row in reader:
            source = str(row.get("source") or "").strip()
            item_key = str(row.get("interface") or row.get("item_key") or "").strip()
            if not source or not item_key:
                continue

            key = (source, item_key)
            if key in seen_keys:
                raise ValueError(f"Duplicate backfill-analysis row for {_format_source_item_key(source, item_key)}")
            seen_keys.add(key)

            supports_backfill = _parse_cli_bool(row.get("supports_backfill"))
            backfill_mode = str(row.get("backfill_mode") or "").strip().lower() or None
            input_params = str(row.get("input_params") or "").strip() or None
            input_param_details = str(row.get("input_param_details") or "").strip() or None
            analysis_date_params = str(row.get("analysis_date_params") or "").strip() or None

            meta = _build_input_params_meta(
                input_params,
                analysis_date_params,
                supports_backfill,
                backfill_mode,
            )

            items.append(
                {
                    "source": source,
                    "item_key": item_key,
                    "supports_backfill": None if supports_backfill is None else int(supports_backfill),
                    "backfill_mode": backfill_mode,
                    "input_params": input_params,
                    "input_param_details": input_param_details,
                    "analysis_date_params": analysis_date_params,
                    "input_params_meta": meta,
                }
            )
        return items


def cmd_import_backfill_analysis(args):
    """Import analyzed backfill metadata CSV into data_source_items."""
    from app.domains.market.dao.data_source_item_dao import DataSourceItemDao

    csv_path = Path(args.csv_path).expanduser().resolve()
    if not csv_path.exists():
        print(f"CSV not found: {csv_path}")
        return 1

    try:
        items = _build_backfill_analysis_items(csv_path)
    except Exception as e:
        print(f"Failed to parse CSV: {e}")
        return 1

    if not items:
        print(f"No backfill-analysis rows found in {csv_path}")
        return 1

    dao = DataSourceItemDao()
    try:
        missing_keys = dao.find_missing_backfill_analysis_items(items)
    except Exception as e:
        print(f"Failed to validate backfill analysis rows: {e}")
        return 1

    if missing_keys:
        print(
            "Backfill analysis rows do not match existing data_source_items: "
            f"{_summarize_source_item_keys(missing_keys)}"
        )
        return 1

    if getattr(args, "dry_run", False):
        print(f"Validated {len(items)} rows from {csv_path}")
        preview = items[:3]
        for item in preview:
            print(json.dumps(item, ensure_ascii=False))
        if len(items) > len(preview):
            print(f"... {len(items) - len(preview)} more rows")
        return 0

    try:
        updated = dao.batch_update_backfill_analysis(items)
    except Exception as e:
        print(f"Failed to import backfill analysis: {e}")
        return 1

    if updated != len(items):
        print(
            "Backfill analysis import did not update every validated row: "
            f"expected {len(items)}, updated {updated}"
        )
        return 1

    print(f"Imported backfill analysis for {updated} rows from {csv_path}")
    return 0


def cmd_health(args):
    """Check API and database health."""
    try:
        from app.infrastructure.db.connections import connection

        with connection("quantmate") as conn:
            from sqlalchemy import text

            conn.execute(text("SELECT 1"))
        print("Database: OK")
    except Exception as e:
        print(f"Database: FAILED - {e}")
        return 1

    try:
        import redis
        from app.infrastructure.config import get_runtime_float, get_settings

        settings = get_settings()
        r = redis.Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            socket_timeout=get_runtime_float(
                env_keys="CLI_REDIS_SOCKET_TIMEOUT_SECONDS",
                db_key="cli.redis_socket_timeout_seconds",
                default=2.0,
            ),
        )
        r.ping()
        print("Redis: OK")
    except Exception:
        print("Redis: UNAVAILABLE (non-critical)")

    print("Health check passed")
    return 0


def cmd_db_status(args):
    """Show database table counts and migration status."""
    try:
        from app.infrastructure.db.connections import connection
        from sqlalchemy import text

        for db_name in ("quantmate", "tushare", "akshare"):
            try:
                with connection(db_name) as conn:
                    rows = conn.execute(text("SHOW TABLES")).fetchall()
                    print(f"\n{db_name}: {len(rows)} tables")
                    for r in rows:
                        table = list(r)[0]
                        try:
                            count = conn.execute(text(f"SELECT COUNT(*) FROM `{table}`")).fetchone()
                            print(f"  {table}: {count[0]} rows")
                        except Exception:
                            print(f"  {table}: (error reading)")
            except Exception as e:
                print(f"\n{db_name}: FAILED - {e}")
    except Exception as e:
        print(f"Error: {e}")
        return 1
    return 0


def cmd_sync_status(args):
    """Show data sync checkpoint status."""
    try:
        from app.infrastructure.db.connections import connection
        from sqlalchemy import text

        with connection("quantmate") as conn:
            rows = conn.execute(
                text("SELECT item_key, last_sync_date, status, error_count FROM data_source_items ORDER BY item_key")
            ).fetchall()
            print(f"{'Item':<30} {'Last Sync':<20} {'Status':<12} {'Errors'}")
            print("-" * 75)
            for r in rows:
                m = r._mapping
                print(
                    f"{m['item_key']:<30} {str(m.get('last_sync_date', 'N/A')):<20} {m.get('status', 'N/A'):<12} {m.get('error_count', 0)}"
                )
    except Exception as e:
        print(f"Error: {e}")
        return 1
    return 0


def cmd_create_user(args):
    """Create a new user."""
    try:
        from app.domains.auth.dao.user_dao import UserDao
        from app.api.services.auth_service import get_password_hash
        from datetime import datetime

        if not args.password:
            import secrets

            args.password = secrets.token_urlsafe(16)
            print(f"Generated password: {args.password}")

        dao = UserDao()
        existing = dao.get_user_for_login(args.username)
        if existing:
            print(f"User '{args.username}' already exists")
            return 1

        hashed = get_password_hash(args.password)
        dao.insert_user(
            username=args.username,
            email=args.email,
            hashed_password=hashed,
            created_at=datetime.utcnow(),
            must_change_password=True,
        )
        print(f"User '{args.username}' created successfully")
    except Exception as e:
        print(f"Error: {e}")
        return 1
    return 0


def main():
    parser = argparse.ArgumentParser(prog="quantmate", description="QuantMate CLI")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    subparsers.add_parser("health", help="Check system health")
    subparsers.add_parser("db-status", help="Show database status")
    subparsers.add_parser("sync-status", help="Show data sync status")

    user_parser = subparsers.add_parser("create-user", help="Create a new user")
    user_parser.add_argument("--username", required=True)
    user_parser.add_argument("--email", required=True)
    user_parser.add_argument("--password", default=None)

    import_parser = subparsers.add_parser("import-backfill-analysis", help="Import backfill analysis CSV into data_source_items")
    import_parser.add_argument(
        "--csv-path",
        default=str(ROOT / "app" / "datasync" / "metadata" / "tushare_backfill_analysis.csv"),
    )
    import_parser.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return 1

    commands = {
        "health": cmd_health,
        "db-status": cmd_db_status,
        "sync-status": cmd_sync_status,
        "create-user": cmd_create_user,
        "import-backfill-analysis": cmd_import_backfill_analysis,
    }
    return commands[args.command](args)


if __name__ == "__main__":
    sys.exit(main() or 0)
