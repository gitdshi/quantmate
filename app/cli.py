"""QuantMate CLI tool — manage the platform from the command line.

Usage:
    python -m app.cli health
    python -m app.cli db-status
    python -m app.cli sync-status
    python -m app.cli create-user --username admin2 --email admin2@test.com
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Ensure project root is importable
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


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

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return 1

    commands = {
        "health": cmd_health,
        "db-status": cmd_db_status,
        "sync-status": cmd_sync_status,
        "create-user": cmd_create_user,
    }
    return commands[args.command](args)


if __name__ == "__main__":
    sys.exit(main() or 0)
