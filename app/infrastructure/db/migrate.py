"""SQL Migration Runner (Issue #15).

Applies numbered SQL migration scripts from mysql/migrations/ in order.
Tracks applied migrations in the ``schema_migrations`` table.

Usage:
    python -m app.infrastructure.db.migrate          # apply pending migrations
    python -m app.infrastructure.db.migrate --status  # show migration status
"""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path

from sqlalchemy import text

from app.infrastructure.db.connections import get_quantmate_engine

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "mysql" / "migrations"


def _ensure_migration_table(conn):
    """Create the schema_migrations table if it doesn't exist."""
    conn.execute(
        text("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version VARCHAR(14) NOT NULL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            checksum VARCHAR(64)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    )
    conn.commit()


def _get_applied(conn) -> set[str]:
    """Return set of already-applied migration versions."""
    rows = conn.execute(text("SELECT version FROM schema_migrations")).fetchall()
    return {row[0] for row in rows}


def _discover_migrations() -> list[tuple[str, Path]]:
    """Discover migration files sorted by filename.

    Returns list of (version, path) tuples.
    Version is extracted from the numeric prefix of the filename.
    """
    if not MIGRATIONS_DIR.exists():
        return []
    files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    results = []
    seen_versions: dict[str, Path] = {}
    for f in files:
        # Extract version from filename: 001_xxx.sql → "001"
        parts = f.stem.split("_", 1)
        version = parts[0]
        if version in seen_versions:
            prev = seen_versions[version]
            raise ValueError(
                f"Duplicate migration version '{version}' found in {prev.name} and {f.name}"
            )
        seen_versions[version] = f
        results.append((version, f))
    return results


def _file_checksum(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _split_sql_statements(sql_content: str) -> list[str]:
    """Split SQL into statements without breaking on semicolons in strings/comments."""
    statements: list[str] = []
    buf: list[str] = []
    in_single = False
    in_double = False
    in_line_comment = False
    in_block_comment = False
    i = 0
    length = len(sql_content)

    while i < length:
        ch = sql_content[i]
        nxt = sql_content[i + 1] if i + 1 < length else ""

        if in_line_comment:
            buf.append(ch)
            if ch == "\n":
                in_line_comment = False
            i += 1
            continue

        if in_block_comment:
            buf.append(ch)
            if ch == "*" and nxt == "/":
                buf.append(nxt)
                in_block_comment = False
                i += 2
            else:
                i += 1
            continue

        if not in_single and not in_double:
            if ch == "-" and nxt == "-":
                buf.append(ch)
                buf.append(nxt)
                in_line_comment = True
                i += 2
                continue
            if ch == "/" and nxt == "*":
                buf.append(ch)
                buf.append(nxt)
                in_block_comment = True
                i += 2
                continue

        if ch == "'" and not in_double:
            buf.append(ch)
            if in_single and nxt == "'":
                buf.append(nxt)
                i += 2
                continue
            in_single = not in_single
            i += 1
            continue

        if ch == '"' and not in_single:
            buf.append(ch)
            if in_double and nxt == '"':
                buf.append(nxt)
                i += 2
                continue
            in_double = not in_double
            i += 1
            continue

        if ch == ";" and not in_single and not in_double:
            stmt = "".join(buf).strip()
            if stmt:
                statements.append(stmt)
            buf = []
            i += 1
            continue

        buf.append(ch)
        i += 1

    tail = "".join(buf).strip()
    if tail:
        statements.append(tail)

    return statements


def _strip_leading_sql_comments(stmt: str) -> str:
    """Remove only leading SQL comments so commented statements still execute."""
    i = 0
    length = len(stmt)
    while i < length:
        while i < length and stmt[i].isspace():
            i += 1
        if stmt.startswith("--", i):
            newline = stmt.find("\n", i)
            if newline == -1:
                return ""
            i = newline + 1
            continue
        if stmt.startswith("/*", i):
            end = stmt.find("*/", i + 2)
            if end == -1:
                return ""
            i = end + 2
            continue
        break
    return stmt[i:].strip()


def apply_migrations(dry_run: bool = False) -> list[str]:
    """Apply all pending migrations. Returns list of applied versions."""
    engine = get_quantmate_engine()
    applied = []

    with engine.connect() as conn:
        _ensure_migration_table(conn)
        already_applied = _get_applied(conn)
        migrations = _discover_migrations()

        for version, path in migrations:
            if version in already_applied:
                continue

            sql_content = path.read_text(encoding="utf-8")
            checksum = _file_checksum(path)

            if dry_run:
                print(f"[DRY-RUN] Would apply: {path.name} (v{version})")
                applied.append(version)
                continue

            print(f"Applying migration: {path.name} ...")
            # Split safely so embedded strategy code strings can contain semicolons.
            for stmt in _split_sql_statements(sql_content):
                executable = _strip_leading_sql_comments(stmt)
                if not executable:
                    continue
                conn.exec_driver_sql(executable)

            # Record migration
            conn.execute(
                text("INSERT INTO schema_migrations (version, name, checksum) VALUES (:version, :name, :checksum)"),
                {"version": version, "name": path.name, "checksum": checksum},
            )
            conn.commit()
            applied.append(version)
            print(f"  ✓ Applied {path.name}")

    return applied


def show_status():
    """Print migration status."""
    engine = get_quantmate_engine()
    with engine.connect() as conn:
        _ensure_migration_table(conn)
        already_applied = _get_applied(conn)
        migrations = _discover_migrations()

        print(f"{'Version':<16} {'Name':<50} {'Status'}")
        print("-" * 80)
        for version, path in migrations:
            status = "✓ applied" if version in already_applied else "  pending"
            print(f"{version:<16} {path.name:<50} {status}")


if __name__ == "__main__":
    if "--status" in sys.argv:
        show_status()
    elif "--dry-run" in sys.argv:
        applied = apply_migrations(dry_run=True)
        print(f"\n{len(applied)} migration(s) would be applied.")
    else:
        applied = apply_migrations()
        print(f"\n{len(applied)} migration(s) applied.")
