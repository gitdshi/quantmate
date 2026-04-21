"""Dynamic table creation manager for data source interfaces."""

from __future__ import annotations

import logging

from sqlalchemy import text

from app.infrastructure.db.connections import get_akshare_engine, get_tushare_engine, get_quantmate_engine

logger = logging.getLogger(__name__)

_ENGINE_MAP = {
    "tushare": get_tushare_engine,
    "akshare": get_akshare_engine,
}


def _get_engine(database: str):
    factory = _ENGINE_MAP.get(database)
    if factory is None:
        raise ValueError(f"Unknown target database: {database}")
    return factory()


def _mark_table_created(database: str, table: str) -> None:
    qm_engine = get_quantmate_engine()
    with qm_engine.begin() as conn:
        conn.execute(
            text("UPDATE data_source_items SET table_created = 1 WHERE target_database = :db AND target_table = :tbl"),
            {"db": database, "tbl": table},
        )


def ensure_table(database: str, table: str, ddl: str) -> bool:
    """Execute DDL if table does not yet exist. Returns True if created."""
    engine = _get_engine(database)
    with engine.connect() as conn:
        # Check existence
        result = conn.execute(
            text("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = :db AND table_name = :tbl"),
            {"db": database, "tbl": table},
        )
        exists = result.scalar() > 0

    if exists:
        _mark_table_created(database, table)
        logger.debug("Table %s.%s already exists", database, table)
        return False

    logger.info("Creating table %s.%s", database, table)
    engine = _get_engine(database)
    with engine.begin() as conn:
        # DDL may contain multiple statements (CREATE TABLE + INDEX); execute one by one
        for stmt in _split_ddl(ddl):
            stmt = stmt.strip()
            if stmt:
                conn.execute(text(stmt))

    _mark_table_created(database, table)

    logger.info("Table %s.%s created successfully", database, table)
    return True


def ensure_inferred_table(database: str, table: str, schema: dict[str, object]) -> bool:
    """Create or evolve a sample-inferred table schema."""
    ddl = str(schema.get("ddl") or "").strip()
    if not ddl:
        raise ValueError(f"Missing inferred DDL for {database}.{table}")

    engine = _get_engine(database)
    with engine.connect() as conn:
        exists = bool(
            conn.execute(
                text("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = :db AND table_name = :tbl"),
                {"db": database, "tbl": table},
            ).scalar()
        )

    if not exists:
        created = ensure_table(database, table, ddl)
        return created

    column_specs = list(schema.get("column_specs") or [])
    key_columns = tuple(str(column) for column in (schema.get("key_columns") or ()))
    unique_index_name = str(schema.get("unique_index_name") or "")
    existing_columns = _get_existing_columns(database, table)
    statements: list[str] = []

    for column_spec in column_specs:
        name = str(column_spec.get("name") or "").strip()
        if not name or name in existing_columns:
            continue
        statements.append(f"ALTER TABLE `{table}` ADD COLUMN {_column_ddl(column_spec, key_columns)}")

    if _column_requires_legacy_relax(existing_columns, "key_hash", expected_type="char(64)"):
        statements.append(f"ALTER TABLE `{table}` MODIFY COLUMN `key_hash` CHAR(64) NULL")
    if _column_requires_legacy_relax(existing_columns, "data", expected_type="json"):
        statements.append(f"ALTER TABLE `{table}` MODIFY COLUMN `data` JSON NULL")

    if key_columns and unique_index_name and not _has_unique_index(database, table, key_columns):
        joined_columns = ", ".join(f"`{column}`" for column in key_columns)
        statements.append(f"ALTER TABLE `{table}` ADD UNIQUE KEY `{unique_index_name}` ({joined_columns})")

    if not statements:
        _mark_table_created(database, table)
        return False

    with engine.begin() as conn:
        for statement in statements:
            conn.execute(text(statement))

    _mark_table_created(database, table)
    logger.info("Synchronized inferred schema for %s.%s", database, table)
    return True


def _get_existing_columns(database: str, table: str) -> dict[str, dict[str, str]]:
    engine = _get_engine(database)
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT column_name, is_nullable, column_type "
                "FROM information_schema.columns "
                "WHERE table_schema = :db AND table_name = :tbl"
            ),
            {"db": database, "tbl": table},
        ).fetchall()
    return {
        str(row[0]): {
            "is_nullable": str(row[1]),
            "column_type": str(row[2]),
        }
        for row in rows
    }


def _column_requires_legacy_relax(
    existing_columns: dict[str, dict[str, str]],
    name: str,
    *,
    expected_type: str,
) -> bool:
    column = existing_columns.get(name)
    if not column:
        return False
    if str(column.get("is_nullable") or "").upper() != "NO":
        return False
    return expected_type.lower() in str(column.get("column_type") or "").lower()


def _has_unique_index(database: str, table: str, key_columns: tuple[str, ...]) -> bool:
    engine = _get_engine(database)
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT index_name, non_unique, seq_in_index, column_name "
                "FROM information_schema.statistics "
                "WHERE table_schema = :db AND table_name = :tbl "
                "ORDER BY index_name ASC, seq_in_index ASC"
            ),
            {"db": database, "tbl": table},
        ).fetchall()

    indexes: dict[str, list[str]] = {}
    for index_name, non_unique, _seq_in_index, column_name in rows:
        if int(non_unique or 0) != 0:
            continue
        indexes.setdefault(str(index_name), []).append(str(column_name))
    return any(tuple(columns) == key_columns for columns in indexes.values())


def _column_ddl(column_spec: dict[str, object], key_columns: tuple[str, ...]) -> str:
    name = str(column_spec.get("name") or "").strip()
    sql_type = str(column_spec.get("sql_type") or "TEXT").strip()
    nullable = "NOT NULL" if name in key_columns else "NULL"
    return f"`{name}` {sql_type} {nullable}"


def _split_ddl(ddl: str) -> list[str]:
    """Split a DDL string on semicolons, respecting that a semicolon inside
    a string literal should not be a split point (simple heuristic)."""
    parts = ddl.split(";")
    return [p.strip() for p in parts if p.strip()]
