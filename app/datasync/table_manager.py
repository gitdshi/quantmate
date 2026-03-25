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

    # Mark table_created in data_source_items
    qm_engine = get_quantmate_engine()
    with qm_engine.begin() as conn:
        conn.execute(
            text("UPDATE data_source_items SET table_created = 1 WHERE target_database = :db AND target_table = :tbl"),
            {"db": database, "tbl": table},
        )

    logger.info("Table %s.%s created successfully", database, table)
    return True


def _split_ddl(ddl: str) -> list[str]:
    """Split a DDL string on semicolons, respecting that a semicolon inside
    a string literal should not be a split point (simple heuristic)."""
    parts = ddl.split(";")
    return [p.strip() for p in parts if p.strip()]
