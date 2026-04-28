"""Dynamic table creation manager for data source interfaces."""

from __future__ import annotations

import logging
import re

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
    """Create a static table or reconcile an older static schema in place."""
    engine = _get_engine(database)
    with engine.connect() as conn:
        # Check existence
        result = conn.execute(
            text("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = :db AND table_name = :tbl"),
            {"db": database, "tbl": table},
        )
        exists = result.scalar() > 0

    if exists:
        changed = _ensure_static_table_schema(database, table, ddl)
        _mark_table_created(database, table)
        if changed:
            logger.info("Synchronized static schema for %s.%s", database, table)
        else:
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


def _ensure_static_table_schema(database: str, table: str, ddl: str) -> bool:
    column_specs, key_columns, index_specs = _parse_static_table_schema(ddl)
    if not column_specs:
        return False

    existing_columns = _get_existing_columns(database, table)
    existing_indexes = _get_existing_indexes(database, table)
    statements: list[str] = []

    for column_spec in column_specs:
        name = str(column_spec.get("name") or "").strip()
        if not name:
            continue

        if name not in existing_columns:
            statements.append(f"ALTER TABLE `{table}` ADD COLUMN {_static_column_ddl(column_spec)}")
            continue

        if _column_requires_widen(existing_columns[name], column_spec):
            statements.append(f"ALTER TABLE `{table}` MODIFY COLUMN {_static_column_ddl(column_spec)}")
            continue

        if _static_column_allows_null(column_spec) and _column_requires_nullable_relax(existing_columns[name], name, key_columns):
            statements.append(f"ALTER TABLE `{table}` MODIFY COLUMN {_static_column_ddl(column_spec)}")

    for index_spec in index_specs:
        if _has_matching_index(existing_indexes, index_spec):
            continue
        statements.append(_build_add_index_statement(table, index_spec))

    if not statements:
        return False

    engine = _get_engine(database)
    with engine.begin() as conn:
        for statement in statements:
            conn.execute(text(statement))
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
        if not name:
            continue
        if name not in existing_columns:
            statements.append(f"ALTER TABLE `{table}` ADD COLUMN {_column_ddl(column_spec, key_columns)}")
            continue
        if _column_requires_widen(existing_columns[name], column_spec):
            statements.append(f"ALTER TABLE `{table}` MODIFY COLUMN {_column_ddl(column_spec, key_columns)}")
            continue
        if _column_requires_nullable_relax(existing_columns[name], name, key_columns):
            relax_spec = dict(column_spec)
            relax_spec["sql_type"] = existing_columns[name]["column_type"]
            statements.append(f"ALTER TABLE `{table}` MODIFY COLUMN {_column_ddl(relax_spec, key_columns)}")

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


def _get_existing_indexes(database: str, table: str) -> list[dict[str, object]]:
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

    indexes: dict[str, dict[str, object]] = {}
    for index_name, non_unique, _seq_in_index, column_name in rows:
        normalized_name = str(index_name or "").strip()
        if not normalized_name or normalized_name.upper() == "PRIMARY":
            continue
        entry = indexes.setdefault(
            normalized_name,
            {
                "name": normalized_name,
                "unique": int(non_unique or 0) == 0,
                "columns": [],
            },
        )
        entry["columns"].append(str(column_name))
    return list(indexes.values())


def _parse_static_table_schema(ddl: str) -> tuple[list[dict[str, object]], tuple[str, ...], list[dict[str, object]]]:
    column_specs: list[dict[str, object]] = []
    key_columns: list[str] = []
    index_specs: list[dict[str, object]] = []
    seen_columns: set[str] = set()
    seen_indexes: set[tuple[str, bool, tuple[str, ...]]] = set()

    for raw_line in ddl.splitlines():
        line = raw_line.strip().rstrip(",")
        if not line:
            continue

        upper_line = line.upper()
        if upper_line.startswith("CREATE TABLE") or line.startswith(")"):
            continue
        if upper_line.startswith("PRIMARY KEY"):
            key_columns.extend(_parse_index_columns(line))
            continue
        if upper_line.startswith("UNIQUE KEY") or upper_line.startswith("UNIQUE INDEX") or upper_line.startswith("KEY ") or upper_line.startswith("INDEX "):
            index_spec = _parse_index_definition(line)
            if index_spec is not None:
                signature = (
                    str(index_spec["name"]),
                    bool(index_spec["unique"]),
                    tuple(str(column) for column in index_spec["columns"]),
                )
                if signature not in seen_indexes:
                    seen_indexes.add(signature)
                    index_specs.append(index_spec)
            continue

        match = re.match(r"`?([A-Za-z0-9_]+)`?\s+(.*)", line)
        if not match:
            continue

        name = str(match.group(1)).strip()
        definition = str(match.group(2)).strip()
        if not name or not definition:
            continue
        if name in seen_columns:
            continue

        if re.search(r"\bPRIMARY\s+KEY\b", definition, flags=re.IGNORECASE):
            key_columns.append(name)
            definition = re.sub(r"\bPRIMARY\s+KEY\b", "", definition, flags=re.IGNORECASE).strip()

        sql_type = _extract_sql_type(definition)
        seen_columns.add(name)
        column_specs.append(
            {
                "name": name,
                "sql_type": sql_type,
                "definition": definition,
            }
        )

    return column_specs, tuple(dict.fromkeys(key_columns)), index_specs


def _parse_index_definition(line: str) -> dict[str, object] | None:
    normalized = line.strip().rstrip(",")
    upper_line = normalized.upper()

    unique = upper_line.startswith("UNIQUE ")
    if unique:
        body = re.sub(r"^UNIQUE\s+(?:KEY|INDEX)\s+", "", normalized, flags=re.IGNORECASE)
    elif upper_line.startswith("KEY ") or upper_line.startswith("INDEX "):
        body = re.sub(r"^(?:KEY|INDEX)\s+", "", normalized, flags=re.IGNORECASE)
    else:
        return None

    match = re.match(r"`?([A-Za-z0-9_]+)`?\s*\((.+)\)$", body)
    if not match:
        return None

    name = str(match.group(1)).strip()
    columns = tuple(_parse_index_columns(f"({match.group(2)})"))
    if not name or not columns:
        return None

    return {
        "name": name,
        "unique": unique,
        "columns": columns,
    }


def _parse_index_columns(line: str) -> list[str]:
    match = re.search(r"\((.+)\)", line)
    if not match:
        return []
    return [
        column.strip().strip("`")
        for column in match.group(1).split(",")
        if column.strip()
    ]


def _extract_sql_type(definition: str) -> str:
    match = re.match(r"([A-Za-z]+(?:\([^)]*\))?)", definition.strip(), flags=re.IGNORECASE)
    if not match:
        return definition.strip().split()[0]
    return match.group(1)


def _static_column_ddl(column_spec: dict[str, object]) -> str:
    name = str(column_spec.get("name") or "").strip()
    definition = str(column_spec.get("definition") or "").strip()
    return f"`{name}` {definition}"


def _static_column_allows_null(column_spec: dict[str, object]) -> bool:
    definition = str(column_spec.get("definition") or "")
    return "NOT NULL" not in definition.upper()


def _has_matching_index(existing_indexes: list[dict[str, object]], index_spec: dict[str, object]) -> bool:
    expected_name = str(index_spec.get("name") or "").strip()
    expected_unique = bool(index_spec.get("unique"))
    expected_columns = tuple(str(column) for column in (index_spec.get("columns") or ()))
    if not expected_name or not expected_columns:
        return True

    for existing in existing_indexes:
        existing_name = str(existing.get("name") or "").strip()
        existing_unique = bool(existing.get("unique"))
        existing_columns = tuple(str(column) for column in (existing.get("columns") or ()))
        if existing_name == expected_name:
            return True
        if existing_unique == expected_unique and existing_columns == expected_columns:
            return True
    return False


def _build_add_index_statement(table: str, index_spec: dict[str, object]) -> str:
    key_ddl = "UNIQUE KEY" if bool(index_spec.get("unique")) else "KEY"
    name = str(index_spec.get("name") or "").strip()
    joined_columns = ", ".join(f"`{column}`" for column in (index_spec.get("columns") or ()))
    return f"ALTER TABLE `{table}` ADD {key_ddl} `{name}` ({joined_columns})"


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


def _parse_text_type(sql_type: str) -> tuple[int, int | None] | None:
    normalized = str(sql_type or "").strip().lower()
    varchar_match = re.fullmatch(r"varchar\((\d+)\)", normalized)
    if varchar_match:
        return (0, int(varchar_match.group(1)))

    text_ranks = {
        "text": (1, None),
        "mediumtext": (2, None),
        "longtext": (3, None),
    }
    return text_ranks.get(normalized)


def _column_requires_widen(existing_column: dict[str, str], column_spec: dict[str, object]) -> bool:
    expected = _parse_text_type(str(column_spec.get("sql_type") or ""))
    if expected is None:
        return False

    current = _parse_text_type(str(existing_column.get("column_type") or ""))
    if current is None:
        return False

    expected_rank, expected_size = expected
    current_rank, current_size = current
    if expected_rank != current_rank:
        return expected_rank > current_rank

    if expected_rank != 0:
        return False

    return int(expected_size or 0) > int(current_size or 0)


def _column_requires_nullable_relax(
    existing_column: dict[str, str],
    name: str,
    key_columns: tuple[str, ...],
) -> bool:
    if not name or name in key_columns:
        return False
    if str(existing_column.get("is_nullable") or "").upper() != "NO":
        return False
    return True


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
