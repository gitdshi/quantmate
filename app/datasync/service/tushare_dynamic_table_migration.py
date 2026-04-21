"""Migrate legacy payload-shaped Tushare catalog tables to parsed schemas."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import logging
import re

from sqlalchemy import text

from app.datasync.registry import DataSourceRegistry, build_default_registry
from app.datasync.sources.tushare import ddl
from app.domains.extdata.dao.tushare_dao import upsert_rows
from app.infrastructure.db.connections import get_tushare_engine

logger = logging.getLogger(__name__)

_SYSTEM_COLUMNS = frozenset({"id", "key_hash", "data", "created_at", "updated_at"})


@dataclass(frozen=True)
class DynamicTableMigrationTarget:
    interface_key: str
    table_name: str
    preferred_date_column: str | None
    preferred_key_fields: tuple[str, ...]


@dataclass(frozen=True)
class DynamicTableMigrationResult:
    table_name: str
    interface_key: str
    status: str
    message: str = ""
    source_rows: int = 0
    migrated_rows: int = 0
    records_processed: int = 0
    temp_table: str | None = None
    backup_table: str | None = None


def list_dynamic_migration_targets(registry: DataSourceRegistry | None = None) -> list[DynamicTableMigrationTarget]:
    resolved_registry = registry or build_default_registry()
    targets: dict[str, DynamicTableMigrationTarget] = {}
    for iface in resolved_registry.all_interfaces():
        info = iface.info
        if info.source_key != "tushare":
            continue
        if not ddl.uses_sample_inferred_schema(info.target_table):
            continue
        if info.target_table in targets:
            continue
        preferred_date_column = _call_optional_noarg(iface, "_date_param")
        preferred_key_fields = tuple(_call_optional_noarg(iface, "_payload_key_fields") or ())
        targets[info.target_table] = DynamicTableMigrationTarget(
            interface_key=info.interface_key,
            table_name=info.target_table,
            preferred_date_column=preferred_date_column,
            preferred_key_fields=preferred_key_fields,
        )
    return list(targets.values())


def migrate_dynamic_tables(
    *,
    table_names: list[str] | None = None,
    dry_run: bool = False,
    keep_legacy_backup: bool = True,
    sample_size: int = 200,
    batch_size: int = 1000,
    registry: DataSourceRegistry | None = None,
) -> list[DynamicTableMigrationResult]:
    selected_tables = {str(name).strip() for name in (table_names or []) if str(name).strip()}
    results: list[DynamicTableMigrationResult] = []
    for target in list_dynamic_migration_targets(registry=registry):
        if selected_tables and target.table_name not in selected_tables:
            continue
        try:
            results.append(
                migrate_dynamic_table(
                    target,
                    dry_run=dry_run,
                    keep_legacy_backup=keep_legacy_backup,
                    sample_size=sample_size,
                    batch_size=batch_size,
                )
            )
        except Exception as exc:
            logger.exception("Failed to migrate dynamic Tushare table %s", target.table_name)
            results.append(
                DynamicTableMigrationResult(
                    table_name=target.table_name,
                    interface_key=target.interface_key,
                    status="error",
                    message=str(exc),
                )
            )
    return results


def migrate_dynamic_table(
    target: DynamicTableMigrationTarget,
    *,
    dry_run: bool = False,
    keep_legacy_backup: bool = True,
    sample_size: int = 200,
    batch_size: int = 1000,
) -> DynamicTableMigrationResult:
    engine = get_tushare_engine()

    with engine.connect() as conn:
        columns = _get_table_columns(conn, target.table_name)
        if not columns:
            return DynamicTableMigrationResult(
                table_name=target.table_name,
                interface_key=target.interface_key,
                status="skipped",
                message="table not found",
            )

        column_names = [column["name"] for column in columns]
        if "data" not in column_names or "key_hash" not in column_names:
            return DynamicTableMigrationResult(
                table_name=target.table_name,
                interface_key=target.interface_key,
                status="skipped",
                message="table is not a legacy payload table",
            )

        source_rows = _count_rows(conn, target.table_name)
        if source_rows <= 0:
            return DynamicTableMigrationResult(
                table_name=target.table_name,
                interface_key=target.interface_key,
                status="skipped",
                message="table has no rows to migrate",
                source_rows=0,
            )

        sample_records = _load_sample_records(conn, target.table_name, column_names, sample_size)
        if not sample_records:
            return DynamicTableMigrationResult(
                table_name=target.table_name,
                interface_key=target.interface_key,
                status="skipped",
                message="no parseable rows found in legacy payload data",
                source_rows=source_rows,
            )

    schema = ddl.infer_dynamic_table_schema(
        target.table_name,
        sample_records,
        preferred_date_column=target.preferred_date_column,
        preferred_key_fields=target.preferred_key_fields,
    )
    temp_table = _derive_table_name(target.table_name, "__parsed_tmp")
    backup_table = _derive_table_name(target.table_name, "__legacy_payload")

    if dry_run:
        return DynamicTableMigrationResult(
            table_name=target.table_name,
            interface_key=target.interface_key,
            status="dry_run",
            message="schema inferred and migration planned",
            source_rows=source_rows,
            migrated_rows=source_rows,
            records_processed=source_rows,
            temp_table=temp_table,
            backup_table=backup_table,
        )

    temp_ddl = ddl.build_dynamic_table_ddl(temp_table, schema["column_specs"], schema["key_columns"])
    migrated_input_rows = _copy_into_replacement_table(
        source_table=target.table_name,
        temp_table=temp_table,
        temp_ddl=temp_ddl,
        column_specs=list(schema["column_specs"]),
        key_columns=tuple(schema["key_columns"]),
        batch_size=batch_size,
    )

    with engine.connect() as conn:
        migrated_rows = _count_rows(conn, temp_table)
        if migrated_rows <= 0 and source_rows > 0:
            raise RuntimeError(f"Replacement table {temp_table} is empty after migrating {target.table_name}")
        if _table_exists(conn, backup_table):
            raise RuntimeError(f"Legacy backup table {backup_table} already exists")

    _swap_replacement_tables(
        source_table=target.table_name,
        temp_table=temp_table,
        backup_table=backup_table,
        keep_legacy_backup=keep_legacy_backup,
    )

    return DynamicTableMigrationResult(
        table_name=target.table_name,
        interface_key=target.interface_key,
        status="migrated",
        message="replacement table created and swapped",
        source_rows=source_rows,
        migrated_rows=migrated_rows,
        records_processed=migrated_input_rows,
        temp_table=temp_table,
        backup_table=backup_table if keep_legacy_backup else None,
    )


def _copy_into_replacement_table(
    *,
    source_table: str,
    temp_table: str,
    temp_ddl: str,
    column_specs: list[dict],
    key_columns: tuple[str, ...],
    batch_size: int,
) -> int:
    engine = get_tushare_engine()
    records_processed = 0

    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS `{_quote_identifier(temp_table)}`"))
        conn.execute(text(temp_ddl))

    with engine.connect() as conn:
        column_names = [column["name"] for column in _get_table_columns(conn, source_table)]

    for batch in _iter_source_batches(engine, source_table, column_names, batch_size):
        records = _build_records_from_rows(batch, column_names)
        if not records:
            continue
        upsert_rows(temp_table, records, column_specs=column_specs, key_columns=key_columns)
        records_processed += len(records)

    return records_processed


def _swap_replacement_tables(
    *,
    source_table: str,
    temp_table: str,
    backup_table: str,
    keep_legacy_backup: bool,
) -> None:
    engine = get_tushare_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                f"RENAME TABLE "
                f"`{_quote_identifier(source_table)}` TO `{_quote_identifier(backup_table)}`, "
                f"`{_quote_identifier(temp_table)}` TO `{_quote_identifier(source_table)}`"
            )
        )
        if not keep_legacy_backup:
            conn.execute(text(f"DROP TABLE `{_quote_identifier(backup_table)}`"))


def _iter_source_batches(engine, table_name: str, column_names: list[str], batch_size: int):
    select_columns = ", ".join(f"`{_quote_identifier(name)}`" for name in column_names)
    has_id = "id" in column_names
    last_id = 0

    while True:
        if has_id:
            query = text(
                f"SELECT {select_columns} FROM `{_quote_identifier(table_name)}` "
                "WHERE id > :last_id ORDER BY id ASC LIMIT :limit"
            )
            params = {"last_id": last_id, "limit": batch_size}
        else:
            query = text(f"SELECT {select_columns} FROM `{_quote_identifier(table_name)}` LIMIT :limit")
            params = {"limit": batch_size}

        with engine.connect() as conn:
            rows = conn.execute(query, params).fetchall()

        if not rows:
            return

        yield rows

        if not has_id:
            return
        last_id = int(rows[-1]._mapping["id"] or 0)


def _load_sample_records(conn, table_name: str, column_names: list[str], sample_size: int) -> list[dict]:
    select_columns = ", ".join(f"`{_quote_identifier(name)}`" for name in column_names)
    order_clause = " ORDER BY id ASC" if "id" in column_names else ""
    rows = conn.execute(
        text(f"SELECT {select_columns} FROM `{_quote_identifier(table_name)}`{order_clause} LIMIT :limit"),
        {"limit": sample_size},
    ).fetchall()
    return _build_records_from_rows(rows, column_names)


def _build_records_from_rows(rows, column_names: list[str]) -> list[dict]:
    business_columns = [name for name in column_names if name not in _SYSTEM_COLUMNS]
    records: list[dict] = []
    for row in rows:
        record = _legacy_row_to_record(row._mapping, business_columns)
        if record:
            records.append(record)
    return records


def _legacy_row_to_record(row_mapping, business_columns: list[str]) -> dict[str, object]:
    record: dict[str, object] = {}
    payload = _decode_payload(row_mapping.get("data"))
    if isinstance(payload, dict):
        record.update({str(key): value for key, value in payload.items()})

    for column in business_columns:
        value = row_mapping.get(column)
        if value is not None:
            record[column] = value
    return record


def _decode_payload(value):
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    if isinstance(value, str):
        text_value = value.strip()
        if not text_value:
            return None
        try:
            return json.loads(text_value)
        except json.JSONDecodeError:
            return None
    return None


def _get_table_columns(conn, table_name: str) -> list[dict[str, str]]:
    rows = conn.execute(
        text(
            "SELECT column_name, column_type "
            "FROM information_schema.columns "
            "WHERE table_schema = :db AND table_name = :tbl "
            "ORDER BY ordinal_position ASC"
        ),
        {"db": "tushare", "tbl": table_name},
    ).fetchall()
    return [{"name": str(row[0]), "type": str(row[1])} for row in rows]


def _table_exists(conn, table_name: str) -> bool:
    return bool(
        conn.execute(
            text(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = :db AND table_name = :tbl"
            ),
            {"db": "tushare", "tbl": table_name},
        ).scalar()
    )


def _count_rows(conn, table_name: str) -> int:
    return int(conn.execute(text(f"SELECT COUNT(*) FROM `{_quote_identifier(table_name)}`")).scalar() or 0)


def _derive_table_name(base_table: str, suffix: str) -> str:
    candidate = f"{base_table}{suffix}"
    if len(candidate) <= 64:
        return candidate
    digest = hashlib.sha1(candidate.encode("utf-8")).hexdigest()[:8]
    limit = 64 - len(suffix) - len(digest) - 1
    return f"{base_table[:limit]}_{digest}{suffix}"


def _call_optional_noarg(obj, name: str):
    value = getattr(obj, name, None)
    if callable(value):
        return value()
    return value


def _quote_identifier(name: str) -> str:
    normalized = str(name or "").strip()
    if not re.fullmatch(r"[0-9A-Za-z_]+", normalized):
        raise ValueError(f"Unsafe SQL identifier: {name!r}")
    return normalized
