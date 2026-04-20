"""Read-only browser for Tushare database tables and rows."""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
import logging
from typing import Any

from sqlalchemy import MetaData, String, Table, and_, cast, func, inspect, select
from sqlalchemy.sql.sqltypes import Boolean, Date, DateTime, Float, Integer, Numeric, Time

from app.domains.market.dao.data_source_item_dao import DataSourceItemDao
from app.infrastructure.db.connections import get_tushare_engine


MAX_PAGE_SIZE = 100
DEFAULT_PAGE_SIZE = 50
MAX_FILTERS = 10
MAX_IN_VALUES = 50


logger = logging.getLogger(__name__)


def _is_unknown_data_source_items_column_error(exc: Exception) -> bool:
    lowered = str(exc or "").lower()
    return "unknown column" in lowered or "no such column" in lowered


class TushareBrowserError(ValueError):
    """Raised when a browser request is invalid."""


class TushareBrowserService:
    """Provides safe, read-only access to Tushare database metadata and rows."""

    def __init__(self) -> None:
        self._engine = get_tushare_engine()

    def list_tables(
        self,
        keyword: str | None = None,
        category: str | None = None,
        sub_category: str | None = None,
    ) -> list[dict[str, Any]]:
        inspector = inspect(self._engine)
        metadata_tables = self._list_metadata_tables(
            inspector,
            keyword=keyword,
            category=category,
            sub_category=sub_category,
        )
        if metadata_tables is not None:
            return metadata_tables

        return self._list_physical_tables(inspector, keyword=keyword)

    def _list_metadata_tables(
        self,
        inspector: Any,
        *,
        keyword: str | None = None,
        category: str | None = None,
        sub_category: str | None = None,
    ) -> list[dict[str, Any]] | None:
        try:
            rows = DataSourceItemDao().list_with_categories(source="tushare", category=category)
        except Exception as exc:
            if not _is_unknown_data_source_items_column_error(exc):
                raise
            logger.warning("Falling back to physical Tushare table listing: %s", exc)
            return None

        keyword_lower = keyword.strip().lower() if keyword else None
        sub_category_lower = sub_category.strip().lower() if sub_category else None
        target_databases = self._resolve_target_databases()
        available_tables = set(inspector.get_table_names())
        items: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()

        for row in rows:
            target_database = str(row.get("target_database") or "").strip()
            target_table = str(row.get("target_table") or "").strip()
            if not target_database or target_database.lower() not in target_databases:
                continue
            if not target_table or target_table not in available_tables:
                continue
            if sub_category_lower and str(row.get("sub_category") or "").strip().lower() != sub_category_lower:
                continue

            item_name = str(row.get("item_name") or row.get("display_name") or "").strip()
            if keyword_lower and keyword_lower not in target_table.lower() and keyword_lower not in item_name.lower():
                continue

            dedupe_key = (target_database.lower(), target_table.lower())
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)

            columns = inspector.get_columns(target_table)
            primary_keys = inspector.get_pk_constraint(target_table).get("constrained_columns") or []
            items.append(
                {
                    "name": target_table,
                    "target_database": target_database,
                    "target_table": target_table,
                    "table_created": bool(row.get("table_created")),
                    "item_key": row.get("item_key"),
                    "item_name": row.get("item_name") or row.get("display_name"),
                    "category": row.get("category"),
                    "sub_category": row.get("sub_category"),
                    "column_count": len(columns),
                    "primary_keys": primary_keys,
                }
            )

        return items

    def _list_physical_tables(self, inspector: Any, *, keyword: str | None = None) -> list[dict[str, Any]]:
        keyword_lower = keyword.strip().lower() if keyword else None
        items: list[dict[str, Any]] = []
        target_database = self._default_target_database()

        for table_name in sorted(inspector.get_table_names()):
            if keyword_lower and keyword_lower not in table_name.lower():
                continue
            columns = inspector.get_columns(table_name)
            primary_keys = inspector.get_pk_constraint(table_name).get("constrained_columns") or []
            items.append(
                {
                    "name": table_name,
                    "target_database": target_database,
                    "target_table": table_name,
                    "table_created": True,
                    "item_key": table_name,
                    "item_name": table_name,
                    "category": None,
                    "sub_category": None,
                    "column_count": len(columns),
                    "primary_keys": primary_keys,
                }
            )
        return items

    def _default_target_database(self) -> str:
        database_name = str(getattr(self._engine.url, "database", "") or "").strip()
        return database_name or "tushare"

    def _resolve_target_databases(self) -> set[str]:
        return {"tushare", self._default_target_database().lower()}

    def get_schema(self, table_name: str) -> dict[str, Any]:
        inspector = inspect(self._engine)
        resolved_table_name = self._validate_table_name(table_name, inspector)
        columns = inspector.get_columns(resolved_table_name)
        primary_keys = set(inspector.get_pk_constraint(resolved_table_name).get("constrained_columns") or [])
        indexed_columns: set[str] = set()
        for index in inspector.get_indexes(resolved_table_name):
            indexed_columns.update(index.get("column_names") or [])

        return {
            "table": resolved_table_name,
            "columns": [
                {
                    "name": column["name"],
                    "data_type": str(column["type"]),
                    "nullable": bool(column.get("nullable", True)),
                    "default": None if column.get("default") is None else str(column.get("default")),
                    "primary_key": column["name"] in primary_keys,
                    "indexed": column["name"] in indexed_columns,
                }
                for column in columns
            ],
        }

    def query_rows(
        self,
        table_name: str,
        *,
        page: int = 1,
        page_size: int = DEFAULT_PAGE_SIZE,
        sort_by: str | None = None,
        sort_dir: str = "desc",
        filters: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        if page < 1:
            raise TushareBrowserError("page must be >= 1")
        if page_size < 1 or page_size > MAX_PAGE_SIZE:
            raise TushareBrowserError(f"page_size must be between 1 and {MAX_PAGE_SIZE}")

        safe_filters = filters or []
        if len(safe_filters) > MAX_FILTERS:
            raise TushareBrowserError(f"filters exceed maximum of {MAX_FILTERS}")

        inspector = inspect(self._engine)
        resolved_table_name = self._validate_table_name(table_name, inspector)
        table = Table(resolved_table_name, MetaData(), autoload_with=self._engine)
        conditions = [self._compile_filter(table, filter_item) for filter_item in safe_filters]

        order_column, resolved_sort_dir = self._resolve_sort(table, sort_by, sort_dir)

        query = select(*table.c)
        total_query = select(func.count()).select_from(table)
        if conditions:
            where_clause = and_(*conditions)
            query = query.where(where_clause)
            total_query = total_query.where(where_clause)

        order_expr = order_column.desc() if resolved_sort_dir == "desc" else order_column.asc()
        query = query.order_by(order_expr).limit(page_size).offset((page - 1) * page_size)

        with self._engine.connect() as conn:
            total = int(conn.execute(total_query).scalar() or 0)
            rows = conn.execute(query).mappings().all()

        total_pages = max(1, (total + page_size - 1) // page_size)
        return {
            "table": resolved_table_name,
            "data": [self._normalize_row(dict(row)) for row in rows],
            "meta": {
                "page": page,
                "page_size": page_size,
                "total": total,
                "total_pages": total_pages,
                "sort_by": order_column.name,
                "sort_dir": resolved_sort_dir,
            },
        }

    def _validate_table_name(self, table_name: str, inspector: Any) -> str:
        available_tables = set(inspector.get_table_names())
        if table_name not in available_tables:
            raise TushareBrowserError(f"unknown table: {table_name}")
        return table_name

    def _resolve_sort(self, table: Table, sort_by: str | None, sort_dir: str) -> tuple[Any, str]:
        resolved_sort_dir = sort_dir.lower() if sort_dir else "desc"
        if resolved_sort_dir not in {"asc", "desc"}:
            raise TushareBrowserError("sort_dir must be 'asc' or 'desc'")

        if sort_by:
            if sort_by not in table.c:
                raise TushareBrowserError(f"unknown sort column: {sort_by}")
            return table.c[sort_by], resolved_sort_dir

        for candidate in [
            "trade_date",
            "ann_date",
            "end_date",
            "f_ann_date",
            "datetime",
            "date",
            "created_at",
            "updated_at",
        ]:
            if candidate in table.c:
                return table.c[candidate], "desc"

        primary_keys = list(table.primary_key.columns)
        if primary_keys:
            return primary_keys[0], "asc"

        return next(iter(table.c)), "asc"

    def _compile_filter(self, table: Table, filter_item: dict[str, Any]) -> Any:
        column_name = filter_item.get("column")
        operator = filter_item.get("operator")
        if not column_name or column_name not in table.c:
            raise TushareBrowserError(f"unknown filter column: {column_name}")
        if not operator:
            raise TushareBrowserError("filter operator is required")

        column = table.c[column_name]
        raw_value = filter_item.get("value")
        raw_values = filter_item.get("values")

        if operator == "eq":
            return column.is_(None) if raw_value is None else column == self._coerce_value(column.type, raw_value)
        if operator == "ne":
            return column.is_not(None) if raw_value is None else column != self._coerce_value(column.type, raw_value)
        if operator == "gt":
            return column > self._coerce_value(column.type, raw_value)
        if operator == "gte":
            return column >= self._coerce_value(column.type, raw_value)
        if operator == "lt":
            return column < self._coerce_value(column.type, raw_value)
        if operator == "lte":
            return column <= self._coerce_value(column.type, raw_value)
        if operator == "like":
            if raw_value in (None, ""):
                raise TushareBrowserError("like filter requires a value")
            return cast(column, String).like(f"%{str(raw_value).strip()}%")
        if operator == "in":
            if not isinstance(raw_values, list) or not raw_values:
                raise TushareBrowserError("in filter requires a non-empty values list")
            if len(raw_values) > MAX_IN_VALUES:
                raise TushareBrowserError(f"in filter exceeds maximum of {MAX_IN_VALUES} values")
            return column.in_([self._coerce_value(column.type, value) for value in raw_values])
        if operator == "between":
            if not isinstance(raw_values, list) or len(raw_values) != 2:
                raise TushareBrowserError("between filter requires exactly 2 values")
            start_value = self._coerce_value(column.type, raw_values[0])
            end_value = self._coerce_value(column.type, raw_values[1])
            return column.between(start_value, end_value)
        if operator == "is_null":
            return column.is_(None)
        if operator == "is_not_null":
            return column.is_not(None)

        raise TushareBrowserError(f"unsupported filter operator: {operator}")

    def _coerce_value(self, column_type: Any, raw_value: Any) -> Any:
        if raw_value is None:
            return None
        if isinstance(column_type, Boolean):
            if isinstance(raw_value, bool):
                return raw_value
            raw_lower = str(raw_value).strip().lower()
            if raw_lower in {"1", "true", "yes", "on"}:
                return True
            if raw_lower in {"0", "false", "no", "off"}:
                return False
            raise TushareBrowserError(f"invalid boolean value: {raw_value}")
        if isinstance(column_type, Integer):
            return int(raw_value)
        if isinstance(column_type, (Float, Numeric)):
            return Decimal(str(raw_value))
        if isinstance(column_type, Date) and not isinstance(column_type, DateTime):
            if isinstance(raw_value, date) and not isinstance(raw_value, datetime):
                return raw_value
            return date.fromisoformat(str(raw_value))
        if isinstance(column_type, DateTime):
            if isinstance(raw_value, datetime):
                return raw_value
            return datetime.fromisoformat(str(raw_value).replace("Z", "+00:00"))
        if isinstance(column_type, Time):
            if isinstance(raw_value, time):
                return raw_value
            return time.fromisoformat(str(raw_value))
        return raw_value

    def _normalize_row(self, row: dict[str, Any]) -> dict[str, Any]:
        return {key: self._normalize_value(value) for key, value in row.items()}

    def _normalize_value(self, value: Any) -> Any:
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, time):
            return value.isoformat()
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, bytes):
            try:
                return value.decode("utf-8")
            except UnicodeDecodeError:
                return value.hex()
        return value