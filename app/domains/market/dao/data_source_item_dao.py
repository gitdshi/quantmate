"""Data source items DAO (Issue #5)."""

from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import text

from app.infrastructure.db.connections import connection


class DataSourceItemDao:
    """CRUD for data_source_items configuration table."""

    def list_all(self, source: Optional[str] = None) -> list[dict[str, Any]]:
        with connection("quantmate") as conn:
            if source:
                rows = conn.execute(
                    text("SELECT * FROM data_source_items WHERE source = :src ORDER BY sync_priority, id"),
                    {"src": source},
                ).fetchall()
            else:
                rows = conn.execute(
                    text("SELECT * FROM data_source_items ORDER BY source, sync_priority, id")
                ).fetchall()
            return [dict(r._mapping) for r in rows]

    def get_by_key(self, source: str, item_key: str) -> Optional[dict[str, Any]]:
        with connection("quantmate") as conn:
            row = conn.execute(
                text("SELECT * FROM data_source_items WHERE source = :src AND item_key = :key"),
                {"src": source, "key": item_key},
            ).fetchone()
            return dict(row._mapping) if row else None

    def update_enabled(self, source: str, item_key: str, enabled: bool) -> bool:
        with connection("quantmate") as conn:
            result = conn.execute(
                text("UPDATE data_source_items SET enabled = :en WHERE source = :src AND item_key = :key"),
                {"en": int(enabled), "src": source, "key": item_key},
            )
            conn.commit()
            return result.rowcount > 0  # type: ignore[union-attr]

    def mark_table_created(self, source: str, item_key: str) -> bool:
        with connection("quantmate") as conn:
            result = conn.execute(
                text("UPDATE data_source_items SET table_created = 1 WHERE source = :src AND item_key = :key"),
                {"en": 1, "src": source, "key": item_key},
            )
            conn.commit()
            return result.rowcount > 0  # type: ignore[union-attr]

    def batch_update(self, items: list[dict]) -> int:
        """Update enabled status for multiple items.
        Each dict: {"source": "tushare", "item_key": "stock_basic", "enabled": True}
        """
        updated = 0
        with connection("quantmate") as conn:
            for item in items:
                result = conn.execute(
                    text("UPDATE data_source_items SET enabled = :en WHERE source = :src AND item_key = :key"),
                    {"en": int(item["enabled"]), "src": item["source"], "key": item["item_key"]},
                )
                updated += result.rowcount  # type: ignore[union-attr]
            conn.commit()
        return updated

    def list_enabled(self, source: Optional[str] = None) -> list[dict[str, Any]]:
        with connection("quantmate") as conn:
            if source:
                rows = conn.execute(
                    text(
                        "SELECT * FROM data_source_items WHERE enabled = 1 AND source = :src ORDER BY sync_priority, id"
                    ),
                    {"src": source},
                ).fetchall()
            else:
                rows = conn.execute(
                    text("SELECT * FROM data_source_items WHERE enabled = 1 ORDER BY source, sync_priority, id")
                ).fetchall()
            return [dict(r._mapping) for r in rows]


class DataSourceConfigDao:
    """CRUD for data_source_configs table."""

    def list_all(self) -> list[dict[str, Any]]:
        with connection("quantmate") as conn:
            rows = conn.execute(text("SELECT * FROM data_source_configs ORDER BY id")).fetchall()
            return [dict(r._mapping) for r in rows]

    def get_by_key(self, source_key: str) -> Optional[dict[str, Any]]:
        with connection("quantmate") as conn:
            row = conn.execute(
                text("SELECT * FROM data_source_configs WHERE source_key = :sk"),
                {"sk": source_key},
            ).fetchone()
            return dict(row._mapping) if row else None

    def update_enabled(self, source_key: str, enabled: bool) -> bool:
        with connection("quantmate") as conn:
            result = conn.execute(
                text("UPDATE data_source_configs SET enabled = :en WHERE source_key = :sk"),
                {"en": int(enabled), "sk": source_key},
            )
            conn.commit()
            return result.rowcount > 0  # type: ignore[union-attr]

    def update_config(self, source_key: str, config_json: Optional[str] = None, enabled: Optional[bool] = None) -> bool:
        parts = []
        params: dict[str, Any] = {"sk": source_key}
        if config_json is not None:
            parts.append("config_json = :cfg")
            params["cfg"] = config_json
        if enabled is not None:
            parts.append("enabled = :en")
            params["en"] = int(enabled)
        if not parts:
            return False
        sql = f"UPDATE data_source_configs SET {', '.join(parts)} WHERE source_key = :sk"
        with connection("quantmate") as conn:
            result = conn.execute(text(sql), params)
            conn.commit()
            return result.rowcount > 0  # type: ignore[union-attr]
