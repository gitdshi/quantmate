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
                    text("SELECT * FROM data_source_items WHERE source = :src ORDER BY id"),
                    {"src": source},
                ).fetchall()
            else:
                rows = conn.execute(
                    text("SELECT * FROM data_source_items ORDER BY source, id")
                ).fetchall()
            return [dict(r._mapping) for r in rows]

    def get_by_key(self, source: str, item_key: str) -> Optional[dict[str, Any]]:
        with connection("quantmate") as conn:
            row = conn.execute(
                text(
                    "SELECT * FROM data_source_items "
                    "WHERE source = :src AND item_key = :key"
                ),
                {"src": source, "key": item_key},
            ).fetchone()
            return dict(row._mapping) if row else None

    def update_enabled(self, source: str, item_key: str, enabled: bool) -> bool:
        with connection("quantmate") as conn:
            result = conn.execute(
                text(
                    "UPDATE data_source_items SET enabled = :en "
                    "WHERE source = :src AND item_key = :key"
                ),
                {"en": int(enabled), "src": source, "key": item_key},
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
                    text(
                        "UPDATE data_source_items SET enabled = :en "
                        "WHERE source = :src AND item_key = :key"
                    ),
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
                        "SELECT * FROM data_source_items "
                        "WHERE enabled = 1 AND source = :src ORDER BY id"
                    ),
                    {"src": source},
                ).fetchall()
            else:
                rows = conn.execute(
                    text("SELECT * FROM data_source_items WHERE enabled = 1 ORDER BY source, id")
                ).fetchall()
            return [dict(r._mapping) for r in rows]
