"""System configuration DAO."""
import json
from sqlalchemy import text
from app.infrastructure.db.connections import get_quantmate_engine


class SystemConfigDao:
    """Data access for system_configs table."""

    def __init__(self):
        self.engine = get_quantmate_engine()

    def list_all(self, category: str = None) -> list:
        q = "SELECT * FROM system_configs"
        params = {}
        if category:
            q += " WHERE category = :category"
            params["category"] = category
        q += " ORDER BY category, config_key"
        with self.engine.connect() as conn:
            rows = conn.execute(text(q), params).mappings().all()
            return [dict(r) for r in rows]

    def get(self, key: str) -> dict | None:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT * FROM system_configs WHERE config_key = :key"),
                {"key": key},
            ).mappings().first()
            return dict(row) if row else None

    def upsert(self, key: str, value: str, category: str = "general",
               description: str = None, user_overridable: bool = False) -> None:
        with self.engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO system_configs (config_key, config_value, category, description, user_overridable)
                VALUES (:key, :value, :category, :desc, :overridable)
                ON DUPLICATE KEY UPDATE config_value = :value, description = :desc, user_overridable = :overridable
            """), {"key": key, "value": value, "category": category,
                   "desc": description, "overridable": user_overridable})

    def delete(self, key: str) -> bool:
        with self.engine.begin() as conn:
            result = conn.execute(
                text("DELETE FROM system_configs WHERE config_key = :key"),
                {"key": key},
            )
            return result.rowcount > 0


class DataSourceConfigDao:
    """Data access for data_source_configs table."""

    def __init__(self):
        self.engine = get_quantmate_engine()

    def list_all(self) -> list:
        with self.engine.connect() as conn:
            rows = conn.execute(text("SELECT * FROM data_source_configs ORDER BY source_name")).mappings().all()
            result = []
            for r in rows:
                d = dict(r)
                if d.get("token_encrypted"):
                    d["token_encrypted"] = "***"
                return result
            return result

    def get(self, source_name: str) -> dict | None:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT * FROM data_source_configs WHERE source_name = :name"),
                {"name": source_name},
            ).mappings().first()
            if row:
                d = dict(row)
                d["token_encrypted"] = "***" if d.get("token_encrypted") else None
                return d
            return None

    def upsert(self, source_name: str, is_enabled: bool = True,
               rate_limit_per_min: int = None, priority: int = 0) -> None:
        with self.engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO data_source_configs (source_name, is_enabled, rate_limit_per_min, priority)
                VALUES (:name, :enabled, :rate, :priority)
                ON DUPLICATE KEY UPDATE is_enabled = :enabled, rate_limit_per_min = :rate, priority = :priority
            """), {"name": source_name, "enabled": is_enabled,
                   "rate": rate_limit_per_min, "priority": priority})
