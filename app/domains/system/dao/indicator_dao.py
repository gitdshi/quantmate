"""Indicator configuration DAO."""

import json
from sqlalchemy import text
from app.infrastructure.db.connections import get_quantmate_engine


class IndicatorConfigDao:
    """Data access for indicator_configs table."""

    def __init__(self):
        self.engine = get_quantmate_engine()

    def list_all(self, category: str = None) -> list:
        q = "SELECT * FROM indicator_configs"
        params = {}
        if category:
            q += " WHERE category = :category"
            params["category"] = category
        q += " ORDER BY category, name"
        with self.engine.connect() as conn:
            rows = conn.execute(text(q), params).mappings().all()
            result = []
            for r in rows:
                d = dict(r)
                if isinstance(d.get("default_params"), str):
                    d["default_params"] = json.loads(d["default_params"])
                result.append(d)
            return result

    def get_by_id(self, indicator_id: int) -> dict | None:
        with self.engine.connect() as conn:
            row = (
                conn.execute(
                    text("SELECT * FROM indicator_configs WHERE id = :id"),
                    {"id": indicator_id},
                )
                .mappings()
                .first()
            )
            if row:
                d = dict(row)
                if isinstance(d.get("default_params"), str):
                    d["default_params"] = json.loads(d["default_params"])
                return d
            return None

    def create(
        self,
        name: str,
        display_name: str,
        category: str,
        description: str = None,
        default_params: dict = None,
        formula: str = None,
    ) -> int:
        with self.engine.begin() as conn:
            result = conn.execute(
                text("""
                INSERT INTO indicator_configs (name, display_name, category, description, default_params, formula)
                VALUES (:name, :display_name, :category, :desc, :params, :formula)
            """),
                {
                    "name": name,
                    "display_name": display_name,
                    "category": category,
                    "desc": description,
                    "params": json.dumps(default_params or {}),
                    "formula": formula,
                },
            )
            return result.lastrowid

    def update(self, indicator_id: int, **kwargs) -> bool:
        if not kwargs:
            return False
        if "default_params" in kwargs and isinstance(kwargs["default_params"], dict):
            kwargs["default_params"] = json.dumps(kwargs["default_params"])
        set_clause = ", ".join(f"{k} = :{k}" for k in kwargs)
        kwargs["id"] = indicator_id
        with self.engine.begin() as conn:
            result = conn.execute(
                text(f"UPDATE indicator_configs SET {set_clause} WHERE id = :id"),
                kwargs,
            )
            return result.rowcount > 0

    def delete(self, indicator_id: int) -> bool:
        with self.engine.connect() as conn:
            result = conn.execute(
                text("DELETE FROM indicator_configs WHERE id = :id AND is_builtin = FALSE"),
                {"id": indicator_id},
            )
            conn.commit()
            return result.rowcount > 0
