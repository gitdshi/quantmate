"""Factor Lab DAO — factor definitions and evaluations."""

from __future__ import annotations

import json
from typing import Any, Optional

from sqlalchemy import text

from app.infrastructure.db.connections import connection


class FactorDefinitionDao:
    """CRUD for factor_definitions."""

    def list_for_user(
        self, user_id: int, category: Optional[str] = None, limit: int = 50, offset: int = 0
    ) -> list[dict[str, Any]]:
        query = "SELECT * FROM factor_definitions WHERE user_id = :uid"
        params: dict[str, Any] = {"uid": user_id, "limit": limit, "offset": offset}
        if category:
            query += " AND category = :cat"
            params["cat"] = category
        query += " ORDER BY updated_at DESC LIMIT :limit OFFSET :offset"
        with connection("quantmate") as conn:
            rows = conn.execute(text(query), params).fetchall()
            return [dict(r._mapping) for r in rows]

    def count_for_user(self, user_id: int) -> int:
        with connection("quantmate") as conn:
            row = conn.execute(
                text("SELECT COUNT(*) AS cnt FROM factor_definitions WHERE user_id = :uid"),
                {"uid": user_id},
            ).fetchone()
            return row._mapping["cnt"] if row else 0

    def get(self, factor_id: int, user_id: int) -> Optional[dict[str, Any]]:
        with connection("quantmate") as conn:
            row = conn.execute(
                text("SELECT * FROM factor_definitions WHERE id = :fid AND user_id = :uid"),
                {"fid": factor_id, "uid": user_id},
            ).fetchone()
            return dict(row._mapping) if row else None

    def create(
        self,
        user_id: int,
        name: str,
        expression: str,
        category: Optional[str] = None,
        description: Optional[str] = None,
        params: Optional[dict] = None,
    ) -> int:
        with connection("quantmate") as conn:
            result = conn.execute(
                text(
                    "INSERT INTO factor_definitions (user_id, name, category, expression, description, params) "
                    "VALUES (:uid, :name, :cat, :expr, :desc, :params)"
                ),
                {
                    "uid": user_id,
                    "name": name,
                    "cat": category,
                    "expr": expression,
                    "desc": description,
                    "params": json.dumps(params) if params else None,
                },
            )
            conn.commit()
            return result.lastrowid  # type: ignore[return-value]

    def update(self, factor_id: int, user_id: int, **fields) -> None:
        allowed = {"name", "category", "expression", "description", "status"}
        data = {k: v for k, v in fields.items() if k in allowed and v is not None}
        if "params" in fields and fields["params"] is not None:
            data["params"] = json.dumps(fields["params"])
        if not data:
            return
        set_clause = ", ".join(f"{k} = :{k}" for k in data)
        with connection("quantmate") as conn:
            conn.execute(
                text(
                    f"UPDATE factor_definitions SET {set_clause}, updated_at = NOW() WHERE id = :fid AND user_id = :uid"
                ),
                {**data, "fid": factor_id, "uid": user_id},
            )
            conn.commit()

    def delete(self, factor_id: int, user_id: int) -> bool:
        with connection("quantmate") as conn:
            result = conn.execute(
                text("DELETE FROM factor_definitions WHERE id = :fid AND user_id = :uid"),
                {"fid": factor_id, "uid": user_id},
            )
            conn.commit()
            return result.rowcount > 0  # type: ignore[union-attr]


class FactorEvaluationDao:
    """CRUD for factor_evaluations."""

    def list_for_factor(self, factor_id: int) -> list[dict[str, Any]]:
        with connection("quantmate") as conn:
            rows = conn.execute(
                text("SELECT * FROM factor_evaluations WHERE factor_id = :fid ORDER BY created_at DESC"),
                {"fid": factor_id},
            ).fetchall()
            return [dict(r._mapping) for r in rows]

    def get(self, eval_id: int) -> Optional[dict[str, Any]]:
        with connection("quantmate") as conn:
            row = conn.execute(
                text("SELECT * FROM factor_evaluations WHERE id = :eid"),
                {"eid": eval_id},
            ).fetchone()
            return dict(row._mapping) if row else None

    def create(self, factor_id: int, start_date: str, end_date: str, metrics: Optional[dict] = None, **kwargs) -> int:
        fields = {"factor_id": factor_id, "start_date": start_date, "end_date": end_date}
        for k in ("ic_mean", "ic_ir", "turnover", "long_ret", "short_ret", "long_short_ret"):
            if k in kwargs:
                fields[k] = kwargs[k]
        if metrics:
            fields["metrics"] = json.dumps(metrics)
        cols = ", ".join(fields.keys())
        vals = ", ".join(f":{k}" for k in fields.keys())
        with connection("quantmate") as conn:
            result = conn.execute(text(f"INSERT INTO factor_evaluations ({cols}) VALUES ({vals})"), fields)
            conn.commit()
            return result.lastrowid  # type: ignore[return-value]

    def delete(self, eval_id: int) -> bool:
        with connection("quantmate") as conn:
            result = conn.execute(
                text("DELETE FROM factor_evaluations WHERE id = :eid"),
                {"eid": eval_id},
            )
            conn.commit()
            return result.rowcount > 0  # type: ignore[union-attr]
