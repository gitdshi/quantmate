"""Strategy template DAO — templates, comments, ratings."""

from __future__ import annotations

import json
from typing import Any, Optional

from sqlalchemy import text

from app.infrastructure.db.connections import connection


class StrategyTemplateDao:
    """CRUD for strategy_templates."""

    def list_public(
        self,
        category: Optional[str] = None,
        template_type: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        query = (
            "SELECT id, author_id, name, category, template_type, layer, sub_type, "
            "description, version, visibility, downloads, created_at, updated_at "
            "FROM strategy_templates WHERE visibility = 'public'"
        )
        params: dict[str, Any] = {"limit": limit, "offset": offset}
        if category:
            query += " AND category = :cat"
            params["cat"] = category
        if template_type:
            query += " AND template_type = :ttype"
            params["ttype"] = template_type
        query += " ORDER BY downloads DESC, updated_at DESC LIMIT :limit OFFSET :offset"
        with connection("quantmate") as conn:
            rows = conn.execute(text(query), params).fetchall()
            return [dict(r._mapping) for r in rows]

    def count_public(self, category: Optional[str] = None, template_type: Optional[str] = None) -> int:
        query = "SELECT COUNT(*) AS cnt FROM strategy_templates WHERE visibility = 'public'"
        params: dict[str, Any] = {}
        if category:
            query += " AND category = :cat"
            params["cat"] = category
        if template_type:
            query += " AND template_type = :ttype"
            params["ttype"] = template_type
        with connection("quantmate") as conn:
            row = conn.execute(text(query), params).fetchone()
            return row._mapping["cnt"] if row else 0

    def list_for_user(
        self,
        user_id: int,
        source: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        query = "SELECT * FROM strategy_templates WHERE author_id = :uid"
        params: dict[str, Any] = {"uid": user_id, "limit": limit, "offset": offset}
        if source:
            query += " AND source = :source"
            params["source"] = source
        query += " ORDER BY updated_at DESC LIMIT :limit OFFSET :offset"
        with connection("quantmate") as conn:
            rows = conn.execute(text(query), params).fetchall()
            return [dict(r._mapping) for r in rows]

    def count_for_user(self, user_id: int, source: Optional[str] = None) -> int:
        query = "SELECT COUNT(*) AS cnt FROM strategy_templates WHERE author_id = :uid"
        params: dict[str, Any] = {"uid": user_id}
        if source:
            query += " AND source = :source"
            params["source"] = source
        with connection("quantmate") as conn:
            row = conn.execute(text(query), params).fetchone()
            return row._mapping["cnt"] if row else 0

    def get(self, template_id: int) -> Optional[dict[str, Any]]:
        with connection("quantmate") as conn:
            row = conn.execute(
                text("SELECT * FROM strategy_templates WHERE id = :tid"),
                {"tid": template_id},
            ).fetchone()
            return dict(row._mapping) if row else None

    def create(
        self,
        author_id: int,
        name: str,
        code: str,
        category: Optional[str] = None,
        description: Optional[str] = None,
        params_schema: Optional[dict] = None,
        default_params: Optional[dict] = None,
        visibility: str = "private",
        source_template_id: Optional[int] = None,
        source: str = "personal",
        template_type: str = "standalone",
    ) -> int:
        with connection("quantmate") as conn:
            result = conn.execute(
                text(
                    "INSERT INTO strategy_templates "
                    "(author_id, source_template_id, source, name, category, description, code, "
                    "params_schema, default_params, visibility, template_type) "
                    "VALUES (:author, :src_id, :src, :name, :cat, :desc, :code, :schema, :defaults, :vis, :ttype)"
                ),
                {
                    "author": author_id,
                    "src_id": source_template_id,
                    "src": source,
                    "name": name,
                    "cat": category,
                    "desc": description,
                    "code": code,
                    "schema": json.dumps(params_schema) if params_schema else None,
                    "defaults": json.dumps(default_params) if default_params else None,
                    "vis": visibility,
                    "ttype": template_type,
                },
            )
            conn.commit()
            return result.lastrowid  # type: ignore[return-value]

    def update(self, template_id: int, author_id: int, **fields) -> None:
        allowed = {"name", "category", "description", "code", "visibility", "version"}
        data: dict[str, Any] = {}
        for k, v in fields.items():
            if k in allowed and v is not None:
                data[k] = v
        if "params_schema" in fields and fields["params_schema"] is not None:
            data["params_schema"] = json.dumps(fields["params_schema"])
        if "default_params" in fields and fields["default_params"] is not None:
            data["default_params"] = json.dumps(fields["default_params"])
        if not data:
            return
        set_clause = ", ".join(f"{k} = :{k}" for k in data)
        with connection("quantmate") as conn:
            conn.execute(
                text(
                    f"UPDATE strategy_templates SET {set_clause}, updated_at = NOW() WHERE id = :tid AND author_id = :uid"
                ),
                {**data, "tid": template_id, "uid": author_id},
            )
            conn.commit()

    def delete(self, template_id: int, author_id: int) -> bool:
        with connection("quantmate") as conn:
            result = conn.execute(
                text("DELETE FROM strategy_templates WHERE id = :tid AND author_id = :uid"),
                {"tid": template_id, "uid": author_id},
            )
            conn.commit()
            return result.rowcount > 0  # type: ignore[union-attr]

    def increment_downloads(self, template_id: int) -> None:
        with connection("quantmate") as conn:
            conn.execute(
                text("UPDATE strategy_templates SET downloads = downloads + 1 WHERE id = :tid"),
                {"tid": template_id},
            )
            conn.commit()


class StrategyCommentDao:
    """CRUD for strategy_comments."""

    def list_for_template(self, template_id: int) -> list[dict[str, Any]]:
        with connection("quantmate") as conn:
            rows = conn.execute(
                text("SELECT * FROM strategy_comments WHERE template_id = :tid ORDER BY created_at DESC"),
                {"tid": template_id},
            ).fetchall()
            return [dict(r._mapping) for r in rows]

    def create(self, template_id: int, user_id: int, content: str, parent_id: Optional[int] = None) -> int:
        with connection("quantmate") as conn:
            result = conn.execute(
                text(
                    "INSERT INTO strategy_comments (template_id, user_id, content, parent_id) "
                    "VALUES (:tid, :uid, :content, :pid)"
                ),
                {"tid": template_id, "uid": user_id, "content": content, "pid": parent_id},
            )
            conn.commit()
            return result.lastrowid  # type: ignore[return-value]

    def delete(self, comment_id: int, user_id: int) -> bool:
        with connection("quantmate") as conn:
            result = conn.execute(
                text("DELETE FROM strategy_comments WHERE id = :cid AND user_id = :uid"),
                {"cid": comment_id, "uid": user_id},
            )
            conn.commit()
            return result.rowcount > 0  # type: ignore[union-attr]


class StrategyRatingDao:
    """CRUD for strategy_ratings."""

    def get_for_template(self, template_id: int) -> dict[str, Any]:
        with connection("quantmate") as conn:
            row = conn.execute(
                text(
                    "SELECT AVG(rating) AS avg_rating, COUNT(*) AS count FROM strategy_ratings WHERE template_id = :tid"
                ),
                {"tid": template_id},
            ).fetchone()
            return (
                {"avg_rating": float(row._mapping["avg_rating"] or 0), "count": row._mapping["count"]}
                if row
                else {"avg_rating": 0, "count": 0}
            )

    def upsert(self, template_id: int, user_id: int, rating: int, review: Optional[str] = None) -> None:
        with connection("quantmate") as conn:
            existing = conn.execute(
                text("SELECT id FROM strategy_ratings WHERE template_id = :tid AND user_id = :uid"),
                {"tid": template_id, "uid": user_id},
            ).fetchone()
            if existing:
                conn.execute(
                    text("UPDATE strategy_ratings SET rating = :r, review = :rev WHERE id = :rid"),
                    {"r": rating, "rev": review, "rid": existing._mapping["id"]},
                )
            else:
                conn.execute(
                    text(
                        "INSERT INTO strategy_ratings (template_id, user_id, rating, review) "
                        "VALUES (:tid, :uid, :r, :rev)"
                    ),
                    {"tid": template_id, "uid": user_id, "r": rating, "rev": review},
                )
            conn.commit()

    def list_for_template(self, template_id: int) -> list[dict[str, Any]]:
        with connection("quantmate") as conn:
            rows = conn.execute(
                text("SELECT * FROM strategy_ratings WHERE template_id = :tid ORDER BY created_at DESC"),
                {"tid": template_id},
            ).fetchall()
            return [dict(r._mapping) for r in rows]
