"""Strategy Components DAO.

All SQL touching `quantmate.strategy_components` lives here.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from app.infrastructure.db.connections import connection


class StrategyComponentDao:

    def list_for_user(self, user_id: int, layer: Optional[str] = None) -> list[dict[str, Any]]:
        with connection("quantmate") as conn:
            from sqlalchemy import text

            if layer:
                rows = conn.execute(
                    text(
                        """
                        SELECT id, name, layer, sub_type, description, version, is_active, created_at, updated_at
                        FROM strategy_components
                        WHERE user_id = :uid AND layer = :layer
                        ORDER BY layer, updated_at DESC
                        """
                    ),
                    {"uid": user_id, "layer": layer},
                ).fetchall()
            else:
                rows = conn.execute(
                    text(
                        """
                        SELECT id, name, layer, sub_type, description, version, is_active, created_at, updated_at
                        FROM strategy_components
                        WHERE user_id = :uid
                        ORDER BY layer, updated_at DESC
                        """
                    ),
                    {"uid": user_id},
                ).fetchall()
            return [dict(r._mapping) for r in rows]

    def count_for_user(self, user_id: int, layer: Optional[str] = None) -> int:
        with connection("quantmate") as conn:
            from sqlalchemy import text

            if layer:
                row = conn.execute(
                    text("SELECT COUNT(*) AS cnt FROM strategy_components WHERE user_id = :uid AND layer = :layer"),
                    {"uid": user_id, "layer": layer},
                ).fetchone()
            else:
                row = conn.execute(
                    text("SELECT COUNT(*) AS cnt FROM strategy_components WHERE user_id = :uid"),
                    {"uid": user_id},
                ).fetchone()
            return row._mapping["cnt"] if row else 0

    def list_for_user_paginated(
        self, user_id: int, limit: int, offset: int, layer: Optional[str] = None
    ) -> list[dict[str, Any]]:
        with connection("quantmate") as conn:
            from sqlalchemy import text

            if layer:
                rows = conn.execute(
                    text(
                        """
                        SELECT id, name, layer, sub_type, description, version, is_active, created_at, updated_at
                        FROM strategy_components
                        WHERE user_id = :uid AND layer = :layer
                        ORDER BY layer, updated_at DESC
                        LIMIT :limit OFFSET :offset
                        """
                    ),
                    {"uid": user_id, "layer": layer, "limit": limit, "offset": offset},
                ).fetchall()
            else:
                rows = conn.execute(
                    text(
                        """
                        SELECT id, name, layer, sub_type, description, version, is_active, created_at, updated_at
                        FROM strategy_components
                        WHERE user_id = :uid
                        ORDER BY layer, updated_at DESC
                        LIMIT :limit OFFSET :offset
                        """
                    ),
                    {"uid": user_id, "limit": limit, "offset": offset},
                ).fetchall()
            return [dict(r._mapping) for r in rows]

    def name_exists_for_user(self, user_id: int, name: str, layer: str) -> bool:
        with connection("quantmate") as conn:
            from sqlalchemy import text

            row = conn.execute(
                text(
                    "SELECT 1 FROM strategy_components WHERE user_id = :uid AND name = :name AND layer = :layer LIMIT 1"
                ),
                {"uid": user_id, "name": name, "layer": layer},
            ).fetchone()
            return bool(row)

    def insert(
        self,
        user_id: int,
        name: str,
        layer: str,
        sub_type: str,
        description: Optional[str],
        code: Optional[str],
        config_json: Optional[str],
        parameters_json: Optional[str],
        created_at: datetime,
        updated_at: datetime,
    ) -> int:
        with connection("quantmate") as conn:
            from sqlalchemy import text

            result = conn.execute(
                text(
                    """
                    INSERT INTO strategy_components
                        (user_id, name, layer, sub_type, description, code, config, parameters,
                         version, is_active, created_at, updated_at)
                    VALUES
                        (:user_id, :name, :layer, :sub_type, :description, :code, :config, :parameters,
                         1, 1, :created_at, :updated_at)
                    """
                ),
                {
                    "user_id": user_id,
                    "name": name,
                    "layer": layer,
                    "sub_type": sub_type,
                    "description": description,
                    "code": code,
                    "config": config_json,
                    "parameters": parameters_json,
                    "created_at": created_at,
                    "updated_at": updated_at,
                },
            )
            conn.commit()
            return int(result.lastrowid)

    def get_for_user(self, component_id: int, user_id: int) -> Optional[dict[str, Any]]:
        with connection("quantmate") as conn:
            from sqlalchemy import text

            row = conn.execute(
                text(
                    """
                    SELECT id, user_id, name, layer, sub_type, description, code, config,
                           parameters, version, is_active, created_at, updated_at
                    FROM strategy_components
                    WHERE id = :cid AND user_id = :uid
                    """
                ),
                {"cid": component_id, "uid": user_id},
            ).fetchone()
            return dict(row._mapping) if row else None

    def update(self, component_id: int, user_id: int, set_clause: str, params: dict[str, Any]) -> None:
        with connection("quantmate") as conn:
            from sqlalchemy import text

            conn.execute(
                text(
                    f"UPDATE strategy_components SET {set_clause} WHERE id = :cid AND user_id = :uid"
                ),
                {**params, "cid": component_id, "uid": user_id},
            )
            conn.commit()

    def delete_for_user(self, component_id: int, user_id: int) -> bool:
        with connection("quantmate") as conn:
            from sqlalchemy import text

            result = conn.execute(
                text("DELETE FROM strategy_components WHERE id = :cid AND user_id = :uid"),
                {"cid": component_id, "uid": user_id},
            )
            conn.commit()
            return result.rowcount > 0

    def get_ids_for_user(self, component_ids: list[int], user_id: int) -> list[int]:
        """Verify that all component_ids belong to user_id. Returns matching ids."""
        if not component_ids:
            return []
        with connection("quantmate") as conn:
            from sqlalchemy import text

            placeholders = ", ".join(str(int(cid)) for cid in component_ids)
            rows = conn.execute(
                text(
                    f"SELECT id FROM strategy_components WHERE id IN ({placeholders}) AND user_id = :uid"
                ),
                {"uid": user_id},
            ).fetchall()
            return [r._mapping["id"] for r in rows]
