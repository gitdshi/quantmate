"""Composite Strategies DAO.

All SQL touching `quantmate.composite_strategies` and
`quantmate.composite_component_bindings` lives here.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from app.infrastructure.db.connections import connection


class CompositeStrategyDao:

    # ── Composite Strategy CRUD ──────────────────────────────────────────

    def list_for_user(self, user_id: int) -> list[dict[str, Any]]:
        with connection("quantmate") as conn:
            from sqlalchemy import text

            rows = conn.execute(
                text(
                    """
                    SELECT cs.id, cs.name, cs.description, cs.execution_mode, cs.is_active,
                           cs.created_at, cs.updated_at,
                           COUNT(ccb.id) AS component_count
                    FROM composite_strategies cs
                    LEFT JOIN composite_component_bindings ccb ON ccb.composite_strategy_id = cs.id
                    WHERE cs.user_id = :uid
                    GROUP BY cs.id
                    ORDER BY cs.updated_at DESC
                    """
                ),
                {"uid": user_id},
            ).fetchall()
            return [dict(r._mapping) for r in rows]

    def count_for_user(self, user_id: int) -> int:
        with connection("quantmate") as conn:
            from sqlalchemy import text

            row = conn.execute(
                text("SELECT COUNT(*) AS cnt FROM composite_strategies WHERE user_id = :uid"),
                {"uid": user_id},
            ).fetchone()
            return row._mapping["cnt"] if row else 0

    def list_for_user_paginated(self, user_id: int, limit: int, offset: int) -> list[dict[str, Any]]:
        with connection("quantmate") as conn:
            from sqlalchemy import text

            rows = conn.execute(
                text(
                    """
                    SELECT cs.id, cs.name, cs.description, cs.execution_mode, cs.is_active,
                           cs.created_at, cs.updated_at,
                           COUNT(ccb.id) AS component_count
                    FROM composite_strategies cs
                    LEFT JOIN composite_component_bindings ccb ON ccb.composite_strategy_id = cs.id
                    WHERE cs.user_id = :uid
                    GROUP BY cs.id
                    ORDER BY cs.updated_at DESC
                    LIMIT :limit OFFSET :offset
                    """
                ),
                {"uid": user_id, "limit": limit, "offset": offset},
            ).fetchall()
            return [dict(r._mapping) for r in rows]

    def name_exists_for_user(self, user_id: int, name: str) -> bool:
        with connection("quantmate") as conn:
            from sqlalchemy import text

            row = conn.execute(
                text("SELECT 1 FROM composite_strategies WHERE user_id = :uid AND name = :name LIMIT 1"),
                {"uid": user_id, "name": name},
            ).fetchone()
            return bool(row)

    def insert(
        self,
        user_id: int,
        name: str,
        description: Optional[str],
        portfolio_config_json: Optional[str],
        market_constraints_json: Optional[str],
        execution_mode: str,
        created_at: datetime,
        updated_at: datetime,
    ) -> int:
        with connection("quantmate") as conn:
            from sqlalchemy import text

            result = conn.execute(
                text(
                    """
                    INSERT INTO composite_strategies
                        (user_id, name, description, portfolio_config, market_constraints,
                         execution_mode, is_active, created_at, updated_at)
                    VALUES
                        (:user_id, :name, :description, :portfolio_config, :market_constraints,
                         :execution_mode, 1, :created_at, :updated_at)
                    """
                ),
                {
                    "user_id": user_id,
                    "name": name,
                    "description": description,
                    "portfolio_config": portfolio_config_json,
                    "market_constraints": market_constraints_json,
                    "execution_mode": execution_mode,
                    "created_at": created_at,
                    "updated_at": updated_at,
                },
            )
            conn.commit()
            return int(result.lastrowid)

    def get_for_user(self, composite_id: int, user_id: int) -> Optional[dict[str, Any]]:
        with connection("quantmate") as conn:
            from sqlalchemy import text

            row = conn.execute(
                text(
                    """
                    SELECT id, user_id, name, description, portfolio_config, market_constraints,
                           execution_mode, is_active, created_at, updated_at
                    FROM composite_strategies
                    WHERE id = :cid AND user_id = :uid
                    """
                ),
                {"cid": composite_id, "uid": user_id},
            ).fetchone()
            return dict(row._mapping) if row else None

    def update(self, composite_id: int, user_id: int, set_clause: str, params: dict[str, Any]) -> None:
        with connection("quantmate") as conn:
            from sqlalchemy import text

            conn.execute(
                text(
                    f"UPDATE composite_strategies SET {set_clause} WHERE id = :cid AND user_id = :uid"
                ),
                {**params, "cid": composite_id, "uid": user_id},
            )
            conn.commit()

    def delete_for_user(self, composite_id: int, user_id: int) -> bool:
        with connection("quantmate") as conn:
            from sqlalchemy import text

            result = conn.execute(
                text("DELETE FROM composite_strategies WHERE id = :cid AND user_id = :uid"),
                {"cid": composite_id, "uid": user_id},
            )
            conn.commit()
            return result.rowcount > 0

    # ── Component Bindings ───────────────────────────────────────────────

    def get_bindings(self, composite_id: int) -> list[dict[str, Any]]:
        with connection("quantmate") as conn:
            from sqlalchemy import text

            rows = conn.execute(
                text(
                    """
                    SELECT ccb.id, ccb.composite_strategy_id, ccb.component_id, ccb.layer,
                           ccb.ordinal, ccb.weight, ccb.config_override,
                           sc.name AS component_name, sc.sub_type AS component_sub_type
                    FROM composite_component_bindings ccb
                    JOIN strategy_components sc ON sc.id = ccb.component_id
                    WHERE ccb.composite_strategy_id = :cid
                    ORDER BY ccb.layer, ccb.ordinal
                    """
                ),
                {"cid": composite_id},
            ).fetchall()
            return [dict(r._mapping) for r in rows]

    def replace_bindings(
        self, composite_id: int, bindings: list[dict[str, Any]]
    ) -> None:
        """Replace all bindings for a composite strategy atomically."""
        with connection("quantmate") as conn:
            from sqlalchemy import text

            conn.execute(
                text("DELETE FROM composite_component_bindings WHERE composite_strategy_id = :cid"),
                {"cid": composite_id},
            )
            for b in bindings:
                conn.execute(
                    text(
                        """
                        INSERT INTO composite_component_bindings
                            (composite_strategy_id, component_id, layer, ordinal, weight, config_override)
                        VALUES
                            (:cid, :component_id, :layer, :ordinal, :weight, :config_override)
                        """
                    ),
                    {
                        "cid": composite_id,
                        "component_id": b["component_id"],
                        "layer": b["layer"],
                        "ordinal": b.get("ordinal", 0),
                        "weight": b.get("weight", 1.0),
                        "config_override": b.get("config_override"),
                    },
                )
            conn.commit()

    def add_binding(self, composite_id: int, binding: dict[str, Any]) -> int:
        with connection("quantmate") as conn:
            from sqlalchemy import text

            result = conn.execute(
                text(
                    """
                    INSERT INTO composite_component_bindings
                        (composite_strategy_id, component_id, layer, ordinal, weight, config_override)
                    VALUES
                        (:cid, :component_id, :layer, :ordinal, :weight, :config_override)
                    """
                ),
                {
                    "cid": composite_id,
                    "component_id": binding["component_id"],
                    "layer": binding["layer"],
                    "ordinal": binding.get("ordinal", 0),
                    "weight": binding.get("weight", 1.0),
                    "config_override": binding.get("config_override"),
                },
            )
            conn.commit()
            return int(result.lastrowid)

    def remove_binding(self, composite_id: int, binding_id: int) -> bool:
        with connection("quantmate") as conn:
            from sqlalchemy import text

            result = conn.execute(
                text(
                    "DELETE FROM composite_component_bindings WHERE id = :bid AND composite_strategy_id = :cid"
                ),
                {"bid": binding_id, "cid": composite_id},
            )
            conn.commit()
            return result.rowcount > 0
