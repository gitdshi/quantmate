"""Broker Config DAO.

All SQL touching `quantmate.broker_configs` lives here.
"""
from __future__ import annotations

from typing import Optional

from sqlalchemy import text

from app.infrastructure.db.connections import connection


class BrokerConfigDao:
    def list_by_user(self, user_id: int) -> list[dict]:
        with connection("quantmate") as conn:
            rows = conn.execute(
                text("""
                    SELECT id, user_id, broker_type, name, is_active, created_at, updated_at
                    FROM broker_configs WHERE user_id = :uid ORDER BY created_at DESC
                """),
                {"uid": user_id},
            ).fetchall()
            return [
                {
                    "id": r.id, "user_id": r.user_id, "broker_type": r.broker_type,
                    "name": r.name, "is_active": bool(r.is_active),
                    "created_at": r.created_at, "updated_at": r.updated_at,
                }
                for r in rows
            ]

    def create(self, user_id: int, broker_type: str, name: str, config_json_encrypted: str) -> int:
        with connection("quantmate") as conn:
            result = conn.execute(
                text("""
                    INSERT INTO broker_configs (user_id, broker_type, name, config_json_encrypted)
                    VALUES (:uid, :bt, :name, :cfg)
                """),
                {"uid": user_id, "bt": broker_type, "name": name, "cfg": config_json_encrypted},
            )
            conn.commit()
            return int(result.lastrowid)

    def update(self, config_id: int, user_id: int, **kwargs) -> bool:
        allowed = {"name", "broker_type", "config_json_encrypted", "is_active"}
        updates = []
        params: dict = {"cid": config_id, "uid": user_id}
        for key, val in kwargs.items():
            if key in allowed:
                updates.append(f"{key} = :{key}")
                params[key] = val
        if not updates:
            return False
        with connection("quantmate") as conn:
            result = conn.execute(
                text(f"UPDATE broker_configs SET {', '.join(updates)} WHERE id = :cid AND user_id = :uid"),
                params,
            )
            conn.commit()
            return result.rowcount > 0

    def delete(self, config_id: int, user_id: int) -> bool:
        with connection("quantmate") as conn:
            result = conn.execute(
                text("DELETE FROM broker_configs WHERE id = :cid AND user_id = :uid"),
                {"cid": config_id, "uid": user_id},
            )
            conn.commit()
            return result.rowcount > 0
