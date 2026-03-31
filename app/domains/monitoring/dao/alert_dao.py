"""Alert DAO.

All SQL touching `quantmate.alert_rules`, `alert_history`, `notification_channels` lives here.
"""

from __future__ import annotations

import json
from typing import Optional

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from app.infrastructure.db.connections import connection


def _is_missing_table_error(exc: Exception, table_name: str) -> bool:
    message = str(getattr(exc, "orig", exc)).lower()
    return table_name.lower() in message and ("doesn't exist" in message or "no such table" in message)


class AlertRuleDao:
    def list_by_user(self, user_id: int) -> list[dict]:
        try:
            with connection("quantmate") as conn:
                rows = conn.execute(
                    text("""
                        SELECT id, user_id, name, metric, comparator, threshold,
                               time_window, level, is_active, created_at, updated_at
                        FROM alert_rules WHERE user_id = :uid ORDER BY created_at DESC
                    """),
                    {"uid": user_id},
                ).fetchall()
                return [self._rule_to_dict(r) for r in rows]
        except SQLAlchemyError as exc:
            if _is_missing_table_error(exc, "alert_rules"):
                return []
            raise

    def create(
        self,
        user_id: int,
        name: str,
        metric: str,
        comparator: str,
        threshold: float,
        level: str = "warning",
        time_window: Optional[int] = None,
    ) -> int:
        with connection("quantmate") as conn:
            result = conn.execute(
                text("""
                    INSERT INTO alert_rules (user_id, name, metric, comparator, threshold, time_window, level)
                    VALUES (:uid, :name, :metric, :comp, :thresh, :tw, :level)
                """),
                {
                    "uid": user_id,
                    "name": name,
                    "metric": metric,
                    "comp": comparator,
                    "thresh": threshold,
                    "tw": time_window,
                    "level": level,
                },
            )
            conn.commit()
            return int(result.lastrowid)

    def update(self, rule_id: int, user_id: int, **kwargs) -> bool:
        allowed = {"name", "metric", "comparator", "threshold", "time_window", "level", "is_active"}
        updates = []
        params: dict = {"rid": rule_id, "uid": user_id}
        for key, val in kwargs.items():
            if key in allowed:
                updates.append(f"{key} = :{key}")
                params[key] = val
        if not updates:
            return False
        with connection("quantmate") as conn:
            result = conn.execute(
                text(f"UPDATE alert_rules SET {', '.join(updates)} WHERE id = :rid AND user_id = :uid"),
                params,
            )
            conn.commit()
            return result.rowcount > 0

    def delete(self, rule_id: int, user_id: int) -> bool:
        with connection("quantmate") as conn:
            result = conn.execute(
                text("DELETE FROM alert_rules WHERE id = :rid AND user_id = :uid"),
                {"rid": rule_id, "uid": user_id},
            )
            conn.commit()
            return result.rowcount > 0

    def _rule_to_dict(self, row) -> dict:
        return {
            "id": row.id,
            "user_id": row.user_id,
            "name": row.name,
            "metric": row.metric,
            "comparator": row.comparator,
            "threshold": float(row.threshold),
            "time_window": row.time_window,
            "level": row.level,
            "is_active": bool(row.is_active),
            "created_at": row.created_at,
            "updated_at": row.updated_at,
        }


class AlertHistoryDao:
    def list_by_user(
        self, user_id: int, level: Optional[str] = None, page: int = 1, page_size: int = 20
    ) -> tuple[list[dict], int]:
        try:
            with connection("quantmate") as conn:
                conditions = ["user_id = :uid"]
                params: dict = {"uid": user_id}
                if level:
                    conditions.append("level = :level")
                    params["level"] = level
                where = " AND ".join(conditions)

                total_row = conn.execute(
                    text(f"SELECT COUNT(*) as cnt FROM alert_history WHERE {where}"), params
                ).fetchone()

                params["limit"] = page_size
                params["offset"] = (page - 1) * page_size
                rows = conn.execute(
                    text(f"""
                        SELECT id, rule_id, user_id, triggered_at, level, message, status
                        FROM alert_history WHERE {where}
                        ORDER BY triggered_at DESC LIMIT :limit OFFSET :offset
                    """),
                    params,
                ).fetchall()
                return [
                    {
                        "id": r.id,
                        "rule_id": r.rule_id,
                        "user_id": r.user_id,
                        "triggered_at": r.triggered_at,
                        "level": r.level,
                        "message": r.message,
                        "status": r.status,
                    }
                    for r in rows
                ], total_row.cnt
        except SQLAlchemyError as exc:
            if _is_missing_table_error(exc, "alert_history"):
                return [], 0
            raise

    def insert(self, rule_id: Optional[int], user_id: int, level: str, message: str) -> int:
        with connection("quantmate") as conn:
            result = conn.execute(
                text("""
                    INSERT INTO alert_history (rule_id, user_id, level, message)
                    VALUES (:rid, :uid, :level, :msg)
                """),
                {"rid": rule_id, "uid": user_id, "level": level, "msg": message},
            )
            conn.commit()
            return int(result.lastrowid)

    def acknowledge(self, alert_id: int, user_id: int) -> bool:
        with connection("quantmate") as conn:
            result = conn.execute(
                text("UPDATE alert_history SET status = 'acknowledged' WHERE id = :aid AND user_id = :uid"),
                {"aid": alert_id, "uid": user_id},
            )
            conn.commit()
            return result.rowcount > 0


class NotificationChannelDao:
    def list_by_user(self, user_id: int) -> list[dict]:
        try:
            with connection("quantmate") as conn:
                rows = conn.execute(
                    text("""
                        SELECT id, user_id, channel_type, config_json, is_active, created_at
                        FROM notification_channels WHERE user_id = :uid ORDER BY created_at DESC
                    """),
                    {"uid": user_id},
                ).fetchall()
                return [
                    {
                        "id": r.id,
                        "user_id": r.user_id,
                        "channel_type": r.channel_type,
                        "config": json.loads(r.config_json) if isinstance(r.config_json, str) else r.config_json,
                        "is_active": bool(r.is_active),
                        "created_at": r.created_at,
                    }
                    for r in rows
                ]
        except SQLAlchemyError as exc:
            if _is_missing_table_error(exc, "notification_channels"):
                return []
            raise

    def create(self, user_id: int, channel_type: str, config: dict) -> int:
        with connection("quantmate") as conn:
            result = conn.execute(
                text("""
                    INSERT INTO notification_channels (user_id, channel_type, config_json)
                    VALUES (:uid, :ct, :cfg)
                """),
                {"uid": user_id, "ct": channel_type, "cfg": json.dumps(config)},
            )
            conn.commit()
            return int(result.lastrowid)

    def update(self, channel_id: int, user_id: int, **kwargs) -> bool:
        updates = []
        params: dict = {"cid": channel_id, "uid": user_id}
        if "config" in kwargs:
            updates.append("config_json = :cfg")
            params["cfg"] = json.dumps(kwargs["config"])
        if "is_active" in kwargs:
            updates.append("is_active = :active")
            params["active"] = kwargs["is_active"]
        if not updates:
            return False
        with connection("quantmate") as conn:
            result = conn.execute(
                text(f"UPDATE notification_channels SET {', '.join(updates)} WHERE id = :cid AND user_id = :uid"),
                params,
            )
            conn.commit()
            return result.rowcount > 0

    def delete(self, channel_id: int, user_id: int) -> bool:
        with connection("quantmate") as conn:
            result = conn.execute(
                text("DELETE FROM notification_channels WHERE id = :cid AND user_id = :uid"),
                {"cid": channel_id, "uid": user_id},
            )
            conn.commit()
            return result.rowcount > 0
