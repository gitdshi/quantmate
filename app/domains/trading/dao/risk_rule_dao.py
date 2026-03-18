"""Risk Rule DAO.

All SQL touching `quantmate.risk_rules` lives here.
"""

from __future__ import annotations

from typing import Optional

from sqlalchemy import text

from app.infrastructure.db.connections import connection


class RiskRuleDao:
    def list_by_user(self, user_id: int, active_only: bool = False) -> list[dict]:
        with connection("quantmate") as conn:
            where = "user_id = :uid"
            if active_only:
                where += " AND is_active = 1"
            rows = conn.execute(
                text(f"""
                    SELECT id, user_id, name, rule_type, condition_expr, threshold,
                           action, is_active, created_at, updated_at
                    FROM risk_rules WHERE {where} ORDER BY created_at DESC
                """),
                {"uid": user_id},
            ).fetchall()
            return [self._row_to_dict(r) for r in rows]

    def create(
        self,
        user_id: int,
        name: str,
        rule_type: str,
        threshold: float,
        action: str = "warn",
        condition_expr: Optional[str] = None,
    ) -> int:
        with connection("quantmate") as conn:
            result = conn.execute(
                text("""
                    INSERT INTO risk_rules (user_id, name, rule_type, condition_expr, threshold, action)
                    VALUES (:uid, :name, :rt, :cond, :thresh, :action)
                """),
                {
                    "uid": user_id,
                    "name": name,
                    "rt": rule_type,
                    "cond": condition_expr,
                    "thresh": threshold,
                    "action": action,
                },
            )
            conn.commit()
            return int(result.lastrowid)

    def update(self, rule_id: int, user_id: int, **kwargs) -> bool:
        allowed = {"name", "rule_type", "condition_expr", "threshold", "action", "is_active"}
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
                text(f"UPDATE risk_rules SET {', '.join(updates)} WHERE id = :rid AND user_id = :uid"),
                params,
            )
            conn.commit()
            return result.rowcount > 0

    def delete(self, rule_id: int, user_id: int) -> bool:
        with connection("quantmate") as conn:
            result = conn.execute(
                text("DELETE FROM risk_rules WHERE id = :rid AND user_id = :uid"),
                {"rid": rule_id, "uid": user_id},
            )
            conn.commit()
            return result.rowcount > 0

    def _row_to_dict(self, row) -> dict:
        return {
            "id": row.id,
            "user_id": row.user_id,
            "name": row.name,
            "rule_type": row.rule_type,
            "condition_expr": row.condition_expr,
            "threshold": float(row.threshold),
            "action": row.action,
            "is_active": bool(row.is_active),
            "created_at": row.created_at,
            "updated_at": row.updated_at,
        }
