"""Collaboration DAO — team workspaces, members, strategy shares."""

from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import text

from app.infrastructure.db.connections import connection


class TeamWorkspaceDao:
    """CRUD for team_workspaces."""

    def list_for_user(self, user_id: int) -> list[dict[str, Any]]:
        with connection("quantmate") as conn:
            rows = conn.execute(
                text(
                    "SELECT tw.*, wm.role AS role, "
                    "COALESCE(member_counts.members, 0) AS members, "
                    "COALESCE(strategy_counts.strategies, 0) AS strategies "
                    "FROM team_workspaces tw "
                    "INNER JOIN workspace_members wm ON tw.id = wm.workspace_id AND wm.user_id = :uid "
                    "LEFT JOIN ("
                    "SELECT workspace_id, COUNT(*) AS members FROM workspace_members GROUP BY workspace_id"
                    ") member_counts ON member_counts.workspace_id = tw.id "
                    "LEFT JOIN ("
                    "SELECT shared_with_team_id AS workspace_id, COUNT(*) AS strategies "
                    "FROM strategy_shares WHERE shared_with_team_id IS NOT NULL GROUP BY shared_with_team_id"
                    ") strategy_counts ON strategy_counts.workspace_id = tw.id "
                    "WHERE tw.status = 'active' "
                    "ORDER BY tw.name"
                ),
                {"uid": user_id},
            ).fetchall()
            return [dict(r._mapping) for r in rows]

    def get(self, workspace_id: int) -> Optional[dict[str, Any]]:
        with connection("quantmate") as conn:
            row = conn.execute(
                text("SELECT * FROM team_workspaces WHERE id = :wid"),
                {"wid": workspace_id},
            ).fetchone()
            return dict(row._mapping) if row else None

    def create(self, owner_id: int, name: str, description: Optional[str] = None) -> int:
        with connection("quantmate") as conn:
            result = conn.execute(
                text("INSERT INTO team_workspaces (name, description, owner_id) VALUES (:name, :desc, :oid)"),
                {"name": name, "desc": description, "oid": owner_id},
            )
            workspace_id = result.lastrowid
            # Auto-add owner as member with 'owner' role
            conn.execute(
                text("INSERT INTO workspace_members (workspace_id, user_id, role) VALUES (:wid, :uid, 'owner')"),
                {"wid": workspace_id, "uid": owner_id},
            )
            conn.commit()
            return workspace_id  # type: ignore[return-value]

    def update(self, workspace_id: int, owner_id: int, **fields) -> None:
        allowed = {"name", "description", "max_members", "status"}
        data = {k: v for k, v in fields.items() if k in allowed and v is not None}
        if not data:
            return
        set_clause = ", ".join(f"{k} = :{k}" for k in data)
        with connection("quantmate") as conn:
            conn.execute(
                text(
                    f"UPDATE team_workspaces SET {set_clause}, updated_at = NOW() WHERE id = :wid AND owner_id = :oid"
                ),
                {**data, "wid": workspace_id, "oid": owner_id},
            )
            conn.commit()

    def delete(self, workspace_id: int, owner_id: int) -> bool:
        with connection("quantmate") as conn:
            result = conn.execute(
                text("DELETE FROM team_workspaces WHERE id = :wid AND owner_id = :oid"),
                {"wid": workspace_id, "oid": owner_id},
            )
            conn.commit()
            return result.rowcount > 0  # type: ignore[union-attr]


class WorkspaceMemberDao:
    """CRUD for workspace_members."""

    def list_members(self, workspace_id: int) -> list[dict[str, Any]]:
        with connection("quantmate") as conn:
            rows = conn.execute(
                text(
                    "SELECT wm.id, wm.workspace_id, wm.user_id, wm.role, wm.joined_at, "
                    "u.username, u.email "
                    "FROM workspace_members wm "
                    "LEFT JOIN users u ON u.id = wm.user_id "
                    "WHERE wm.workspace_id = :wid "
                    "ORDER BY wm.joined_at"
                ),
                {"wid": workspace_id},
            ).fetchall()
            return [dict(r._mapping) for r in rows]

    def get_member(self, workspace_id: int, user_id: int) -> Optional[dict[str, Any]]:
        with connection("quantmate") as conn:
            row = conn.execute(
                text("SELECT * FROM workspace_members WHERE workspace_id = :wid AND user_id = :uid"),
                {"wid": workspace_id, "uid": user_id},
            ).fetchone()
            return dict(row._mapping) if row else None

    def add_member(self, workspace_id: int, user_id: int, role: str = "member") -> None:
        with connection("quantmate") as conn:
            conn.execute(
                text("INSERT IGNORE INTO workspace_members (workspace_id, user_id, role) VALUES (:wid, :uid, :role)"),
                {"wid": workspace_id, "uid": user_id, "role": role},
            )
            conn.commit()

    def update_role(self, workspace_id: int, user_id: int, role: str) -> None:
        with connection("quantmate") as conn:
            conn.execute(
                text("UPDATE workspace_members SET role = :role WHERE workspace_id = :wid AND user_id = :uid"),
                {"role": role, "wid": workspace_id, "uid": user_id},
            )
            conn.commit()

    def remove_member(self, workspace_id: int, user_id: int) -> bool:
        with connection("quantmate") as conn:
            result = conn.execute(
                text("DELETE FROM workspace_members WHERE workspace_id = :wid AND user_id = :uid AND role != 'owner'"),
                {"wid": workspace_id, "uid": user_id},
            )
            conn.commit()
            return result.rowcount > 0  # type: ignore[union-attr]

    def count_members(self, workspace_id: int) -> int:
        with connection("quantmate") as conn:
            row = conn.execute(
                text("SELECT COUNT(*) AS cnt FROM workspace_members WHERE workspace_id = :wid"),
                {"wid": workspace_id},
            ).fetchone()
            return row._mapping["cnt"] if row else 0


class StrategyShareDao:
    """CRUD for strategy_shares."""

    def list_for_strategy(self, strategy_id: int) -> list[dict[str, Any]]:
        with connection("quantmate") as conn:
            rows = conn.execute(
                text(
                    "SELECT ss.*, s.name AS strategy_name, "
                    "sender.username AS shared_by_username, recipient.username AS shared_with_username, "
                    "tw.name AS shared_with_team_name "
                    "FROM strategy_shares ss "
                    "LEFT JOIN strategies s ON s.id = ss.strategy_id "
                    "LEFT JOIN users sender ON sender.id = ss.shared_by "
                    "LEFT JOIN users recipient ON recipient.id = ss.shared_with_user_id "
                    "LEFT JOIN team_workspaces tw ON tw.id = ss.shared_with_team_id "
                    "WHERE ss.strategy_id = :sid "
                    "ORDER BY ss.created_at DESC"
                ),
                {"sid": strategy_id},
            ).fetchall()
            return [dict(r._mapping) for r in rows]

    def list_shared_with_user(self, user_id: int) -> list[dict[str, Any]]:
        with connection("quantmate") as conn:
            rows = conn.execute(
                text(
                    "SELECT DISTINCT ss.*, s.name AS strategy_name, "
                    "sender.username AS shared_by_username, recipient.username AS shared_with_username, "
                    "tw.name AS shared_with_team_name "
                    "FROM strategy_shares ss "
                    "LEFT JOIN strategies s ON s.id = ss.strategy_id "
                    "LEFT JOIN users sender ON sender.id = ss.shared_by "
                    "LEFT JOIN users recipient ON recipient.id = ss.shared_with_user_id "
                    "LEFT JOIN team_workspaces tw ON tw.id = ss.shared_with_team_id "
                    "WHERE ss.shared_with_user_id = :uid "
                    "OR ss.shared_with_team_id IN ("
                    "SELECT workspace_id FROM workspace_members WHERE user_id = :uid"
                    ") "
                    "ORDER BY ss.created_at DESC"
                ),
                {"uid": user_id},
            ).fetchall()
            return [dict(r._mapping) for r in rows]

    def list_shared_by_user(self, user_id: int) -> list[dict[str, Any]]:
        with connection("quantmate") as conn:
            rows = conn.execute(
                text(
                    "SELECT ss.*, s.name AS strategy_name, "
                    "sender.username AS shared_by_username, recipient.username AS shared_with_username, "
                    "tw.name AS shared_with_team_name "
                    "FROM strategy_shares ss "
                    "LEFT JOIN strategies s ON s.id = ss.strategy_id "
                    "LEFT JOIN users sender ON sender.id = ss.shared_by "
                    "LEFT JOIN users recipient ON recipient.id = ss.shared_with_user_id "
                    "LEFT JOIN team_workspaces tw ON tw.id = ss.shared_with_team_id "
                    "WHERE ss.shared_by = :uid "
                    "ORDER BY ss.created_at DESC"
                ),
                {"uid": user_id},
            ).fetchall()
            return [dict(r._mapping) for r in rows]

    def share(
        self,
        strategy_id: int,
        shared_by: int,
        shared_with_user_id: Optional[int] = None,
        shared_with_team_id: Optional[int] = None,
        permission: str = "view",
    ) -> int:
        with connection("quantmate") as conn:
            result = conn.execute(
                text(
                    "INSERT INTO strategy_shares (strategy_id, shared_by, shared_with_user_id, shared_with_team_id, permission) "
                    "VALUES (:sid, :by, :uid, :tid, :perm)"
                ),
                {
                    "sid": strategy_id,
                    "by": shared_by,
                    "uid": shared_with_user_id,
                    "tid": shared_with_team_id,
                    "perm": permission,
                },
            )
            conn.commit()
            return result.lastrowid  # type: ignore[return-value]

    def revoke(self, share_id: int, shared_by: int) -> bool:
        with connection("quantmate") as conn:
            result = conn.execute(
                text("DELETE FROM strategy_shares WHERE id = :sid AND shared_by = :by"),
                {"sid": share_id, "by": shared_by},
            )
            conn.commit()
            return result.rowcount > 0  # type: ignore[union-attr]
