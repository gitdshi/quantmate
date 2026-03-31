"""DAO helpers for RBAC tables."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Optional

from sqlalchemy import text

from app.infrastructure.db.connections import connection


class RoleDao:
    def list_all(self) -> list[dict[str, Any]]:
        with connection("quantmate") as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT id, name, description, is_system, created_at, updated_at
                    FROM roles
                    ORDER BY is_system DESC, name ASC
                    """
                )
            ).fetchall()
            return [dict(row._mapping) for row in rows]

    def get(self, role_id: int) -> Optional[dict[str, Any]]:
        with connection("quantmate") as conn:
            row = conn.execute(
                text(
                    """
                    SELECT id, name, description, is_system, created_at, updated_at
                    FROM roles
                    WHERE id = :role_id
                    """
                ),
                {"role_id": role_id},
            ).fetchone()
            return dict(row._mapping) if row else None

    def get_by_name(self, role_name: str) -> Optional[dict[str, Any]]:
        with connection("quantmate") as conn:
            row = conn.execute(
                text(
                    """
                    SELECT id, name, description, is_system, created_at, updated_at
                    FROM roles
                    WHERE name = :role_name
                    """
                ),
                {"role_name": role_name},
            ).fetchone()
            return dict(row._mapping) if row else None

    def create(self, name: str, description: Optional[str], is_system: bool = False) -> int:
        with connection("quantmate") as conn:
            result = conn.execute(
                text(
                    """
                    INSERT INTO roles (name, description, is_system)
                    VALUES (:name, :description, :is_system)
                    """
                ),
                {
                    "name": name,
                    "description": description,
                    "is_system": 1 if is_system else 0,
                },
            )
            conn.commit()
            return int(result.lastrowid)

    def update(self, role_id: int, description: Optional[str]) -> bool:
        with connection("quantmate") as conn:
            result = conn.execute(
                text(
                    """
                    UPDATE roles
                    SET description = :description, updated_at = CURRENT_TIMESTAMP
                    WHERE id = :role_id
                    """
                ),
                {"role_id": role_id, "description": description},
            )
            conn.commit()
            return result.rowcount > 0

    def delete(self, role_id: int) -> bool:
        with connection("quantmate") as conn:
            role = conn.execute(
                text("SELECT is_system FROM roles WHERE id = :role_id"),
                {"role_id": role_id},
            ).fetchone()
            if not role or bool(role.is_system):
                return False
            conn.execute(text("DELETE FROM role_permissions WHERE role_id = :role_id"), {"role_id": role_id})
            conn.execute(text("DELETE FROM user_roles WHERE role_id = :role_id"), {"role_id": role_id})
            result = conn.execute(text("DELETE FROM roles WHERE id = :role_id"), {"role_id": role_id})
            conn.commit()
            return result.rowcount > 0

    def set_permissions(self, role_id: int, permission_ids: list[int]) -> None:
        with connection("quantmate") as conn:
            conn.execute(text("DELETE FROM role_permissions WHERE role_id = :role_id"), {"role_id": role_id})
            for permission_id in permission_ids:
                conn.execute(
                    text(
                        """
                        INSERT INTO role_permissions (role_id, permission_id)
                        VALUES (:role_id, :permission_id)
                        """
                    ),
                    {"role_id": role_id, "permission_id": permission_id},
                )
            conn.commit()

    def list_role_permissions(self, role_id: int) -> list[dict[str, Any]]:
        with connection("quantmate") as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT p.id, p.resource, p.action, p.description
                    FROM role_permissions rp
                    JOIN permissions p ON p.id = rp.permission_id
                    WHERE rp.role_id = :role_id
                    ORDER BY p.resource ASC, p.action ASC
                    """
                ),
                {"role_id": role_id},
            ).fetchall()
            return [dict(row._mapping) for row in rows]


class PermissionDao:
    def list_all(self) -> list[dict[str, Any]]:
        with connection("quantmate") as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT id, resource, action, description, is_system, created_at, updated_at
                    FROM permissions
                    ORDER BY resource ASC, action ASC
                    """
                )
            ).fetchall()
            return [dict(row._mapping) for row in rows]


class UserRoleDao:
    def list_user_roles(self, user_id: int) -> list[dict[str, Any]]:
        with connection("quantmate") as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT r.id, r.name, r.description, ur.assigned_at, ur.is_active
                    FROM user_roles ur
                    JOIN roles r ON r.id = ur.role_id
                    WHERE ur.user_id = :user_id AND ur.is_active = 1
                    ORDER BY r.is_system DESC, r.name ASC
                    """
                ),
                {"user_id": user_id},
            ).fetchall()
            return [dict(row._mapping) for row in rows]

    def list_user_permissions(self, user_id: int) -> list[str]:
        with connection("quantmate") as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT DISTINCT CONCAT(p.resource, '.', p.action) AS permission_key
                    FROM user_roles ur
                    JOIN role_permissions rp ON rp.role_id = ur.role_id
                    JOIN permissions p ON p.id = rp.permission_id
                    WHERE ur.user_id = :user_id AND ur.is_active = 1
                    """
                ),
                {"user_id": user_id},
            ).fetchall()
            return [row.permission_key for row in rows]

    def set_user_roles(self, user_id: int, role_ids: list[int], assigned_by: Optional[int]) -> None:
        with connection("quantmate") as conn:
            conn.execute(text("DELETE FROM user_roles WHERE user_id = :user_id"), {"user_id": user_id})
            for role_id in role_ids:
                conn.execute(
                    text(
                        """
                        INSERT INTO user_roles (user_id, role_id, assigned_by, is_active)
                        VALUES (:user_id, :role_id, :assigned_by, 1)
                        """
                    ),
                    {
                        "user_id": user_id,
                        "role_id": role_id,
                        "assigned_by": assigned_by,
                    },
                )
            conn.commit()

    def list_users_with_roles(self) -> list[dict[str, Any]]:
        with connection("quantmate") as conn:
            users = conn.execute(
                text(
                    """
                    SELECT id, username, email, is_active, created_at
                    FROM users
                    ORDER BY created_at DESC, id DESC
                    """
                )
            ).fetchall()
            role_rows = conn.execute(
                text(
                    """
                    SELECT ur.user_id, r.id AS role_id, r.name, r.description
                    FROM user_roles ur
                    JOIN roles r ON r.id = ur.role_id
                    WHERE ur.is_active = 1
                    ORDER BY r.name ASC
                    """
                )
            ).fetchall()
        roles_by_user: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for row in role_rows:
            roles_by_user[row.user_id].append(
                {
                    "id": row.role_id,
                    "name": row.name,
                    "description": row.description,
                }
            )
        return [
            {
                **dict(row._mapping),
                "roles": roles_by_user.get(row.id, []),
            }
            for row in users
        ]
