"""RBAC permission query and fallback service."""

from __future__ import annotations

import os
from functools import lru_cache

from sqlalchemy.exc import SQLAlchemyError

from app.domains.rbac.dao.rbac_dao import RoleDao, UserRoleDao

SYSTEM_RESOURCES = (
    "strategies",
    "backtests",
    "data",
    "portfolios",
    "alerts",
    "trading",
    "reports",
    "system",
    "account",
    "templates",
    "teams",
)
SYSTEM_ACTIONS = ("read", "write", "manage")


def _permission_set(resource_names: tuple[str, ...], actions: tuple[str, ...]) -> set[str]:
    return {f"{resource}.{action}" for resource in resource_names for action in actions}


ALL_SYSTEM_PERMISSIONS = _permission_set(SYSTEM_RESOURCES, SYSTEM_ACTIONS)

DEFAULT_ROLE_PERMISSIONS: dict[str, set[str]] = {
    "admin": set(ALL_SYSTEM_PERMISSIONS),
    "trader": {
        *[f"{resource}.read" for resource in SYSTEM_RESOURCES],
        "strategies.write",
        "backtests.write",
        "portfolios.write",
        "alerts.write",
        "alerts.manage",
        "trading.write",
        "templates.write",
        "teams.write",
    },
    "researcher": {
        *[f"{resource}.read" for resource in SYSTEM_RESOURCES],
        "strategies.write",
        "backtests.write",
        "data.write",
        "reports.write",
        "templates.write",
    },
    "viewer": {
        "strategies.read",
        "backtests.read",
        "data.read",
        "portfolios.read",
        "alerts.read",
        "trading.read",
        "reports.read",
        "system.read",
        "account.read",
        "templates.read",
        "teams.read",
    },
}


@lru_cache(maxsize=1)
def get_default_admin_username() -> str:
    return os.getenv("ADMIN_USERNAME", "admin")


class RbacService:
    def __init__(self) -> None:
        self.roles = RoleDao()
        self.user_roles = UserRoleDao()

    def get_default_permissions(self, role_name: str) -> set[str]:
        if role_name == "admin":
            return set(DEFAULT_ROLE_PERMISSIONS["admin"])
        return set(DEFAULT_ROLE_PERMISSIONS.get(role_name, DEFAULT_ROLE_PERMISSIONS["trader"]))

    def get_user_roles(self, user_id: int, username: str | None = None) -> list[str]:
        try:
            roles = [row["name"] for row in self.user_roles.list_user_roles(user_id)]
        except SQLAlchemyError:
            roles = []
        if roles:
            return roles
        if username == get_default_admin_username():
            return ["admin"]
        return ["trader"]

    def get_primary_role(self, user_id: int, username: str | None = None) -> str:
        roles = self.get_user_roles(user_id, username)
        priority = ["admin", "trader", "researcher", "viewer"]
        for role_name in priority:
            if role_name in roles:
                return role_name
        return roles[0] if roles else "trader"

    def get_user_permissions(self, user_id: int, username: str | None = None) -> set[str]:
        try:
            permissions = set(self.user_roles.list_user_permissions(user_id))
        except SQLAlchemyError:
            permissions = set()
        if permissions:
            return permissions

        role_names = self.get_user_roles(user_id, username)
        combined: set[str] = set()
        for role_name in role_names:
            combined.update(self.get_default_permissions(role_name))
        return combined

    def check_permission(self, user_id: int, resource: str, action: str, username: str | None = None) -> bool:
        permissions = self.get_user_permissions(user_id, username)
        required = f"{resource}.{action}"
        manage_key = f"{resource}.manage"
        return required in permissions or manage_key in permissions
