"""Collaboration domain service — teams and sharing."""

from __future__ import annotations

from typing import Any, Optional

from app.domains.collaboration.dao.collaboration_dao import (
    TeamWorkspaceDao,
    WorkspaceMemberDao,
    StrategyShareDao,
)


class CollaborationService:
    def __init__(self) -> None:
        self._workspace_dao = TeamWorkspaceDao()
        self._member_dao = WorkspaceMemberDao()
        self._share_dao = StrategyShareDao()

    # --- Workspaces ---

    def list_workspaces(self, user_id: int) -> list[dict[str, Any]]:
        return self._workspace_dao.list_for_user(user_id)

    def get_workspace(self, workspace_id: int, user_id: int) -> dict[str, Any]:
        ws = self._workspace_dao.get(workspace_id)
        if not ws:
            raise KeyError("Workspace not found")
        member = self._member_dao.get_member(workspace_id, user_id)
        if not member:
            raise PermissionError("Not a member of this workspace")
        ws["my_role"] = member["role"]
        return ws

    def create_workspace(self, user_id: int, name: str, description: Optional[str] = None) -> dict[str, Any]:
        ws_id = self._workspace_dao.create(user_id, name, description)
        return self.get_workspace(ws_id, user_id)

    def update_workspace(self, user_id: int, workspace_id: int, **fields) -> dict[str, Any]:
        ws = self._workspace_dao.get(workspace_id)
        if not ws or ws["owner_id"] != user_id:
            raise KeyError("Workspace not found or not owner")
        self._workspace_dao.update(workspace_id, user_id, **fields)
        return self.get_workspace(workspace_id, user_id)

    def delete_workspace(self, user_id: int, workspace_id: int) -> None:
        if not self._workspace_dao.delete(workspace_id, user_id):
            raise KeyError("Workspace not found or not owner")

    # --- Members ---

    def list_members(self, workspace_id: int, user_id: int) -> list[dict[str, Any]]:
        self.get_workspace(workspace_id, user_id)  # access check
        return self._member_dao.list_members(workspace_id)

    def add_member(self, workspace_id: int, user_id: int, target_user_id: int, role: str = "member") -> None:
        ws = self._workspace_dao.get(workspace_id)
        if not ws:
            raise KeyError("Workspace not found")
        member = self._member_dao.get_member(workspace_id, user_id)
        if not member or member["role"] not in ("owner", "admin"):
            raise PermissionError("Insufficient permissions")
        count = self._member_dao.count_members(workspace_id)
        if count >= ws.get("max_members", 10):
            raise ValueError("Workspace member limit reached")
        self._member_dao.add_member(workspace_id, target_user_id, role)

    def remove_member(self, workspace_id: int, user_id: int, target_user_id: int) -> None:
        member = self._member_dao.get_member(workspace_id, user_id)
        if not member or member["role"] not in ("owner", "admin"):
            raise PermissionError("Insufficient permissions")
        if not self._member_dao.remove_member(workspace_id, target_user_id):
            raise KeyError("Member not found or cannot remove owner")

    # --- Strategy Sharing ---

    def list_shares(self, strategy_id: int, user_id: int) -> list[dict[str, Any]]:
        return self._share_dao.list_for_strategy(strategy_id)

    def list_shared_with_me(self, user_id: int) -> list[dict[str, Any]]:
        return self._share_dao.list_shared_with_user(user_id)

    def share_strategy(
        self,
        strategy_id: int,
        user_id: int,
        shared_with_user_id: Optional[int] = None,
        shared_with_team_id: Optional[int] = None,
        permission: str = "view",
    ) -> int:
        return self._share_dao.share(
            strategy_id,
            user_id,
            shared_with_user_id=shared_with_user_id,
            shared_with_team_id=shared_with_team_id,
            permission=permission,
        )

    def revoke_share(self, share_id: int, user_id: int) -> None:
        if not self._share_dao.revoke(share_id, user_id):
            raise KeyError("Share not found")
