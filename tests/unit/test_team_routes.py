"""Tests for Team Collaboration routes."""
import pytest
from unittest.mock import patch
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.routes import teams
from app.api.exception_handlers import register_exception_handlers


@pytest.fixture
def mock_user():
    return {"id": 1, "username": "testuser"}


@pytest.fixture
def client(mock_user):
    test_app = FastAPI()
    register_exception_handlers(test_app)
    test_app.include_router(teams.router, prefix="/api/v1")

    async def override_auth():
        return mock_user

    test_app.dependency_overrides[teams.get_current_user] = override_auth
    return TestClient(test_app)


SAMPLE_WORKSPACE = {
    "id": 1, "name": "Team Alpha", "description": "Alpha team",
    "owner_id": 1, "max_members": 10, "status": "active",
}


class TestWorkspaceRoutes:

    @patch("app.domains.collaboration.service.TeamWorkspaceDao")
    @patch("app.domains.collaboration.service.WorkspaceMemberDao")
    def test_list_workspaces(self, MockMemberDao, MockWsDao, client):
        MockWsDao.return_value.list_for_user.return_value = [SAMPLE_WORKSPACE]
        resp = client.get("/api/v1/teams/workspaces")
        assert resp.status_code == 200

    @patch("app.domains.collaboration.service.WorkspaceMemberDao")
    @patch("app.domains.collaboration.service.TeamWorkspaceDao")
    def test_create_workspace(self, MockWsDao, MockMemberDao, client):
        MockWsDao.return_value.create.return_value = 1
        MockWsDao.return_value.get.return_value = SAMPLE_WORKSPACE
        MockMemberDao.return_value.get_member.return_value = {"workspace_id": 1, "user_id": 1, "role": "owner"}
        resp = client.post("/api/v1/teams/workspaces", json={"name": "Team Alpha", "description": "test"})
        assert resp.status_code == 201

    @patch("app.domains.collaboration.service.WorkspaceMemberDao")
    @patch("app.domains.collaboration.service.TeamWorkspaceDao")
    def test_get_workspace(self, MockWsDao, MockMemberDao, client):
        MockWsDao.return_value.get.return_value = SAMPLE_WORKSPACE
        MockMemberDao.return_value.get_member.return_value = {"workspace_id": 1, "user_id": 1, "role": "owner"}
        resp = client.get("/api/v1/teams/workspaces/1")
        assert resp.status_code == 200

    @patch("app.domains.collaboration.service.WorkspaceMemberDao")
    @patch("app.domains.collaboration.service.TeamWorkspaceDao")
    def test_get_workspace_not_found(self, MockWsDao, MockMemberDao, client):
        MockWsDao.return_value.get.return_value = None
        resp = client.get("/api/v1/teams/workspaces/999")
        assert resp.status_code == 404

    @patch("app.domains.collaboration.service.WorkspaceMemberDao")
    @patch("app.domains.collaboration.service.TeamWorkspaceDao")
    def test_get_workspace_not_member(self, MockWsDao, MockMemberDao, client):
        MockWsDao.return_value.get.return_value = SAMPLE_WORKSPACE
        MockMemberDao.return_value.get_member.return_value = None
        resp = client.get("/api/v1/teams/workspaces/1")
        assert resp.status_code == 403

    @patch("app.domains.collaboration.service.WorkspaceMemberDao")
    @patch("app.domains.collaboration.service.TeamWorkspaceDao")
    def test_update_workspace(self, MockWsDao, MockMemberDao, client):
        MockWsDao.return_value.get.return_value = SAMPLE_WORKSPACE
        MockWsDao.return_value.update.return_value = None
        MockMemberDao.return_value.get_member.return_value = {"workspace_id": 1, "user_id": 1, "role": "owner"}
        resp = client.put("/api/v1/teams/workspaces/1", json={"name": "Updated"})
        assert resp.status_code == 200

    @patch("app.domains.collaboration.service.WorkspaceMemberDao")
    @patch("app.domains.collaboration.service.TeamWorkspaceDao")
    def test_delete_workspace(self, MockWsDao, MockMemberDao, client):
        MockWsDao.return_value.delete.return_value = True
        resp = client.delete("/api/v1/teams/workspaces/1")
        assert resp.status_code == 204

    @patch("app.domains.collaboration.service.WorkspaceMemberDao")
    @patch("app.domains.collaboration.service.TeamWorkspaceDao")
    def test_delete_workspace_not_owner(self, MockWsDao, MockMemberDao, client):
        MockWsDao.return_value.delete.return_value = False
        resp = client.delete("/api/v1/teams/workspaces/999")
        assert resp.status_code == 404


class TestMemberRoutes:

    @patch("app.domains.collaboration.service.WorkspaceMemberDao")
    @patch("app.domains.collaboration.service.TeamWorkspaceDao")
    def test_list_members(self, MockWsDao, MockMemberDao, client):
        MockWsDao.return_value.get.return_value = SAMPLE_WORKSPACE
        MockMemberDao.return_value.get_member.return_value = {"workspace_id": 1, "user_id": 1, "role": "owner"}
        MockMemberDao.return_value.list_members.return_value = [
            {"workspace_id": 1, "user_id": 1, "role": "owner"}
        ]
        resp = client.get("/api/v1/teams/workspaces/1/members")
        assert resp.status_code == 200

    @patch("app.domains.collaboration.service.WorkspaceMemberDao")
    @patch("app.domains.collaboration.service.TeamWorkspaceDao")
    def test_add_member(self, MockWsDao, MockMemberDao, client):
        MockWsDao.return_value.get.return_value = SAMPLE_WORKSPACE
        MockMemberDao.return_value.get_member.return_value = {"workspace_id": 1, "user_id": 1, "role": "owner"}
        MockMemberDao.return_value.count_members.return_value = 2
        MockMemberDao.return_value.add_member.return_value = None
        resp = client.post("/api/v1/teams/workspaces/1/members", json={"user_id": 2, "role": "member"})
        assert resp.status_code == 201

    @patch("app.domains.collaboration.service.WorkspaceMemberDao")
    @patch("app.domains.collaboration.service.TeamWorkspaceDao")
    def test_add_member_limit_reached(self, MockWsDao, MockMemberDao, client):
        ws = {**SAMPLE_WORKSPACE, "max_members": 2}
        MockWsDao.return_value.get.return_value = ws
        MockMemberDao.return_value.get_member.return_value = {"workspace_id": 1, "user_id": 1, "role": "owner"}
        MockMemberDao.return_value.count_members.return_value = 2
        resp = client.post("/api/v1/teams/workspaces/1/members", json={"user_id": 3})
        assert resp.status_code == 400

    @patch("app.domains.collaboration.service.WorkspaceMemberDao")
    @patch("app.domains.collaboration.service.TeamWorkspaceDao")
    def test_remove_member(self, MockWsDao, MockMemberDao, client):
        MockMemberDao.return_value.get_member.return_value = {"workspace_id": 1, "user_id": 1, "role": "owner"}
        MockMemberDao.return_value.remove_member.return_value = True
        resp = client.delete("/api/v1/teams/workspaces/1/members/2")
        assert resp.status_code == 204


class TestShareRoutes:

    @patch("app.domains.collaboration.service.StrategyShareDao")
    def test_list_shared_with_me(self, MockShareDao, client):
        MockShareDao.return_value.list_shared_with_user.return_value = []
        resp = client.get("/api/v1/teams/shares/received")
        assert resp.status_code == 200

    @patch("app.domains.collaboration.service.StrategyShareDao")
    def test_share_strategy(self, MockShareDao, client):
        MockShareDao.return_value.share.return_value = 1
        resp = client.post("/api/v1/teams/shares", json={
            "strategy_id": 1, "shared_with_user_id": 2, "permission": "view",
        })
        assert resp.status_code == 201

    @patch("app.domains.collaboration.service.StrategyShareDao")
    def test_revoke_share(self, MockShareDao, client):
        MockShareDao.return_value.revoke.return_value = True
        resp = client.delete("/api/v1/teams/shares/1")
        assert resp.status_code == 204

    @patch("app.domains.collaboration.service.StrategyShareDao")
    def test_revoke_share_not_found(self, MockShareDao, client):
        MockShareDao.return_value.revoke.return_value = False
        resp = client.delete("/api/v1/teams/shares/999")
        assert resp.status_code == 404
