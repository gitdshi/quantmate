"""Unit tests for app.domains.rbac.dao.rbac_dao."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import app.domains.rbac.dao.rbac_dao as _mod


def _fake_conn():
    conn = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)
    return ctx, conn


@pytest.fixture(autouse=True)
def _mock_connection(monkeypatch):
    ctx, conn = _fake_conn()
    monkeypatch.setattr(_mod, "connection", lambda db: ctx)
    return conn


def _row(**kw):
    m = MagicMock()
    m._mapping = kw
    return m


# ── RoleDao ──────────────────────────────────────────────────────

class TestRoleDao:
    def test_list_all(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(
            fetchall=MagicMock(return_value=[_row(id=1, name="admin")])
        )
        result = _mod.RoleDao().list_all()
        assert isinstance(result, list)

    def test_get(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(
            fetchone=MagicMock(return_value=_row(id=1, name="admin"))
        )
        result = _mod.RoleDao().get(role_id=1)
        assert result is not None

    def test_get_not_found(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(
            fetchone=MagicMock(return_value=None)
        )
        result = _mod.RoleDao().get(role_id=999)
        assert result is None

    def test_get_by_name(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(
            fetchone=MagicMock(return_value=_row(id=1, name="admin"))
        )
        result = _mod.RoleDao().get_by_name("admin")
        assert result is not None

    def test_create(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(lastrowid=5)
        result = _mod.RoleDao().create(name="editor", description="Edit stuff")
        assert result == 5

    def test_update(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(rowcount=1)
        result = _mod.RoleDao().update(role_id=1, description="Updated")
        assert result is True

    def test_delete_non_system(self, _mock_connection):
        # First call: SELECT is_system → fetchone returns role with is_system=0
        role_row = MagicMock()
        role_row.is_system = 0
        # Subsequent calls: DELETE operations with rowcount=1
        delete_result = MagicMock(rowcount=1)
        _mock_connection.execute.side_effect = [
            MagicMock(fetchone=MagicMock(return_value=role_row)),  # SELECT is_system
            MagicMock(rowcount=1),  # DELETE role_permissions
            MagicMock(rowcount=0),  # DELETE user_roles
            delete_result,  # DELETE roles
        ]
        result = _mod.RoleDao().delete(role_id=2)
        assert result is True

    def test_set_permissions(self, _mock_connection):
        _mod.RoleDao().set_permissions(role_id=1, permission_ids=[1, 2, 3])
        assert _mock_connection.execute.called

    def test_list_role_permissions(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(
            fetchall=MagicMock(return_value=[_row(id=1, resource="data", action="read")])
        )
        result = _mod.RoleDao().list_role_permissions(role_id=1)
        assert isinstance(result, list)


# ── PermissionDao ────────────────────────────────────────────────

class TestPermissionDao:
    def test_list_all(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(
            fetchall=MagicMock(return_value=[_row(id=1, resource="data", action="read")])
        )
        result = _mod.PermissionDao().list_all()
        assert isinstance(result, list)


# ── UserRoleDao ──────────────────────────────────────────────────

class TestUserRoleDao:
    def test_list_user_roles(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(
            fetchall=MagicMock(return_value=[_row(role_id=1, role_name="admin")])
        )
        result = _mod.UserRoleDao().list_user_roles(user_id=1)
        assert isinstance(result, list)

    def test_list_user_permissions(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(
            fetchall=MagicMock(return_value=[_row(resource="data", action="read")])
        )
        result = _mod.UserRoleDao().list_user_permissions(user_id=1)
        assert isinstance(result, list)

    def test_set_user_roles(self, _mock_connection):
        _mod.UserRoleDao().set_user_roles(user_id=1, role_ids=[1, 2], assigned_by=99)
        assert _mock_connection.execute.called

    def test_list_users_with_roles(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(
            fetchall=MagicMock(return_value=[
                _row(user_id=1, username="alice", role_id=1, role_name="admin")
            ])
        )
        result = _mod.UserRoleDao().list_users_with_roles()
        assert isinstance(result, list)
