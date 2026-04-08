"""Unit tests for TeamWorkspaceDao, WorkspaceMemberDao, StrategyShareDao."""
import pytest
from app.domains.collaboration.dao.collaboration_dao import (
    TeamWorkspaceDao, WorkspaceMemberDao, StrategyShareDao
)


class FR:
    def __init__(self, rows=None, rowcount=0, lastrowid=0):
        self._rows = rows or []; self.rowcount = rowcount; self.lastrowid = lastrowid
    def fetchone(self): return self._rows[0] if self._rows else None
    def fetchall(self): return self._rows


class FC:
    def __init__(self, result=None, results=None, error=None):
        self._result = result or FR()
        self._results = list(results) if results else None
        self._idx = 0
        self.error = error; self.committed = False; self.executed = []
    def execute(self, *a, **kw):
        self.executed.append((a, kw))
        if self.error: raise self.error
        if self._results:
            r = self._results[self._idx]; self._idx += 1; return r
        return self._result
    def commit(self): self.committed = True


class FCtx:
    def __init__(self, c): self.c = c
    def __enter__(self): return self.c
    def __exit__(self, *a): return False


class R:
    def __init__(self, d):
        self._d = d; self._mapping = d
    def __getattr__(self, n):
        if n.startswith("_"): raise AttributeError
        return self._d.get(n)


import app.domains.collaboration.dao.collaboration_dao as _dao_mod
MOD = "app.domains.collaboration.dao.collaboration_dao"


@pytest.mark.unit
class TestTeamWorkspaceDao:
    @pytest.fixture
    def dao(self): return TeamWorkspaceDao()

    def test_list_for_user(self, dao, monkeypatch):
        row = R({"id": 1, "name": "ws1", "owner_id": 10})
        conn = FC(result=FR(rows=[row]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert len(dao.list_for_user(10)) == 1

    def test_get_found(self, dao, monkeypatch):
        row = R({"id": 1, "name": "ws1"})
        conn = FC(result=FR(rows=[row]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.get(1)["name"] == "ws1"

    def test_get_not_found(self, dao, monkeypatch):
        conn = FC(result=FR(rows=[]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.get(999) is None

    def test_create(self, dao, monkeypatch):
        results = [FR(lastrowid=5), FR()]  # INSERT ws, INSERT member
        conn = FC(results=results)
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        r = dao.create(10, "new ws", "desc")
        assert r == 5
        assert conn.committed

    def test_update(self, dao, monkeypatch):
        conn = FC(result=FR())
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        dao.update(1, 10, name="updated")
        assert conn.committed

    def test_update_no_fields(self, dao, monkeypatch):
        conn = FC()
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        dao.update(1, 10)
        assert not conn.committed

    def test_delete_success(self, dao, monkeypatch):
        conn = FC(result=FR(rowcount=1))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.delete(1, 10) is True

    def test_delete_not_found(self, dao, monkeypatch):
        conn = FC(result=FR(rowcount=0))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.delete(999, 10) is False


@pytest.mark.unit
class TestWorkspaceMemberDao:
    @pytest.fixture
    def dao(self): return WorkspaceMemberDao()

    def test_list_members(self, dao, monkeypatch):
        row = R({"workspace_id": 1, "user_id": 10, "role": "owner"})
        conn = FC(result=FR(rows=[row]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert len(dao.list_members(1)) == 1

    def test_get_member_found(self, dao, monkeypatch):
        row = R({"workspace_id": 1, "user_id": 10, "role": "admin"})
        conn = FC(result=FR(rows=[row]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.get_member(1, 10)["role"] == "admin"

    def test_get_member_not_found(self, dao, monkeypatch):
        conn = FC(result=FR(rows=[]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.get_member(1, 999) is None

    def test_add_member(self, dao, monkeypatch):
        conn = FC(result=FR())
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        dao.add_member(1, 20, "member")
        assert conn.committed

    def test_update_role(self, dao, monkeypatch):
        conn = FC(result=FR())
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        dao.update_role(1, 20, "admin")
        assert conn.committed

    def test_remove_member_success(self, dao, monkeypatch):
        conn = FC(result=FR(rowcount=1))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.remove_member(1, 20) is True

    def test_remove_member_owner_fails(self, dao, monkeypatch):
        conn = FC(result=FR(rowcount=0))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.remove_member(1, 10) is False

    def test_count_members(self, dao, monkeypatch):
        row = R({"cnt": 3})
        conn = FC(result=FR(rows=[row]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.count_members(1) == 3


@pytest.mark.unit
class TestStrategyShareDao:
    @pytest.fixture
    def dao(self): return StrategyShareDao()

    def test_list_for_strategy(self, dao, monkeypatch):
        row = R({"id": 1, "strategy_id": 5, "shared_by": 10})
        conn = FC(result=FR(rows=[row]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert len(dao.list_for_strategy(5)) == 1

    def test_list_shared_with_user(self, dao, monkeypatch):
        row = R({"id": 1, "strategy_id": 5, "shared_with_user_id": 20})
        conn = FC(result=FR(rows=[row]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert len(dao.list_shared_with_user(20)) == 1

    def test_share(self, dao, monkeypatch):
        conn = FC(result=FR(lastrowid=9))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        r = dao.share(5, 10, shared_with_user_id=20)
        assert r == 9
        assert conn.committed

    def test_revoke_success(self, dao, monkeypatch):
        conn = FC(result=FR(rowcount=1))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.revoke(1, 10) is True

    def test_revoke_not_found(self, dao, monkeypatch):
        conn = FC(result=FR(rowcount=0))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.revoke(999, 10) is False
