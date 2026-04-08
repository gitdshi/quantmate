"""Unit tests for FactorDefinitionDao and FactorEvaluationDao."""
import pytest

from app.domains.factors.dao.factor_dao import FactorDefinitionDao, FactorEvaluationDao


class FR:
    def __init__(self, rows=None, rowcount=0, lastrowid=0):
        self._rows = rows or []; self.rowcount = rowcount; self.lastrowid = lastrowid
    def fetchone(self): return self._rows[0] if self._rows else None
    def fetchall(self): return self._rows


class FC:
    def __init__(self, result=None, error=None):
        self.result = result or FR(); self.error = error
        self.committed = False; self.executed = []
    def execute(self, *a, **kw):
        self.executed.append((a, kw))
        if self.error: raise self.error
        return self.result
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


import app.domains.factors.dao.factor_dao as _dao_mod
MOD = "app.domains.factors.dao.factor_dao"


@pytest.mark.unit
class TestFactorDefinitionDao:
    @pytest.fixture
    def dao(self): return FactorDefinitionDao()

    def test_list_for_user(self, dao, monkeypatch):
        row = R({"id": 1, "user_id": 10, "name": "f1", "expression": "close/open"})
        conn = FC(result=FR(rows=[row]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        res = dao.list_for_user(10)
        assert len(res) == 1

    def test_list_for_user_with_category(self, dao, monkeypatch):
        conn = FC(result=FR(rows=[]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        res = dao.list_for_user(10, category="momentum")
        assert res == []

    def test_count_for_user(self, dao, monkeypatch):
        row = R({"cnt": 5})
        conn = FC(result=FR(rows=[row]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.count_for_user(10) == 5

    def test_count_for_user_no_rows(self, dao, monkeypatch):
        conn = FC(result=FR(rows=[]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.count_for_user(10) == 0

    def test_get_found(self, dao, monkeypatch):
        row = R({"id": 1, "user_id": 10, "name": "f1"})
        conn = FC(result=FR(rows=[row]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.get(1, 10)["name"] == "f1"

    def test_get_not_found(self, dao, monkeypatch):
        conn = FC(result=FR(rows=[]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.get(999, 10) is None

    def test_create(self, dao, monkeypatch):
        conn = FC(result=FR(lastrowid=5))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        r = dao.create(10, "factor_x", "close/open", category="value", params={"window": 20})
        assert r == 5
        assert conn.committed

    def test_create_no_optional(self, dao, monkeypatch):
        conn = FC(result=FR(lastrowid=6))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        r = dao.create(10, "factor_y", "volume")
        assert r == 6

    def test_update(self, dao, monkeypatch):
        conn = FC(result=FR())
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        dao.update(1, 10, name="new_name", expression="high/low", params={"window": 10})
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
class TestFactorEvaluationDao:
    @pytest.fixture
    def dao(self): return FactorEvaluationDao()

    def test_list_for_factor(self, dao, monkeypatch):
        row = R({"id": 1, "factor_id": 5, "start_date": "2024-01-01"})
        conn = FC(result=FR(rows=[row]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        res = dao.list_for_factor(5)
        assert len(res) == 1

    def test_get_found(self, dao, monkeypatch):
        row = R({"id": 1, "factor_id": 5})
        conn = FC(result=FR(rows=[row]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.get(1)["factor_id"] == 5

    def test_get_not_found(self, dao, monkeypatch):
        conn = FC(result=FR(rows=[]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.get(999) is None

    def test_create(self, dao, monkeypatch):
        conn = FC(result=FR(lastrowid=3))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        r = dao.create(5, "2024-01-01", "2024-06-01", metrics={"ic_mean": 0.05}, ic_mean=0.05, ic_ir=0.3)
        assert r == 3
        assert conn.committed

    def test_delete_success(self, dao, monkeypatch):
        conn = FC(result=FR(rowcount=1))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.delete(1) is True

    def test_delete_not_found(self, dao, monkeypatch):
        conn = FC(result=FR(rowcount=0))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.delete(999) is False
