"""Unit tests for CompositeStrategyDao and CompositeBacktestDao."""
import json
import pytest
from datetime import datetime
from app.domains.composite.dao.composite_strategy_dao import CompositeStrategyDao
from app.domains.composite.dao.composite_backtest_dao import CompositeBacktestDao


class FR:
    def __init__(self, rows=None, rowcount=0, lastrowid=0):
        self._rows = rows or []; self.rowcount = rowcount; self.lastrowid = lastrowid
    def fetchone(self): return self._rows[0] if self._rows else None
    def fetchall(self): return self._rows
    def first(self): return self._rows[0] if self._rows else None


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


import app.domains.composite.dao.composite_strategy_dao as _cs_mod
MOD_CS = "app.domains.composite.dao.composite_strategy_dao"
import app.domains.composite.dao.composite_backtest_dao as _cb_mod
MOD_CB = "app.domains.composite.dao.composite_backtest_dao"


@pytest.mark.unit
class TestCompositeStrategyDao:
    @pytest.fixture
    def dao(self): return CompositeStrategyDao()

    def test_list_for_user(self, dao, monkeypatch):
        row = R({"id": 1, "name": "combo", "description": "d", "execution_mode": "sequential",
                 "is_active": 1, "created_at": "2024", "updated_at": "2024", "component_count": 3})
        conn = FC(result=FR(rows=[row]))
        monkeypatch.setattr(_cs_mod, "connection", lambda n: FCtx(conn))
        r = dao.list_for_user(10)
        assert len(r) == 1

    def test_count_for_user(self, dao, monkeypatch):
        row = R({"cnt": 5})
        conn = FC(result=FR(rows=[row]))
        monkeypatch.setattr(_cs_mod, "connection", lambda n: FCtx(conn))
        assert dao.count_for_user(10) == 5

    def test_list_for_user_paginated(self, dao, monkeypatch):
        conn = FC(result=FR(rows=[]))
        monkeypatch.setattr(_cs_mod, "connection", lambda n: FCtx(conn))
        r = dao.list_for_user_paginated(10, 20, 0)
        assert r == []

    def test_name_exists_true(self, dao, monkeypatch):
        conn = FC(result=FR(rows=[R({"x": 1})]))
        monkeypatch.setattr(_cs_mod, "connection", lambda n: FCtx(conn))
        assert dao.name_exists_for_user(10, "combo") is True

    def test_name_exists_false(self, dao, monkeypatch):
        conn = FC(result=FR(rows=[]))
        monkeypatch.setattr(_cs_mod, "connection", lambda n: FCtx(conn))
        assert dao.name_exists_for_user(10, "nonexistent") is False

    def test_insert(self, dao, monkeypatch):
        conn = FC(result=FR(lastrowid=7))
        monkeypatch.setattr(_cs_mod, "connection", lambda n: FCtx(conn))
        now = datetime.utcnow()
        r = dao.insert(10, "combo", "desc", None, None, "sequential", now, now)
        assert r == 7
        assert conn.committed

    def test_get_for_user_found(self, dao, monkeypatch):
        row = R({"id": 1, "user_id": 10, "name": "c", "description": "d",
                 "portfolio_config": None, "market_constraints": None,
                 "execution_mode": "sequential", "is_active": 1,
                 "created_at": "2024", "updated_at": "2024"})
        conn = FC(result=FR(rows=[row]))
        monkeypatch.setattr(_cs_mod, "connection", lambda n: FCtx(conn))
        assert dao.get_for_user(1, 10)["name"] == "c"

    def test_get_for_user_not_found(self, dao, monkeypatch):
        conn = FC(result=FR(rows=[]))
        monkeypatch.setattr(_cs_mod, "connection", lambda n: FCtx(conn))
        assert dao.get_for_user(999, 10) is None

    def test_update(self, dao, monkeypatch):
        conn = FC(result=FR())
        monkeypatch.setattr(_cs_mod, "connection", lambda n: FCtx(conn))
        dao.update(1, 10, "name = :name", {"name": "updated"})
        assert conn.committed

    def test_delete_success(self, dao, monkeypatch):
        conn = FC(result=FR(rowcount=1))
        monkeypatch.setattr(_cs_mod, "connection", lambda n: FCtx(conn))
        assert dao.delete_for_user(1, 10) is True

    def test_delete_not_found(self, dao, monkeypatch):
        conn = FC(result=FR(rowcount=0))
        monkeypatch.setattr(_cs_mod, "connection", lambda n: FCtx(conn))
        assert dao.delete_for_user(999, 10) is False

    def test_get_bindings(self, dao, monkeypatch):
        row = R({"id": 1, "composite_strategy_id": 1, "component_id": 5,
                 "layer": "signal", "ordinal": 1, "weight": 1.0,
                 "config_override": None, "component_name": "RSI",
                 "component_sub_type": "indicator"})
        conn = FC(result=FR(rows=[row]))
        monkeypatch.setattr(_cs_mod, "connection", lambda n: FCtx(conn))
        r = dao.get_bindings(1)
        assert len(r) == 1

    def test_replace_bindings(self, dao, monkeypatch):
        results = [FR(), FR()]  # DELETE + INSERT
        conn = FC(results=results)
        monkeypatch.setattr(_cs_mod, "connection", lambda n: FCtx(conn))
        dao.replace_bindings(1, [{"component_id": 5, "layer": "signal"}])
        assert conn.committed

    def test_add_binding(self, dao, monkeypatch):
        conn = FC(result=FR(lastrowid=3))
        monkeypatch.setattr(_cs_mod, "connection", lambda n: FCtx(conn))
        r = dao.add_binding(1, {"component_id": 5, "layer": "signal"})
        assert r == 3

    def test_remove_binding_success(self, dao, monkeypatch):
        conn = FC(result=FR(rowcount=1))
        monkeypatch.setattr(_cs_mod, "connection", lambda n: FCtx(conn))
        assert dao.remove_binding(1, 3) is True

    def test_remove_binding_not_found(self, dao, monkeypatch):
        conn = FC(result=FR(rowcount=0))
        monkeypatch.setattr(_cs_mod, "connection", lambda n: FCtx(conn))
        assert dao.remove_binding(1, 999) is False


@pytest.mark.unit
class TestCompositeBacktestDao:
    @pytest.fixture
    def dao(self): return CompositeBacktestDao()

    def test_insert(self, dao, monkeypatch):
        conn = FC(result=FR(lastrowid=10))
        monkeypatch.setattr(_cb_mod, "connection", lambda n: FCtx(conn))
        r = dao.insert("job-1", 10, 1, "2024-01-01", "2024-06-01")
        assert r == 10
        assert conn.committed

    def test_get_by_job_id_found(self, dao, monkeypatch):
        row = R({"job_id": "j1", "status": "completed"})
        conn = FC(result=FR(rows=[row]))
        monkeypatch.setattr(_cb_mod, "connection", lambda n: FCtx(conn))
        r = dao.get_by_job_id("j1")
        assert r["status"] == "completed"

    def test_get_by_job_id_not_found(self, dao, monkeypatch):
        conn = FC(result=FR(rows=[]))
        monkeypatch.setattr(_cb_mod, "connection", lambda n: FCtx(conn))
        assert dao.get_by_job_id("nonexistent") is None

    def test_get_for_user_found(self, dao, monkeypatch):
        row = R({"id": 1, "user_id": 10})
        conn = FC(result=FR(rows=[row]))
        monkeypatch.setattr(_cb_mod, "connection", lambda n: FCtx(conn))
        assert dao.get_for_user(10, 1) is not None

    def test_list_for_user(self, dao, monkeypatch):
        row = R({"id": 1, "job_id": "j1", "composite_strategy_id": 1,
                 "start_date": "2024-01-01", "end_date": "2024-06-01",
                 "initial_capital": 1000000, "benchmark": "000300.SH",
                 "status": "completed", "error_message": None,
                 "started_at": None, "completed_at": None, "created_at": "2024"})
        conn = FC(result=FR(rows=[row]))
        monkeypatch.setattr(_cb_mod, "connection", lambda n: FCtx(conn))
        r = dao.list_for_user(10)
        assert len(r) == 1

    def test_list_for_user_filtered(self, dao, monkeypatch):
        conn = FC(result=FR(rows=[]))
        monkeypatch.setattr(_cb_mod, "connection", lambda n: FCtx(conn))
        r = dao.list_for_user(10, composite_strategy_id=1)
        assert r == []

    def test_update_status_running(self, dao, monkeypatch):
        conn = FC(result=FR())
        monkeypatch.setattr(_cb_mod, "connection", lambda n: FCtx(conn))
        dao.update_status("j1", "running")
        assert conn.committed

    def test_update_status_completed(self, dao, monkeypatch):
        conn = FC(result=FR())
        monkeypatch.setattr(_cb_mod, "connection", lambda n: FCtx(conn))
        dao.update_status("j1", "completed", result={"sharpe": 1.5}, attribution={"alpha": 0.1})
        assert conn.committed

    def test_update_status_failed(self, dao, monkeypatch):
        conn = FC(result=FR())
        monkeypatch.setattr(_cb_mod, "connection", lambda n: FCtx(conn))
        dao.update_status("j1", "failed", error_message="Oops")
        assert conn.committed

    def test_delete_for_user_success(self, dao, monkeypatch):
        conn = FC(result=FR(rowcount=1))
        monkeypatch.setattr(_cb_mod, "connection", lambda n: FCtx(conn))
        assert dao.delete_for_user(10, 1) is True

    def test_delete_for_user_not_found(self, dao, monkeypatch):
        conn = FC(result=FR(rowcount=0))
        monkeypatch.setattr(_cb_mod, "connection", lambda n: FCtx(conn))
        assert dao.delete_for_user(10, 999) is False
