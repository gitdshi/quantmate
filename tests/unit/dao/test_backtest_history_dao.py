"""Unit tests for BacktestHistoryDao."""
import json
import pytest
from datetime import datetime
from app.domains.backtests.dao.backtest_history_dao import BacktestHistoryDao


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


import app.domains.backtests.dao.backtest_history_dao as _dao_mod
MOD = "app.domains.backtests.dao.backtest_history_dao"


@pytest.mark.unit
class TestBacktestHistoryDao:
    @pytest.fixture
    def dao(self): return BacktestHistoryDao()

    def test_upsert_history(self, dao, monkeypatch):
        conn = FC(result=FR())
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        dao.upsert_history(
            user_id=10, job_id="job-001", strategy_id=1,
            strategy_class="MyStrategy", strategy_version=1,
            vt_symbol="000001.SZ", start_date="2024-01-01", end_date="2024-06-01",
            parameters={"window": 20}, status="completed",
            result={"sharpe": 1.5, "total_return": 0.25},
            error=None, created_at=datetime(2024, 1, 1),
            completed_at=datetime(2024, 1, 1, 1, 0),
            bulk_job_id=None,
        )
        assert conn.committed

    def test_upsert_history_with_none_result(self, dao, monkeypatch):
        conn = FC(result=FR())
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        dao.upsert_history(
            user_id=10, job_id="job-002", strategy_id=None,
            strategy_class=None, strategy_version=None,
            vt_symbol="000001.SZ", start_date="2024-01-01", end_date="2024-06-01",
            parameters={}, status="failed",
            result=None, error="some error",
            created_at=datetime(2024, 1, 1), completed_at=None,
        )
        assert conn.committed

    def test_get_child_result_json_found(self, dao, monkeypatch):
        row = R({"result": json.dumps({"sharpe": 1.5})})
        conn = FC(result=FR(rows=[row]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        r = dao.get_child_result_json("job-001")
        assert r["sharpe"] == 1.5

    def test_get_child_result_json_not_found(self, dao, monkeypatch):
        conn = FC(result=FR(rows=[]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.get_child_result_json("nonexistent") is None

    def test_get_child_result_json_null_result(self, dao, monkeypatch):
        row = R({"result": None})
        conn = FC(result=FR(rows=[row]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.get_child_result_json("job-001") is None

    def test_get_job_row_found(self, dao, monkeypatch):
        row = R({"job_id": "j1", "user_id": 10, "bulk_job_id": None,
                 "strategy_id": 1, "strategy_class": "S", "strategy_version": 1,
                 "vt_symbol": "000001.SZ", "start_date": "2024-01-01",
                 "end_date": "2024-06-01", "parameters": "{}", "status": "completed",
                 "result": None, "error": None, "created_at": "2024",
                 "completed_at": "2024"})
        conn = FC(result=FR(rows=[row]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        r = dao.get_job_row("j1")
        assert r["job_id"] == "j1"

    def test_get_job_row_not_found(self, dao, monkeypatch):
        conn = FC(result=FR(rows=[]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.get_job_row("nonexistent") is None

    def test_get_latest_strategy_run_found(self, dao, monkeypatch):
        row = R({"vt_symbol": "000001.SZ", "start_date": "2024-01-01", "end_date": "2024-06-01"})
        conn = FC(result=FR(rows=[row]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        r = dao.get_latest_strategy_run(user_id=10, strategy_id=1)
        assert r["vt_symbol"] == "000001.SZ"

    def test_get_latest_strategy_run_not_found(self, dao, monkeypatch):
        conn = FC(result=FR(rows=[]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.get_latest_strategy_run(user_id=10, strategy_id=999) is None

    def test_delete_single(self, dao, monkeypatch):
        conn = FC(result=FR())
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        dao.delete_single("j1", 10)
        assert conn.committed

    def test_delete_bulk_children(self, dao, monkeypatch):
        conn = FC(result=FR())
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        dao.delete_bulk_children("bulk-1", 10)
        assert conn.committed

    def test_count_for_user(self, dao, monkeypatch):
        row = R({"total": 15})
        conn = FC(result=FR(rows=[row]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.count_for_user(10) == 15

    def test_list_for_user(self, dao, monkeypatch):
        row = R({"id": 1, "job_id": "j1", "strategy_id": 1, "strategy_class": "S",
                 "strategy_version": 1, "vt_symbol": "000001.SZ",
                 "start_date": "2024-01-01", "end_date": "2024-06-01",
                 "status": "completed", "result": None, "created_at": "2024",
                 "completed_at": "2024"})
        conn = FC(result=FR(rows=[row]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        r = dao.list_for_user(user_id=10, limit=20, offset=0)
        assert len(r) == 1

    def test_get_detail_for_user_found(self, dao, monkeypatch):
        row = R({"id": 1, "job_id": "j1", "strategy_id": 1, "strategy_class": "S",
                 "strategy_version": 1, "vt_symbol": "000001.SZ",
                 "start_date": "2024-01-01", "end_date": "2024-06-01",
                 "parameters": "{}", "status": "completed", "result": None,
                 "error": None, "created_at": "2024", "completed_at": "2024"})
        conn = FC(result=FR(rows=[row]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        r = dao.get_detail_for_user(job_id="j1", user_id=10)
        assert r["job_id"] == "j1"

    def test_get_detail_for_user_not_found(self, dao, monkeypatch):
        conn = FC(result=FR(rows=[]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.get_detail_for_user(job_id="nonexistent", user_id=10) is None
