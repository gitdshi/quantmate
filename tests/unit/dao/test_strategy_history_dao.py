"""Unit tests for StrategyHistoryDao."""

from __future__ import annotations

from datetime import datetime

import pytest

import app.domains.strategies.dao.strategy_history_dao as _dao_mod
MOD = "app.domains.strategies.dao.strategy_history_dao"


class FakeRow:
    def __init__(self, d):
        self._mapping = d
        for k, v in d.items():
            setattr(self, k, v)


class FakeResult:
    def __init__(self, rows=None, lastrowid=0):
        self._rows = rows or []
        self.lastrowid = lastrowid

    def fetchall(self):
        return [FakeRow(r) for r in self._rows]

    def fetchone(self):
        return FakeRow(self._rows[0]) if self._rows else None


class FakeConn:
    def __init__(self, results=None):
        self._results = list(results) if results else []
        self._idx = 0
        self.committed = False

    def execute(self, *a, **kw):
        if self._results:
            r = self._results[self._idx % len(self._results)]
            self._idx += 1
            return r
        return FakeResult()

    def commit(self):
        self.committed = True


class FCtx:
    def __init__(self, conn):
        self._conn = conn

    def __enter__(self):
        return self._conn

    def __exit__(self, *a):
        pass


@pytest.fixture
def patch_conn(monkeypatch):
    def _factory(results=None):
        conn = FakeConn(results)
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        return conn
    return _factory


def test_insert_history(patch_conn):
    conn = patch_conn()
    from app.domains.strategies.dao.strategy_history_dao import StrategyHistoryDao
    StrategyHistoryDao().insert_history(1, "s", "C", "d", 1, "{}", "code", datetime.utcnow())
    assert conn.committed


def test_rotate_keep_latest(patch_conn):
    conn = patch_conn([FakeResult([{"id": 10}, {"id": 9}, {"id": 8}]), FakeResult()])
    from app.domains.strategies.dao.strategy_history_dao import StrategyHistoryDao
    StrategyHistoryDao().rotate_keep_latest(1, keep=2)
    assert conn.committed


def test_rotate_keep_latest_empty(patch_conn):
    patch_conn([FakeResult()])
    from app.domains.strategies.dao.strategy_history_dao import StrategyHistoryDao
    StrategyHistoryDao().rotate_keep_latest(1, keep=5)


def test_list_history(patch_conn):
    patch_conn([FakeResult([
        {"id": 1, "created_at": datetime(2025, 1, 1), "size": 100,
         "strategy_name": "s", "class_name": "C", "description": "d",
         "version": 1, "parameters": "{}"},
    ])])
    from app.domains.strategies.dao.strategy_history_dao import StrategyHistoryDao
    rows = StrategyHistoryDao().list_history(1)
    assert len(rows) == 1
    assert rows[0]["id"] == 1


def test_get_history_found(patch_conn):
    patch_conn([FakeResult([
        {"id": 5, "code": "x", "strategy_name": "s", "class_name": "C",
         "description": "", "version": 1, "parameters": "{}"},
    ])])
    from app.domains.strategies.dao.strategy_history_dao import StrategyHistoryDao
    row = StrategyHistoryDao().get_history(1, 5)
    assert row["code"] == "x"


def test_get_history_not_found(patch_conn):
    patch_conn([FakeResult()])
    from app.domains.strategies.dao.strategy_history_dao import StrategyHistoryDao
    assert StrategyHistoryDao().get_history(1, 999) is None
