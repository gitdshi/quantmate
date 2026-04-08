"""Unit tests for StrategyDao."""

from __future__ import annotations

from datetime import datetime

import pytest

import app.domains.strategies.dao.strategy_dao as _dao_mod
MOD = "app.domains.strategies.dao.strategy_dao"


class FakeRow:
    def __init__(self, d):
        self._mapping = d


class FakeResult:
    def __init__(self, rows=None, lastrowid=0, rowcount=0):
        self._rows = rows or []
        self.lastrowid = lastrowid
        self.rowcount = rowcount

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


def test_list_for_user(patch_conn):
    patch_conn([FakeResult([{"id": 1, "name": "s1"}])])
    from app.domains.strategies.dao.strategy_dao import StrategyDao
    rows = StrategyDao().list_for_user(1)
    assert len(rows) == 1
    assert rows[0]["id"] == 1


def test_count_for_user(patch_conn):
    patch_conn([FakeResult([{"cnt": 5}])])
    from app.domains.strategies.dao.strategy_dao import StrategyDao
    assert StrategyDao().count_for_user(1) == 5


def test_count_for_user_none(patch_conn):
    patch_conn([FakeResult()])
    from app.domains.strategies.dao.strategy_dao import StrategyDao
    assert StrategyDao().count_for_user(1) == 0


def test_list_for_user_paginated(patch_conn):
    patch_conn([FakeResult([{"id": 2}])])
    from app.domains.strategies.dao.strategy_dao import StrategyDao
    rows = StrategyDao().list_for_user_paginated(1, 10, 0)
    assert rows == [{"id": 2}]


def test_name_exists_true(patch_conn):
    patch_conn([FakeResult([{"1": 1}])])
    from app.domains.strategies.dao.strategy_dao import StrategyDao
    assert StrategyDao().name_exists_for_user(1, "s") is True


def test_name_exists_false(patch_conn):
    patch_conn([FakeResult()])
    from app.domains.strategies.dao.strategy_dao import StrategyDao
    assert StrategyDao().name_exists_for_user(1, "x") is False


def test_insert_strategy(patch_conn):
    conn = patch_conn([FakeResult(lastrowid=42)])
    from app.domains.strategies.dao.strategy_dao import StrategyDao
    now = datetime.utcnow()
    sid = StrategyDao().insert_strategy(1, "s", "S", "d", "{}", "code", now, now)
    assert sid == 42
    assert conn.committed


def test_get_for_user_found(patch_conn):
    patch_conn([FakeResult([{"id": 1, "name": "s"}])])
    from app.domains.strategies.dao.strategy_dao import StrategyDao
    row = StrategyDao().get_for_user(1, 1)
    assert row["id"] == 1


def test_get_for_user_not_found(patch_conn):
    patch_conn([FakeResult()])
    from app.domains.strategies.dao.strategy_dao import StrategyDao
    assert StrategyDao().get_for_user(999, 1) is None


def test_get_existing_for_update(patch_conn):
    patch_conn([FakeResult([{"id": 1, "code": "x"}])])
    from app.domains.strategies.dao.strategy_dao import StrategyDao
    row = StrategyDao().get_existing_for_update(1, 1)
    assert row["code"] == "x"


def test_update_strategy(patch_conn):
    conn = patch_conn()
    from app.domains.strategies.dao.strategy_dao import StrategyDao
    StrategyDao().update_strategy(1, 1, "name = :name", {"name": "new"})
    assert conn.committed


def test_delete_for_user_found(patch_conn):
    conn = patch_conn([FakeResult([{"1": 1}]), FakeResult()])
    from app.domains.strategies.dao.strategy_dao import StrategyDao
    assert StrategyDao().delete_for_user(1, 1) is True
    assert conn.committed


def test_delete_for_user_not_found(patch_conn):
    patch_conn([FakeResult()])
    from app.domains.strategies.dao.strategy_dao import StrategyDao
    assert StrategyDao().delete_for_user(999, 1) is False
