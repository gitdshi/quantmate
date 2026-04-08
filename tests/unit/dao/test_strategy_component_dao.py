"""Unit tests for StrategyComponentDao."""

from __future__ import annotations

from datetime import datetime

import pytest

import app.domains.composite.dao.strategy_component_dao as _dao_mod
MOD = "app.domains.composite.dao.strategy_component_dao"


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


def test_list_for_user_all(patch_conn):
    patch_conn([FakeResult([{"id": 1, "name": "c1"}])])
    from app.domains.composite.dao.strategy_component_dao import StrategyComponentDao
    rows = StrategyComponentDao().list_for_user(1)
    assert len(rows) == 1


def test_list_for_user_by_layer(patch_conn):
    patch_conn([FakeResult([{"id": 2}])])
    from app.domains.composite.dao.strategy_component_dao import StrategyComponentDao
    rows = StrategyComponentDao().list_for_user(1, layer="trading")
    assert rows == [{"id": 2}]


def test_count_for_user(patch_conn):
    patch_conn([FakeResult([{"cnt": 3}])])
    from app.domains.composite.dao.strategy_component_dao import StrategyComponentDao
    assert StrategyComponentDao().count_for_user(1) == 3


def test_count_for_user_by_layer(patch_conn):
    patch_conn([FakeResult([{"cnt": 1}])])
    from app.domains.composite.dao.strategy_component_dao import StrategyComponentDao
    assert StrategyComponentDao().count_for_user(1, layer="risk") == 1


def test_list_paginated(patch_conn):
    patch_conn([FakeResult([{"id": 1}])])
    from app.domains.composite.dao.strategy_component_dao import StrategyComponentDao
    rows = StrategyComponentDao().list_for_user_paginated(1, 10, 0)
    assert len(rows) == 1


def test_list_paginated_by_layer(patch_conn):
    patch_conn([FakeResult([{"id": 2}])])
    from app.domains.composite.dao.strategy_component_dao import StrategyComponentDao
    rows = StrategyComponentDao().list_for_user_paginated(1, 10, 0, layer="universe")
    assert rows == [{"id": 2}]


def test_name_exists_true(patch_conn):
    patch_conn([FakeResult([{"1": 1}])])
    from app.domains.composite.dao.strategy_component_dao import StrategyComponentDao
    assert StrategyComponentDao().name_exists_for_user(1, "c", "trading") is True


def test_name_exists_false(patch_conn):
    patch_conn([FakeResult()])
    from app.domains.composite.dao.strategy_component_dao import StrategyComponentDao
    assert StrategyComponentDao().name_exists_for_user(1, "c", "risk") is False


def test_insert(patch_conn):
    conn = patch_conn([FakeResult(lastrowid=42)])
    from app.domains.composite.dao.strategy_component_dao import StrategyComponentDao
    now = datetime.utcnow()
    cid = StrategyComponentDao().insert(
        1, "comp", "trading", "ma_crossover", "desc", "code", "{}", "{}", now, now,
    )
    assert cid == 42
    assert conn.committed


def test_get_for_user_found(patch_conn):
    patch_conn([FakeResult([{"id": 1, "name": "c"}])])
    from app.domains.composite.dao.strategy_component_dao import StrategyComponentDao
    row = StrategyComponentDao().get_for_user(1, 1)
    assert row["name"] == "c"


def test_get_for_user_not_found(patch_conn):
    patch_conn([FakeResult()])
    from app.domains.composite.dao.strategy_component_dao import StrategyComponentDao
    assert StrategyComponentDao().get_for_user(999, 1) is None


def test_update(patch_conn):
    conn = patch_conn()
    from app.domains.composite.dao.strategy_component_dao import StrategyComponentDao
    StrategyComponentDao().update(1, 1, "name = :name", {"name": "new"})
    assert conn.committed


def test_delete_for_user_ok(patch_conn):
    conn = patch_conn([FakeResult(rowcount=1)])
    from app.domains.composite.dao.strategy_component_dao import StrategyComponentDao
    assert StrategyComponentDao().delete_for_user(1, 1) is True
    assert conn.committed


def test_delete_for_user_not_found(patch_conn):
    patch_conn([FakeResult(rowcount=0)])
    from app.domains.composite.dao.strategy_component_dao import StrategyComponentDao
    assert StrategyComponentDao().delete_for_user(999, 1) is False


def test_get_ids_for_user(patch_conn):
    patch_conn([FakeResult([{"id": 1}, {"id": 2}])])
    from app.domains.composite.dao.strategy_component_dao import StrategyComponentDao
    ids = StrategyComponentDao().get_ids_for_user([1, 2, 3], 1)
    assert ids == [1, 2]


def test_get_ids_for_user_empty():
    from app.domains.composite.dao.strategy_component_dao import StrategyComponentDao
    assert StrategyComponentDao().get_ids_for_user([], 1) == []
