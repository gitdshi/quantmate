"""Unit tests for StrategyTemplateDao, StrategyCommentDao, StrategyRatingDao."""
import json
import pytest

from app.domains.templates.dao.template_dao import (
    StrategyTemplateDao, StrategyCommentDao, StrategyRatingDao, _parse_json_cols
)


class FR:
    """Fake result."""
    def __init__(self, rows=None, rowcount=0, lastrowid=0):
        self._rows = rows or []; self.rowcount = rowcount; self.lastrowid = lastrowid
    def fetchone(self): return self._rows[0] if self._rows else None
    def fetchall(self): return self._rows


class FC:
    """Fake connection."""
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


import app.domains.templates.dao.template_dao as _dao_mod
MOD = "app.domains.templates.dao.template_dao"


@pytest.mark.unit
class TestParseJsonCols:
    def test_parses_json_string(self):
        row = {"params_schema": '{"type": "object"}', "default_params": '{"n": 5}', "name": "test"}
        result = _parse_json_cols(row)
        assert result["params_schema"] == {"type": "object"}
        assert result["default_params"] == {"n": 5}

    def test_leaves_non_string(self):
        row = {"params_schema": {"type": "object"}, "name": "test"}
        result = _parse_json_cols(row)
        assert result["params_schema"] == {"type": "object"}

    def test_invalid_json_unchanged(self):
        row = {"params_schema": "not-json{", "name": "test"}
        result = _parse_json_cols(row)
        assert result["params_schema"] == "not-json{"


@pytest.mark.unit
class TestStrategyTemplateDao:
    @pytest.fixture
    def dao(self): return StrategyTemplateDao()

    def test_list_public(self, dao, monkeypatch):
        row = R({"id": 1, "author_id": 5, "name": "t", "category": "trend",
                 "template_type": "standalone", "layer": None, "sub_type": None,
                 "description": "d", "version": 1, "visibility": "public",
                 "downloads": 10, "created_at": "2024", "updated_at": "2024"})
        conn = FC(result=FR(rows=[row]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        res = dao.list_public()
        assert len(res) == 1

    def test_list_public_with_filters(self, dao, monkeypatch):
        conn = FC(result=FR(rows=[]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        res = dao.list_public(category="trend", template_type="standalone")
        assert res == []

    def test_count_public(self, dao, monkeypatch):
        row = R({"cnt": 5})
        conn = FC(result=FR(rows=[row]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.count_public() == 5

    def test_count_public_with_filters(self, dao, monkeypatch):
        row = R({"cnt": 2})
        conn = FC(result=FR(rows=[row]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.count_public(category="trend", template_type="standalone") == 2

    def test_list_for_user(self, dao, monkeypatch):
        row = R({"id": 1, "author_id": 10, "name": "t"})
        conn = FC(result=FR(rows=[row]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        res = dao.list_for_user(10)
        assert len(res) == 1

    def test_list_for_user_with_source(self, dao, monkeypatch):
        conn = FC(result=FR(rows=[]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        res = dao.list_for_user(10, source="marketplace")
        assert res == []

    def test_count_for_user(self, dao, monkeypatch):
        row = R({"cnt": 3})
        conn = FC(result=FR(rows=[row]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.count_for_user(10) == 3

    def test_count_for_user_with_source(self, dao, monkeypatch):
        row = R({"cnt": 1})
        conn = FC(result=FR(rows=[row]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.count_for_user(10, source="personal") == 1

    def test_get_found(self, dao, monkeypatch):
        row = R({"id": 1, "name": "test"})
        conn = FC(result=FR(rows=[row]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.get(1)["name"] == "test"

    def test_get_not_found(self, dao, monkeypatch):
        conn = FC(result=FR(rows=[]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.get(999) is None

    def test_create(self, dao, monkeypatch):
        conn = FC(result=FR(lastrowid=7))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        r = dao.create(10, "test", "code", category="trend", params_schema={"x": 1}, default_params={"x": 5})
        assert r == 7
        assert conn.committed

    def test_update(self, dao, monkeypatch):
        conn = FC(result=FR())
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        dao.update(1, 10, name="new", code="code2", params_schema={"y": 1}, default_params={"y": 2})
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

    def test_increment_downloads(self, dao, monkeypatch):
        conn = FC(result=FR())
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        dao.increment_downloads(1)
        assert conn.committed


@pytest.mark.unit
class TestStrategyCommentDao:
    @pytest.fixture
    def dao(self): return StrategyCommentDao()

    def test_list_for_template(self, dao, monkeypatch):
        row = R({"id": 1, "template_id": 1, "user_id": 10, "content": "great"})
        conn = FC(result=FR(rows=[row]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        res = dao.list_for_template(1)
        assert len(res) == 1

    def test_create(self, dao, monkeypatch):
        conn = FC(result=FR(lastrowid=4))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        r = dao.create(1, 10, "nice strategy", parent_id=None)
        assert r == 4
        assert conn.committed

    def test_delete_success(self, dao, monkeypatch):
        conn = FC(result=FR(rowcount=1))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.delete(1, 10) is True

    def test_delete_not_found(self, dao, monkeypatch):
        conn = FC(result=FR(rowcount=0))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.delete(999, 10) is False


@pytest.mark.unit
class TestStrategyRatingDao:
    @pytest.fixture
    def dao(self): return StrategyRatingDao()

    def test_get_for_template(self, dao, monkeypatch):
        row = R({"avg_rating": 4.5, "count": 10})
        conn = FC(result=FR(rows=[row]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        res = dao.get_for_template(1)
        assert res["avg_rating"] == 4.5
        assert res["count"] == 10

    def test_get_for_template_no_ratings(self, dao, monkeypatch):
        row = R({"avg_rating": None, "count": 0})
        conn = FC(result=FR(rows=[row]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        res = dao.get_for_template(1)
        assert res["avg_rating"] == 0

    def test_upsert_insert(self, dao, monkeypatch):
        conn = FC()
        call_idx = [0]
        orig_result = FR(rows=[])
        insert_result = FR()
        def fake_execute(*a, **kw):
            conn.executed.append((a, kw))
            if call_idx[0] == 0:
                call_idx[0] += 1
                return orig_result  # no existing
            return insert_result
        conn.execute = fake_execute
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        dao.upsert(1, 10, 5, review="great")
        assert conn.committed

    def test_upsert_update(self, dao, monkeypatch):
        existing = R({"id": 3})
        conn = FC()
        call_idx = [0]
        def fake_execute(*a, **kw):
            conn.executed.append((a, kw))
            if call_idx[0] == 0:
                call_idx[0] += 1
                return FR(rows=[existing])
            return FR()
        conn.execute = fake_execute
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        dao.upsert(1, 10, 4)
        assert conn.committed

    def test_list_for_template(self, dao, monkeypatch):
        row = R({"id": 1, "template_id": 1, "user_id": 10, "rating": 5})
        conn = FC(result=FR(rows=[row]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        res = dao.list_for_template(1)
        assert len(res) == 1
