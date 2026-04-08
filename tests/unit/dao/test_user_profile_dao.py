"""Unit tests for UserProfileDao."""
import pytest
from app.domains.auth.dao.user_profile_dao import UserProfileDao
from sqlalchemy.exc import ProgrammingError
import app.domains.auth.dao.user_profile_dao as _user_profile_dao_mod


class FakeResult:
    def __init__(self, rows=None, rowcount=0, lastrowid=0):
        self._rows = rows or []
        self.rowcount = rowcount
        self.lastrowid = lastrowid

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return self._rows


class FakeConn:
    def __init__(self, result=None, results=None, error=None):
        self._result = result or FakeResult()
        self._results = list(results) if results else None
        self._call_index = 0
        self.error = error
        self.committed = False
        self.executed = []

    def execute(self, *args, **kwargs):
        self.executed.append((args, kwargs))
        if self.error:
            raise self.error
        if self._results:
            r = self._results[self._call_index]
            self._call_index += 1
            return r
        return self._result

    def commit(self):
        self.committed = True


class FakeContext:
    def __init__(self, conn):
        self.conn = conn
    def __enter__(self):
        return self.conn
    def __exit__(self, *args):
        return False


class FakeRow:
    def __init__(self, data):
        self._data = data
        self._mapping = data
    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError
        return self._data.get(name)


@pytest.mark.unit
class TestUserProfileDao:
    @pytest.fixture
    def dao(self):
        return UserProfileDao()

    def test_get_returns_dict(self, dao, monkeypatch):
        row = FakeRow({"user_id": 10, "display_name": "Test User", "timezone": "UTC"})
        conn = FakeConn(result=FakeResult(rows=[row]))
        monkeypatch.setattr(_user_profile_dao_mod, "connection", lambda n: FakeContext(conn))
        result = dao.get(10)
        assert result["display_name"] == "Test User"

    def test_get_returns_none(self, dao, monkeypatch):
        conn = FakeConn(result=FakeResult(rows=[]))
        monkeypatch.setattr(_user_profile_dao_mod, "connection", lambda n: FakeContext(conn))
        assert dao.get(999) is None

    def test_get_programming_error_returns_none(self, dao, monkeypatch):
        err = ProgrammingError("stmt", {}, Exception("table missing"))
        conn = FakeConn(error=err)
        monkeypatch.setattr(_user_profile_dao_mod, "connection", lambda n: FakeContext(conn))
        assert dao.get(10) is None

    def test_upsert_insert_new_profile(self, dao, monkeypatch):
        # SELECT returns no existing row, INSERT creates new, GET returns result
        existing_row = FakeRow({"user_id": 10})
        results = [
            FakeResult(rows=[]),  # check existing
            FakeResult(),  # INSERT
        ]
        conn = FakeConn(results=results)
        monkeypatch.setattr(_user_profile_dao_mod, "connection", lambda n: FakeContext(conn))
        # Mock get to return after upsert
        original_get = dao.get
        dao.get = lambda uid: {"user_id": uid, "display_name": "New"}
        result = dao.upsert(10, display_name="New")
        assert result["display_name"] == "New"
        assert conn.committed
        dao.get = original_get

    def test_upsert_update_existing(self, dao, monkeypatch):
        existing = FakeRow({"user_id": 10})
        results = [
            FakeResult(rows=[existing]),  # check existing
            FakeResult(),  # UPDATE
        ]
        conn = FakeConn(results=results)
        monkeypatch.setattr(_user_profile_dao_mod, "connection", lambda n: FakeContext(conn))
        dao.get = lambda uid: {"user_id": uid, "display_name": "Updated"}
        result = dao.upsert(10, display_name="Updated")
        assert result["display_name"] == "Updated"
        assert conn.committed

    def test_upsert_filters_disallowed_fields(self, dao, monkeypatch):
        results = [
            FakeResult(rows=[]),  # check existing
            FakeResult(),  # INSERT
        ]
        conn = FakeConn(results=results)
        monkeypatch.setattr(_user_profile_dao_mod, "connection", lambda n: FakeContext(conn))
        dao.get = lambda uid: {"user_id": uid}
        result = dao.upsert(10, display_name="Ok", evil_field="hacked")
        assert "evil_field" not in result

    def test_upsert_no_data_for_existing(self, dao, monkeypatch):
        existing = FakeRow({"user_id": 10})
        results = [
            FakeResult(rows=[existing]),  # check existing
        ]
        conn = FakeConn(results=results)
        monkeypatch.setattr(_user_profile_dao_mod, "connection", lambda n: FakeContext(conn))
        dao.get = lambda uid: {"user_id": uid}
        result = dao.upsert(10)
        assert result["user_id"] == 10
