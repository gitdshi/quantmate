"""Unit tests for KycDao."""
import pytest
from app.domains.auth.dao.kyc_dao import KycDao
import app.domains.auth.dao.kyc_dao as _kyc_dao_mod


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
    def __init__(self, result=None, error=None):
        self.result = result or FakeResult()
        self.error = error
        self.committed = False
        self.executed = []

    def execute(self, *args, **kwargs):
        self.executed.append((args, kwargs))
        if self.error:
            raise self.error
        return self.result

    def commit(self):
        self.committed = True


class FakeContext:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self.conn

    def __exit__(self, *args):
        return False


class FakeMapping:
    def __init__(self, data):
        self._data = data
        self._mapping = data

    def __getitem__(self, key):
        return self._data[key]

    def items(self):
        return self._data.items()

    def keys(self):
        return self._data.keys()


class FakeRow:
    def __init__(self, data):
        self._data = data
        self._mapping = data

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        return self._data.get(name)


@pytest.mark.unit
class TestKycDao:
    @pytest.fixture
    def dao(self):
        return KycDao()

    def test_get_latest_returns_dict(self, dao, monkeypatch):
        row = FakeRow({"id": 1, "user_id": 10, "status": "pending", "real_name": "Test"})
        conn = FakeConn(result=FakeResult(rows=[row]))
        monkeypatch.setattr(_kyc_dao_mod, "connection", lambda name: FakeContext(conn))
        result = dao.get_latest(10)
        assert result["user_id"] == 10
        assert result["status"] == "pending"

    def test_get_latest_returns_none_when_no_row(self, dao, monkeypatch):
        conn = FakeConn(result=FakeResult(rows=[]))
        monkeypatch.setattr(_kyc_dao_mod, "connection", lambda name: FakeContext(conn))
        assert dao.get_latest(999) is None

    def test_insert_returns_lastrowid(self, dao, monkeypatch):
        conn = FakeConn(result=FakeResult(lastrowid=42))
        monkeypatch.setattr(_kyc_dao_mod, "connection", lambda name: FakeContext(conn))
        result = dao.insert(10, real_name="Test", id_type="passport")
        assert result == 42
        assert conn.committed

    def test_update_status_commits(self, dao, monkeypatch):
        conn = FakeConn(result=FakeResult())
        monkeypatch.setattr(_kyc_dao_mod, "connection", lambda name: FakeContext(conn))
        dao.update_status(1, "approved", reviewer_id=5, review_notes="ok")
        assert conn.committed
        assert len(conn.executed) == 1

    def test_list_pending_returns_list(self, dao, monkeypatch):
        row = FakeRow({"id": 1, "user_id": 10, "status": "pending", "real_name": "A", "id_type": "id_card", "created_at": "2024-01-01"})
        conn = FakeConn(result=FakeResult(rows=[row]))
        monkeypatch.setattr(_kyc_dao_mod, "connection", lambda name: FakeContext(conn))
        result = dao.list_pending(limit=10, offset=0)
        assert len(result) == 1

    def test_list_pending_returns_empty(self, dao, monkeypatch):
        conn = FakeConn(result=FakeResult(rows=[]))
        monkeypatch.setattr(_kyc_dao_mod, "connection", lambda name: FakeContext(conn))
        result = dao.list_pending()
        assert result == []

    def test_count_pending(self, dao, monkeypatch):
        row = FakeRow({"cnt": 5})
        conn = FakeConn(result=FakeResult(rows=[row]))
        monkeypatch.setattr(_kyc_dao_mod, "connection", lambda name: FakeContext(conn))
        assert dao.count_pending() == 5

    def test_count_pending_no_rows(self, dao, monkeypatch):
        conn = FakeConn(result=FakeResult(rows=[]))
        monkeypatch.setattr(_kyc_dao_mod, "connection", lambda name: FakeContext(conn))
        assert dao.count_pending() == 0
