"""Unit tests for MfaDao."""
import pytest
from app.domains.auth.dao.mfa_dao import MfaDao
import app.domains.auth.dao.mfa_dao as _mfa_dao_mod


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
    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError
        return self._data.get(name)


@pytest.mark.unit
class TestMfaDao:
    @pytest.fixture
    def dao(self):
        return MfaDao()

    def test_get_by_user_id_found(self, dao, monkeypatch):
        row = FakeRow({
            "id": 1, "user_id": 10, "mfa_type": "totp",
            "secret_encrypted": "enc_secret", "is_enabled": 1,
            "recovery_codes_hash": "hash", "created_at": "2024-01-01"
        })
        conn = FakeConn(result=FakeResult(rows=[row]))
        monkeypatch.setattr(_mfa_dao_mod, "connection", lambda name: FakeContext(conn))
        result = dao.get_by_user_id(10)
        assert result["mfa_type"] == "totp"
        assert result["is_enabled"] is True

    def test_get_by_user_id_not_found(self, dao, monkeypatch):
        conn = FakeConn(result=FakeResult(rows=[]))
        monkeypatch.setattr(_mfa_dao_mod, "connection", lambda name: FakeContext(conn))
        assert dao.get_by_user_id(999) is None

    def test_upsert_insert(self, dao, monkeypatch):
        # First call: SELECT returns None, Second call: INSERT
        results = [FakeResult(rows=[]), FakeResult(lastrowid=5)]
        conn = FakeConn(results=results)
        monkeypatch.setattr(_mfa_dao_mod, "connection", lambda name: FakeContext(conn))
        result = dao.upsert(10, "totp", "enc", "codes_hash")
        assert result == 5
        assert conn.committed

    def test_upsert_update(self, dao, monkeypatch):
        existing = FakeRow({"id": 3})
        results = [FakeResult(rows=[existing]), FakeResult()]
        conn = FakeConn(results=results)
        monkeypatch.setattr(_mfa_dao_mod, "connection", lambda name: FakeContext(conn))
        result = dao.upsert(10, "totp", "enc", "codes_hash")
        assert result == 3
        assert conn.committed

    def test_enable(self, dao, monkeypatch):
        conn = FakeConn(result=FakeResult(rowcount=1))
        monkeypatch.setattr(_mfa_dao_mod, "connection", lambda name: FakeContext(conn))
        assert dao.enable(10) is True

    def test_enable_not_found(self, dao, monkeypatch):
        conn = FakeConn(result=FakeResult(rowcount=0))
        monkeypatch.setattr(_mfa_dao_mod, "connection", lambda name: FakeContext(conn))
        assert dao.enable(999) is False

    def test_disable(self, dao, monkeypatch):
        conn = FakeConn(result=FakeResult(rowcount=1))
        monkeypatch.setattr(_mfa_dao_mod, "connection", lambda name: FakeContext(conn))
        assert dao.disable(10) is True

    def test_disable_not_found(self, dao, monkeypatch):
        conn = FakeConn(result=FakeResult(rowcount=0))
        monkeypatch.setattr(_mfa_dao_mod, "connection", lambda name: FakeContext(conn))
        assert dao.disable(999) is False

    def test_delete(self, dao, monkeypatch):
        conn = FakeConn(result=FakeResult(rowcount=1))
        monkeypatch.setattr(_mfa_dao_mod, "connection", lambda name: FakeContext(conn))
        assert dao.delete(10) is True

    def test_delete_not_found(self, dao, monkeypatch):
        conn = FakeConn(result=FakeResult(rowcount=0))
        monkeypatch.setattr(_mfa_dao_mod, "connection", lambda name: FakeContext(conn))
        assert dao.delete(999) is False
