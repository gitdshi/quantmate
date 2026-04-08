"""Unit tests for ApiKeyDao."""
import json
import pytest
from app.domains.auth.dao.api_key_dao import ApiKeyDao, _is_missing_table
from sqlalchemy.exc import OperationalError, ProgrammingError
import app.domains.auth.dao.api_key_dao as _api_key_dao_mod


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


class FakeRow:
    def __init__(self, data):
        self._data = data
    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError
        return self._data.get(name)


@pytest.mark.unit
class TestIsMissingTable:
    def test_detects_missing_table_mysql(self):
        err = OperationalError("stmt", {}, Exception("Table 'api_keys' doesn't exist"))
        assert _is_missing_table(err, "api_keys") is True

    def test_detects_missing_table_sqlite(self):
        err = OperationalError("stmt", {}, Exception("no such table: api_keys"))
        assert _is_missing_table(err, "api_keys") is True

    def test_non_matching_error(self):
        err = OperationalError("stmt", {}, Exception("connection refused"))
        assert _is_missing_table(err, "api_keys") is False


@pytest.mark.unit
class TestApiKeyDao:
    @pytest.fixture
    def dao(self):
        return ApiKeyDao()

    def test_list_by_user_returns_list(self, dao, monkeypatch):
        row = FakeRow({
            "id": 1, "user_id": 10, "key_id": "qm_abc", "name": "test key",
            "permissions": '["read"]', "expires_at": None, "ip_whitelist": None,
            "rate_limit": 60, "is_active": 1, "created_at": "2024-01-01",
            "last_used_at": None
        })
        conn = FakeConn(result=FakeResult(rows=[row]))
        monkeypatch.setattr(_api_key_dao_mod, "connection", lambda n: FakeContext(conn))
        result = dao.list_by_user(10)
        assert len(result) == 1
        assert result[0]["key_id"] == "qm_abc"
        assert result[0]["permissions"] == ["read"]

    def test_list_by_user_missing_table(self, dao, monkeypatch):
        err = ProgrammingError("stmt", {}, Exception("Table 'api_keys' doesn't exist"))
        conn = FakeConn(error=err)
        monkeypatch.setattr(_api_key_dao_mod, "connection", lambda n: FakeContext(conn))
        assert dao.list_by_user(10) == []

    def test_count_by_user(self, dao, monkeypatch):
        row = FakeRow({"cnt": 3})
        conn = FakeConn(result=FakeResult(rows=[row]))
        monkeypatch.setattr(_api_key_dao_mod, "connection", lambda n: FakeContext(conn))
        assert dao.count_by_user(10) == 3

    def test_count_by_user_missing_table(self, dao, monkeypatch):
        err = OperationalError("stmt", {}, Exception("Table 'api_keys' doesn't exist"))
        conn = FakeConn(error=err)
        monkeypatch.setattr(_api_key_dao_mod, "connection", lambda n: FakeContext(conn))
        assert dao.count_by_user(10) == 0

    def test_get_by_key_id_found(self, dao, monkeypatch):
        row = FakeRow({
            "id": 1, "user_id": 10, "key_id": "qm_abc", "secret_hash": "hash123",
            "name": "test", "permissions": None, "expires_at": None,
            "ip_whitelist": None, "rate_limit": 60, "is_active": 1,
            "created_at": "2024-01-01", "last_used_at": None
        })
        conn = FakeConn(result=FakeResult(rows=[row]))
        monkeypatch.setattr(_api_key_dao_mod, "connection", lambda n: FakeContext(conn))
        result = dao.get_by_key_id("qm_abc")
        assert result["secret_hash"] == "hash123"

    def test_get_by_key_id_not_found(self, dao, monkeypatch):
        conn = FakeConn(result=FakeResult(rows=[]))
        monkeypatch.setattr(_api_key_dao_mod, "connection", lambda n: FakeContext(conn))
        assert dao.get_by_key_id("nonexistent") is None

    def test_create_returns_lastrowid(self, dao, monkeypatch):
        conn = FakeConn(result=FakeResult(lastrowid=7))
        monkeypatch.setattr(_api_key_dao_mod, "connection", lambda n: FakeContext(conn))
        result = dao.create(10, "qm_abc", "hash", "my key", permissions=["read"], ip_whitelist=["127.0.0.1"])
        assert result == 7
        assert conn.committed

    def test_create_no_optional_fields(self, dao, monkeypatch):
        conn = FakeConn(result=FakeResult(lastrowid=8))
        monkeypatch.setattr(_api_key_dao_mod, "connection", lambda n: FakeContext(conn))
        result = dao.create(10, "qm_def", "hash", "my key")
        assert result == 8

    def test_revoke(self, dao, monkeypatch):
        conn = FakeConn(result=FakeResult(rowcount=1))
        monkeypatch.setattr(_api_key_dao_mod, "connection", lambda n: FakeContext(conn))
        assert dao.revoke(1, 10) is True
        assert conn.committed

    def test_revoke_not_found(self, dao, monkeypatch):
        conn = FakeConn(result=FakeResult(rowcount=0))
        monkeypatch.setattr(_api_key_dao_mod, "connection", lambda n: FakeContext(conn))
        assert dao.revoke(999, 10) is False

    def test_update_last_used(self, dao, monkeypatch):
        conn = FakeConn(result=FakeResult())
        monkeypatch.setattr(_api_key_dao_mod, "connection", lambda n: FakeContext(conn))
        dao.update_last_used("qm_abc")
        assert conn.committed

    def test_row_to_dict_json_parsing(self, dao):
        row = FakeRow({
            "id": 1, "user_id": 10, "key_id": "k", "name": "n",
            "permissions": json.dumps(["read", "write"]),
            "expires_at": None, "ip_whitelist": json.dumps(["1.2.3.4"]),
            "rate_limit": 100, "is_active": 1, "created_at": "2024-01-01",
            "last_used_at": None
        })
        result = dao._row_to_dict(row)
        assert result["permissions"] == ["read", "write"]
        assert result["ip_whitelist"] == ["1.2.3.4"]
        assert result["is_active"] is True
