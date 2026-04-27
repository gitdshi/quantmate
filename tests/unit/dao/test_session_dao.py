from datetime import datetime, timedelta

from sqlalchemy.exc import OperationalError

from app.domains.auth.dao import session_dao


class FakeResult:
    def __init__(self, rows=None, rowcount=0, lastrowid=0):
        self._rows = rows or []
        self.rowcount = rowcount
        self.lastrowid = lastrowid

    def fetchall(self):
        return self._rows


class FakeConn:
    def __init__(self, result=None, error=None):
        self.result = result or FakeResult()
        self.error = error
        self.committed = False

    def execute(self, *args, **kwargs):
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

    def __exit__(self, exc_type, exc, tb):
        return False


def test_is_missing_table_detects_mysql_and_sqlite_messages():
    mysql_err = OperationalError("stmt", {}, Exception("Table 'user_sessions' doesn't exist"))
    sqlite_err = OperationalError("stmt", {}, Exception("no such table: user_sessions"))

    assert session_dao._is_missing_table(mysql_err, "user_sessions") is True
    assert session_dao._is_missing_table(sqlite_err, "user_sessions") is True


class TestSessionDao:
    def test_create_returns_lastrowid(self, monkeypatch):
        conn = FakeConn(result=FakeResult(lastrowid=9))
        monkeypatch.setattr(session_dao, "connection", lambda name: FakeContext(conn))

        dao = session_dao.SessionDao()
        result = dao.create(1, "hash", "device", "127.0.0.1", datetime.utcnow() + timedelta(days=1))

        assert result == 9
        assert conn.committed is True

    def test_list_by_user_returns_serialized_rows(self, monkeypatch):
        row = type("Row", (), {
            "id": 1,
            "user_id": 2,
            "device_info": "ios",
            "ip_address": "127.0.0.1",
            "login_at": "login",
            "last_active_at": "active",
            "expires_at": "exp",
        })()
        conn = FakeConn(result=FakeResult(rows=[row]))
        monkeypatch.setattr(session_dao, "connection", lambda name: FakeContext(conn))

        dao = session_dao.SessionDao()
        assert dao.list_by_user(2) == [{
            "id": 1,
            "user_id": 2,
            "device_info": "ios",
            "ip_address": "127.0.0.1",
            "login_at": "login",
            "last_active_at": "active",
            "expires_at": "exp",
        }]

    def test_list_by_user_returns_empty_for_missing_table(self, monkeypatch):
        err = OperationalError("stmt", {}, Exception("Unknown table user_sessions doesn't exist"))
        conn = FakeConn(error=err)
        monkeypatch.setattr(session_dao, "connection", lambda name: FakeContext(conn))

        assert session_dao.SessionDao().list_by_user(1) == []

    def test_delete_and_delete_all_for_user_commit_and_return_counts(self, monkeypatch):
        delete_conn = FakeConn(result=FakeResult(rowcount=1))
        delete_all_conn = FakeConn(result=FakeResult(rowcount=3))
        calls = iter([FakeContext(delete_conn), FakeContext(delete_all_conn)])
        monkeypatch.setattr(session_dao, "connection", lambda name: next(calls))

        dao = session_dao.SessionDao()
        assert dao.delete(1, 2) is True
        assert dao.delete_all_for_user(2) == 3
        assert delete_conn.committed is True
        assert delete_all_conn.committed is True

    def test_touch_and_cleanup_expired_commit(self, monkeypatch):
        touch_conn = FakeConn(result=FakeResult())
        cleanup_conn = FakeConn(result=FakeResult(rowcount=4))
        calls = iter([FakeContext(touch_conn), FakeContext(cleanup_conn)])
        monkeypatch.setattr(session_dao, "connection", lambda name: next(calls))

        dao = session_dao.SessionDao()
        dao.touch("abc")
        assert dao.cleanup_expired() == 4
        assert touch_conn.committed is True
        assert cleanup_conn.committed is True
