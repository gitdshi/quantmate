from unittest.mock import MagicMock

from sqlalchemy.exc import OperationalError, ProgrammingError

from app.domains.auth.dao import kyc_dao


class FakeResult:
    def __init__(self, row=None, rows=None):
        self._row = row
        self._rows = rows or []

    def fetchone(self):
        return self._row

    def fetchall(self):
        return self._rows


class FakeConn:
    def __init__(self, results=None):
        self.results = list(results or [])

    def execute(self, *args, **kwargs):
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    def commit(self):
        return None


class FakeContext:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self.conn

    def __exit__(self, exc_type, exc, tb):
        return False


def test_get_latest_returns_none_when_kyc_table_missing(monkeypatch):
    err = ProgrammingError("stmt", {}, Exception("Table 'quantmate.kyc_submissions' doesn't exist"))
    conn = FakeConn(results=[err])
    monkeypatch.setattr(kyc_dao, "connection", lambda name: FakeContext(conn))

    assert kyc_dao.KycDao().get_latest(10) is None


def test_list_pending_and_count_pending_gracefully_handle_missing_table(monkeypatch):
    err1 = OperationalError("stmt", {}, Exception("Table 'quantmate.kyc_submissions' doesn't exist"))
    err2 = ProgrammingError("stmt", {}, Exception("Table 'quantmate.kyc_submissions' doesn't exist"))
    conn = FakeConn(results=[err1, err2])
    calls = iter([FakeContext(conn), FakeContext(conn)])
    monkeypatch.setattr(kyc_dao, "connection", lambda name: next(calls))

    dao = kyc_dao.KycDao()
    assert dao.list_pending() == []
    assert dao.count_pending() == 0
