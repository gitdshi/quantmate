from unittest.mock import MagicMock

from sqlalchemy.exc import OperationalError

from app.domains.market.dao import watchlist_dao


class FakeResult:
    def __init__(self, rows=None):
        self._rows = rows or []

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


def test_list_for_user_falls_back_when_sort_order_missing(monkeypatch):
    err = OperationalError("stmt", {}, Exception("Unknown column 'sort_order' in 'order clause'"))
    row = MagicMock()
    row._mapping = {"id": 1, "user_id": 10, "name": "WL"}
    conn = FakeConn(results=[err, FakeResult(rows=[row])])
    monkeypatch.setattr(watchlist_dao, "connection", lambda name: FakeContext(conn))

    result = watchlist_dao.WatchlistDao().list_for_user(10)

    assert result == [{"id": 1, "user_id": 10, "name": "WL"}]


def test_list_for_user_returns_empty_when_watchlists_table_missing(monkeypatch):
    err = OperationalError("stmt", {}, Exception("Table 'watchlists' doesn't exist"))
    conn = FakeConn(results=[err])
    monkeypatch.setattr(watchlist_dao, "connection", lambda name: FakeContext(conn))

    assert watchlist_dao.WatchlistDao().list_for_user(10) == []
