"""Unit tests for AlertRuleDao, AlertHistoryDao, NotificationChannelDao."""
import json
import pytest
from sqlalchemy.exc import SQLAlchemyError, OperationalError

from app.domains.monitoring.dao import alert_dao as _alert_mod
from app.domains.monitoring.dao.alert_dao import (
    AlertRuleDao, AlertHistoryDao, NotificationChannelDao, _is_missing_table_error
)


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
        self._idx = 0
        self.error = error
        self.committed = False
        self.executed = []
    def execute(self, *a, **kw):
        self.executed.append((a, kw))
        if self.error:
            raise self.error
        if self._results:
            r = self._results[self._idx]; self._idx += 1; return r
        return self._result
    def commit(self):
        self.committed = True


class FakeContext:
    def __init__(self, c): self.conn = c
    def __enter__(self): return self.conn
    def __exit__(self, *a): return False


class R:
    """Fake row."""
    def __init__(self, d): self._d = d
    def __getattr__(self, n):
        if n.startswith("_"): raise AttributeError
        return self._d.get(n)


MOD = "app.domains.monitoring.dao.alert_dao"


@pytest.mark.unit
class TestMissingTableError:
    def test_detects_doesnt_exist(self):
        err = OperationalError("s", {}, Exception("Table 'alert_rules' doesn't exist"))
        assert _is_missing_table_error(err, "alert_rules") is True

    def test_detects_no_such_table(self):
        err = OperationalError("s", {}, Exception("no such table: alert_rules"))
        assert _is_missing_table_error(err, "alert_rules") is True

    def test_non_matching(self):
        err = OperationalError("s", {}, Exception("connection timeout"))
        assert _is_missing_table_error(err, "alert_rules") is False


@pytest.mark.unit
class TestAlertRuleDao:
    @pytest.fixture
    def dao(self): return AlertRuleDao()

    def test_list_by_user(self, dao, monkeypatch):
        row = R({"id": 1, "user_id": 10, "name": "r", "metric": "pnl", "comparator": ">",
                 "threshold": 1000.0, "time_window": 60, "level": "warning",
                 "is_active": 1, "created_at": "2024", "updated_at": "2024"})
        conn = FakeConn(result=FakeResult(rows=[row]))
        monkeypatch.setattr(_alert_mod, "connection", lambda n: FakeContext(conn))
        res = dao.list_by_user(10)
        assert len(res) == 1
        assert res[0]["threshold"] == 1000.0
        assert res[0]["is_active"] is True

    def test_list_by_user_missing_table(self, dao, monkeypatch):
        err = OperationalError("s", {}, Exception("Table 'alert_rules' doesn't exist"))
        conn = FakeConn(error=err)
        monkeypatch.setattr(_alert_mod, "connection", lambda n: FakeContext(conn))
        assert dao.list_by_user(10) == []

    def test_create(self, dao, monkeypatch):
        conn = FakeConn(result=FakeResult(lastrowid=5))
        monkeypatch.setattr(_alert_mod, "connection", lambda n: FakeContext(conn))
        r = dao.create(10, "test", "pnl", ">", 500.0, level="critical", time_window=30)
        assert r == 5
        assert conn.committed

    def test_update_success(self, dao, monkeypatch):
        conn = FakeConn(result=FakeResult(rowcount=1))
        monkeypatch.setattr(_alert_mod, "connection", lambda n: FakeContext(conn))
        assert dao.update(1, 10, name="new", threshold=200.0) is True

    def test_update_no_valid_fields(self, dao, monkeypatch):
        conn = FakeConn()
        monkeypatch.setattr(_alert_mod, "connection", lambda n: FakeContext(conn))
        assert dao.update(1, 10, invalid_field="x") is False

    def test_delete_success(self, dao, monkeypatch):
        conn = FakeConn(result=FakeResult(rowcount=1))
        monkeypatch.setattr(_alert_mod, "connection", lambda n: FakeContext(conn))
        assert dao.delete(1, 10) is True
        assert conn.committed

    def test_delete_not_found(self, dao, monkeypatch):
        conn = FakeConn(result=FakeResult(rowcount=0))
        monkeypatch.setattr(_alert_mod, "connection", lambda n: FakeContext(conn))
        assert dao.delete(999, 10) is False


@pytest.mark.unit
class TestAlertHistoryDao:
    @pytest.fixture
    def dao(self): return AlertHistoryDao()

    def test_list_by_user(self, dao, monkeypatch):
        total_row = R({"cnt": 1})
        data_row = R({"id": 1, "rule_id": 2, "user_id": 10, "triggered_at": "2024",
                       "level": "warning", "message": "alert!", "status": "triggered"})
        results = [FakeResult(rows=[total_row]), FakeResult(rows=[data_row])]
        conn = FakeConn(results=results)
        monkeypatch.setattr(_alert_mod, "connection", lambda n: FakeContext(conn))
        items, total = dao.list_by_user(10, page=1, page_size=20)
        assert total == 1
        assert len(items) == 1

    def test_list_by_user_with_level(self, dao, monkeypatch):
        results = [FakeResult(rows=[R({"cnt": 0})]), FakeResult(rows=[])]
        conn = FakeConn(results=results)
        monkeypatch.setattr(_alert_mod, "connection", lambda n: FakeContext(conn))
        items, total = dao.list_by_user(10, level="critical")
        assert total == 0
        assert items == []

    def test_list_by_user_missing_table(self, dao, monkeypatch):
        err = OperationalError("s", {}, Exception("Table 'alert_history' doesn't exist"))
        conn = FakeConn(error=err)
        monkeypatch.setattr(_alert_mod, "connection", lambda n: FakeContext(conn))
        items, total = dao.list_by_user(10)
        assert items == []
        assert total == 0

    def test_insert(self, dao, monkeypatch):
        conn = FakeConn(result=FakeResult(lastrowid=9))
        monkeypatch.setattr(_alert_mod, "connection", lambda n: FakeContext(conn))
        r = dao.insert(1, 10, "warning", "test message")
        assert r == 9
        assert conn.committed

    def test_acknowledge_success(self, dao, monkeypatch):
        conn = FakeConn(result=FakeResult(rowcount=1))
        monkeypatch.setattr(_alert_mod, "connection", lambda n: FakeContext(conn))
        assert dao.acknowledge(1, 10) is True

    def test_acknowledge_not_found(self, dao, monkeypatch):
        conn = FakeConn(result=FakeResult(rowcount=0))
        monkeypatch.setattr(_alert_mod, "connection", lambda n: FakeContext(conn))
        assert dao.acknowledge(999, 10) is False


@pytest.mark.unit
class TestNotificationChannelDao:
    @pytest.fixture
    def dao(self): return NotificationChannelDao()

    def test_list_by_user(self, dao, monkeypatch):
        row = R({"id": 1, "user_id": 10, "channel_type": "email",
                 "config_json": json.dumps({"to": "a@b.com"}), "is_active": 1, "created_at": "2024"})
        conn = FakeConn(result=FakeResult(rows=[row]))
        monkeypatch.setattr(_alert_mod, "connection", lambda n: FakeContext(conn))
        res = dao.list_by_user(10)
        assert len(res) == 1
        assert res[0]["config"] == {"to": "a@b.com"}

    def test_list_by_user_missing_table(self, dao, monkeypatch):
        err = OperationalError("s", {}, Exception("Table 'notification_channels' doesn't exist"))
        conn = FakeConn(error=err)
        monkeypatch.setattr(_alert_mod, "connection", lambda n: FakeContext(conn))
        assert dao.list_by_user(10) == []

    def test_create(self, dao, monkeypatch):
        conn = FakeConn(result=FakeResult(lastrowid=3))
        monkeypatch.setattr(_alert_mod, "connection", lambda n: FakeContext(conn))
        r = dao.create(10, "webhook", {"url": "http://example.com"})
        assert r == 3

    def test_update_config(self, dao, monkeypatch):
        conn = FakeConn(result=FakeResult(rowcount=1))
        monkeypatch.setattr(_alert_mod, "connection", lambda n: FakeContext(conn))
        assert dao.update(1, 10, config={"url": "new"}) is True

    def test_update_is_active(self, dao, monkeypatch):
        conn = FakeConn(result=FakeResult(rowcount=1))
        monkeypatch.setattr(_alert_mod, "connection", lambda n: FakeContext(conn))
        assert dao.update(1, 10, is_active=False) is True

    def test_update_no_fields(self, dao, monkeypatch):
        conn = FakeConn()
        monkeypatch.setattr(_alert_mod, "connection", lambda n: FakeContext(conn))
        assert dao.update(1, 10) is False

    def test_delete_success(self, dao, monkeypatch):
        conn = FakeConn(result=FakeResult(rowcount=1))
        monkeypatch.setattr(_alert_mod, "connection", lambda n: FakeContext(conn))
        assert dao.delete(1, 10) is True

    def test_delete_not_found(self, dao, monkeypatch):
        conn = FakeConn(result=FakeResult(rowcount=0))
        monkeypatch.setattr(_alert_mod, "connection", lambda n: FakeContext(conn))
        assert dao.delete(999, 10) is False
