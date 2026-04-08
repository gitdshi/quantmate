"""Unit tests for PaperAccountDao."""
import pytest
from datetime import date
from app.domains.trading.dao.paper_account_dao import PaperAccountDao


class FR:
    def __init__(self, rows=None, rowcount=0, lastrowid=0):
        self._rows = rows or []; self.rowcount = rowcount; self.lastrowid = lastrowid
    def fetchone(self): return self._rows[0] if self._rows else None
    def fetchall(self): return self._rows


class FC:
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
    def __init__(self, d): self._d = d
    def __getattr__(self, n):
        if n.startswith("_"): raise AttributeError
        return self._d.get(n)


import app.domains.trading.dao.paper_account_dao as _dao_mod
MOD = "app.domains.trading.dao.paper_account_dao"


def _account_row(**overrides):
    base = {
        "id": 1, "user_id": 10, "name": "test",
        "initial_capital": 1000000.0, "balance": 900000.0, "frozen": 50000.0,
        "market_value": 100000.0, "total_pnl": 50000.0,
        "currency": "CNY", "market": "CN", "status": "active",
        "created_at": "2024-01-01", "updated_at": "2024-01-02"
    }
    base.update(overrides)
    return R(base)


@pytest.mark.unit
class TestPaperAccountDao:
    @pytest.fixture
    def dao(self): return PaperAccountDao()

    def test_create(self, dao, monkeypatch):
        conn = FC(result=FR(lastrowid=5))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        r = dao.create(10, "test acct", 1000000.0)
        assert r == 5
        assert conn.committed

    def test_get_by_id_found(self, dao, monkeypatch):
        conn = FC(result=FR(rows=[_account_row()]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        r = dao.get_by_id(1, 10)
        assert r["name"] == "test"
        assert r["total_equity"] == 1000000.0  # balance + market_value

    def test_get_by_id_not_found(self, dao, monkeypatch):
        conn = FC(result=FR(rows=[]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.get_by_id(999, 10) is None

    def test_list_by_user(self, dao, monkeypatch):
        conn = FC(result=FR(rows=[_account_row()]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        r = dao.list_by_user(10)
        assert len(r) == 1

    def test_list_by_user_with_status(self, dao, monkeypatch):
        conn = FC(result=FR(rows=[]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        r = dao.list_by_user(10, status="closed")
        assert r == []

    def test_close_account(self, dao, monkeypatch):
        conn = FC(result=FR(rowcount=1))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.close_account(1, 10) is True

    def test_close_account_already_closed(self, dao, monkeypatch):
        conn = FC(result=FR(rowcount=0))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.close_account(1, 10) is False

    def test_freeze_funds_success(self, dao, monkeypatch):
        conn = FC(result=FR(rowcount=1))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.freeze_funds(1, 10000.0) is True

    def test_freeze_funds_insufficient(self, dao, monkeypatch):
        conn = FC(result=FR(rowcount=0))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.freeze_funds(1, 999999999.0) is False

    def test_release_funds(self, dao, monkeypatch):
        conn = FC(result=FR(rowcount=1))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.release_funds(1, 5000.0) is True

    def test_settle_buy(self, dao, monkeypatch):
        conn = FC(result=FR(rowcount=1))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.settle_buy(1, 10000.0, 9500.0) is True

    def test_settle_sell(self, dao, monkeypatch):
        conn = FC(result=FR(rowcount=1))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.settle_sell(1, 15000.0) is True

    def test_update_market_value(self, dao, monkeypatch):
        conn = FC(result=FR(rowcount=1))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.update_market_value(1, 120000.0, 20000.0) is True

    def test_insert_snapshot(self, dao, monkeypatch):
        conn = FC(result=FR(lastrowid=3))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        r = dao.insert_snapshot(1, date(2024, 1, 1), 900000.0, 100000.0, 1000000.0, 5000.0)
        assert r == 3
        assert conn.committed

    def test_get_equity_curve(self, dao, monkeypatch):
        row = R({"snapshot_date": date(2024, 1, 1), "balance": 900000.0,
                 "market_value": 100000.0, "total_equity": 1000000.0, "daily_pnl": 5000.0})
        conn = FC(result=FR(rows=[row]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        r = dao.get_equity_curve(1)
        assert len(r) == 1
        assert r[0]["total_equity"] == 1000000.0

    def test_row_to_dict_return_pct(self):
        row = _account_row(initial_capital=1000000.0, balance=950000.0, market_value=100000.0)
        result = PaperAccountDao._row_to_dict(row)
        assert result["return_pct"] == 5.0

    def test_row_to_dict_zero_capital(self):
        row = _account_row(initial_capital=0.0, balance=0.0, market_value=0.0)
        result = PaperAccountDao._row_to_dict(row)
        assert result["return_pct"] == 0.0
