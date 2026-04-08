"""Unit tests for OrderDao."""
import pytest
from app.domains.trading.dao.order_dao import OrderDao


class FR:
    def __init__(self, rows=None, rowcount=0, lastrowid=0):
        self._rows = rows or []; self.rowcount = rowcount; self.lastrowid = lastrowid
    def fetchone(self): return self._rows[0] if self._rows else None
    def fetchall(self): return self._rows


class FC:
    def __init__(self, result=None, results=None, error=None):
        self._result = result or FR()
        self._results = list(results) if results else None
        self._idx = 0
        self.error = error; self.committed = False; self.executed = []
    def execute(self, *a, **kw):
        self.executed.append((a, kw))
        if self.error: raise self.error
        if self._results:
            r = self._results[self._idx]; self._idx += 1; return r
        return self._result
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


import app.domains.trading.dao.order_dao as _dao_mod
MOD = "app.domains.trading.dao.order_dao"


@pytest.mark.unit
class TestOrderDao:
    @pytest.fixture
    def dao(self): return OrderDao()

    def test_create(self, dao, monkeypatch):
        conn = FC(result=FR(lastrowid=10))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        r = dao.create(1, "000001.SZ", "buy", "limit", 100, price=10.0)
        assert r == 10
        assert conn.committed

    def test_create_with_all_params(self, dao, monkeypatch):
        conn = FC(result=FR(lastrowid=11))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        r = dao.create(1, "000001.SZ", "buy", "limit", 100, price=10.0,
                       stop_price=9.0, strategy_id=5, portfolio_id=2,
                       mode="paper", paper_account_id=3, buy_date="2024-01-01")
        assert r == 11

    def test_get_by_id_found(self, dao, monkeypatch):
        row = R({"id": 1, "user_id": 10, "portfolio_id": None, "symbol": "000001.SZ",
                 "direction": "buy", "order_type": "limit", "quantity": 100,
                 "price": 10.5, "stop_price": None, "status": "filled",
                 "filled_quantity": 100, "avg_fill_price": 10.5, "fee": 5.0,
                 "strategy_id": None, "mode": "paper", "paper_account_id": 1,
                 "buy_date": "2024-01-01", "created_at": "2024", "updated_at": "2024"})
        conn = FC(result=FR(rows=[row]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        r = dao.get_by_id(1, 10)
        assert r["symbol"] == "000001.SZ"
        assert r["price"] == 10.5

    def test_get_by_id_not_found(self, dao, monkeypatch):
        conn = FC(result=FR(rows=[]))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.get_by_id(999, 10) is None

    def test_list_by_user(self, dao, monkeypatch):
        total_row = R({"cnt": 1})
        data_row = R({"id": 1, "user_id": 10, "portfolio_id": None, "symbol": "000001.SZ",
                       "direction": "buy", "order_type": "market", "quantity": 100,
                       "price": None, "stop_price": None, "status": "created",
                       "filled_quantity": 0, "avg_fill_price": None, "fee": None,
                       "strategy_id": None, "mode": "paper", "paper_account_id": None,
                       "buy_date": None, "created_at": "2024", "updated_at": "2024"})
        results = [FR(rows=[total_row]), FR(rows=[data_row])]
        conn = FC(results=results)
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        items, total = dao.list_by_user(10)
        assert total == 1
        assert len(items) == 1

    def test_list_by_user_with_filters(self, dao, monkeypatch):
        results = [FR(rows=[R({"cnt": 0})]), FR(rows=[])]
        conn = FC(results=results)
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        items, total = dao.list_by_user(10, status="filled", mode="paper")
        assert total == 0

    def test_update_status(self, dao, monkeypatch):
        conn = FC(result=FR(rowcount=1))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.update_status(1, "filled", filled_quantity=100, avg_fill_price=10.5, fee=5.0) is True

    def test_update_status_minimal(self, dao, monkeypatch):
        conn = FC(result=FR(rowcount=1))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.update_status(1, "cancelled") is True

    def test_cancel_success(self, dao, monkeypatch):
        conn = FC(result=FR(rowcount=1))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.cancel(1, 10) is True

    def test_cancel_not_found(self, dao, monkeypatch):
        conn = FC(result=FR(rowcount=0))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        assert dao.cancel(999, 10) is False

    def test_insert_trade(self, dao, monkeypatch):
        conn = FC(result=FR(lastrowid=20))
        monkeypatch.setattr(_dao_mod, "connection", lambda n: FCtx(conn))
        r = dao.insert_trade(1, 100, 10.5, fee=5.0)
        assert r == 20
        assert conn.committed

    def test_row_to_dict_none_values(self, dao):
        row = R({"id": 1, "user_id": 10, "portfolio_id": None, "symbol": "000001.SZ",
                 "direction": "buy", "order_type": "market", "quantity": 100,
                 "price": None, "stop_price": None, "status": "created",
                 "filled_quantity": 0, "avg_fill_price": None, "fee": None,
                 "strategy_id": None, "mode": "paper", "paper_account_id": None,
                 "buy_date": None, "created_at": "2024", "updated_at": "2024"})
        result = dao._row_to_dict(row)
        assert result["price"] is None
        assert result["fee"] == 0
