"""Unit tests for app.domains.market.dao.trade_log_dao."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest

import app.domains.market.dao.trade_log_dao as _mod


def _fake_conn():
    conn = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)
    return ctx, conn


@pytest.fixture(autouse=True)
def _mock_connection(monkeypatch):
    ctx, conn = _fake_conn()
    monkeypatch.setattr(_mod, "connection", lambda db: ctx)
    return conn


def _row(**kw):
    m = MagicMock()
    m._mapping = kw
    return m


class TestTradeLogDao:
    def test_insert(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(lastrowid=1)
        result = _mod.TradeLogDao().insert(
            symbol="000001.SZ", event_type="BUY", direction="LONG",
            quantity=100, price=10.5
        )
        assert result == 1

    def test_query_no_filters(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(
            fetchall=MagicMock(return_value=[_row(id=1, symbol="000001.SZ")])
        )
        result = _mod.TradeLogDao().query()
        assert isinstance(result, list)

    def test_query_with_filters(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(
            fetchall=MagicMock(return_value=[])
        )
        result = _mod.TradeLogDao().query(
            symbol="000001.SZ", event_type="BUY",
            start_date=date(2024, 1, 1), end_date=date(2024, 12, 31)
        )
        assert result == []

    def test_count(self, _mock_connection):
        row = MagicMock()
        row._mapping = {"cnt": 10}
        _mock_connection.execute.return_value = MagicMock(
            fetchone=MagicMock(return_value=row)
        )
        result = _mod.TradeLogDao().count()
        assert result == 10

    def test_is_missing_table(self):
        from sqlalchemy.exc import OperationalError
        exc = OperationalError("stmt", {}, Exception("Table 'trade_logs' doesn't exist"))
        assert _mod._is_missing_table(exc, "trade_logs") is True

    def test_is_missing_table_false(self):
        from sqlalchemy.exc import OperationalError
        exc = OperationalError("stmt", {}, Exception("Connection refused"))
        assert _mod._is_missing_table(exc, "trade_logs") is False
