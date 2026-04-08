"""Unit tests for app.domains.market.dao.tushare_market_dao."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest

import app.domains.market.dao.tushare_market_dao as _mod


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


class TestTushareMarketDao:
    def test_list_stock_basic(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(
            fetchall=MagicMock(return_value=[_row(ts_code="000001.SZ", name="平安银行")])
        )
        result = _mod.TushareMarketDao().list_stock_basic()
        assert isinstance(result, list)

    def test_list_stock_basic_with_keyword(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(
            fetchall=MagicMock(return_value=[])
        )
        result = _mod.TushareMarketDao().list_stock_basic(keyword="平安")
        assert result == []

    def test_list_stock_basic_with_exchange(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(
            fetchall=MagicMock(return_value=[])
        )
        result = _mod.TushareMarketDao().list_stock_basic(exchange="SSE")
        assert result == []

    def test_get_stock_daily_history(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(
            fetchall=MagicMock(return_value=[
                _row(trade_date=date(2024, 1, 5), open=10.0, close=10.5)
            ])
        )
        result = _mod.TushareMarketDao().get_stock_daily_history(
            ts_code="000001.SZ",
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31)
        )
        assert isinstance(result, list)

    def test_exchange_counts(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(
            fetchall=MagicMock(return_value=[_row(exchange="SSE", cnt=1000)])
        )
        result = _mod.TushareMarketDao().exchange_counts()
        assert isinstance(result, dict)

    def test_stock_daily_date_range(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(
            fetchone=MagicMock(return_value=_row(min_date=date(2020, 1, 1), max_date=date(2024, 12, 31)))
        )
        result = _mod.TushareMarketDao().stock_daily_date_range()
        assert isinstance(result, dict)

    def test_sectors(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(
            fetchall=MagicMock(return_value=[_row(industry="银行", cnt=20)])
        )
        result = _mod.TushareMarketDao().sectors()
        assert isinstance(result, list)

    def test_exchanges(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(
            fetchall=MagicMock(return_value=[_row(exchange="SSE", cnt=1000)])
        )
        result = _mod.TushareMarketDao().exchanges()
        assert isinstance(result, list)

    def test_symbols_by_filter(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(
            fetchall=MagicMock(return_value=[])
        )
        result = _mod.TushareMarketDao().symbols_by_filter(industry="银行")
        assert result == []
