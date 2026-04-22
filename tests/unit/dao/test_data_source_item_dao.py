"""Unit tests for app.domains.market.dao.data_source_item_dao."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import app.domains.market.dao.data_source_item_dao as _mod


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


# ── DataSourceItemDao ────────────────────────────────────────────

class TestDataSourceItemDao:
    def test_list_all(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(
            fetchall=MagicMock(return_value=[_row(id=1, source="tushare", item_key="stock_daily")])
        )
        result = _mod.DataSourceItemDao().list_all()
        assert isinstance(result, list)

    def test_list_all_with_source(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(
            fetchall=MagicMock(return_value=[])
        )
        result = _mod.DataSourceItemDao().list_all(source="tushare")
        assert result == []

    def test_list_all_deduplicates_source_item_pairs(self, _mock_connection):
        rows = [
            _row(id=1, source="tushare", item_key="stock_daily"),
            _row(id=2, source="tushare", item_key="stock_daily"),
            _row(id=3, source="tushare", item_key="daily_basic"),
        ]
        _mock_connection.execute.return_value = MagicMock(
            fetchall=MagicMock(return_value=rows)
        )

        result = _mod.DataSourceItemDao().list_all(source="tushare")

        assert result == [
            {"id": 1, "source": "tushare", "item_key": "stock_daily"},
            {"id": 3, "source": "tushare", "item_key": "daily_basic"},
        ]

    def test_get_by_key(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(
            fetchone=MagicMock(return_value=_row(id=1, source="tushare"))
        )
        result = _mod.DataSourceItemDao().get_by_key(source="tushare", item_key="stock_daily")
        assert result is not None

    def test_get_by_key_not_found(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(
            fetchone=MagicMock(return_value=None)
        )
        result = _mod.DataSourceItemDao().get_by_key(source="x", item_key="y")
        assert result is None

    def test_update_enabled(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(rowcount=1)
        result = _mod.DataSourceItemDao().update_enabled(source="tushare", item_key="stock_daily", enabled=True)
        assert result is True

    def test_mark_table_created(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(rowcount=1)
        result = _mod.DataSourceItemDao().mark_table_created(source="tushare", item_key="stock_daily")
        assert result is True

    def test_batch_update(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(rowcount=1)
        items = [{"source": "tushare", "item_key": "stock_daily", "enabled": True}]
        result = _mod.DataSourceItemDao().batch_update(items)
        assert isinstance(result, int)

    def test_list_enabled(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(
            fetchall=MagicMock(return_value=[])
        )
        result = _mod.DataSourceItemDao().list_enabled()
        assert result == []


# ── DataSourceConfigDao ──────────────────────────────────────────

class TestDataSourceConfigDao:
    def test_list_all(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(
            fetchall=MagicMock(return_value=[_row(source_key="tushare", enabled=1)])
        )
        result = _mod.DataSourceConfigDao().list_all()
        assert isinstance(result, list)

    def test_get_by_key(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(
            fetchone=MagicMock(return_value=_row(source_key="tushare"))
        )
        result = _mod.DataSourceConfigDao().get_by_key(source_key="tushare")
        assert result is not None

    def test_update_enabled(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(rowcount=1)
        result = _mod.DataSourceConfigDao().update_enabled(source_key="tushare", enabled=True)
        assert result is True

    def test_update_config(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(rowcount=1)
        result = _mod.DataSourceConfigDao().update_config(
            source_key="tushare", config_json='{"key": "val"}', enabled=True
        )
        assert result is True
