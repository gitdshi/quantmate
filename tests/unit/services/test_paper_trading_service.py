"""Unit tests for app.domains.trading.paper_trading_service."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import app.domains.trading.paper_trading_service as _mod


def _fake_conn():
    conn = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)
    return ctx, conn


def _row(**kw):
    m = MagicMock()
    m._mapping = kw
    for k, v in kw.items():
        setattr(m, k, v)
    return m


@pytest.fixture(autouse=True)
def _mock_connection(monkeypatch):
    ctx, conn = _fake_conn()
    monkeypatch.setattr(_mod, "connection", lambda db: ctx)
    return conn


class TestPaperTradingService:
    def test_deploy(self, _mock_connection):
        # strategy lookup
        strat_row = MagicMock()
        strat_row._mapping = {"id": 1, "name": "MA Cross", "class_name": "MaCross"}
        # insert deployment
        insert_result = MagicMock(lastrowid=10)
        _mock_connection.execute.side_effect = [
            MagicMock(fetchone=MagicMock(return_value=strat_row)),
            insert_result,
        ]
        result = _mod.PaperTradingService().deploy(
            user_id=1, strategy_id=1, vt_symbol="000001.SZ",
            parameters={"fast": 5}, paper_account_id=1
        )
        assert isinstance(result, dict)

    def test_list_deployments(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(
            fetchall=MagicMock(return_value=[_row(
                id=1, strategy_id=1, strategy_name="MA", vt_symbol="000001.SZ",
                parameters='{"fast": 5}', status="running",
                started_at="2024-01-01", stopped_at=None
            )])
        )
        result = _mod.PaperTradingService().list_deployments(user_id=1)
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["parameters"]["fast"] == 5

    def test_stop_deployment(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(rowcount=1)
        result = _mod.PaperTradingService().stop_deployment(deployment_id=1, user_id=1)
        assert result is True

    def test_get_positions(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(
            fetchall=MagicMock(return_value=[_row(symbol="000001.SZ", quantity=100)])
        )
        result = _mod.PaperTradingService().get_positions(user_id=1)
        assert isinstance(result, list)

    def test_get_performance(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(
            fetchall=MagicMock(return_value=[]),
            fetchone=MagicMock(return_value=None)
        )
        result = _mod.PaperTradingService().get_performance(user_id=1)
        assert isinstance(result, dict)
