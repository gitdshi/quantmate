"""Unit tests for app.domains.trading.paper_analytics_service."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import app.domains.trading.paper_analytics_service as _mod


def _fake_conn():
    conn = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)
    return ctx, conn


def _row(**kw):
    m = MagicMock()
    m._mapping = kw
    return m


@pytest.fixture(autouse=True)
def _patch(monkeypatch):
    ctx, conn = _fake_conn()
    monkeypatch.setattr(_mod, "connection", lambda db: ctx)
    return conn


class TestPaperAnalyticsService:
    """Tests for PaperAnalyticsService."""

    def test_get_analytics_account_not_found(self, _patch):
        _patch.execute.return_value = MagicMock(
            fetchone=MagicMock(return_value=None)
        )
        svc = _mod.PaperAnalyticsService()
        result = svc.get_analytics(account_id=99, user_id=1)
        # Should return empty/default analytics when account not found
        assert isinstance(result, dict)

    def test_get_analytics_ok(self, _patch):
        acct = _row(id=1, user_id=1, initial_capital=100000.0, current_balance=105000.0)
        _patch.execute.side_effect = [
            MagicMock(fetchone=MagicMock(return_value=acct)),  # account lookup
            MagicMock(fetchall=MagicMock(return_value=[])),     # closed trades
            MagicMock(fetchall=MagicMock(return_value=[])),     # equity curve
        ]
        svc = _mod.PaperAnalyticsService()
        result = svc.get_analytics(account_id=1, user_id=1)
        assert isinstance(result, dict)

    def test_compute_trade_metrics_empty(self):
        svc = _mod.PaperAnalyticsService()
        m = svc._compute_trade_metrics([])
        assert m["total_trades"] == 0
        assert m["win_rate"] == 0.0

    def test_compute_trade_metrics_mixed(self):
        trades = [
            {"symbol": "000001.SZ", "direction": "buy", "quantity": 100, "price": 10.0, "fee": 5.0},
            {"symbol": "000001.SZ", "direction": "sell", "quantity": 100, "price": 12.0, "fee": 5.0},
            {"symbol": "000002.SZ", "direction": "buy", "quantity": 50, "price": 20.0, "fee": 3.0},
            {"symbol": "000002.SZ", "direction": "sell", "quantity": 50, "price": 19.0, "fee": 3.0},
        ]
        svc = _mod.PaperAnalyticsService()
        m = svc._compute_trade_metrics(trades)
        assert m["total_trades"] == 4
        assert m["win_rate"] > 0

    def test_compute_risk_metrics_empty_curve(self):
        svc = _mod.PaperAnalyticsService()
        m = svc._compute_risk_metrics([], 100000)
        assert isinstance(m, dict)

    def test_compute_risk_metrics_with_data(self):
        curve = [
            {"equity": 100000},
            {"equity": 105000},
            {"equity": 103000},
            {"equity": 110000},
            {"equity": 108000},
        ]
        svc = _mod.PaperAnalyticsService()
        m = svc._compute_risk_metrics(curve, 100000)
        assert "max_drawdown" in m
        assert "sharpe_ratio" in m

    def test_get_closed_trades(self, _patch):
        _patch.execute.return_value = MagicMock(
            fetchall=MagicMock(return_value=[
                _row(direction="LONG", quantity=100, price=10.0, pnl=50.0)
            ])
        )
        svc = _mod.PaperAnalyticsService()
        trades = svc._get_closed_trades(account_id=1, user_id=1)
        assert isinstance(trades, list)

    def test_get_equity_curve(self, _patch):
        _patch.execute.return_value = MagicMock(
            fetchall=MagicMock(return_value=[
                _row(equity=100000, snapshot_time="2024-01-01")
            ])
        )
        svc = _mod.PaperAnalyticsService()
        curve = svc._get_equity_curve(account_id=1)
        assert isinstance(curve, list)
