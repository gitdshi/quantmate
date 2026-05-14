"""Unit tests for app.domains.trading.paper_settlement_service."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import app.domains.trading.paper_settlement_service as _mod


def _fake_conn():
    conn = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)
    return ctx, conn


def _row(**kw):
    m = MagicMock()
    for key, value in kw.items():
        setattr(m, key, value)
    return m


@pytest.fixture(autouse=True)
def _patch_conn(monkeypatch):
    ctx, conn = _fake_conn()
    monkeypatch.setattr(_mod, "connection", lambda db: ctx)
    return conn


def test_settle_account_uses_open_lots(monkeypatch, _patch_conn):
    service = _mod.PaperSettlementService()

    class FakeQuoteService:
        def get_quote(self, symbol, market):
            return {"last_price": 10.0}

    monkeypatch.setattr("app.domains.market.realtime_quote_service.RealtimeQuoteService", FakeQuoteService)

    _patch_conn.execute.side_effect = [
        MagicMock(fetchall=MagicMock(return_value=[_row(symbol="000001.SZ", side="long", total_qty=100, avg_cost=9.5)])),
        MagicMock(fetchone=MagicMock(return_value=_row(balance=100000.0, frozen=0.0))),
        MagicMock(),
        MagicMock(),
    ]

    service._settle_account(account_id=1, user_id=1, market="CN", initial_capital=100000.0, today=_mod.date.today())

    assert _patch_conn.execute.call_count >= 4