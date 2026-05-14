"""Unit tests for app.domains.trading.paper_portfolio_executor."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

import app.domains.trading.paper_portfolio_executor as _mod
from app.domains.trading.paper_gateway import PaperGateway


def _fake_conn():
    conn = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)
    return ctx, conn


def _row(**kw):
    m = MagicMock()
    for k, v in kw.items():
        setattr(m, k, v)
    return m


@pytest.fixture(autouse=True)
def _patch_conn(monkeypatch):
    ctx, conn = _fake_conn()
    monkeypatch.setattr(_mod, "connection", lambda db: ctx)
    return conn


@pytest.fixture(autouse=True)
def _patch_ledger(monkeypatch):
    monkeypatch.setattr(_mod.PaperExecutionLedger, "record_fill", lambda *args, **kwargs: None)
    monkeypatch.setattr(_mod.PaperExecutionLedger, "write_checkpoint", lambda *args, **kwargs: None)


class TestPaperPortfolioEngine:
    @patch("app.domains.trading.dao.order_dao.OrderDao")
    @patch("app.domains.trading.paper_account_service.PaperAccountService")
    @patch("app.domains.trading.matching_engine.try_fill_market_order")
    @patch("app.domains.market.realtime_quote_service.RealtimeQuoteService")
    def test_send_order_updates_strategy_state(self, MockQuoteSvc, mock_fill, MockAccountSvc, MockOrderDao, _patch_conn):
        from vnpy.trader.constant import Direction, Offset

        gateway = PaperGateway("PAPER.20")
        engine = _mod._PaperPortfolioEngine(
            executor=MagicMock(),
            deployment_id=20,
            paper_account_id=1,
            user_id=1,
            vt_symbols=["000001.SZSE", "000002.SZSE"],
            execution_mode="auto",
            gateway=gateway,
        )
        strategy = MagicMock(strategy_name="PairTrade")

        MockQuoteSvc.return_value.get_quote.return_value = {"last_price": 10.0}
        mock_fill.return_value = MagicMock(filled=True, fill_price=10.1, fill_quantity=100, fee=MagicMock(total=1.5))
        MockOrderDao.return_value.create.return_value = 9
        MockAccountSvc.return_value.freeze_funds.return_value = True

        order_ids = engine.send_order(strategy, "000001.SZSE", Direction.LONG, Offset.OPEN, 10.0, 100)

        assert order_ids == ["PAPER.20.0"]
        strategy.update_order.assert_called_once()
        strategy.update_trade.assert_called_once()


class TestPaperPortfolioExecutor:
    def test_run_strategy_calls_on_bars(self, monkeypatch, _patch_conn):
        executor = _mod.PaperPortfolioExecutor()
        created = {}

        class FakeStrategy:
            def __init__(self, engine, strategy_name, vt_symbols, parameters):
                self.inited = False
                self.trading = False
                self.on_init = MagicMock()
                self.on_start = MagicMock()
                self.on_bars = MagicMock()
                self.on_stop = MagicMock()
                created["instance"] = self

        class FakeSourceDao:
            def get_strategy_source_for_user(self, strategy_id, user_id):
                return "code", "FakeStrategy", None

        class FakeQuoteService:
            def get_quote(self, symbol, market):
                return {"last_price": 10.0, "open": 9.9, "high": 10.1, "low": 9.8, "volume": 100}

        monkeypatch.setattr("app.api.services.strategy_service.compile_strategy", lambda code, cls: FakeStrategy)
        monkeypatch.setattr("app.domains.backtests.dao.strategy_source_dao.StrategySourceDao", FakeSourceDao)
        monkeypatch.setattr("app.domains.market.realtime_quote_service.RealtimeQuoteService", FakeQuoteService)

        stop_event = MagicMock()
        stop_event.is_set.side_effect = [False, True]
        stop_event.wait.return_value = None

        executor._run_strategy(
            deployment_id=30,
            paper_account_id=1,
            user_id=1,
            strategy_class_name="FakeStrategy",
            vt_symbol="000001.SZSE,000002.SZSE",
            parameters={},
            execution_mode="auto",
            strategy_id=1,
            stop_event=stop_event,
            gateway=PaperGateway("PAPER.30"),
        )

        created["instance"].on_start.assert_called_once()
        created["instance"].on_bars.assert_called_once()
        created["instance"].on_stop.assert_called_once()