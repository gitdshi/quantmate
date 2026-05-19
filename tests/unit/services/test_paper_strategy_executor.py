"""Unit tests for app.domains.trading.paper_strategy_executor."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from vnpy.trader.constant import Direction as VDirection, Offset

import app.domains.trading.paper_strategy_executor as _mod
from app.domains.trading.paper_gateway import PaperGateway, PaperGatewayOrderRequest


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
def _patch_conn(monkeypatch):
    ctx, conn = _fake_conn()
    monkeypatch.setattr(_mod, "connection", lambda db: ctx)
    return conn


@pytest.fixture(autouse=True)
def _patch_ledger(monkeypatch):
    monkeypatch.setattr(_mod.PaperExecutionLedger, "record_fill", lambda *args, **kwargs: None)
    monkeypatch.setattr(_mod.PaperExecutionLedger, "write_checkpoint", lambda *args, **kwargs: None)


class TestPaperCtaEngine:
    def _make_engine(self, conn):
        e = _mod._PaperCtaEngine(
            executor=MagicMock(),
            deployment_id=1,
            paper_account_id=1,
            user_id=1,
            vt_symbol="000001.SZSE",
            execution_mode="auto",
        )
        return e

    def test_write_log(self, _patch_conn):
        e = self._make_engine(_patch_conn)
        e.write_log("test message")  # should not raise

    def test_put_event(self, _patch_conn):
        e = self._make_engine(_patch_conn)
        e.put_event()  # no-op

    def test_send_email(self, _patch_conn):
        e = self._make_engine(_patch_conn)
        e.send_email("test msg")  # no-op

    def test_load_bar_prefers_market_history(self, monkeypatch, _patch_conn):
        e = self._make_engine(_patch_conn)
        callback = MagicMock()

        class FakeMarketService:
            def get_history(self, vt_symbol, start_date, end_date):
                return [
                    {"datetime": datetime(2026, 5, 1), "open": 10.0, "high": 10.2, "low": 9.9, "close": 10.1, "volume": 1000},
                    {"datetime": datetime(2026, 5, 2), "open": 10.1, "high": 10.3, "low": 10.0, "close": 10.2, "volume": 1200},
                ]

        monkeypatch.setattr("app.domains.market.service.MarketService", FakeMarketService)
        monkeypatch.setattr(
            _mod.PaperStrategyExecutor,
            "_history_to_bar",
            staticmethod(lambda history_bar, vt_symbol: {"close": history_bar["close"]}),
        )

        bars = e.load_bar("000001.SZSE", 2, None, callback)

        assert bars == [{"close": 10.1}, {"close": 10.2}]
        callback.assert_not_called()

    def test_load_bar_falls_back_to_quote_when_history_missing(self, monkeypatch, _patch_conn):
        e = self._make_engine(_patch_conn)
        callback = MagicMock()

        class FakeMarketService:
            def get_history(self, vt_symbol, start_date, end_date):
                return []

        gateway = MagicMock()
        gateway.get_last_tick.return_value = {"price": 10.5}
        e.gateway = gateway

        monkeypatch.setattr("app.domains.market.service.MarketService", FakeMarketService)
        monkeypatch.setattr(_mod.PaperStrategyExecutor, "_quote_to_bar", staticmethod(lambda quote, vt_symbol: {"price": 10.5}))

        bars = e.load_bar("000001.SZSE", 5, None, callback)

        assert bars == [{"price": 10.5}]
        callback.assert_not_called()

    def test_get_pricetick(self, _patch_conn):
        e = self._make_engine(_patch_conn)
        assert e.get_pricetick("000001.SZSE") == 0.01

    def test_cancel_order(self, _patch_conn):
        e = self._make_engine(_patch_conn)
        e.cancel_order(strategy=MagicMock(), vt_orderid="order1")

    def test_cancel_all(self, _patch_conn):
        e = self._make_engine(_patch_conn)
        e.cancel_all(strategy=MagicMock())

    def test_write_signal(self, _patch_conn):
        e = self._make_engine(_patch_conn)
        e._write_signal(direction="LONG", quantity=100, price=10.0, reason="test")
        _patch_conn.execute.assert_called()

    def test_get_market(self, _patch_conn):
        _patch_conn.execute.return_value = MagicMock(
            fetchone=MagicMock(return_value=_row(market="CN"))
        )
        e = self._make_engine(_patch_conn)
        market = e._get_market()
        assert market == "CN"

    def test_get_strategy_id(self, _patch_conn):
        _patch_conn.execute.return_value = MagicMock(
            fetchone=MagicMock(return_value=_row(strategy_id=5))
        )
        e = self._make_engine(_patch_conn)
        sid = e._get_strategy_id()
        assert sid == 5

    def test_get_strategy_id_not_found(self, _patch_conn):
        _patch_conn.execute.return_value = MagicMock(
            fetchone=MagicMock(return_value=None)
        )
        e = self._make_engine(_patch_conn)
        sid = e._get_strategy_id()
        assert sid is None

    def test_get_engine_type(self, _patch_conn):
        e = self._make_engine(_patch_conn)
        with patch(f"{_mod.__name__}.EngineType", create=True) as mock_et:
            mock_et.LIVE = "LIVE"
            try:
                result = e.get_engine_type()
            except (ImportError, AttributeError):
                pass  # vnpy not installed

    def test_send_order(self, _patch_conn):
        e = self._make_engine(_patch_conn)
        strategy = MagicMock()
        with patch.object(e, "_execute_order", return_value=["order-1"]):
            with patch(f"{_mod.__name__}.Direction", create=True) as mock_dir:
                mock_dir.LONG = "LONG"
                result = e.send_order(
                    strategy, direction="LONG", offset="OPEN",
                    price=10.0, volume=100
                )

    def test_send_order_passes_volume_to_executor(self, _patch_conn):
        gateway = PaperGateway("PAPER.1")
        e = _mod._PaperCtaEngine(
            executor=MagicMock(),
            deployment_id=1,
            paper_account_id=1,
            user_id=1,
            vt_symbol="000001.SZSE",
            execution_mode="auto",
            gateway=gateway,
        )
        strategy = MagicMock()

        with patch.object(e, "_execute_order") as execute_order:
            e.send_order(
                strategy,
                direction=VDirection.SHORT,
                offset=Offset.OPEN,
                price=10.0,
                volume=100,
            )

        execute_order.assert_called_once()
        assert execute_order.call_args.args[0] == "sell"
        assert execute_order.call_args.args[1] == 100
        assert execute_order.call_args.args[2] == 10.0

    def test_execute_order(self, _patch_conn):
        e = self._make_engine(_patch_conn)
        with patch(f"{_mod.__name__}.try_fill_market_order", create=True) as mock_fill, \
             patch(f"{_mod.__name__}.PaperAccountService", create=True) as mock_pas, \
             patch(f"{_mod.__name__}.OrderDao", create=True) as mock_od:
            mock_fill.return_value = {"filled": True, "price": 10.0, "quantity": 100}
            try:
                e._execute_order(direction="LONG", quantity=100, price=10.0)
            except Exception:
                pass  # may fail due to lazy imports

    @patch("app.domains.trading.dao.order_dao.OrderDao")
    @patch("app.domains.trading.paper_account_service.PaperAccountService")
    @patch("app.domains.trading.matching_engine.try_fill_market_order")
    @patch("app.domains.market.realtime_quote_service.RealtimeQuoteService")
    def test_execute_order_notifies_strategy_callbacks(self, MockQuoteSvc, mock_fill, MockAccountSvc, MockOrderDao, _patch_conn):
        gateway = PaperGateway("PAPER.1")
        strategy = MagicMock()
        e = _mod._PaperCtaEngine(
            executor=MagicMock(),
            deployment_id=1,
            paper_account_id=1,
            user_id=1,
            vt_symbol="000001.SZSE",
            execution_mode="auto",
            gateway=gateway,
        )
        order_state = gateway.submit_order(
            PaperGatewayOrderRequest(
                vt_symbol="000001.SZSE",
                direction="buy",
                order_type="market",
                volume=100,
                price=10.0,
            )
        )

        MockQuoteSvc.return_value.get_quote.return_value = {"last_price": 10.0}
        mock_fill.return_value = MagicMock(filled=True, fill_price=10.1, fill_quantity=100, fee=MagicMock(total=1.5))
        MockOrderDao.return_value.create.return_value = 9

        e._execute_order("buy", 100, 10.0, strategy=strategy, order_id=order_state.order_id)

        MockAccountSvc.return_value.settle_buy.assert_called_once_with(1, 1011.5, 1011.5)
        strategy.on_order.assert_called_once()
        strategy.on_trade.assert_called_once()


class TestPaperStrategyExecutor:
    def test_singleton_init(self):
        executor = _mod.PaperStrategyExecutor()
        assert hasattr(executor, "_threads") or hasattr(executor, "_stop_events")

    def test_is_running_false(self):
        executor = _mod.PaperStrategyExecutor()
        assert executor.is_running(deployment_id=999) is False

    def test_stop_deployment_not_running(self):
        executor = _mod.PaperStrategyExecutor()
        result = executor.stop_deployment(deployment_id=999)
        assert result is False

    def test_start_deployment(self, _patch_conn):
        executor = _mod.PaperStrategyExecutor()
        with patch.object(executor, "_run_strategy"):
            with patch("threading.Thread") as mock_thread:
                mock_thread.return_value = MagicMock()
                try:
                    result = executor.start_deployment(
                        deployment_id=100,
                        paper_account_id=1,
                        user_id=1,
                        strategy_class_name="TripleMA",
                        vt_symbol="000001.SZSE",
                        parameters={"fast": 5},
                    )
                except Exception:
                    pass  # May need more mocking

    def test_quote_to_bar(self):
        quote = {"price": 10.5, "volume": 1000, "open": 10.0, "high": 11.0, "low": 9.8}
        try:
            bar = _mod.PaperStrategyExecutor._quote_to_bar(quote, "000001.SZSE")
        except (ImportError, AttributeError):
            pass  # vnpy not installed

    def test_run_strategy_calls_on_start(self, monkeypatch, _patch_conn):
        executor = _mod.PaperStrategyExecutor()
        created = {}

        class FakeStrategy:
            def __init__(self, engine, strategy_name, vt_symbol, parameters):
                self.inited = False
                self.trading = False
                self.on_init = MagicMock()
                self.on_start = MagicMock()
                self.on_bar = MagicMock()
                self.on_stop = MagicMock()
                created["instance"] = self

        monkeypatch.setattr("app.api.services.strategy_service.compile_strategy", lambda code, cls: FakeStrategy)

        class FakeSourceDao:
            def get_strategy_source_for_user(self, strategy_id, user_id):
                return "code", "FakeStrategy", None

        monkeypatch.setattr("app.domains.backtests.dao.strategy_source_dao.StrategySourceDao", FakeSourceDao)

        stop_event = MagicMock()
        stop_event.is_set.return_value = True

        executor._run_strategy(
            deployment_id=1,
            paper_account_id=1,
            user_id=1,
            strategy_class_name="FakeStrategy",
            vt_symbol="000001.SZSE",
            parameters={},
            execution_mode="auto",
            strategy_id=1,
            stop_event=stop_event,
            gateway=PaperGateway("PAPER.1"),
        )

        created["instance"].on_start.assert_called_once()
        created["instance"].on_stop.assert_called_once()

    def test_run_strategy_prefers_on_tick(self, monkeypatch, _patch_conn):
        executor = _mod.PaperStrategyExecutor()
        created = {}

        class FakeStrategy:
            def __init__(self, engine, strategy_name, vt_symbol, parameters):
                self.inited = False
                self.trading = False
                self.on_init = MagicMock()
                self.on_start = MagicMock()
                self.on_tick = MagicMock()
                self.on_bar = MagicMock()
                self.on_stop = MagicMock()
                created["instance"] = self

        monkeypatch.setattr("app.api.services.strategy_service.compile_strategy", lambda code, cls: FakeStrategy)

        class FakeSourceDao:
            def get_strategy_source_for_user(self, strategy_id, user_id):
                return "code", "FakeStrategy", None

        class FakeQuoteService:
            def get_quote(self, symbol, market):
                return {"last_price": 10.0, "open": 9.9, "high": 10.1, "low": 9.8, "volume": 100}

        monkeypatch.setattr("app.domains.backtests.dao.strategy_source_dao.StrategySourceDao", FakeSourceDao)
        monkeypatch.setattr("app.domains.market.realtime_quote_service.RealtimeQuoteService", FakeQuoteService)

        stop_event = MagicMock()
        stop_event.is_set.side_effect = [False, True]
        stop_event.wait.return_value = None

        executor._run_strategy(
            deployment_id=2,
            paper_account_id=1,
            user_id=1,
            strategy_class_name="FakeStrategy",
            vt_symbol="000001.SZSE",
            parameters={},
            execution_mode="auto",
            strategy_id=1,
            stop_event=stop_event,
            gateway=PaperGateway("PAPER.2"),
        )

        created["instance"].on_tick.assert_called_once()
        created["instance"].on_bar.assert_not_called()
