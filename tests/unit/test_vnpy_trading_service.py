"""Tests for VnpyTradingService singleton and gateway management."""
import pytest
from unittest.mock import MagicMock

from app.domains.trading.vnpy_trading_service import (
    VnpyTradingService,
    GatewayType,
    OrderEvent,
    TradeEvent,
)


@pytest.fixture(autouse=True)
def reset_singleton():
    """Reset the singleton instance before each test."""
    VnpyTradingService._instance = None
    yield
    VnpyTradingService._instance = None


class TestSingleton:
    def test_is_singleton(self):
        svc1 = VnpyTradingService()
        svc2 = VnpyTradingService()
        assert svc1 is svc2

    def test_initialized_once(self):
        svc = VnpyTradingService()
        assert svc._initialized is True


class TestGatewayManagement:
    def test_connect_simulated_gateway(self):
        svc = VnpyTradingService()
        ok = svc.connect_gateway(GatewayType.SIMULATED, {"key": "val"}, "test_sim")
        assert ok is True

    def test_connect_gateway_returns_false_when_real_gateway_setup_fails(self, monkeypatch):
        svc = VnpyTradingService()
        monkeypatch.setattr(svc, "_resolve_gateway_class", lambda gateway_type: (_ for _ in ()).throw(RuntimeError("boom")))
        assert svc.connect_gateway(GatewayType.CTP, {"user": "a"}, "ctp1") is False

    def test_list_gateways_after_connect(self):
        svc = VnpyTradingService()
        svc.connect_gateway(GatewayType.SIMULATED, {}, "sim1")
        gateways = svc.list_gateways()
        assert len(gateways) == 1
        assert gateways[0]["name"] == "sim1"
        assert gateways[0]["type"] == "sim"
        assert gateways[0]["connected"] is True

    def test_disconnect_gateway(self):
        svc = VnpyTradingService()
        svc.connect_gateway(GatewayType.SIMULATED, {}, "sim1")
        assert svc.disconnect_gateway("sim1") is True
        assert len(svc.list_gateways()) == 0

    def test_disconnect_real_gateway_closes_engine(self):
        svc = VnpyTradingService()
        svc._main_engine = MagicMock()
        svc._connected_gateways["ctp1"] = {"type": GatewayType.CTP, "connected": True, "gateway_class_name": "CTP"}
        assert svc.disconnect_gateway("ctp1") is True
        svc._main_engine.close.assert_called_once()

    def test_disconnect_nonexistent_returns_false(self):
        svc = VnpyTradingService()
        assert svc.disconnect_gateway("no_such") is False

    def test_default_gateway_name_returns_first_connected(self):
        svc = VnpyTradingService()
        assert svc._default_gateway_name() is None
        svc.connect_gateway(GatewayType.SIMULATED, {}, "sim1")
        svc.connect_gateway(GatewayType.SIMULATED, {}, "sim2")
        assert svc._default_gateway_name() == "sim1"


class TestSimulatedOrders:
    def test_send_order_simulated(self):
        svc = VnpyTradingService()
        svc.connect_gateway(GatewayType.SIMULATED, {}, "sim")
        oid = svc.send_order("000001.SZ", "buy", "market", 100, gateway_name="sim")
        assert oid is not None
        assert oid.startswith("SIM-")

    def test_send_order_no_gateway_returns_none(self):
        svc = VnpyTradingService()
        oid = svc.send_order("000001.SZ", "buy", "market", 100)
        assert oid is None

    def test_send_order_returns_none_for_disconnected_or_uninitialized_real_gateway(self):
        svc = VnpyTradingService()
        svc._connected_gateways["ctp1"] = {"type": GatewayType.CTP, "connected": False}
        assert svc.send_order("000001.SZ", "buy", "limit", 1, gateway_name="ctp1") is None

        svc._connected_gateways["ctp1"] = {"type": GatewayType.CTP, "connected": True}
        svc._main_engine = None
        assert svc.send_order("000001.SZ", "buy", "limit", 1, gateway_name="ctp1") is None

    def test_cancel_order_handles_missing_engine_and_exceptions(self):
        svc = VnpyTradingService()
        assert svc.cancel_order("OID") is False

        svc._main_engine = MagicMock()
        svc._connected_gateways["sim1"] = {"type": GatewayType.SIMULATED, "connected": True}
        assert svc.cancel_order("OID", gateway_name="sim1") is False


class TestQueries:
    def test_query_positions_no_engine(self):
        svc = VnpyTradingService()
        positions = svc.query_positions()
        assert positions == []

    def test_query_positions_maps_engine_payload(self):
        svc = VnpyTradingService()
        pos = MagicMock()
        pos.symbol = "000001"
        pos.exchange.value = "SZ"
        pos.direction.value = "long"
        pos.volume = 2
        pos.frozen = 1
        pos.price = 10
        pos.pnl = 3
        svc._main_engine = MagicMock()
        svc._main_engine.get_all_positions.return_value = [pos]

        result = svc.query_positions()
        assert result[0].symbol == "000001.SZ"
        assert result[0].avg_price == 10
        assert result[0].pnl == 3

    def test_query_positions_returns_empty_on_exception(self):
        svc = VnpyTradingService()
        svc._main_engine = MagicMock()
        svc._main_engine.get_all_positions.side_effect = RuntimeError("boom")
        assert svc.query_positions() == []

    def test_query_account_no_engine(self):
        svc = VnpyTradingService()
        acct = svc.query_account()
        assert acct is None

    def test_query_account_maps_first_account_and_handles_empty_or_error(self):
        svc = VnpyTradingService()
        acct = MagicMock(balance=100, available=80, frozen=5, margin=7)
        svc._main_engine = MagicMock()
        svc._main_engine.get_all_accounts.return_value = [acct]
        result = svc.query_account()
        assert result.balance == 100
        assert result.available == 80
        assert result.frozen == 5
        assert result.margin == 7

        svc._main_engine.get_all_accounts.return_value = []
        assert svc.query_account() is None

        svc._main_engine.get_all_accounts.side_effect = RuntimeError("boom")
        assert svc.query_account() is None


class TestCallbacksAndHelpers:
    def test_on_order_and_on_trade_register_callbacks(self):
        svc = VnpyTradingService()
        order_cb = lambda evt: evt
        trade_cb = lambda evt: evt
        svc.on_order(order_cb)
        svc.on_trade(trade_cb)
        assert svc._order_callbacks == [order_cb]
        assert svc._trade_callbacks == [trade_cb]

    def test_resolve_gateway_class_invalid_raises(self):
        with pytest.raises(ValueError, match="Unsupported gateway type"):
            VnpyTradingService._resolve_gateway_class(GatewayType.SIMULATED)

    def test_register_event_handlers_relays_order_and_trade_events(self, monkeypatch):
        svc = VnpyTradingService()
        callbacks = {}

        class EventEngine:
            def register(self, event_name, callback):
                callbacks[event_name] = callback

        svc._main_engine = MagicMock(event_engine=EventEngine())
        order_events = []
        trade_events = []
        svc.on_order(order_events.append)
        svc.on_trade(trade_events.append)

        import sys
        import types

        event_mod = types.SimpleNamespace(EVENT_ORDER="ORDER", EVENT_TRADE="TRADE")
        monkeypatch.setitem(sys.modules, "vnpy.event", event_mod)

        svc._register_event_handlers()

        order_data = MagicMock()
        order_data.vt_orderid = "OID"
        order_data.symbol = "000001"
        order_data.exchange.value = "SZ"
        order_data.direction.value = "long"
        order_data.status.value = "submitted"
        order_data.traded = 2
        order_data.datetime = None
        order_data.gateway_name = "gw"

        trade_data = MagicMock()
        trade_data.vt_tradeid = "TID"
        trade_data.vt_orderid = "OID"
        trade_data.symbol = "000001"
        trade_data.exchange.value = "SZ"
        trade_data.direction.value = "long"
        trade_data.price = 10
        trade_data.volume = 1
        trade_data.datetime = None

        callbacks["ORDER"](MagicMock(data=order_data))
        callbacks["TRADE"](MagicMock(data=trade_data))

        assert isinstance(order_events[0], OrderEvent)
        assert order_events[0].symbol == "000001.SZ"
        assert isinstance(trade_events[0], TradeEvent)
        assert trade_events[0].price == 10

    def test_register_event_handlers_ignores_missing_engine(self):
        svc = VnpyTradingService()
        svc._main_engine = None
        svc._register_event_handlers()


class TestGatewayTypeEnum:
    def test_ctp(self):
        assert GatewayType("ctp") == GatewayType.CTP

    def test_xtp(self):
        assert GatewayType("xtp") == GatewayType.XTP

    def test_sim(self):
        assert GatewayType("sim") == GatewayType.SIMULATED

    def test_invalid_raises(self):
        with pytest.raises(ValueError):
            GatewayType("invalid")
