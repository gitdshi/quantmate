"""Tests for VnpyTradingService singleton and gateway management."""
import pytest
from unittest.mock import patch, MagicMock

from app.domains.trading.vnpy_trading_service import (
    VnpyTradingService,
    GatewayType,
    PositionSnapshot,
    AccountSnapshot,
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

    def test_disconnect_nonexistent_returns_false(self):
        svc = VnpyTradingService()
        assert svc.disconnect_gateway("no_such") is False


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


class TestQueries:

    def test_query_positions_no_engine(self):
        svc = VnpyTradingService()
        positions = svc.query_positions()
        assert positions == []

    def test_query_account_no_engine(self):
        svc = VnpyTradingService()
        acct = svc.query_account()
        assert acct is None


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
